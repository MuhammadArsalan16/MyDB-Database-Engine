/*
 * dml.cpp — INSERT, UPDATE, DELETE handlers.
 *
 * Phase 4  : exec_insert   — fully implemented.
 * Phase 6  : exec_update, exec_delete — stubs with auto-commit pattern.
 */

#include "ast_executor.hpp"
#include "exec_internal.h"
#include "result_fmt.hpp"
#include "value_cast.hpp"
#include "expr_eval.hpp"
#include "exec_access_path.hpp"   /* AccessPath, extract_sargs, plan_to_ap */
#include "planner.h"              /* Sarg, PlanNode, planner_choose_path   */

#include <cstring>
#include <cstdio>
#include <ctime>

/* ======================================================================
 * INSERT
 * ====================================================================== */

/*
 * Get the current local time encoded as YYYYMMDDHHmmSS (DATETIME) or
 * YYYYMMDD (DATE).  Used when a column has DEFAULT NOW.
 */
static Value get_now_value(DataType type)
{
    Value v;
    memset(&v, 0, sizeof(v));
    v.type = type;

    time_t     t  = time(NULL);
    struct tm *tm = localtime(&t);

    if (type == TYPE_DATETIME) {
        v.v.datetime_val = (int64_t)(tm->tm_year + 1900) * 10000000000LL
                         + (int64_t)(tm->tm_mon  + 1)    *   100000000LL
                         + (int64_t) tm->tm_mday          *     1000000LL
                         + (int64_t) tm->tm_hour          *       10000LL
                         + (int64_t) tm->tm_min           *         100LL
                         + (int64_t) tm->tm_sec;
    } else {
        /* TYPE_DATE */
        v.v.date_val = (tm->tm_year + 1900) * 10000
                     + (tm->tm_mon  + 1)    *   100
                     + tm->tm_mday;
    }
    return v;
}

/*
 * Return true when a ColumnDef's stored default value is the NOW sentinel.
 *
 * The sentinel is: has_default=1, is_null=0, and the encoded value is 0
 * (year-zero is not a valid real date in either DATE or DATETIME encoding,
 * so 0 is safe as a special marker meaning "use current timestamp").
 */
static int is_now_sentinel(const ColumnDef *col)
{
    if (!col->has_default || col->default_value.is_null) return 0;
    if (col->type == TYPE_DATETIME) return (col->default_value.v.datetime_val == 0);
    if (col->type == TYPE_DATE)     return (col->default_value.v.date_val     == 0);
    return 0;
}

int exec_insert(EngineState *eng, const InsertStatement *s,
                char *out, size_t cap)
{
    REQUIRE_LOGIN(eng);
    REQUIRE_SCHEMA(eng);

    /* write access check */
    int rc = engine_check_access(eng, 1);
    if (rc != MYDB_OK) {
        format_error(rc, out, cap, s->table_name.c_str());
        return rc;
    }

    /* look up the table */
    const RelationDef *rel_c = engine_find_relation(eng, s->table_name.c_str());
    if (!rel_c) {
        snprintf(out, cap, "  Error: table '%s' does not exist",
                 s->table_name.c_str());
        return MYDB_ERR_NOT_FOUND;
    }
    /* storage takes non-const — safe cast: storage is the single writer */
    RelationDef *rel = (RelationDef *)rel_c;

    if (s->rows.empty()) {
        snprintf(out, cap, "  Error: INSERT has no values");
        return MYDB_ERR;
    }

    /* ------------------------------------------------------------------
     * Count non-AUTO_INCREMENT columns.
     *
     * Positional INSERT skips AUTO_INCREMENT columns entirely — the user
     * provides values only for the columns the engine cannot fill in.
     * Named INSERT must not mention any AUTO_INCREMENT column at all.
     * ------------------------------------------------------------------ */
    bool named = !s->target_columns.empty();

    int non_ai_cols = 0;
    for (int i = 0; i < rel->num_columns; i++) {
        if (!rel->columns[i].is_auto_increment)
            non_ai_cols++;
    }

    /* ------------------------------------------------------------------
     * Validate value counts per row.
     *
     * Named:      expected count = number of columns listed.
     * Positional: expected count = non-AUTO_INCREMENT columns only.
     * ------------------------------------------------------------------ */
    for (size_t ri = 0; ri < s->rows.size(); ri++) {
        size_t have = s->rows[ri].size();
        size_t want = named ? s->target_columns.size()
                            : (size_t)non_ai_cols;
        if (have != want) {
            snprintf(out, cap,
                     "  Error: row %zu has %zu value%s but %zu %s expected",
                     ri + 1, have, have == 1 ? "" : "s", want,
                     named ? "columns listed"
                           : "non-AUTO_INCREMENT columns in table");
            return MYDB_ERR;
        }
    }

    /* ------------------------------------------------------------------
     * Build col_src[]:  col_src[i] = index into the row's values vector
     *                                for column i, or -1 if not provided.
     *
     * Named:      scan target_columns; reject if any column is AUTO_INCREMENT
     *             (the user should never supply a value for it).
     * Positional: AUTO_INCREMENT columns are implicitly skipped; values map
     *             to non-AUTO_INCREMENT columns in declaration order.
     * ------------------------------------------------------------------ */
    int col_src[MAX_COLUMNS];
    memset(col_src, -1, sizeof(col_src));

    if (named) {
        for (int ci = 0; ci < (int)s->target_columns.size(); ci++) {
            int idx = resolve_col(rel, s->target_columns[(size_t)ci]);
            if (idx < 0) {
                snprintf(out, cap, "  Error: unknown column '%s'",
                         s->target_columns[(size_t)ci].c_str());
                return MYDB_ERR;
            }
            if (rel->columns[idx].is_auto_increment) {
                snprintf(out, cap,
                         "  Error: column '%s' is AUTO_INCREMENT — "
                         "value must not be provided",
                         rel->columns[idx].name);
                return MYDB_ERR;
            }
            col_src[idx] = ci;
        }
    } else {
        /* Positional: skip AUTO_INCREMENT columns; assign value slots
         * to the remaining columns in declaration order. */
        int val_idx = 0;
        for (int i = 0; i < rel->num_columns; i++) {
            if (!rel->columns[i].is_auto_increment)
                col_src[i] = val_idx++;
            /* AUTO_INCREMENT columns keep col_src[i] = -1 */
        }
    }

    /* ------------------------------------------------------------------
     * Insert all rows inside one transaction (all-or-nothing).
     * ------------------------------------------------------------------ */
    struct timespec _ins_t_start, _ins_t_commit, _ins_t_end;
    clock_gettime(CLOCK_MONOTONIC, &_ins_t_start);
    AUTOCOMMIT_BEGIN();

    size_t ninserted = 0;

    for (size_t ri = 0; ri < s->rows.size(); ri++) {
        const std::vector<std::string> &vals = s->rows[ri];

        Row row;
        memset(&row, 0, sizeof(row));
        row.num_cols = (uint8_t)rel->num_columns;

        for (int ci = 0; ci < rel->num_columns; ci++) {
            const ColumnDef *col = &rel->columns[ci];
            Value           *v   = &row.cols[ci];
            v->type = col->type;

            if (col_src[ci] >= 0) {
                /* explicit value provided in the INSERT */
                const std::string &raw = vals[(size_t)col_src[ci]];
                if (!validate_literal(raw, *col)) {
                    rc = MYDB_ERR;
                    AUTOCOMMIT_END(rc);
                    if (col->type == TYPE_INT)
                        snprintf(out, cap,
                                 "  Error: value '%s' is out of range for column '%s'"
                                 " (INT is 32-bit signed: -2147483648 to 2147483647)",
                                 raw.c_str(), col->name);
                    else
                        snprintf(out, cap,
                                 "  Error: value '%s' is not valid for column '%s' (%s)",
                                 raw.c_str(), col->name,
                                 vals[(size_t)col_src[ci]].c_str());
                    return rc;
                }
                *v = cast_literal(raw, *col);

            } else if (col->is_auto_increment) {
                /* AUTO_INCREMENT — storage fills in the next counter value */
                v->is_null = 1;

            } else if (col->has_default) {
                if (is_now_sentinel(col)) {
                    /* DEFAULT NOW — substitute current timestamp */
                    *v = get_now_value(col->type);
                } else {
                    *v = col->default_value;
                }

            } else {
                /* no value, no default — column gets NULL */
                v->is_null = 1;
            }

            /*
             * NOT NULL check.
             * AUTO_INCREMENT columns are exempt: they arrive as is_null=1
             * here but storage fills them in before writing the row.
             */
            if (col->is_not_null && !col->is_auto_increment && v->is_null) {
                rc = MYDB_ERR_NULL_VIOLATION;
                AUTOCOMMIT_END(rc);
                snprintf(out, cap,
                         "  Error: column '%s' cannot be NULL", col->name);
                return rc;
            }
        }

        rc = storage_insert(rel, &row);
        if (rc != MYDB_OK) {
            AUTOCOMMIT_END(rc);
            format_error(rc, out, cap, s->table_name.c_str());
            return rc;
        }
        ninserted++;
    }

    rc = MYDB_OK;
    clock_gettime(CLOCK_MONOTONIC, &_ins_t_commit);
    AUTOCOMMIT_END(rc);
    clock_gettime(CLOCK_MONOTONIC, &_ins_t_end);

    {
        double _total   = (_ins_t_end.tv_sec    - _ins_t_start.tv_sec)  * 1e3
                        + (_ins_t_end.tv_nsec   - _ins_t_start.tv_nsec) * 1e-6;
        double _commit  = (_ins_t_end.tv_sec    - _ins_t_commit.tv_sec) * 1e3
                        + (_ins_t_end.tv_nsec   - _ins_t_commit.tv_nsec) * 1e-6;
        fprintf(stderr,
                "[INSERT_TIMING] %zu row%s: total=%.3fms  "
                "commit(bp_flush)=%.3fms  avg_per_row=%.3fms\n",
                ninserted, ninserted == 1 ? "" : "s",
                _total, _commit, ninserted ? _total / (double)ninserted : 0.0);
    }

    snprintf(out, cap, "OK  %zu row%s affected",
             ninserted, ninserted == 1 ? "" : "s");
    return MYDB_OK;
}

/* ======================================================================
 * UPDATE
 * ====================================================================== */

int exec_update(EngineState *eng,
                const UpdateStatement *s,
                char *out, size_t cap)
{
    REQUIRE_LOGIN(eng);
    REQUIRE_SCHEMA(eng);

    /* write access check */
    int rc = engine_check_access(eng, 1);
    if (rc != MYDB_OK) {
        format_error(rc, out, cap, s->table_name.c_str());
        return rc;
    }

    /* find the table */
    const RelationDef *rel_c = engine_find_relation(eng, s->table_name.c_str());
    if (!rel_c) {
        snprintf(out, cap, "  Error: table '%s' does not exist",
                 s->table_name.c_str());
        return MYDB_ERR_NOT_FOUND;
    }
    RelationDef *rel = (RelationDef *)rel_c;

    /* validate every SET column before touching any data */
    for (const auto &assign : s->assignments) {
        int ci = resolve_col(rel, assign.first);
        if (ci < 0) {
            snprintf(out, cap, "  Error: unknown column '%s'",
                     assign.first.c_str());
            return MYDB_ERR;
        }
        if (rel->columns[ci].is_auto_increment) {
            snprintf(out, cap,
                     "  Error: column '%s' is AUTO_INCREMENT and cannot be updated",
                     rel->columns[ci].name);
            return MYDB_ERR;
        }
        if (!validate_literal(assign.second, rel->columns[ci])) {
            snprintf(out, cap,
                     "  Error: value '%s' is not valid for column '%s'",
                     assign.second.c_str(), rel->columns[ci].name);
            return MYDB_ERR;
        }
    }

    /* validate WHERE column references */
    if (s->where_clause) {
        const char *bad = validate_expr_cols(s->where_clause->root.get(), rel);
        if (bad) {
            snprintf(out, cap,
                     "  Error: column '%s' does not exist in table '%s'",
                     bad, s->table_name.c_str());
            return MYDB_ERR;
        }
    }

    /* choose the cheapest access path via the planner */
    Sarg       sargs[32];
    int        n_sargs = extract_sargs(s->where_clause.get(), rel, sargs, 32);
    PlanNode   plan    = planner_choose_path(eng, rel, sargs, n_sargs);
    AccessPath ap      = plan_to_ap(plan, s->where_clause.get(), rel);

    AUTOCOMMIT_BEGIN();

    size_t naffected = 0;
    rc = MYDB_OK;

    /* apply SET values to one matching row and call storage_update */
    auto update_row = [&](Row *row) -> bool {
        if (!row) return true;
        if (!where_matches(s->where_clause.get(), rel, row)) return true;

        RID rid     = row->rid;   /* save before any storage call overwrites it */
        Row new_row = *row;       /* copy all current column values */

        /* overwrite only the columns named in the SET clause */
        for (const auto &assign : s->assignments) {
            int ci = resolve_col(rel, assign.first);
            new_row.cols[ci] = cast_literal(assign.second, rel->columns[ci]);
        }

        /* NOT NULL check on the new values */
        for (int i = 0; i < rel->num_columns; i++) {
            if (rel->columns[i].is_not_null && new_row.cols[i].is_null) {
                snprintf(out, cap, "  Error: column '%s' cannot be NULL",
                         rel->columns[i].name);
                rc = MYDB_ERR_NULL_VIOLATION;
                return false;   /* stop the scan */
            }
        }

        rc = storage_update(rel, rid, &new_row);
        if (rc != MYDB_OK) {
            format_error(rc, out, cap, s->table_name.c_str());
            return false;   /* stop the scan */
        }

        naffected++;
        return true;   /* continue scanning */
    };

    /* execute the chosen access path */
    switch (ap.kind) {

    case AP_GET_PK: {
        Row *row = storage_get_by_pk(rel, &ap.key);
        update_row(row);
        break;
    }
    case AP_GET_INDEX: {
        Row *row = storage_get_by_index(rel, ap.col_idx, &ap.key);
        update_row(row);
        break;
    }
    case AP_SCAN_FROM: {
        Cursor *cur = storage_scan_from(rel, &ap.key);
        if (cur) {
            Row *row;
            while ((row = cursor_next(cur)) != NULL)
                if (!update_row(row)) break;
            cursor_close(cur);
        }
        break;
    }
    case AP_SCAN_BY_INDEX: {
        Cursor *cur = storage_scan_by_index(rel, ap.col_idx, &ap.key);
        if (cur) {
            Row *row;
            while ((row = cursor_next(cur)) != NULL)
                if (!update_row(row)) break;
            cursor_close(cur);
        }
        break;
    }
    default: {
        Cursor *cur = storage_scan(rel);
        if (cur) {
            Row *row;
            while ((row = cursor_next(cur)) != NULL)
                if (!update_row(row)) break;
            cursor_close(cur);
        }
        break;
    }
    }

    AUTOCOMMIT_END(rc);

    if (rc != MYDB_OK) return rc;

    snprintf(out, cap, "OK  %zu row%s affected",
             naffected, naffected == 1 ? "" : "s");
    return MYDB_OK;
}

/* ======================================================================
 * DELETE
 * ====================================================================== */

int exec_delete(EngineState *eng,
                const DeleteStatement *s,
                char *out, size_t cap)
{
    REQUIRE_LOGIN(eng);
    REQUIRE_SCHEMA(eng);

    /* write access check */
    int rc = engine_check_access(eng, 1);
    if (rc != MYDB_OK) {
        format_error(rc, out, cap, s->table_name.c_str());
        return rc;
    }

    /* find the table */
    const RelationDef *rel_c = engine_find_relation(eng, s->table_name.c_str());
    if (!rel_c) {
        snprintf(out, cap, "  Error: table '%s' does not exist",
                 s->table_name.c_str());
        return MYDB_ERR_NOT_FOUND;
    }
    RelationDef *rel = (RelationDef *)rel_c;

    /* validate WHERE column references */
    if (s->where_clause) {
        const char *bad = validate_expr_cols(s->where_clause->root.get(), rel);
        if (bad) {
            snprintf(out, cap,
                     "  Error: column '%s' does not exist in table '%s'",
                     bad, s->table_name.c_str());
            return MYDB_ERR;
        }
    }

    /* choose the cheapest access path via the planner */
    Sarg       sargs[32];
    int        n_sargs = extract_sargs(s->where_clause.get(), rel, sargs, 32);
    PlanNode   plan    = planner_choose_path(eng, rel, sargs, n_sargs);
    AccessPath ap      = plan_to_ap(plan, s->where_clause.get(), rel);

    AUTOCOMMIT_BEGIN();

    size_t naffected = 0;
    rc = MYDB_OK;

    switch (ap.kind) {

    case AP_GET_PK: {
        Row *row = storage_get_by_pk(rel, &ap.key);
        if (row && where_matches(s->where_clause.get(), rel, row)) {
            RID rid = row->rid;
            rc = storage_delete(rel, rid);
            if (rc == MYDB_OK) naffected++;
            else format_error(rc, out, cap, s->table_name.c_str());
        }
        break;
    }

    case AP_GET_INDEX: {
        Row *row = storage_get_by_index(rel, ap.col_idx, &ap.key);
        if (row && where_matches(s->where_clause.get(), rel, row)) {
            RID rid = row->rid;
            rc = storage_delete(rel, rid);
            if (rc == MYDB_OK) naffected++;
            else format_error(rc, out, cap, s->table_name.c_str());
        }
        break;
    }

    case AP_SCAN_FROM: {
        Cursor *cur = storage_scan_from(rel, &ap.key);
        if (cur) {
            Row *row;
            while ((row = cursor_next(cur)) != NULL) {
                if (!where_matches(s->where_clause.get(), rel, row)) continue;
                RID rid = row->rid;
                rc = storage_delete(rel, rid);
                if (rc != MYDB_OK) {
                    format_error(rc, out, cap, s->table_name.c_str());
                    break;
                }
                naffected++;
            }
            cursor_close(cur);
        }
        break;
    }

    case AP_SCAN_BY_INDEX: {
        Cursor *cur = storage_scan_by_index(rel, ap.col_idx, &ap.key);
        if (cur) {
            Row *row;
            while ((row = cursor_next(cur)) != NULL) {
                if (!where_matches(s->where_clause.get(), rel, row)) continue;
                RID rid = row->rid;
                rc = storage_delete(rel, rid);
                if (rc != MYDB_OK) {
                    format_error(rc, out, cap, s->table_name.c_str());
                    break;
                }
                naffected++;
            }
            cursor_close(cur);
        }
        break;
    }

    default: {
        Cursor *cur = storage_scan(rel);
        if (cur) {
            Row *row;
            while ((row = cursor_next(cur)) != NULL) {
                if (!where_matches(s->where_clause.get(), rel, row)) continue;
                RID rid = row->rid;
                rc = storage_delete(rel, rid);
                if (rc != MYDB_OK) {
                    format_error(rc, out, cap, s->table_name.c_str());
                    break;
                }
                naffected++;
            }
            cursor_close(cur);
        }
        break;
    }
    }

    AUTOCOMMIT_END(rc);

    if (rc != MYDB_OK) return rc;

    snprintf(out, cap, "OK  %zu row%s affected",
             naffected, naffected == 1 ? "" : "s");
    return MYDB_OK;
}
