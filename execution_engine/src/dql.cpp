/*
 * dql.cpp — SELECT handler.
 *
 * Sub-phase 5.1 : single-table SELECT, projection, WHERE filtering,
 *                 LIMIT / OFFSET, access-path selection.
 * Sub-phase 5.2 : ORDER BY — materialise + sort; PK-ASC stream optimisation.
 * Sub-phase 5.3 : scalar aggregates.
 * Sub-phase 5.4 : GROUP BY + aggregates, HAVING (GROUP BY columns only).
 * Sub-phase 5.5 : JOINs                              (TODO)
 */

#include "ast_executor.hpp"
#include "exec_internal.h"
#include "result_fmt.hpp"
#include "value_cast.hpp"
#include "expr_eval.hpp"

#include <algorithm>
#include <map>
#include <vector>
#include <cstring>
#include <cstdio>

/* ======================================================================
 * validate_expr_cols
 *
 * Walk an Expr tree and verify every ColumnRef names a real column in rel.
 * Returns NULL on success, or a pointer to the offending column name string.
 * The pointer is stable for the lifetime of the AST (c_str() of a
 * std::string owned by the Expr node).
 * ====================================================================== */

static const char *validate_expr_cols(const Expr *e, const RelationDef *rel)
{
    if (!e) return NULL;

    switch (e->kind) {

    case Expr::Kind::ColumnRef: {
        const ColumnRefExpr *cr = static_cast<const ColumnRefExpr *>(e);
        if (resolve_col(rel, cr->column) < 0)
            return cr->column.c_str();
        return NULL;
    }

    case Expr::Kind::Binary: {
        const BinaryExpr *b = static_cast<const BinaryExpr *>(e);
        const char *bad = validate_expr_cols(b->lhs.get(), rel);
        if (bad) return bad;
        return validate_expr_cols(b->rhs.get(), rel);
    }

    case Expr::Kind::Unary: {
        const UnaryExpr *u = static_cast<const UnaryExpr *>(e);
        return validate_expr_cols(u->child.get(), rel);
    }

    case Expr::Kind::IsNull: {
        const IsNullExpr *isn = static_cast<const IsNullExpr *>(e);
        return validate_expr_cols(isn->child.get(), rel);
    }

    case Expr::Kind::Between: {
        const BetweenExpr *bw = static_cast<const BetweenExpr *>(e);
        const char *bad = validate_expr_cols(bw->v.get(),  rel); if (bad) return bad;
                    bad = validate_expr_cols(bw->lo.get(), rel); if (bad) return bad;
        return validate_expr_cols(bw->hi.get(), rel);
    }

    case Expr::Kind::In: {
        const InExpr *in = static_cast<const InExpr *>(e);
        return validate_expr_cols(in->v.get(), rel);
    }

    case Expr::Kind::Like: {
        const LikeExpr *lk = static_cast<const LikeExpr *>(e);
        return validate_expr_cols(lk->v.get(), rel);
    }

    default:
        return NULL;
    }
}

/* ======================================================================
 * Access-path selection
 *
 * Inspects the root of the WHERE tree for simple patterns:
 *   col = literal  (PK column)    → AP_GET_PK
 *   col = literal  (UNIQUE col)   → AP_GET_INDEX
 *   col >= literal (PK column)    → AP_SCAN_FROM
 *   anything else                 → AP_SCAN (full scan + filter)
 * ====================================================================== */

#define AP_SCAN       0
#define AP_SCAN_FROM  1
#define AP_GET_PK     2
#define AP_GET_INDEX  3

struct AccessPath {
    int   kind;
    Value key;
    int   col_idx;
};

static AccessPath pick_access_path(const WhereClause *w,
                                   const RelationDef  *rel)
{
    AccessPath ap;
    memset(&ap, 0, sizeof(ap));
    ap.kind = AP_SCAN;

    if (!w || !w->root) return ap;

    const Expr *root = w->root.get();
    if (root->kind != Expr::Kind::Binary) return ap;

    const BinaryExpr *bin = static_cast<const BinaryExpr *>(root);
    const char       *op  = bin->op.c_str();

    bool is_eq = (strcmp(op, "=")  == 0);
    bool is_ge = (strcmp(op, ">=") == 0);
    if (!is_eq && !is_ge) return ap;

    const Expr *col_side = NULL;
    const Expr *lit_side = NULL;

    if (bin->lhs && bin->lhs->kind == Expr::Kind::ColumnRef &&
        bin->rhs && bin->rhs->kind == Expr::Kind::Literal) {
        col_side = bin->lhs.get();
        lit_side = bin->rhs.get();
    } else if (is_eq &&
               bin->rhs && bin->rhs->kind == Expr::Kind::ColumnRef &&
               bin->lhs && bin->lhs->kind == Expr::Kind::Literal) {
        col_side = bin->rhs.get();
        lit_side = bin->lhs.get();
    }

    if (!col_side || !lit_side) return ap;

    const ColumnRefExpr *cr  = static_cast<const ColumnRefExpr *>(col_side);
    const LiteralExpr   *lit = static_cast<const LiteralExpr   *>(lit_side);

    int col_idx = resolve_col(rel, cr->column);
    if (col_idx < 0) return ap;

    const ColumnDef *cd  = &rel->columns[col_idx];
    Value            key = cast_literal(lit->raw, *cd);

    if (is_eq) {
        if (cd->is_primary_key) {
            ap.kind = AP_GET_PK;
            ap.key  = key;
        } else if (cd->is_unique) {
            ap.kind    = AP_GET_INDEX;
            ap.key     = key;
            ap.col_idx = col_idx;
        }
    } else {
        if (cd->is_primary_key) {
            ap.kind = AP_SCAN_FROM;
            ap.key  = key;
        }
    }

    return ap;
}

/* ======================================================================
 * process_row  (stream path)
 *
 * Apply WHERE filter, OFFSET skip, and LIMIT cap on one candidate row.
 * Emits the header the first time a row is actually output.
 *
 * Returns 1 → LIMIT reached, caller must stop the scan.
 *         0 → row skipped or emitted, continue.
 * ====================================================================== */

static int process_row(const WhereClause     *where,
                       const RelationDef     *rel,
                       const Row             *row,
                       const SelectStatement *s,
                       ResultBuf             *rb,
                       bool                  *header_emitted,
                       int64_t               *skipped,
                       int64_t               *emitted)
{
    if (!where_matches(where, rel, row)) return 0;

    if (*skipped < s->offset) { (*skipped)++; return 0; }

    if (s->limit >= 0 && *emitted >= s->limit) return 1;

    if (!*header_emitted) {
        emit_header(*rb, rel, s);
        *header_emitted = true;
    }

    emit_row(*rb, rel, row, s);
    (*emitted)++;

    return (s->limit >= 0 && *emitted >= s->limit) ? 1 : 0;
}

/* ======================================================================
 * Scalar aggregate helpers  (sub-phase 5.3)
 * ====================================================================== */

/*
 * Per-function accumulator state.
 * One AggState slot per aggregate item in the SELECT list.
 */
struct AggState {
    int64_t count;      /* rows counted (COUNT(*) = all; COUNT(col) = non-NULL) */
    double  sum;        /* running total for SUM / AVG                          */
    Value   min_val;    /* running minimum for MIN                              */
    Value   max_val;    /* running maximum for MAX                              */
    bool    has_value;  /* any non-NULL value seen yet (SUM/AVG/MIN/MAX)        */
    int     col_idx;    /* column index in rel->columns[]; -1 for COUNT(*)      */
};

/*
 * Convert a numeric Value to double for accumulation.
 * TYPE_INT and TYPE_DECIMAL only — other types return 0.
 */
static double value_to_double(const Value &v, const ColumnDef &cd)
{
    if (v.type == TYPE_INT)
        return (double)v.v.int_val;

    if (v.type == TYPE_DECIMAL) {
        int scale = (cd.scale > 0) ? cd.scale : 2;
        double divisor = 1.0;
        for (int i = 0; i < scale; i++) divisor *= 10.0;
        return (double)v.v.decimal_val / divisor;
    }
    return 0.0;
}

/*
 * Update all aggregate accumulators for one passing row.
 */
static void agg_accumulate(AggState *states, int n,
                            const SelectStatement *s,
                            const RelationDef *rel,
                            const Row *row)
{
    for (int i = 0; i < n; i++) {
        AggState             &st   = states[i];
        const SelectItem     &item = s->select_list[(size_t)i];

        /* Skip non-aggregate items (Column / Star) — occurs in GROUP BY
         * queries where states[] is parallel to the full select list. */
        if (item.kind != SelectItem::Kind::Aggregate) continue;

        const std::string &func = item.agg_func;

        /* COUNT(*) — count every row, no column access */
        if (func == "COUNT" && st.col_idx < 0) {
            st.count++;
            continue;
        }

        const Value     &v  = row->cols[st.col_idx];
        const ColumnDef &cd = rel->columns[st.col_idx];

        /* COUNT(col) — count non-NULL values */
        if (func == "COUNT") {
            if (!v.is_null) st.count++;
            continue;
        }

        /* all other functions skip NULL values */
        if (v.is_null) continue;

        if (func == "SUM" || func == "AVG") {
            st.sum += value_to_double(v, cd);
            st.count++;
            st.has_value = true;

        } else if (func == "MIN") {
            if (!st.has_value || compare_values(&v, &st.min_val) < 0)
                st.min_val = v;
            st.has_value = true;

        } else if (func == "MAX") {
            if (!st.has_value || compare_values(&v, &st.max_val) > 0)
                st.max_val = v;
            st.has_value = true;
        }
    }
}

/*
 * Emit the single aggregate result row into rb.
 * Called once after the scan finishes.
 */
static void emit_agg_row(ResultBuf &rb, const RelationDef *rel,
                         const SelectStatement *s,
                         const AggState *states, int n)
{
    char tmp[64];
    bool first = true;

    for (int i = 0; i < n; i++) {
        if (!first) rb.append(" | ");
        first = false;

        const AggState    &st   = states[i];
        const SelectItem  &item = s->select_list[(size_t)i];
        const std::string &func = item.agg_func;

        if (func == "COUNT") {
            snprintf(tmp, sizeof(tmp), "%lld", (long long)st.count);
            rb.append(tmp);

        } else if (func == "SUM") {
            if (!st.has_value) {
                rb.append("NULL");
            } else if (st.col_idx >= 0 &&
                       rel->columns[st.col_idx].type == TYPE_INT) {
                snprintf(tmp, sizeof(tmp), "%lld", (long long)(int64_t)st.sum);
                rb.append(tmp);
            } else {
                /* DECIMAL or other numeric */
                int scale = (st.col_idx >= 0 && rel->columns[st.col_idx].scale > 0)
                            ? rel->columns[st.col_idx].scale : 2;
                char fmt[16];
                snprintf(fmt, sizeof(fmt), "%%.%df", scale);
                snprintf(tmp, sizeof(tmp), fmt, st.sum);
                rb.append(tmp);
            }

        } else if (func == "AVG") {
            if (st.count == 0) {
                rb.append("NULL");
            } else {
                snprintf(tmp, sizeof(tmp), "%.2f", st.sum / (double)st.count);
                rb.append(tmp);
            }

        } else if (func == "MIN" || func == "MAX") {
            if (!st.has_value) {
                rb.append("NULL");
            } else {
                const Value &val = (func == "MIN") ? st.min_val : st.max_val;
                if (st.col_idx >= 0)
                    rb.append_value(val, rel->columns[st.col_idx]);
                else
                    rb.append("NULL");
            }
        }
    }
    rb.append("\n");
}

/* ======================================================================
 * RowLess — ORDER BY comparator for std::sort / std::partial_sort.
 *
 * Walks the order_by list in priority order (first column is primary sort,
 * next is tiebreaker, etc.).  Flip comparison direction for DESC columns.
 * If all columns tie, treat rows as equal (return false).
 * ====================================================================== */

struct RowLess {
    const RelationDef              *rel;
    const std::vector<OrderByItem> *order_by;

    bool operator()(const Row &a, const Row &b) const
    {
        for (const auto &item : *order_by) {
            int idx = resolve_col(rel, item.column);
            if (idx < 0) continue;
            int cmp = compare_values(&a.cols[idx], &b.cols[idx]);
            if (cmp != 0)
                return item.descending ? (cmp > 0) : (cmp < 0);
        }
        return false;
    }
};

/* ======================================================================
 * GROUP BY helpers  (sub-phase 5.4)
 * ====================================================================== */

/*
 * Comparison key for one group — the values of every GROUP BY column.
 *
 * NULL handling: two NULLs in the same position are equal (grouped
 * together), matching standard SQL behaviour.  NULL sorts before any
 * non-NULL value so the map ordering is deterministic.
 */
struct GroupKey {
    std::vector<Value> vals;

    bool operator<(const GroupKey &o) const
    {
        for (size_t i = 0; i < vals.size(); i++) {
            bool an = (vals[i].is_null   != 0);
            bool bn = (o.vals[i].is_null != 0);
            if (an && bn) continue;        /* both NULL → same bucket */
            if (an)       return true;     /* NULL < non-NULL */
            if (bn)       return false;
            int c = compare_values(&vals[i], &o.vals[i]);
            if (c != 0) return c < 0;
        }
        return false;   /* equal */
    }
};

/* Extract a GroupKey from a row given pre-resolved GROUP BY column indices. */
static GroupKey make_group_key(const Row *row, const std::vector<int> &idxs)
{
    GroupKey k;
    k.vals.reserve(idxs.size());
    for (int idx : idxs)
        k.vals.push_back(row->cols[idx]);
    return k;
}

/*
 * Build a partial Row where GROUP BY column values are placed at their
 * real positions in rel->columns[].  Every other slot is set to NULL.
 *
 * This Row is used for two purposes:
 *   1. HAVING evaluation  — eval_expr sees GROUP BY col values correctly;
 *                           non-GROUP-BY cols are NULL and comparisons
 *                           against them will return false (safe).
 *   2. ORDER BY sort key  — RowLess reads only the ORDER BY columns;
 *                           if the user orders by a GROUP BY column it
 *                           finds the correct value; other columns are NULL
 *                           (will compare as equal, which is fine).
 */
static Row make_key_row(const GroupKey      &key,
                        const std::vector<int> &idxs,
                        int                  num_cols)
{
    Row r;
    memset(&r, 0, sizeof(r));
    for (int i = 0; i < num_cols; i++) r.cols[i].is_null = 1;
    for (size_t i = 0; i < idxs.size(); i++) r.cols[idxs[i]] = key.vals[i];
    return r;
}

/*
 * Emit one GROUP BY result row.
 *
 * states[] is parallel to s->select_list:
 *   Column items   → value taken from key_row at the column's real index.
 *   Aggregate items → value computed from states[i].
 */
static void emit_group_row(ResultBuf               &rb,
                           const RelationDef       *rel,
                           const SelectStatement   *s,
                           const Row               &key_row,
                           const std::vector<AggState> &states)
{
    char tmp[64];
    bool first = true;

    for (size_t i = 0; i < s->select_list.size(); i++) {
        if (!first) rb.append(" | ");
        first = false;

        const SelectItem &item = s->select_list[i];

        if (item.kind == SelectItem::Kind::Column) {
            int idx = resolve_col(rel, item.column);
            if (idx >= 0) rb.append_value(key_row.cols[idx], rel->columns[idx]);
            else          rb.append("NULL");

        } else if (item.kind == SelectItem::Kind::Aggregate) {
            const AggState    &st   = states[i];
            const std::string &func = item.agg_func;

            if (func == "COUNT") {
                snprintf(tmp, sizeof(tmp), "%lld", (long long)st.count);
                rb.append(tmp);

            } else if (func == "SUM") {
                if (!st.has_value) {
                    rb.append("NULL");
                } else if (st.col_idx >= 0 &&
                           rel->columns[st.col_idx].type == TYPE_INT) {
                    snprintf(tmp, sizeof(tmp), "%lld", (long long)(int64_t)st.sum);
                    rb.append(tmp);
                } else {
                    int scale = (st.col_idx >= 0 && rel->columns[st.col_idx].scale > 0)
                                ? rel->columns[st.col_idx].scale : 2;
                    char fmt[16];
                    snprintf(fmt, sizeof(fmt), "%%.%df", scale);
                    snprintf(tmp, sizeof(tmp), fmt, st.sum);
                    rb.append(tmp);
                }

            } else if (func == "AVG") {
                if (st.count == 0) rb.append("NULL");
                else {
                    snprintf(tmp, sizeof(tmp), "%.2f", st.sum / (double)st.count);
                    rb.append(tmp);
                }

            } else if (func == "MIN" || func == "MAX") {
                if (!st.has_value) rb.append("NULL");
                else {
                    const Value &val = (func == "MIN") ? st.min_val : st.max_val;
                    if (st.col_idx >= 0)
                        rb.append_value(val, rel->columns[st.col_idx]);
                    else
                        rb.append("NULL");
                }
            }
        }
        /* SelectItem::Kind::Star is blocked by the pre-check in exec_select */
    }
    rb.append("\n");
}

/*
 * GROUP BY execution path.
 *
 * Called by exec_select after all pre-checks pass.  Performs a full
 * scan, groups rows by the GROUP BY columns, accumulates aggregates
 * per group, applies HAVING, sorts (ORDER BY), slices (LIMIT/OFFSET),
 * and emits.
 *
 * states[] in the group map is parallel to s->select_list:
 *   - Aggregate slots are live (col_idx, counters, etc.).
 *   - Column slots are zero-initialised and never read.
 */
static int exec_group_by(const SelectStatement *s,
                         const RelationDef     *rel,
                         RelationDef           *rel_rw,
                         char *out, size_t cap)
{
    /* Resolve GROUP BY column indices (already validated). */
    std::vector<int> grp_idxs;
    grp_idxs.reserve(s->group_by.size());
    for (const auto &name : s->group_by)
        grp_idxs.push_back(resolve_col(rel, name));

    int n_sel = (int)s->select_list.size();

    /* Build the zero-initialised AggState template (parallel to select_list).
     * Non-aggregate slots stay zeroed — agg_accumulate skips them. */
    std::vector<AggState> agg_tmpl((size_t)n_sel);
    for (int i = 0; i < n_sel; i++) {
        const SelectItem &item = s->select_list[(size_t)i];
        if (item.kind != SelectItem::Kind::Aggregate) continue;
        agg_tmpl[(size_t)i].col_idx =
            (item.column == "*") ? -1 : resolve_col(rel, item.column);
    }

    /* Map: GroupKey → AggState vector.
     * insert_order preserves first-seen order for stable output
     * when there is no ORDER BY clause. */
    std::map<GroupKey, std::vector<AggState>> groups;
    std::vector<GroupKey> insert_order;

    /* Feed one candidate row into the group map. */
    auto feed_row = [&](const Row *row) {
        if (!where_matches(s->where_clause.get(), rel, row)) return;

        GroupKey key = make_group_key(row, grp_idxs);

        auto [it, inserted] = groups.emplace(key, agg_tmpl);
        if (inserted) insert_order.push_back(key);

        agg_accumulate(it->second.data(), n_sel, s, rel, row);
    };

    /* Full scan — GROUP BY must see every row. */
    AccessPath ap = pick_access_path(s->where_clause.get(), rel);

    switch (ap.kind) {
    case AP_GET_PK: {
        Row *r = storage_get_by_pk(rel_rw, &ap.key);
        if (r) feed_row(r);
        break;
    }
    case AP_GET_INDEX: {
        Row *r = storage_get_by_index(rel_rw, ap.col_idx, &ap.key);
        if (r) feed_row(r);
        break;
    }
    case AP_SCAN_FROM: {
        Cursor *cur = storage_scan_from(rel_rw, &ap.key);
        if (cur) {
            Row *r;
            while ((r = cursor_next(cur)) != NULL) feed_row(r);
            cursor_close(cur);
        }
        break;
    }
    default: {
        Cursor *cur = storage_scan(rel_rw);
        if (cur) {
            Row *r;
            while ((r = cursor_next(cur)) != NULL) feed_row(r);
            cursor_close(cur);
        }
        break;
    }
    }

    /* Apply HAVING and build the final result list.
     * key_row has GROUP BY column values at their real positions; all
     * other columns are NULL.  HAVING (Option A: GROUP BY cols only)
     * is evaluated directly by eval_expr against this row. */
    using GResult = std::pair<Row, std::vector<AggState>>;
    std::vector<GResult> results;
    results.reserve(groups.size());

    for (const auto &key : insert_order) {
        const std::vector<AggState> &st = groups[key];
        Row key_row = make_key_row(key, grp_idxs, (int)rel->num_columns);

        if (s->having && !eval_expr(s->having.get(), rel, &key_row))
            continue;

        results.emplace_back(key_row, st);
    }

    /* ORDER BY — RowLess operates on key_row, which has correct values
     * for GROUP BY columns and NULL for everything else.  Ordering by a
     * non-GROUP-BY column will see NULLs and treat all groups as equal
     * for that column (consistent, no crash). */
    if (!s->order_by.empty()) {
        RowLess row_cmp { rel, &s->order_by };

        size_t total = results.size();
        size_t start = ((size_t)s->offset < total) ? (size_t)s->offset : total;
        size_t end   = (s->limit >= 0)
                       ? std::min(start + (size_t)s->limit, total)
                       : total;

        auto pair_cmp = [&row_cmp](const GResult &a, const GResult &b) {
            return row_cmp(a.first, b.first);
        };

        if (end > 0 && end < total)
            std::partial_sort(results.begin(),
                              results.begin() + (ptrdiff_t)end,
                              results.end(), pair_cmp);
        else if (total > 1)
            std::sort(results.begin(), results.end(), pair_cmp);

        ResultBuf rb(out, cap);
        emit_header(rb, rel, s);
        for (size_t i = start; i < end; i++)
            emit_group_row(rb, rel, s, results[i].first, results[i].second);
        rb.finalize(end - start);
        return MYDB_OK;
    }

    /* No ORDER BY — insertion order, slice by LIMIT/OFFSET. */
    size_t total = results.size();
    size_t start = ((size_t)s->offset < total) ? (size_t)s->offset : total;
    size_t end   = (s->limit >= 0)
                   ? std::min(start + (size_t)s->limit, total)
                   : total;

    ResultBuf rb(out, cap);
    emit_header(rb, rel, s);
    for (size_t i = start; i < end; i++)
        emit_group_row(rb, rel, s, results[i].first, results[i].second);
    rb.finalize(end - start);
    return MYDB_OK;
}


/* ======================================================================
 * exec_select — entry point
 * ====================================================================== */

int exec_select(EngineState *eng, const SelectStatement *s,
                char *out, size_t cap)
{
    REQUIRE_LOGIN(eng);
    REQUIRE_SCHEMA(eng);

    /* ------------------------------------------------------------------
     * Classify the SELECT list.
     * ------------------------------------------------------------------ */
    bool has_agg = false;
    bool has_col = false;
    for (const auto &item : s->select_list) {
        if (item.kind == SelectItem::Kind::Aggregate) has_agg = true;
        if (item.kind == SelectItem::Kind::Column)    has_col = true;
    }

    /* ------------------------------------------------------------------
     * Defer unimplemented sub-phases gracefully.
     * ------------------------------------------------------------------ */
    if (s->join_clause) {
        snprintf(out, cap, "not implemented: JOIN");
        return MYDB_ERR;
    }

    /* Without GROUP BY: mixing aggregate and non-aggregate columns is
     * undefined (which non-aggregate value would appear in the result?).
     * With GROUP BY this mix is valid — non-agg cols must be in GROUP BY. */
    if (has_agg && has_col && s->group_by.empty()) {
        snprintf(out, cap,
                 "ERROR: cannot mix aggregate and non-aggregate columns "
                 "without GROUP BY");
        return MYDB_ERR;
    }

    /* ORDER BY is not meaningful for scalar aggregates (one-row result).
     * With GROUP BY, ORDER BY applies to the grouped result — allowed. */
    if (has_agg && !s->order_by.empty() && s->group_by.empty()) {
        snprintf(out, cap,
                 "ERROR: ORDER BY is not applicable to scalar aggregates");
        return MYDB_ERR;
    }

    /* ------------------------------------------------------------------
     * Step 1: read access check + find the table.
     * ------------------------------------------------------------------ */
    int rc = engine_check_access(eng, 0);
    if (rc != MYDB_OK) {
        format_error(rc, out, cap, s->table_name.c_str());
        return rc;
    }

    const RelationDef *rel = engine_find_relation(eng, s->table_name.c_str());
    if (!rel) {
        snprintf(out, cap, "ERROR: table '%s' does not exist",
                 s->table_name.c_str());
        return MYDB_ERR_NOT_FOUND;
    }
    RelationDef *rel_rw = (RelationDef *)rel;   /* storage API takes non-const */

    /* ------------------------------------------------------------------
     * Step 2: validate projection columns.
     * ------------------------------------------------------------------ */
    if (!s->is_select_all) {
        for (const auto &item : s->select_list) {
            if (item.kind == SelectItem::Kind::Column) {
                if (resolve_col(rel, item.column) < 0) {
                    snprintf(out, cap,
                             "ERROR: column '%s' does not exist in table '%s'",
                             item.column.c_str(), s->table_name.c_str());
                    return MYDB_ERR;
                }
            }
        }
    }

    /* ------------------------------------------------------------------
     * Step 3: validate WHERE column references.
     * ------------------------------------------------------------------ */
    if (s->where_clause) {
        const char *bad =
            validate_expr_cols(s->where_clause->root.get(), rel);
        if (bad) {
            snprintf(out, cap,
                     "ERROR: column '%s' does not exist in table '%s'",
                     bad, s->table_name.c_str());
            return MYDB_ERR;
        }
    }

    /* ------------------------------------------------------------------
     * Step 4: validate ORDER BY columns (non-aggregate queries only).
     * ------------------------------------------------------------------ */
    if (!has_agg) {
        for (const auto &item : s->order_by) {
            if (resolve_col(rel, item.column) < 0) {
                snprintf(out, cap,
                         "ERROR: column '%s' does not exist in table '%s'",
                         item.column.c_str(), s->table_name.c_str());
                return MYDB_ERR;
            }
        }
    }

    /* ------------------------------------------------------------------
     * Step 5a: validate aggregate column references.
     * ------------------------------------------------------------------ */
    if (has_agg) {
        for (const auto &item : s->select_list) {
            if (item.kind != SelectItem::Kind::Aggregate) continue;
            if (item.column == "*") continue;   /* COUNT(*) — no column */
            if (resolve_col(rel, item.column) < 0) {
                snprintf(out, cap,
                         "ERROR: column '%s' does not exist in table '%s'",
                         item.column.c_str(), s->table_name.c_str());
                return MYDB_ERR;
            }
        }
    }

    /* ------------------------------------------------------------------
     * Step 5b: GROUP BY pre-checks and routing.
     * ------------------------------------------------------------------ */
    if (!s->group_by.empty()) {

        /* SELECT * with GROUP BY is ambiguous — which columns form the key? */
        if (s->is_select_all) {
            snprintf(out, cap, "ERROR: SELECT * is not allowed with GROUP BY");
            return MYDB_ERR;
        }

        /* All GROUP BY columns must exist in the relation. */
        for (const auto &col_name : s->group_by) {
            if (resolve_col(rel, col_name) < 0) {
                snprintf(out, cap,
                         "ERROR: column '%s' does not exist in table '%s'",
                         col_name.c_str(), s->table_name.c_str());
                return MYDB_ERR;
            }
        }

        /* Every non-aggregate SELECT column must appear in the GROUP BY list.
         * Standard SQL: a non-grouped column has no single value per group. */
        for (const auto &item : s->select_list) {
            if (item.kind != SelectItem::Kind::Column) continue;
            bool in_grp = false;
            for (const auto &gc : s->group_by)
                if (gc == item.column) { in_grp = true; break; }
            if (!in_grp) {
                snprintf(out, cap,
                         "ERROR: column '%s' must appear in GROUP BY or be "
                         "used in an aggregate function",
                         item.column.c_str());
                return MYDB_ERR;
            }
        }

        /* HAVING expression columns must exist in the relation. */
        if (s->having) {
            const char *bad = validate_expr_cols(s->having.get(), rel);
            if (bad) {
                snprintf(out, cap,
                         "ERROR: column '%s' does not exist in table '%s'",
                         bad, s->table_name.c_str());
                return MYDB_ERR;
            }
        }

        return exec_group_by(s, rel, rel_rw, out, cap);
    }

    /* ------------------------------------------------------------------
     * Step 5c: decide execution strategy for non-GROUP-BY queries.
     *
     * PK-ASC optimisation: if the first ORDER BY column is the PK and
     * direction is ASC, the B+ tree already returns rows in the correct
     * order — stream directly, no materialisation or sorting needed.
     *
     * Default materialise path: collect all matching rows, sort, then
     * slice [offset, offset+limit) and emit.
     * ------------------------------------------------------------------ */
    bool has_order  = !s->order_by.empty();
    bool pk_stream  = false;

    if (!has_agg && has_order) {
        int first_idx = resolve_col(rel, s->order_by[0].column);
        if (first_idx == (int)rel->pk_col_idx && !s->order_by[0].descending)
            pk_stream = true;
    }

    /* ------------------------------------------------------------------
     * Step 5d: choose access path (pure AST inspection — zero I/O).
     * ------------------------------------------------------------------ */
    AccessPath ap = pick_access_path(s->where_clause.get(), rel);

    /* ==================================================================
     * AGGREGATE PATH — all SELECT items are aggregate functions.
     *
     * Stream all rows that pass WHERE, accumulate into AggState array,
     * then emit exactly one result row.
     * ================================================================== */
    if (has_agg) {
        int       n = (int)s->select_list.size();
        AggState  states[MAX_COLUMNS];
        memset(states, 0, sizeof(states));

        /* resolve column indices once */
        for (int i = 0; i < n; i++) {
            const SelectItem &item = s->select_list[(size_t)i];
            states[i].col_idx = (item.column == "*")
                                 ? -1
                                 : resolve_col(rel, item.column);
        }

        /* scan — no early termination, need all rows */
        switch (ap.kind) {

        case AP_GET_PK: {
            Row *row = storage_get_by_pk(rel_rw, &ap.key);
            if (row && where_matches(s->where_clause.get(), rel, row))
                agg_accumulate(states, n, s, rel, row);
            break;
        }
        case AP_GET_INDEX: {
            Row *row = storage_get_by_index(rel_rw, ap.col_idx, &ap.key);
            if (row && where_matches(s->where_clause.get(), rel, row))
                agg_accumulate(states, n, s, rel, row);
            break;
        }
        case AP_SCAN_FROM: {
            Cursor *cur = storage_scan_from(rel_rw, &ap.key);
            if (cur) {
                Row *row;
                while ((row = cursor_next(cur)) != NULL) {
                    if (where_matches(s->where_clause.get(), rel, row))
                        agg_accumulate(states, n, s, rel, row);
                }
                cursor_close(cur);
            }
            break;
        }
        case AP_SCAN:
        default: {
            Cursor *cur = storage_scan(rel_rw);
            if (cur) {
                Row *row;
                while ((row = cursor_next(cur)) != NULL) {
                    if (where_matches(s->where_clause.get(), rel, row))
                        agg_accumulate(states, n, s, rel, row);
                }
                cursor_close(cur);
            }
            break;
        }
        }

        /* emit single result row */
        ResultBuf rb(out, cap);
        emit_header(rb, rel, s);
        emit_agg_row(rb, rel, s, states, n);
        rb.finalize(1);
        return MYDB_OK;
    }

    /* ==================================================================
     * STREAM PATH — no ORDER BY, or ORDER BY pk ASC (optimised).
     * Rows arrive in B+ tree order (= PK ascending), which is already
     * the correct order for the pk_stream case.
     * ================================================================== */
    if (!has_order || pk_stream) {

        ResultBuf rb(out, cap);
        bool      header_emitted = false;
        int64_t   skipped        = 0;
        int64_t   emitted        = 0;

        switch (ap.kind) {

        case AP_GET_PK: {
            Row *row = storage_get_by_pk(rel_rw, &ap.key);
            if (row)
                process_row(s->where_clause.get(), rel, row, s,
                            &rb, &header_emitted, &skipped, &emitted);
            break;
        }

        case AP_GET_INDEX: {
            Row *row = storage_get_by_index(rel_rw, ap.col_idx, &ap.key);
            if (row)
                process_row(s->where_clause.get(), rel, row, s,
                            &rb, &header_emitted, &skipped, &emitted);
            break;
        }

        case AP_SCAN_FROM: {
            Cursor *cur = storage_scan_from(rel_rw, &ap.key);
            if (cur) {
                Row *row;
                while ((row = cursor_next(cur)) != NULL) {
                    if (process_row(s->where_clause.get(), rel, row, s,
                                    &rb, &header_emitted, &skipped, &emitted))
                        break;
                }
                cursor_close(cur);
            }
            break;
        }

        case AP_SCAN:
        default: {
            Cursor *cur = storage_scan(rel_rw);
            if (cur) {
                Row *row;
                while ((row = cursor_next(cur)) != NULL) {
                    if (process_row(s->where_clause.get(), rel, row, s,
                                    &rb, &header_emitted, &skipped, &emitted))
                        break;
                }
                cursor_close(cur);
            }
            break;
        }
        }

        if (!header_emitted)
            emit_header(rb, rel, s);

        rb.finalize((size_t)emitted);
        return MYDB_OK;
    }

    /* ==================================================================
     * MATERIALISE PATH — ORDER BY on a non-PK column, or DESC on PK.
     *
     * 1. Collect every row that passes the WHERE filter.
     * 2. Sort:
     *      with LIMIT   → std::partial_sort (only sort as many as needed)
     *      without LIMIT → std::sort
     * 3. Slice [offset, offset+limit) from the sorted result.
     * 4. Emit header + rows.
     * ================================================================== */

    std::vector<Row> results;

    /* Collect — no LIMIT applied here; we need all rows to sort correctly */
    switch (ap.kind) {

    case AP_GET_PK: {
        Row *row = storage_get_by_pk(rel_rw, &ap.key);
        if (row && where_matches(s->where_clause.get(), rel, row))
            results.push_back(*row);
        break;
    }

    case AP_GET_INDEX: {
        Row *row = storage_get_by_index(rel_rw, ap.col_idx, &ap.key);
        if (row && where_matches(s->where_clause.get(), rel, row))
            results.push_back(*row);
        break;
    }

    case AP_SCAN_FROM: {
        Cursor *cur = storage_scan_from(rel_rw, &ap.key);
        if (cur) {
            Row *row;
            while ((row = cursor_next(cur)) != NULL) {
                if (where_matches(s->where_clause.get(), rel, row))
                    results.push_back(*row);
            }
            cursor_close(cur);
        }
        break;
    }

    case AP_SCAN:
    default: {
        Cursor *cur = storage_scan(rel_rw);
        if (cur) {
            Row *row;
            while ((row = cursor_next(cur)) != NULL) {
                if (where_matches(s->where_clause.get(), rel, row))
                    results.push_back(*row);
            }
            cursor_close(cur);
        }
        break;
    }
    }

    /* Sort */
    RowLess cmp { rel, &s->order_by };

    size_t total = results.size();
    size_t start = (s->offset > 0 && (size_t)s->offset < total)
                   ? (size_t)s->offset : (total > 0 ? 0 : 0);
    if ((size_t)s->offset >= total) start = total;   /* offset past end */

    size_t end = (s->limit >= 0)
                 ? std::min(start + (size_t)s->limit, total)
                 : total;

    /*
     * partial_sort guarantees [begin, begin+end) is sorted — the tail is
     * unordered.  This is cheaper than full sort when end << total.
     * Use full sort when end == total (no LIMIT, or LIMIT >= row count).
     */
    if (end > 0 && end < total) {
        std::partial_sort(results.begin(),
                          results.begin() + (ptrdiff_t)end,
                          results.end(), cmp);
    } else if (total > 1) {
        std::sort(results.begin(), results.end(), cmp);
    }

    /* Emit */
    ResultBuf rb(out, cap);
    emit_header(rb, rel, s);

    for (size_t i = start; i < end; i++)
        emit_row(rb, rel, &results[i], s);

    rb.finalize(end - start);
    return MYDB_OK;
}
