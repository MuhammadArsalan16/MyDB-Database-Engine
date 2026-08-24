/*
 * dql.cpp — SELECT handler.
 *
 * Sub-phase 5.1 : single-table SELECT, projection, WHERE filtering,
 *                 LIMIT / OFFSET, access-path selection.
 * Sub-phase 5.2 : ORDER BY — materialise + sort; PK-ASC stream optimisation.
 * Sub-phase 5.3 : scalar aggregates.
 * Sub-phase 5.4 : GROUP BY + aggregates, HAVING (GROUP BY columns only).
 * Sub-phase 5.5 : JOINs — INNER / LEFT / RIGHT / FULL OUTER, implicit comma
 *                 joins, and chained multi-table joins.  Per-step access path
 *                 (sort-merge / index-NLJ / hash) chosen from column indexes.
 *                 Ported to the v3 ExecContext / pm_* partition API.
 */

#include "ast_executor.hpp"
#include "exec_internal.h"
#include "result_fmt.hpp"
#include "value_cast.hpp"
#include "expr_eval.hpp"
#include "planner.h"    /* planner_choose_path, Sarg, PlanNode — extern "C" guards inside */
#include "exec_access_path.hpp" /* AccessPath, AP_*, extract_sargs, plan_to_ap — shared with dml.cpp */
#include "relation_guard.hpp"

#include <algorithm>
#include <map>
#include <vector>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <cstring>
#include <cstdio>

/* validate_expr_cols
 * → moved to exec_access_path.hpp (shared with dml.cpp) */

/* ======================================================================
 * Access-path selection
 *
 * Inspects the root of the WHERE tree for simple patterns:
 *   col = literal  (PK column)    → AP_GET_PK
 *   col = literal  (UNIQUE col)   → AP_GET_INDEX
 *   col >= literal (PK column)    → AP_SCAN_FROM
 *   anything else                 → AP_SCAN (full scan + filter)
 * ====================================================================== */


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

/* extract_sargs, literal_to_i64, extract_sargs_from_expr
 * → moved to exec_access_path.hpp (shared with dml.cpp) */

/* find_eq_value, find_range_lo, plan_to_ap
 * → moved to exec_access_path.hpp (shared with dml.cpp) */

/* ======================================================================
 * Design 3 row-building helpers
 *
 * These functions return std::vector<std::string> suitable for feeding
 * into a TableBuilder.  They replace the old emit_header / emit_row /
 * emit_agg_row / emit_group_row functions that wrote directly into a
 * ResultBuf.
 * ====================================================================== */

/* Build column header labels for a SELECT statement. */
static std::vector<std::string>
make_select_header(const RelationDef *rel, const SelectStatement *s)
{
    std::vector<std::string> cols;
    if (s->is_select_all) {
        for (int i = 0; i < rel->num_columns; i++)
            cols.push_back(rel->columns[i].name);
    } else {
        for (const auto &item : s->select_list) {
            if (item.kind == SelectItem::Kind::Aggregate) {
                if (!item.alias.empty()) {
                    cols.push_back(item.alias);
                } else {
                    std::string lbl = item.agg_func + "(";
                    if (item.agg_distinct) lbl += "DISTINCT ";
                    lbl += item.column + ")";
                    cols.push_back(lbl);
                }
            } else {
                cols.push_back(item.alias.empty() ? item.column : item.alias);
            }
        }
    }
    return cols;
}

/* Build one plain result row from a storage Row. */
static std::vector<std::string>
make_select_row(const RelationDef *rel,
                const Row *row,
                const SelectStatement *s)
{
    std::vector<std::string> cells;
    if (s->is_select_all) {
        for (int i = 0; i < rel->num_columns; i++)
            cells.push_back(value_to_str(row->cols[i], rel->columns[i]));
    } else {
        for (const auto &item : s->select_list) {
            int idx = (item.kind == SelectItem::Kind::Column)
                      ? resolve_col(rel, item.column) : -1;
            cells.push_back(idx >= 0
                            ? value_to_str(row->cols[idx], rel->columns[idx])
                            : "NULL");
        }
    }
    return cells;
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


/* Build one aggregate result row from the AggState accumulators. */
static std::vector<std::string>
make_agg_row(const RelationDef *rel,
             const SelectStatement *s,
             const AggState *states, int n)
{
    std::vector<std::string> cells;
    char tmp[64];

    for (int i = 0; i < n; i++) {
        const AggState    &st   = states[i];
        const std::string &func = s->select_list[(size_t)i].agg_func;

        if (func == "COUNT") {
            snprintf(tmp, sizeof(tmp), "%lld", (long long)st.count);
            cells.push_back(tmp);
        } else if (func == "SUM") {
            if (!st.has_value) {
                cells.push_back("NULL");
            } else if (st.col_idx >= 0 &&
                       rel->columns[st.col_idx].type == TYPE_INT) {
                snprintf(tmp, sizeof(tmp), "%lld", (long long)(int64_t)st.sum);
                cells.push_back(tmp);
            } else {
                int scale = (st.col_idx >= 0 && rel->columns[st.col_idx].scale > 0)
                            ? rel->columns[st.col_idx].scale : 2;
                char fmt[16];
                snprintf(fmt, sizeof(fmt), "%%.%df", scale);
                snprintf(tmp, sizeof(tmp), fmt, st.sum);
                cells.push_back(tmp);
            }
        } else if (func == "AVG") {
            if (st.count == 0) {
                cells.push_back("NULL");
            } else {
                snprintf(tmp, sizeof(tmp), "%.2f", st.sum / (double)st.count);
                cells.push_back(tmp);
            }
        } else if (func == "MIN" || func == "MAX") {
            if (!st.has_value) {
                cells.push_back("NULL");
            } else {
                const Value &val = (func == "MIN") ? st.min_val : st.max_val;
                cells.push_back(st.col_idx >= 0
                                ? value_to_str(val, rel->columns[st.col_idx])
                                : "NULL");
            }
        } else {
            cells.push_back("NULL");
        }
    }
    return cells;
}

/* Build one GROUP BY result row. */
static std::vector<std::string>
make_group_row(const RelationDef *rel,
               const SelectStatement *s,
               const Row &key_row,
               const std::vector<AggState> &states)
{
    std::vector<std::string> cells;
    char tmp[64];

    for (size_t i = 0; i < s->select_list.size(); i++) {
        const SelectItem &item = s->select_list[i];

        if (item.kind == SelectItem::Kind::Column) {
            int idx = resolve_col(rel, item.column);
            cells.push_back(idx >= 0
                            ? value_to_str(key_row.cols[idx], rel->columns[idx])
                            : "NULL");
        } else if (item.kind == SelectItem::Kind::Aggregate) {
            const AggState    &st   = states[i];
            const std::string &func = item.agg_func;

            if (func == "COUNT") {
                snprintf(tmp, sizeof(tmp), "%lld", (long long)st.count);
                cells.push_back(tmp);
            } else if (func == "SUM") {
                if (!st.has_value) {
                    cells.push_back("NULL");
                } else if (st.col_idx >= 0 &&
                           rel->columns[st.col_idx].type == TYPE_INT) {
                    snprintf(tmp, sizeof(tmp), "%lld",
                             (long long)(int64_t)st.sum);
                    cells.push_back(tmp);
                } else {
                    int scale = (st.col_idx >= 0 &&
                                 rel->columns[st.col_idx].scale > 0)
                                ? rel->columns[st.col_idx].scale : 2;
                    char fmt[16];
                    snprintf(fmt, sizeof(fmt), "%%.%df", scale);
                    snprintf(tmp, sizeof(tmp), fmt, st.sum);
                    cells.push_back(tmp);
                }
            } else if (func == "AVG") {
                if (st.count == 0)
                    cells.push_back("NULL");
                else {
                    snprintf(tmp, sizeof(tmp), "%.2f",
                             st.sum / (double)st.count);
                    cells.push_back(tmp);
                }
            } else if (func == "MIN" || func == "MAX") {
                if (!st.has_value)
                    cells.push_back("NULL");
                else {
                    const Value &val = (func == "MIN") ? st.min_val : st.max_val;
                    cells.push_back(st.col_idx >= 0
                                    ? value_to_str(val, rel->columns[st.col_idx])
                                    : "NULL");
                }
            } else {
                cells.push_back("NULL");
            }
        } else {
            cells.push_back("NULL");
        }
    }
    return cells;
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

            /*
             * NULL-first policy (mirrors GroupKey::operator<):
             *   NULL < non-NULL for ASC  → NULL rows sort to the top.
             *   NULL > non-NULL for DESC → NULL rows sort to the bottom.
             * Two NULLs are considered equal for this column; move on.
             *
             * This guarantees strict-weak-ordering for std::sort — without
             * it, compare_values() returns 1 whenever either operand is NULL,
             * which breaks the irreflexivity requirement and causes UB.
             */
            bool an = (a.cols[idx].is_null != 0);
            bool bn = (b.cols[idx].is_null != 0);
            if (an && bn) continue;          /* both NULL → equal, next col */
            if (an) return !item.descending; /* a is NULL: a < b for ASC    */
            if (bn) return  item.descending; /* b is NULL: a > b for ASC    */

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
static int exec_group_by(ExecContext           *ectx,
                         const SelectStatement *s,
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
        Row *r = pm_get_by_pk(ectx->partition, rel_rw, &ap.key);
        if (r) feed_row(r);
        break;
    }
    case AP_GET_INDEX: {
        Row *r = pm_get_by_index(ectx->partition, rel_rw, ap.col_idx, &ap.key);
        if (r) feed_row(r);
        break;
    }
    case AP_SCAN_FROM: {
        Cursor *cur = pm_scan_from(ectx->partition, rel_rw, &ap.key);
        if (cur) {
            Row *r;
            while ((r = cursor_next(cur)) != NULL) feed_row(r);
            cursor_close(cur);
        }
        break;
    }
    case AP_SCAN_BY_INDEX: {
        Cursor *cur = pm_scan_by_index(ectx->partition, rel_rw, ap.col_idx, &ap.key);
        if (cur) {
            Row *r;
            while ((r = cursor_next(cur)) != NULL) feed_row(r);
            cursor_close(cur);
        }
        break;
    }
    default: {
        Cursor *cur = pm_scan(ectx->partition, rel_rw);
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
    /* Compute slice bounds (before any sort). */
    size_t total = results.size();
    size_t start = ((size_t)s->offset < total) ? (size_t)s->offset : total;
    size_t end   = (s->limit >= 0)
                   ? std::min(start + (size_t)s->limit, total)
                   : total;

    if (!s->order_by.empty()) {
        RowLess row_cmp { rel, &s->order_by };

        auto pair_cmp = [&row_cmp](const GResult &a, const GResult &b) {
            return row_cmp(a.first, b.first);
        };

        if (end > 0 && end < total)
            std::partial_sort(results.begin(),
                              results.begin() + (ptrdiff_t)end,
                              results.end(), pair_cmp);
        else if (total > 1)
            std::sort(results.begin(), results.end(), pair_cmp);
    }

    ResultBuf rb(out, cap);
    TableBuilder tb;
    tb.set_headers(make_select_header(rel, s));
    for (size_t i = start; i < end; i++)
        tb.add_row(make_group_row(rel, s, results[i].first, results[i].second));
    tb.render(rb);
    return MYDB_OK;
}


/* ======================================================================
 * JOIN execution  (sub-phase 5.5)
 *
 * Supports INNER / LEFT / RIGHT / FULL OUTER joins, implicit comma joins
 * (FROM a, b WHERE a.x = b.y), and chained joins over 2+ tables.
 *
 * Model — a left-deep join: start with the first FROM table, then fold in
 * each additional table one step at a time, accumulating a set of
 * "combined rows" (one storage Row per participating table).
 *
 * Per-step access path (planner-style, driven by indexes):
 *   - both join columns indexed (2-table case)  -> SORT_MERGE   (O(n+m))
 *   - inner table join column indexed           -> INDEX_NLJ    (O(n log m))
 *   - neither indexed                           -> HASH_JOIN    (O(n+m))
 *
 * v3: all storage access goes through the pm_* wrappers on the partition
 * context (ectx->partition); the ON condition pairs rows, the WHERE clause
 * is applied once at the end over the fully combined row.
 * ====================================================================== */

/* One table participating in the join.
 *
 * Owns a RelationGuard so the RelationDef* pinned via pm_find_relation_const()
 * (Phase 1 pin/release discipline, PARTITION_BUFFER_DESIGN.md) is released
 * automatically when this segment is destroyed — whether that's a normal
 * return, an early error return with segs[] only partially populated, or
 * segs[] fully built. `rel` stays a plain non-owning alias of guard.get()
 * so the existing segs[i].rel read sites throughout this file need no
 * changes. */
struct JoinSeg {
    RelationGuard       guard;
    const RelationDef  *rel;
    std::string         label;   /* alias if given, else relation_name */

    JoinSeg(PartitionCtx *ctx, const RelationDef *r, std::string lbl)
        : guard(ctx, r), rel(r), label(std::move(lbl)) {}
};

/* One fold step: add segs[right_seg] to the accumulated result. */
struct JoinStep {
    int          right_seg;          /* segment being added */
    JoinType     type;               /* INNER / LEFT / RIGHT / FULL */
    bool         cross;              /* true = no ON condition (cartesian) */
    int          prev_seg, prev_col; /* earlier-table column in the ON equality */
    int          new_col;            /* right_seg-table column in the ON equality */
    JoinAlgoType forced_algo = JOIN_ALGO_HASH; /* planner-chosen algorithm; HASH is always safe */
};

/* One combined row spanning all segments. cells[i] holds segment i's row;
 * isnull[i] == 1 means segment i is NULL-padded (outer-join fill). */
struct JoinedRow {
    std::vector<Row>  cells;
    std::vector<char> isnull;
};

/* Split "table.col" into prefix + column ("col" alone leaves prefix empty). */
static void split_qual(const std::string &q, std::string &tbl, std::string &col)
{
    size_t dot = q.find('.');
    if (dot == std::string::npos) { tbl.clear(); col = q; }
    else { tbl = q.substr(0, dot); col = q.substr(dot + 1); }
}

/* Resolve (prefix, column) against the segment list.
 * prefix may match a segment's alias or its real table name; empty prefix
 * searches every segment. Returns true and writes seg/col index on success. */
static bool seg_resolve(const std::vector<JoinSeg> &segs,
                        const std::string &prefix, const std::string &col,
                        int &seg_out, int &col_out)
{
    for (size_t i = 0; i < segs.size(); i++) {
        if (!prefix.empty() &&
            prefix != segs[i].label &&
            strcmp(prefix.c_str(), segs[i].rel->relation_name) != 0)
            continue;
        for (int c = 0; c < segs[i].rel->num_columns; c++) {
            if (col == segs[i].rel->columns[c].name) {
                seg_out = (int)i; col_out = c; return true;
            }
        }
    }
    return false;
}

/* Index classification of a column: 0 = none, 1 = unique (PK or UNIQUE
 * secondary, at most one match), 2 = non-unique secondary (many matches). */
static int seg_index_kind(const RelationDef *rel, int col)
{
    if (col == (int)rel->pk_col_idx) return 1;
    for (int j = 0; j < rel->num_secondary_indexes; j++)
        if ((int)rel->secondary_col_idx[j] == col)
            return rel->columns[col].is_unique ? 1 : 2;
    return 0;
}

/* Canonical hash/equality key for a Value (consistent with compare_values). */
static std::string vkey(const Value &v)
{
    char b[64];
    switch (v.type) {
    case TYPE_INT:      snprintf(b, sizeof b, "i%d",   v.v.int_val);                 return b;
    case TYPE_DECIMAL:  snprintf(b, sizeof b, "d%lld", (long long)v.v.decimal_val);  return b;
    case TYPE_DATE:     snprintf(b, sizeof b, "D%d",   v.v.date_val);                return b;
    case TYPE_DATETIME: snprintf(b, sizeof b, "T%lld", (long long)v.v.datetime_val); return b;
    case TYPE_BOOL:     snprintf(b, sizeof b, "b%d",   v.v.bool_val);                return b;
    case TYPE_ENUM:     snprintf(b, sizeof b, "e%d",   v.v.enum_val);                return b;
    case TYPE_VARCHAR:  return std::string("s") +
                               std::string(v.v.varchar_val.data, v.v.varchar_val.len);
    }
    return "";
}

/* Open a cursor that returns rows sorted by `col` (PK scan or index scan). */
static Cursor *open_sorted_cursor(PartitionCtx *pc, RelationDef *rel, int col)
{
    if (col == (int)rel->pk_col_idx) return pm_scan(pc, rel);
    return pm_scan_by_index(pc, rel, col, nullptr);
}

/* Fetch all inner rows whose `col` equals key. rk is the index kind. */
static std::vector<Row> lookup_right(PartitionCtx *pc, RelationDef *rrel,
                                     int col, const Value &key, int rk)
{
    std::vector<Row> res;
    Value k = key;   /* pm API takes non-const */

    if (col == (int)rrel->pk_col_idx) {
        Row *r = pm_get_by_pk(pc, rrel, &k);
        if (r) res.push_back(*r);
    } else if (rk == 1) {
        Row *r = pm_get_by_index(pc, rrel, col, &k);
        if (r) res.push_back(*r);
    } else {
        Cursor *c = pm_scan_by_index(pc, rrel, col, &k);
        if (c) {
            Row *r;
            while ((r = cursor_next(c)) != NULL) {
                if (compare_values(&r->cols[col], &k) == 0) res.push_back(*r);
                else break;   /* index is sorted — past the key */
            }
            cursor_close(c);
        }
    }
    return res;
}

/* ------------------------------------------------------------------ */
/*  Join-aware WHERE evaluation (resolves columns across all segments) */
/* ------------------------------------------------------------------ */

static int j_like_match(const char *str, const char *pat)
{
    while (*pat) {
        if (*pat == '%') {
            pat++;
            do { if (j_like_match(str, pat)) return 1; } while (*str++);
            return 0;
        }
        if (*pat == '_') { if (!*str) return 0; str++; pat++; }
        else { if (*str != *pat) return 0; str++; pat++; }
    }
    return *str == '\0';
}

/* Column definition behind a ColumnRef expr (for literal type hints). */
static const ColumnDef *j_coldef(const Expr *e, const std::vector<JoinSeg> &segs)
{
    if (e && e->kind == Expr::Kind::ColumnRef) {
        const ColumnRefExpr *cr = static_cast<const ColumnRefExpr *>(e);
        int sg, ci;
        if (seg_resolve(segs, cr->table, cr->column, sg, ci))
            return &segs[sg].rel->columns[ci];
    }
    return nullptr;
}

/* Value of an expression (ColumnRef or Literal) within a combined row. */
static Value j_eval_value(const Expr *e, const std::vector<JoinSeg> &segs,
                          const JoinedRow &jr, const ColumnDef *hint)
{
    Value v; memset(&v, 0, sizeof(v)); v.is_null = 1;
    if (!e) return v;

    if (e->kind == Expr::Kind::ColumnRef) {
        const ColumnRefExpr *cr = static_cast<const ColumnRefExpr *>(e);
        int sg, ci;
        if (!seg_resolve(segs, cr->table, cr->column, sg, ci)) return v;
        if (jr.isnull[sg]) return v;        /* NULL-padded segment */
        return jr.cells[sg].cols[ci];
    }
    if (e->kind == Expr::Kind::Literal) {
        const LiteralExpr *lit = static_cast<const LiteralExpr *>(e);
        if (hint) return cast_literal(lit->raw, *hint);
        /* no hint: best-guess as VARCHAR text */
        v.is_null = 0; v.type = TYPE_VARCHAR;
        size_t n = lit->raw.size(); if (n > MAX_VARCHAR_LEN) n = MAX_VARCHAR_LEN;
        v.v.varchar_val.len = (uint16_t)n;
        memcpy(v.v.varchar_val.data, lit->raw.c_str(), n);
        return v;
    }
    return v;
}

/* Evaluate a WHERE expression against a combined row. */
static bool j_eval(const Expr *e, const std::vector<JoinSeg> &segs,
                   const JoinedRow &jr)
{
    if (!e) return true;

    switch (e->kind) {

    case Expr::Kind::Binary: {
        const BinaryExpr *b = static_cast<const BinaryExpr *>(e);
        const char *op = b->op.c_str();

        if (strcmp(op, "AND") == 0)
            return j_eval(b->lhs.get(), segs, jr) && j_eval(b->rhs.get(), segs, jr);
        if (strcmp(op, "OR") == 0)
            return j_eval(b->lhs.get(), segs, jr) || j_eval(b->rhs.get(), segs, jr);

        /* comparison: a literal on one side takes its type from the column
         * on the other side. */
        const ColumnDef *lhs_hint = j_coldef(b->rhs.get(), segs);  /* from rhs column */
        const ColumnDef *rhs_hint = j_coldef(b->lhs.get(), segs);  /* from lhs column */
        Value lv = j_eval_value(b->lhs.get(), segs, jr, lhs_hint);
        Value rv = j_eval_value(b->rhs.get(), segs, jr, rhs_hint);
        if (lv.is_null || rv.is_null) return false;

        int cmp = compare_values(&lv, &rv);
        if (strcmp(op, "=")  == 0) return cmp == 0;
        if (strcmp(op, "!=") == 0 || strcmp(op, "<>") == 0) return cmp != 0;
        if (strcmp(op, "<")  == 0) return cmp <  0;
        if (strcmp(op, ">")  == 0) return cmp >  0;
        if (strcmp(op, "<=") == 0) return cmp <= 0;
        if (strcmp(op, ">=") == 0) return cmp >= 0;
        return false;
    }

    case Expr::Kind::Unary: {
        const UnaryExpr *u = static_cast<const UnaryExpr *>(e);
        if (strcmp(u->op.c_str(), "NOT") == 0)
            return !j_eval(u->child.get(), segs, jr);
        return false;
    }

    case Expr::Kind::IsNull: {
        const IsNullExpr *isn = static_cast<const IsNullExpr *>(e);
        Value v = j_eval_value(isn->child.get(), segs, jr, nullptr);
        bool is_null = (v.is_null != 0);
        return isn->negated ? !is_null : is_null;
    }

    case Expr::Kind::Between: {
        const BetweenExpr *bw = static_cast<const BetweenExpr *>(e);
        const ColumnDef *hint = j_coldef(bw->v.get(), segs);
        Value v  = j_eval_value(bw->v.get(),  segs, jr, nullptr);
        Value lo = j_eval_value(bw->lo.get(), segs, jr, hint);
        Value hi = j_eval_value(bw->hi.get(), segs, jr, hint);
        if (v.is_null || lo.is_null || hi.is_null) return false;
        bool r = compare_values(&v, &lo) >= 0 && compare_values(&v, &hi) <= 0;
        return bw->negated ? !r : r;
    }

    case Expr::Kind::In: {
        const InExpr *in = static_cast<const InExpr *>(e);
        const ColumnDef *hint = j_coldef(in->v.get(), segs);
        Value v = j_eval_value(in->v.get(), segs, jr, nullptr);
        if (v.is_null) return false;
        bool found = false;
        for (const auto &lit : in->list) {
            Value lv = hint ? cast_literal(lit->raw, *hint)
                            : j_eval_value(lit.get(), segs, jr, nullptr);
            if (compare_values(&v, &lv) == 0) { found = true; break; }
        }
        return in->negated ? !found : found;
    }

    case Expr::Kind::Like: {
        const LikeExpr *lk = static_cast<const LikeExpr *>(e);
        Value v = j_eval_value(lk->v.get(), segs, jr, nullptr);
        if (v.is_null || v.type != TYPE_VARCHAR) return false;
        bool r = j_like_match(v.v.varchar_val.data, lk->pattern.c_str()) != 0;
        return lk->negated ? !r : r;
    }

    case Expr::Kind::ColumnRef:
    case Expr::Kind::Literal: {
        Value v = j_eval_value(e, segs, jr, nullptr);
        if (v.is_null) return false;
        if (v.type == TYPE_BOOL) return v.v.bool_val != 0;
        if (v.type == TYPE_INT)  return v.v.int_val  != 0;
        return false;
    }
    default:
        return true;
    }
}

/* ------------------------------------------------------------------ */
/*  Sort-merge join (2-table fast path, both join columns indexed)     */
/* ------------------------------------------------------------------ */
static std::vector<JoinedRow>
sort_merge_two(PartitionCtx *pc, RelationDef *L, int lc,
               RelationDef *R, int rc, JoinType type)
{
    std::vector<JoinedRow> out;
    bool keepL = (type == JoinType::LEFT  || type == JoinType::FULL);
    bool keepR = (type == JoinType::RIGHT || type == JoinType::FULL);

    Cursor *lcur = open_sorted_cursor(pc, L, lc);
    Cursor *rcur = open_sorted_cursor(pc, R, rc);

    Row lrow, rrow;
    Row *lp = lcur ? cursor_next(lcur) : nullptr; bool lhave = lp; if (lp) lrow = *lp;
    Row *rp = rcur ? cursor_next(rcur) : nullptr; bool rhave = rp; if (rp) rrow = *rp;

    auto emit = [&](const Row *a, const Row *b) {
        JoinedRow jr; jr.cells.resize(2); jr.isnull.assign(2, 1);
        if (a) { jr.cells[0] = *a; jr.isnull[0] = 0; }
        if (b) { jr.cells[1] = *b; jr.isnull[1] = 0; }
        out.push_back(jr);
    };

    while (lhave && rhave) {
        int c = compare_values(&lrow.cols[lc], &rrow.cols[rc]);
        if (c < 0) {
            if (keepL) emit(&lrow, nullptr);
            lp = cursor_next(lcur); lhave = lp; if (lp) lrow = *lp;
        } else if (c > 0) {
            if (keepR) emit(nullptr, &rrow);
            rp = cursor_next(rcur); rhave = rp; if (rp) rrow = *rp;
        } else {
            Value key = lrow.cols[lc];
            std::vector<Row> lg, rg;
            while (lhave && compare_values(&lrow.cols[lc], &key) == 0) {
                lg.push_back(lrow);
                lp = cursor_next(lcur); lhave = lp; if (lp) lrow = *lp;
            }
            while (rhave && compare_values(&rrow.cols[rc], &key) == 0) {
                rg.push_back(rrow);
                rp = cursor_next(rcur); rhave = rp; if (rp) rrow = *rp;
            }
            for (auto &a : lg) for (auto &b : rg) emit(&a, &b);
        }
    }
    while (lhave) { if (keepL) emit(&lrow, nullptr);
                    lp = cursor_next(lcur); lhave = lp; if (lp) lrow = *lp; }
    while (rhave) { if (keepR) emit(nullptr, &rrow);
                    rp = cursor_next(rcur); rhave = rp; if (rp) rrow = *rp; }

    if (lcur) cursor_close(lcur);
    if (rcur) cursor_close(rcur);
    return out;
}

/* ------------------------------------------------------------------ */
/*  One fold step: accumulated `left` ⋈ segs[step.right_seg]           */
/* ------------------------------------------------------------------ */
static void join_step(PartitionCtx *pc, const std::vector<JoinSeg> &segs,
                      std::vector<JoinedRow> &left, const JoinStep &st)
{
    RelationDef *rrel  = (RelationDef *)segs[st.right_seg].rel;
    size_t       total = segs.size();
    bool keepL = (st.type == JoinType::LEFT  || st.type == JoinType::FULL);
    bool keepR = (st.type == JoinType::RIGHT || st.type == JoinType::FULL);
    std::vector<JoinedRow> out;

    /* Cartesian product (implicit join with no linking equality). */
    if (st.cross) {
        std::vector<Row> rrows;
        Cursor *c = pm_scan(pc, rrel);
        if (c) { Row *r; while ((r = cursor_next(c)) != NULL) rrows.push_back(*r);
                 cursor_close(c); }
        for (auto &lr : left)
            for (auto &R : rrows) {
                JoinedRow nr = lr;
                nr.cells[st.right_seg] = R; nr.isnull[st.right_seg] = 0;
                out.push_back(nr);
            }
        left.swap(out);
        return;
    }

    /* For RIGHT/FULL we must later emit unmatched right rows — remember
     * every left-side join key so we know which right rows never matched. */
    std::unordered_set<std::string> left_keys;
    if (keepR) {
        for (auto &lr : left) {
            if (lr.isnull[st.prev_seg]) continue;
            const Value &v = lr.cells[st.prev_seg].cols[st.prev_col];
            if (!v.is_null) left_keys.insert(vkey(v));
        }
    }

    /* rk is still needed inside the NLJ branch for lookup_right's unique-vs-multi
     * micro-decision; the NLJ-vs-HASH choice itself is now planner-driven. */
    int rk = seg_index_kind(rrel, st.new_col);

    if (st.forced_algo == JOIN_ALGO_NLJ) {
        /* INDEX_NLJ — inner join column is indexed. */
        for (auto &lr : left) {
            if (lr.isnull[st.prev_seg]) { if (keepL) out.push_back(lr); continue; }
            Value key = lr.cells[st.prev_seg].cols[st.prev_col];
            if (key.is_null) { if (keepL) out.push_back(lr); continue; }

            std::vector<Row> ms = lookup_right(pc, rrel, st.new_col, key, rk);
            if (!ms.empty())
                for (auto &R : ms) {
                    JoinedRow nr = lr;
                    nr.cells[st.right_seg] = R; nr.isnull[st.right_seg] = 0;
                    out.push_back(nr);
                }
            else if (keepL) out.push_back(lr);
        }
    } else {
        /* HASH_JOIN — build a hash table on the inner (right) table. */
        std::unordered_map<std::string, std::vector<Row>> hm;
        Cursor *c = pm_scan(pc, rrel);
        if (c) {
            Row *r;
            while ((r = cursor_next(c)) != NULL) {
                const Value &v = r->cols[st.new_col];
                if (!v.is_null) hm[vkey(v)].push_back(*r);
            }
            cursor_close(c);
        }
        for (auto &lr : left) {
            if (lr.isnull[st.prev_seg]) { if (keepL) out.push_back(lr); continue; }
            Value key = lr.cells[st.prev_seg].cols[st.prev_col];
            if (key.is_null) { if (keepL) out.push_back(lr); continue; }

            auto it = hm.find(vkey(key));
            if (it != hm.end())
                for (auto &R : it->second) {
                    JoinedRow nr = lr;
                    nr.cells[st.right_seg] = R; nr.isnull[st.right_seg] = 0;
                    out.push_back(nr);
                }
            else if (keepL) out.push_back(lr);
        }
    }

    /* RIGHT / FULL — append inner rows that never matched any left row. */
    if (keepR) {
        Cursor *c = pm_scan(pc, rrel);
        if (c) {
            Row *r;
            while ((r = cursor_next(c)) != NULL) {
                const Value &v = r->cols[st.new_col];
                if (v.is_null) continue;
                if (left_keys.find(vkey(v)) == left_keys.end()) {
                    JoinedRow nr;
                    nr.cells.assign(total, Row{});
                    nr.isnull.assign(total, 1);
                    nr.cells[st.right_seg] = *r; nr.isnull[st.right_seg] = 0;
                    out.push_back(nr);
                }
            }
            cursor_close(c);
        }
    }

    left.swap(out);
}

/* Collect col=col equalities (under AND) for implicit-join condition mining. */
struct EqLink { int sa, ca, sb, cb; };
static void collect_eq(const Expr *e, const std::vector<JoinSeg> &segs,
                       std::vector<EqLink> &links)
{
    if (!e || e->kind != Expr::Kind::Binary) return;
    const BinaryExpr *b = static_cast<const BinaryExpr *>(e);
    if (b->op == "AND") {
        collect_eq(b->lhs.get(), segs, links);
        collect_eq(b->rhs.get(), segs, links);
        return;
    }
    if (b->op == "=" &&
        b->lhs && b->lhs->kind == Expr::Kind::ColumnRef &&
        b->rhs && b->rhs->kind == Expr::Kind::ColumnRef) {
        const ColumnRefExpr *l = static_cast<const ColumnRefExpr *>(b->lhs.get());
        const ColumnRefExpr *r = static_cast<const ColumnRefExpr *>(b->rhs.get());
        EqLink lk;
        if (seg_resolve(segs, l->table, l->column, lk.sa, lk.ca) &&
            seg_resolve(segs, r->table, r->column, lk.sb, lk.cb))
            links.push_back(lk);
    }
}

/* Half-open index range into steps[] covering a run of INNER-only steps.
 * Groups with fewer than 2 steps (< 3 relations) are not worth DP-ing. */
struct DPGroup { int start, end; };

static std::vector<DPGroup> find_dp_groups(const std::vector<JoinStep> &steps)
{
    std::vector<DPGroup> groups;
    size_t i = 0;
    while (i < steps.size()) {
        if (steps[i].type != JoinType::INNER) { i++; continue; }
        size_t j = i;
        while (j < steps.size() && steps[j].type == JoinType::INNER) j++;
        if (j - i >= 2) groups.push_back({(int)i, (int)j});
        i = j;
    }
    return groups;
}

/* ====================================================================== */
/*  exec_join_select — top-level JOIN handler                            */
/* ====================================================================== */
static int exec_join_select(ExecContext *ectx, const SelectStatement *s,
                            char *out, size_t cap)
{
    PartitionCtx *pc = ectx->partition;

    int rc = engine_check_access(ectx->engine, 0);
    if (rc != MYDB_OK) {
        format_error(rc, out, cap, s->from_list[0].table_name.c_str());
        return rc;
    }

    /* Aggregates / GROUP BY across joins are out of scope for Phase 1. */
    if (!s->group_by.empty()) {
        snprintf(out, cap, "  Error: GROUP BY with JOIN is not supported");
        return MYDB_ERR;
    }
    for (const auto &it : s->select_list)
        if (it.kind == SelectItem::Kind::Aggregate) {
            snprintf(out, cap, "  Error: aggregates with JOIN are not supported");
            return MYDB_ERR;
        }

    /* ---- Build the segment list (FROM tables, then JOIN tables) ---- */
    std::vector<JoinSeg> segs;
    for (const auto &fi : s->from_list) {
        const RelationDef *r = pm_find_relation_const(pc, fi.table_name.c_str());
        if (!r) {
            snprintf(out, cap, "  Error: table '%s' does not exist",
                     fi.table_name.c_str());
            return MYDB_ERR_NOT_FOUND;
        }
        segs.emplace_back(pc, r, fi.alias.empty() ? std::string(r->relation_name) : fi.alias);
    }
    int first_join_seg = (int)segs.size();
    for (const auto &jc : s->join_list) {
        const RelationDef *r = pm_find_relation_const(pc, jc.join_table.c_str());
        if (!r) {
            snprintf(out, cap, "  Error: table '%s' does not exist",
                     jc.join_table.c_str());
            return MYDB_ERR_NOT_FOUND;
        }
        segs.emplace_back(pc, r, jc.join_table_alias.empty()
                                 ? std::string(r->relation_name) : jc.join_table_alias);
    }

    /* ---- Build the fold steps ---- */
    std::vector<JoinStep> steps;

    /* Implicit comma joins: link each extra FROM table to an earlier one. */
    std::vector<EqLink> links;
    if (s->where_clause) collect_eq(s->where_clause->root.get(), segs, links);

    for (int i = 1; i < first_join_seg; i++) {
        JoinStep st; st.right_seg = i; st.type = JoinType::INNER; st.cross = true;
        st.prev_seg = st.prev_col = st.new_col = 0;
        for (const auto &lk : links) {
            if (lk.sa == i && lk.sb < i) {
                st.cross = false; st.prev_seg = lk.sb; st.prev_col = lk.cb; st.new_col = lk.ca; break;
            }
            if (lk.sb == i && lk.sa < i) {
                st.cross = false; st.prev_seg = lk.sa; st.prev_col = lk.ca; st.new_col = lk.cb; break;
            }
        }
        steps.push_back(st);
    }

    /* Explicit joins: ON condition gives the pairing columns. */
    for (size_t j = 0; j < s->join_list.size(); j++) {
        const JoinClause &jc = s->join_list[j];
        JoinStep st; st.right_seg = first_join_seg + (int)j;
        st.type = jc.join_type; st.cross = false;

        std::string lt, lcn, rt, rcn;
        split_qual(jc.left_condition,  lt, lcn);
        split_qual(jc.right_condition, rt, rcn);

        int s1, c1, s2, c2;
        if (!seg_resolve(segs, lt, lcn, s1, c1) ||
            !seg_resolve(segs, rt, rcn, s2, c2)) {
            snprintf(out, cap, "  Error: JOIN ON column does not exist");
            return MYDB_ERR;
        }
        /* One side must reference the table being added (right_seg). */
        if (s1 == st.right_seg) { st.new_col = c1; st.prev_seg = s2; st.prev_col = c2; }
        else if (s2 == st.right_seg) { st.new_col = c2; st.prev_seg = s1; st.prev_col = c1; }
        else {
            snprintf(out, cap,
                     "  Error: JOIN ON must reference the joined table '%s'",
                     jc.join_table.c_str());
            return MYDB_ERR;
        }
        steps.push_back(st);
    }

    /* ---- Build the projection column list ---- */
    struct ProjCol { int seg; int col; std::string label; };
    std::vector<ProjCol> proj;

    auto add_all_seg_cols = [&](int sg) {
        for (int c = 0; c < segs[sg].rel->num_columns; c++)
            proj.push_back({ sg, c,
                segs[sg].label + "." + segs[sg].rel->columns[c].name });
    };

    if (s->is_select_all) {
        for (int sg = 0; sg < (int)segs.size(); sg++) add_all_seg_cols(sg);
    } else {
        for (const auto &it : s->select_list) {
            if (it.kind == SelectItem::Kind::Star) {
                if (!it.table.empty()) {
                    int sg = -1;
                    for (int i = 0; i < (int)segs.size(); i++)
                        if (it.table == segs[i].label ||
                            strcmp(it.table.c_str(), segs[i].rel->relation_name) == 0)
                            { sg = i; break; }
                    if (sg < 0) { snprintf(out, cap,
                        "  Error: unknown table '%s' in SELECT", it.table.c_str());
                        return MYDB_ERR; }
                    add_all_seg_cols(sg);
                } else {
                    for (int sg = 0; sg < (int)segs.size(); sg++) add_all_seg_cols(sg);
                }
            } else { /* Column */
                int sg, ci;
                if (!seg_resolve(segs, it.table, it.column, sg, ci)) {
                    snprintf(out, cap, "  Error: column '%s' does not exist",
                             it.column.c_str());
                    return MYDB_ERR;
                }
                std::string label = !it.alias.empty() ? it.alias
                                  : (it.table.empty() ? it.column
                                                      : it.table + "." + it.column);
                proj.push_back({ sg, ci, label });
            }
        }
    }

    /* ---- Cost-based join ordering and algorithm selection ---- */
    int leading_group_first_seg = 0;
    {
        std::vector<bool> algo_set(steps.size(), false);
        SchemaFile *schema = pctx_active_schema(pc);

        /* 1. DP: reorder each maximal run of 2+ consecutive INNER steps. */
        for (const auto &grp : find_dp_groups(steps)) {
            int gstart = grp.start, gend = grp.end;
            int anchor_seg = (gstart == 0) ? 0 : steps[gstart - 1].right_seg;

            std::vector<int> group_segs;
            group_segs.push_back(anchor_seg);
            for (int k = gstart; k < gend; k++)
                group_segs.push_back(steps[k].right_seg);

            std::unordered_map<int,int> seg_to_local;
            std::vector<const RelationDef*> rels;
            for (size_t li = 0; li < group_segs.size(); li++) {
                seg_to_local[group_segs[li]] = (int)li;
                rels.push_back(segs[group_segs[li]].rel);
            }

            std::vector<JoinEdgeDef> dp_edges;
            for (int k = gstart; k < gend; k++) {
                if (steps[k].cross) continue;
                JoinEdgeDef e;
                e.a_rel_idx = seg_to_local[steps[k].prev_seg];
                e.a_col_idx = steps[k].prev_col;
                e.b_rel_idx = seg_to_local[steps[k].right_seg];
                e.b_col_idx = steps[k].new_col;
                dp_edges.push_back(e);
            }

            if ((int)rels.size() > PLANNER_MAX_JOIN_GROUP) continue;

            /* Pre-load stats pages for each relation in the group. */
            for (auto *r : rels) {
                RelationEntry *ent = schema_find_relation_stat(schema, r->relation_name);
                if (ent && ectx->stats)
                    stats_load_relation(ectx->stats, (int)(ent - schema->relations));
            }

            JoinPlanResult plan = planner_plan_join_order(
                schema, ectx->stats,
                rels.data(), (int)rels.size(),
                dp_edges.data(), (int)dp_edges.size());

            /* Splice the DP-chosen order back into steps[gstart..gend). */
            for (int p = 1; p < plan.n_steps; p++) {
                JoinStep ns;
                ns.right_seg   = group_segs[plan.steps[p].rel_idx];
                ns.type        = JoinType::INNER;
                ns.cross       = plan.steps[p].is_cross;
                ns.prev_seg    = group_segs[plan.steps[p].via_rel_idx];
                ns.prev_col    = plan.steps[p].via_col_idx;
                ns.new_col     = plan.steps[p].join_col_idx;
                ns.forced_algo = plan.steps[p].algo;
                steps[gstart + (p - 1)] = ns;
                algo_set[gstart + (p - 1)] = true;
            }

            if (gstart == 0)
                leading_group_first_seg = group_segs[plan.steps[0].rel_idx];
        }

        /* 2. Algorithm selection for all other steps (barrier LEFT/RIGHT/FULL
         *    and any group that exceeded PLANNER_MAX_JOIN_GROUP). */
        for (size_t k = 0; k < steps.size(); k++) {
            if (algo_set[k]) continue;
            JoinStep &st = steps[k];
            if (st.cross) { st.forced_algo = JOIN_ALGO_HASH; continue; }
            float left_card = 1.0f;
            RelationEntry *ent = schema_find_relation_stat(
                schema, segs[st.prev_seg].rel->relation_name);
            if (ent && ent->num_rows > 0) left_card = (float)ent->num_rows;
            st.forced_algo = planner_choose_join_algo(
                schema, ectx->stats,
                segs[st.prev_seg].rel, st.prev_col, left_card,
                segs[st.right_seg].rel, st.new_col,
                /*is_first_step=*/ k == 0).algo;
        }
    }

    /* ---- Execute the join ---- */
    std::vector<JoinedRow> acc;
    bool done = false;

    /* Sort-merge fast path: exactly two tables, single keyed step, and the
     * planner chose SORT_MERGE (both join columns are indexed). */
    if (segs.size() == 2 && steps.size() == 1 && !steps[0].cross &&
        steps[0].forced_algo == JOIN_ALGO_SORT_MERGE) {
        const JoinStep &st = steps[0];
        acc = sort_merge_two(pc,
                             (RelationDef *)segs[st.prev_seg].rel,  st.prev_col,
                             (RelationDef *)segs[st.right_seg].rel, st.new_col,
                             st.type);
        done = true;
    }

    if (!done) {
        /* General path: seed from the planner-chosen first relation, then
         * fold each step.  leading_group_first_seg == 0 when no DP reordering
         * happened, preserving the original behaviour. */
        int first_seg = leading_group_first_seg;
        Cursor *c = pm_scan(pc, (RelationDef *)segs[first_seg].rel);
        if (c) {
            Row *r;
            while ((r = cursor_next(c)) != NULL) {
                JoinedRow jr;
                jr.cells.assign(segs.size(), Row{});
                jr.isnull.assign(segs.size(), 1);
                jr.cells[first_seg] = *r; jr.isnull[first_seg] = 0;
                acc.push_back(jr);
            }
            cursor_close(c);
        }
        for (const auto &st : steps) join_step(pc, segs, acc, st);
    }

    /* ---- Final WHERE filter over the combined rows ---- */
    if (s->where_clause) {
        std::vector<JoinedRow> kept;
        kept.reserve(acc.size());
        for (auto &jr : acc)
            if (j_eval(s->where_clause->root.get(), segs, jr))
                kept.push_back(std::move(jr));
        acc.swap(kept);
    }

    /* ---- ORDER BY ---- */
    if (!s->order_by.empty()) {
        std::vector<std::pair<int,int>> obcols;
        std::vector<char>               obdesc;
        for (const auto &ob : s->order_by) {
            int sg, ci;
            if (!seg_resolve(segs, ob.table, ob.column, sg, ci)) {
                snprintf(out, cap, "  Error: ORDER BY column '%s' does not exist",
                         ob.column.c_str());
                return MYDB_ERR;
            }
            obcols.push_back({ sg, ci });
            obdesc.push_back(ob.descending ? 1 : 0);
        }
        std::stable_sort(acc.begin(), acc.end(),
            [&](const JoinedRow &a, const JoinedRow &b) -> bool {
                for (size_t k = 0; k < obcols.size(); k++) {
                    int sg = obcols[k].first, ci = obcols[k].second;
                    bool desc = obdesc[k] != 0;
                    bool an = a.isnull[sg] || a.cells[sg].cols[ci].is_null;
                    bool bn = b.isnull[sg] || b.cells[sg].cols[ci].is_null;
                    if (an && bn) continue;
                    if (an) return !desc;   /* NULL sorts first for ASC */
                    if (bn) return  desc;
                    int cmp = compare_values(&a.cells[sg].cols[ci],
                                             &b.cells[sg].cols[ci]);
                    if (cmp != 0) return desc ? cmp > 0 : cmp < 0;
                }
                return false;
            });
    }

    /* ---- LIMIT / OFFSET ---- */
    size_t total = acc.size();
    size_t start = ((size_t)s->offset < total) ? (size_t)s->offset : total;
    size_t end   = (s->limit >= 0)
                   ? std::min(start + (size_t)s->limit, total) : total;

    /* ---- Render ---- */
    ResultBuf rb(out, cap);
    TableBuilder tb;
    std::vector<std::string> headers;
    headers.reserve(proj.size());
    for (auto &p : proj) headers.push_back(p.label);
    tb.set_headers(headers);

    for (size_t i = start; i < end; i++) {
        const JoinedRow &jr = acc[i];
        std::vector<std::string> cells;
        cells.reserve(proj.size());
        for (auto &p : proj) {
            if (jr.isnull[p.seg]) cells.push_back("NULL");
            else cells.push_back(value_to_str(jr.cells[p.seg].cols[p.col],
                                              segs[p.seg].rel->columns[p.col]));
        }
        tb.add_row(cells);
    }
    tb.render(rb);
    return MYDB_OK;
}

/* ======================================================================
 * exec_select — entry point
 * ====================================================================== */

int exec_select(ExecContext *ectx, const SelectStatement *s,
                char *out, size_t cap)
{
    REQUIRE_LOGIN(ectx);
    REQUIRE_SCHEMA(ectx);

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
     * Multi-table query (explicit JOIN or implicit comma join) → dedicated
     * join handler.  Single-table SELECT continues below.
     * ------------------------------------------------------------------ */
    if (!s->join_list.empty() || s->from_list.size() > 1)
        return exec_join_select(ectx, s, out, cap);

    /* Without GROUP BY: mixing aggregate and non-aggregate columns is
     * undefined (which non-aggregate value would appear in the result?).
     * With GROUP BY this mix is valid — non-agg cols must be in GROUP BY. */
    if (has_agg && has_col && s->group_by.empty()) {
        snprintf(out, cap,
                 "  Error: cannot mix aggregate and non-aggregate columns "
                 "without GROUP BY");
        return MYDB_ERR;
    }

    /* ORDER BY is not meaningful for scalar aggregates (one-row result).
     * With GROUP BY, ORDER BY applies to the grouped result — allowed. */
    if (has_agg && !s->order_by.empty() && s->group_by.empty()) {
        snprintf(out, cap,
                 "  Error: ORDER BY is not applicable to scalar aggregates");
        return MYDB_ERR;
    }

    /* ------------------------------------------------------------------
     * Step 1: read access check + find the table.
     * ------------------------------------------------------------------ */
    int rc = engine_check_access(ectx->engine, 0);
    if (rc != MYDB_OK) {
        format_error(rc, out, cap, s->from_list[0].table_name.c_str());
        return rc;
    }

    RelationGuard rel_guard(ectx->partition,
        pm_find_relation_const(ectx->partition, s->from_list[0].table_name.c_str()));
    const RelationDef *rel = rel_guard.get();
    if (!rel) {
        snprintf(out, cap, "  Error: table '%s' does not exist",
                 s->from_list[0].table_name.c_str());
        return MYDB_ERR_NOT_FOUND;
    }
    RelationDef *rel_rw = (RelationDef *)rel;   /* storage API takes non-const */
    /* rel_guard lives in this frame and outlives the exec_group_by(...) tail
     * call below — its destructor fires only once exec_select itself returns,
     * after exec_group_by has already finished. */

    /* ------------------------------------------------------------------
     * Step 2: validate projection columns.
     * ------------------------------------------------------------------ */
    if (!s->is_select_all) {
        for (const auto &item : s->select_list) {
            if (item.kind == SelectItem::Kind::Column) {
                if (resolve_col(rel, item.column) < 0) {
                    snprintf(out, cap,
                             "  Error: column '%s' does not exist in table '%s'",
                             item.column.c_str(), s->from_list[0].table_name.c_str());
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
                     bad, s->from_list[0].table_name.c_str());
            return MYDB_ERR;
        }
    }

    /* ------------------------------------------------------------------
     * Step 3b: strict LIKE type check — LIKE is only valid on VARCHAR.
     * Check before any scan so the user gets a clear error, not zero rows.
     * ------------------------------------------------------------------ */
    {
        const char *like_err = where_validate_likes(s->where_clause.get(), rel);
        if (like_err) {
            snprintf(out, cap, "  Error: %s", like_err);
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
                         item.column.c_str(), s->from_list[0].table_name.c_str());
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
                         item.column.c_str(), s->from_list[0].table_name.c_str());
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
            snprintf(out, cap, "  Error: SELECT * is not allowed with GROUP BY");
            return MYDB_ERR;
        }

        /* All GROUP BY columns must exist in the relation. */
        for (const auto &col_name : s->group_by) {
            if (resolve_col(rel, col_name) < 0) {
                snprintf(out, cap,
                         "ERROR: column '%s' does not exist in table '%s'",
                         col_name.c_str(), s->from_list[0].table_name.c_str());
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
                         "  Error: column '%s' must appear in GROUP BY or be "
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
                         bad, s->from_list[0].table_name.c_str());
                return MYDB_ERR;
            }
        }

        return exec_group_by(ectx, s, rel, rel_rw, out, cap);
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

    /* ------------------------------------------------------------------
     * Step 5d: choose access path via the cost-based planner.
     *
     * extract_sargs  — decode the WHERE tree into sargable predicates.
     * planner_choose_path — short-circuit rules first, then CBO with
     *                       __stats.mydb statistics when available.
     * plan_to_ap     — translate the chosen PlanNode back into the
     *                  AccessPath struct that the scan loops consume.
     * ------------------------------------------------------------------ */
    Sarg       sargs[32];
    int        n_sargs = extract_sargs(s->where_clause.get(), rel, sargs, 32);
    PlanNode   plan    = planner_choose_path(pctx_active_schema(ectx->partition), ectx->stats, rel, sargs, n_sargs);
    AccessPath ap      = plan_to_ap(plan, s->where_clause.get(), rel);

    /* ==================================================================
     * AGGREGATE PATH — all SELECT items are aggregate functions.
     *
     * Scan all rows that pass WHERE, accumulate into AggState array,
     * then emit exactly one result row via TableBuilder.
     * ================================================================== */
    if (has_agg) {
        int       n = (int)s->select_list.size();
        AggState  states[MAX_COLUMNS];
        memset(states, 0, sizeof(states));

        for (int i = 0; i < n; i++) {
            const SelectItem &item = s->select_list[(size_t)i];
            states[i].col_idx = (item.column == "*")
                                 ? -1
                                 : resolve_col(rel, item.column);
        }

        switch (ap.kind) {
        case AP_GET_PK: {
            Row *row = pm_get_by_pk(ectx->partition, rel_rw, &ap.key);
            if (row && where_matches(s->where_clause.get(), rel, row))
                agg_accumulate(states, n, s, rel, row);
            break;
        }
        case AP_GET_INDEX: {
            Row *row = pm_get_by_index(ectx->partition, rel_rw, ap.col_idx, &ap.key);
            if (row && where_matches(s->where_clause.get(), rel, row))
                agg_accumulate(states, n, s, rel, row);
            break;
        }
        case AP_SCAN_FROM: {
            Cursor *cur = pm_scan_from(ectx->partition, rel_rw, &ap.key);
            if (cur) {
                Row *row;
                while ((row = cursor_next(cur)) != NULL)
                    if (where_matches(s->where_clause.get(), rel, row))
                        agg_accumulate(states, n, s, rel, row);
                cursor_close(cur);
            }
            break;
        }
        case AP_SCAN_BY_INDEX: {
            Cursor *cur = pm_scan_by_index(ectx->partition, rel_rw, ap.col_idx, &ap.key);
            if (cur) {
                Row *row;
                while ((row = cursor_next(cur)) != NULL)
                    if (where_matches(s->where_clause.get(), rel, row))
                        agg_accumulate(states, n, s, rel, row);
                cursor_close(cur);
            }
            break;
        }
        default: {
            Cursor *cur = pm_scan(ectx->partition, rel_rw);
            if (cur) {
                Row *row;
                while ((row = cursor_next(cur)) != NULL)
                    if (where_matches(s->where_clause.get(), rel, row))
                        agg_accumulate(states, n, s, rel, row);
                cursor_close(cur);
            }
            break;
        }
        }

        ResultBuf rb(out, cap);
        TableBuilder tb;
        tb.set_headers(make_select_header(rel, s));
        tb.add_row(make_agg_row(rel, s, states, n));
        tb.render(rb);
        return MYDB_OK;
    }

    /* ==================================================================
     * COLLECT PATH (non-aggregate) — gather all matching rows, apply
     * OFFSET/LIMIT, optionally sort (ORDER BY), then render with
     * TableBuilder.  Design 3 requires knowing all cell widths before
     * emitting, so we always collect first regardless of ORDER BY.
     * ================================================================== */

    std::vector<Row> all_rows;

    switch (ap.kind) {
    case AP_GET_PK: {
        Row *row = pm_get_by_pk(ectx->partition, rel_rw, &ap.key);
        if (row && where_matches(s->where_clause.get(), rel, row))
            all_rows.push_back(*row);
        break;
    }
    case AP_GET_INDEX: {
        Row *row = pm_get_by_index(ectx->partition, rel_rw, ap.col_idx, &ap.key);
        if (row && where_matches(s->where_clause.get(), rel, row))
            all_rows.push_back(*row);
        break;
    }
    case AP_SCAN_FROM: {
        Cursor *cur = pm_scan_from(ectx->partition, rel_rw, &ap.key);
        if (cur) {
            Row *row;
            while ((row = cursor_next(cur)) != NULL)
                if (where_matches(s->where_clause.get(), rel, row))
                    all_rows.push_back(*row);
            cursor_close(cur);
        }
        break;
    }
    case AP_SCAN_BY_INDEX: {
        Cursor *cur = pm_scan_by_index(ectx->partition, rel_rw, ap.col_idx, &ap.key);
        if (cur) {
            Row *row;
            while ((row = cursor_next(cur)) != NULL)
                if (where_matches(s->where_clause.get(), rel, row))
                    all_rows.push_back(*row);
            cursor_close(cur);
        }
        break;
    }
    default: {
        Cursor *cur = pm_scan(ectx->partition, rel_rw);
        if (cur) {
            Row *row;
            while ((row = cursor_next(cur)) != NULL)
                if (where_matches(s->where_clause.get(), rel, row))
                    all_rows.push_back(*row);
            cursor_close(cur);
        }
        break;
    }
    }

    /* Sort if ORDER BY requested. */
    size_t total = all_rows.size();

    if (has_order) {
        RowLess cmp { rel, &s->order_by };
        size_t sort_end = (s->limit >= 0)
                          ? std::min((size_t)s->offset + (size_t)s->limit, total)
                          : total;
        if (sort_end > 0 && sort_end < total)
            std::partial_sort(all_rows.begin(),
                              all_rows.begin() + (ptrdiff_t)sort_end,
                              all_rows.end(), cmp);
        else if (total > 1)
            std::sort(all_rows.begin(), all_rows.end(), cmp);
    }

    /* Slice [start, end). */
    size_t start = ((size_t)s->offset < total) ? (size_t)s->offset : total;
    size_t end   = (s->limit >= 0)
                   ? std::min(start + (size_t)s->limit, total)
                   : total;

    /* Build and render table. */
    ResultBuf rb(out, cap);
    TableBuilder tb;
    tb.set_headers(make_select_header(rel, s));
    for (size_t i = start; i < end; i++)
        tb.add_row(make_select_row(rel, &all_rows[i], s));
    tb.render(rb);
    return MYDB_OK;
}
