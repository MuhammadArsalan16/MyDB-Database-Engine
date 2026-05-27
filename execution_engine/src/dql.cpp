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
#include "planner.h"    /* planner_choose_path, Sarg, PlanNode — extern "C" guards inside */

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
 * Sarg extraction — Step 6
 *
 * Walk the WHERE expression tree and collect sargable predicates into a
 * flat Sarg[] array that the planner can consume.
 *
 * Rules:
 *   AND nodes  — recurse into both children (conjunctive predicates are
 *                individually safe to use as access predicates).
 *   OR nodes   — skipped (disjunctive predicates require union costing;
 *                the WHERE filter will handle them at scan time).
 *   BinaryExpr — sargable when one side is a ColumnRef and the other is
 *                a Literal, and the operator is =, !=, <, <=, >, >=.
 *   BetweenExpr — sargable (not-negated) when v is ColumnRef, lo/hi Literal.
 *   IsNullExpr  — always sargable on the child ColumnRef.
 * ====================================================================== */

/* Encode a literal string as int64 using the same scheme as ColumnStats:
 *   INT      → (int64_t)int_val
 *   DECIMAL  → decimal_val (scaled int)
 *   DATE     → (int64_t)date_val
 *   DATETIME → datetime_val
 *   BOOL     → (int64_t)bool_val
 *   ENUM     → (int64_t)enum_val
 *   VARCHAR  → 0  (not range-comparable; planner uses sarg op, not value)
 */
static int64_t literal_to_i64(const LiteralExpr *lit, const ColumnDef &cd)
{
    Value v = cast_literal(lit->raw, cd);
    if (v.is_null) return 0;
    switch (cd.type) {
    case TYPE_INT:      return (int64_t)v.v.int_val;
    case TYPE_DECIMAL:  return v.v.decimal_val;
    case TYPE_DATE:     return (int64_t)v.v.date_val;
    case TYPE_DATETIME: return v.v.datetime_val;
    case TYPE_BOOL:     return (int64_t)v.v.bool_val;
    case TYPE_ENUM:     return (int64_t)v.v.enum_val;
    default:            return 0;
    }
}

static void extract_sargs_from_expr(const Expr *e,
                                     const RelationDef *rel,
                                     Sarg *sargs, int *n, int cap)
{
    if (!e || *n >= cap) return;

    switch (e->kind) {

    case Expr::Kind::Binary: {
        const BinaryExpr *b  = static_cast<const BinaryExpr *>(e);
        const char       *op = b->op.c_str();

        /* AND: both branches can contribute sargs independently */
        if (strcmp(op, "AND") == 0) {
            extract_sargs_from_expr(b->lhs.get(), rel, sargs, n, cap);
            extract_sargs_from_expr(b->rhs.get(), rel, sargs, n, cap);
            return;
        }

        /* Accept only comparison operators */
        if (strcmp(op,"=")  != 0 && strcmp(op,"!=") != 0 &&
            strcmp(op,"<")  != 0 && strcmp(op,"<=") != 0 &&
            strcmp(op,">")  != 0 && strcmp(op,">=") != 0) return;

        /* Normalise to  col op lit  (flip asymmetric ops if literal is lhs) */
        const Expr *col_e = nullptr, *lit_e = nullptr;
        const char *eff_op = op;

        if (b->lhs && b->lhs->kind == Expr::Kind::ColumnRef &&
            b->rhs && b->rhs->kind == Expr::Kind::Literal) {
            col_e = b->lhs.get();  lit_e = b->rhs.get();
        } else if (b->rhs && b->rhs->kind == Expr::Kind::ColumnRef &&
                   b->lhs && b->lhs->kind == Expr::Kind::Literal) {
            col_e = b->rhs.get();  lit_e = b->lhs.get();
            if      (strcmp(op,"<")  == 0) eff_op = ">";
            else if (strcmp(op,"<=") == 0) eff_op = ">=";
            else if (strcmp(op,">")  == 0) eff_op = "<";
            else if (strcmp(op,">=") == 0) eff_op = "<=";
        }
        if (!col_e || !lit_e) return;

        const ColumnRefExpr *cr  = static_cast<const ColumnRefExpr *>(col_e);
        const LiteralExpr   *lit = static_cast<const LiteralExpr   *>(lit_e);
        int ci = resolve_col(rel, cr->column);
        if (ci < 0) return;

        Sarg &s = sargs[(*n)++];
        s.col_idx = ci;
        strncpy(s.op, eff_op, sizeof(s.op) - 1);
        s.op[sizeof(s.op) - 1] = '\0';
        s.lo = literal_to_i64(lit, rel->columns[ci]);
        s.hi = s.lo;
        break;
    }

    case Expr::Kind::Between: {
        const BetweenExpr *bw = static_cast<const BetweenExpr *>(e);
        if (bw->negated) return;   /* NOT BETWEEN: leave for WHERE filter */
        if (!bw->v  || bw->v->kind  != Expr::Kind::ColumnRef) return;
        if (!bw->lo || bw->lo->kind != Expr::Kind::Literal)   return;
        if (!bw->hi || bw->hi->kind != Expr::Kind::Literal)   return;

        const ColumnRefExpr *cr = static_cast<const ColumnRefExpr *>(bw->v.get());
        int ci = resolve_col(rel, cr->column);
        if (ci < 0) return;

        const ColumnDef   &cd  = rel->columns[ci];
        const LiteralExpr *lo  = static_cast<const LiteralExpr *>(bw->lo.get());
        const LiteralExpr *hi  = static_cast<const LiteralExpr *>(bw->hi.get());

        Sarg &s = sargs[(*n)++];
        s.col_idx = ci;
        strncpy(s.op, "BETWEEN", sizeof(s.op) - 1);
        s.op[sizeof(s.op) - 1] = '\0';
        s.lo = literal_to_i64(lo, cd);
        s.hi = literal_to_i64(hi, cd);
        break;
    }

    case Expr::Kind::IsNull: {
        const IsNullExpr *isn = static_cast<const IsNullExpr *>(e);
        if (!isn->child || isn->child->kind != Expr::Kind::ColumnRef) return;
        const ColumnRefExpr *cr = static_cast<const ColumnRefExpr *>(isn->child.get());
        int ci = resolve_col(rel, cr->column);
        if (ci < 0) return;

        Sarg &s = sargs[(*n)++];
        s.col_idx = ci;
        strncpy(s.op, isn->negated ? "IS_NOT_NULL" : "IS_NULL", sizeof(s.op) - 1);
        s.op[sizeof(s.op) - 1] = '\0';
        s.lo = s.hi = 0;
        break;
    }

    default:
        break;
    }
}

/* Collect all sargable predicates from a WhereClause into sargs[0..cap-1].
 * Returns the number of sargs written. */
static int extract_sargs(const WhereClause *w, const RelationDef *rel,
                          Sarg *sargs, int cap)
{
    int n = 0;
    if (w && w->root)
        extract_sargs_from_expr(w->root.get(), rel, sargs, &n, cap);
    return n;
}

/* ======================================================================
 * Key extraction helpers — used by plan_to_ap to reconstruct storage
 * Values from the WHERE tree after the planner has chosen a path type.
 * ====================================================================== */

/* Scan an AND-tree for the first (col_idx = literal) predicate.
 * Returns true and writes *out on success. */
static bool find_eq_value(const Expr *e, const RelationDef *rel,
                           int col_idx, Value *out)
{
    if (!e) return false;

    if (e->kind == Expr::Kind::Binary) {
        const BinaryExpr *b = static_cast<const BinaryExpr *>(e);
        if (strcmp(b->op.c_str(), "AND") == 0)
            return find_eq_value(b->lhs.get(), rel, col_idx, out) ||
                   find_eq_value(b->rhs.get(), rel, col_idx, out);

        if (strcmp(b->op.c_str(), "=") != 0) return false;

        const Expr *col_e = nullptr, *lit_e = nullptr;
        if (b->lhs && b->lhs->kind == Expr::Kind::ColumnRef &&
            b->rhs && b->rhs->kind == Expr::Kind::Literal) {
            col_e = b->lhs.get();  lit_e = b->rhs.get();
        } else if (b->rhs && b->rhs->kind == Expr::Kind::ColumnRef &&
                   b->lhs && b->lhs->kind == Expr::Kind::Literal) {
            col_e = b->rhs.get();  lit_e = b->lhs.get();
        }
        if (!col_e || !lit_e) return false;

        const ColumnRefExpr *cr  = static_cast<const ColumnRefExpr *>(col_e);
        const LiteralExpr   *lit = static_cast<const LiteralExpr   *>(lit_e);
        if (resolve_col(rel, cr->column) != col_idx) return false;

        *out = cast_literal(lit->raw, rel->columns[col_idx]);
        return true;
    }
    return false;
}

/* Scan an AND-tree for the tightest lower bound on col_idx.
 * Handles >=, >, and BETWEEN.lo.  When multiple lower bounds exist on
 * the same column, the highest one wins (tightest range → fewest rows).
 * Returns true if at least one lower-bound predicate was found. */
static bool find_range_lo(const Expr *e, const RelationDef *rel,
                           int col_idx, Value *out)
{
    if (!e) return false;

    if (e->kind == Expr::Kind::Binary) {
        const BinaryExpr *b  = static_cast<const BinaryExpr *>(e);
        const char       *op = b->op.c_str();

        if (strcmp(op, "AND") == 0) {
            Value lo1, lo2;
            memset(&lo1, 0, sizeof(lo1));
            memset(&lo2, 0, sizeof(lo2));
            bool g1 = find_range_lo(b->lhs.get(), rel, col_idx, &lo1);
            bool g2 = find_range_lo(b->rhs.get(), rel, col_idx, &lo2);
            if (g1 && g2) {
                /* take the higher of the two lower bounds */
                *out = (compare_values(&lo1, &lo2) >= 0) ? lo1 : lo2;
                return true;
            }
            if (g1) { *out = lo1; return true; }
            if (g2) { *out = lo2; return true; }
            return false;
        }

        /* col >= lit  or  col > lit */
        if ((strcmp(op, ">=") == 0 || strcmp(op, ">") == 0) &&
            b->lhs && b->lhs->kind == Expr::Kind::ColumnRef &&
            b->rhs && b->rhs->kind == Expr::Kind::Literal) {
            const ColumnRefExpr *cr  = static_cast<const ColumnRefExpr *>(b->lhs.get());
            const LiteralExpr   *lit = static_cast<const LiteralExpr   *>(b->rhs.get());
            if (resolve_col(rel, cr->column) != col_idx) return false;
            *out = cast_literal(lit->raw, rel->columns[col_idx]);
            return true;
        }

        /* lit <= col  or  lit < col  (literal on the left) */
        if ((strcmp(op, "<=") == 0 || strcmp(op, "<") == 0) &&
            b->rhs && b->rhs->kind == Expr::Kind::ColumnRef &&
            b->lhs && b->lhs->kind == Expr::Kind::Literal) {
            const ColumnRefExpr *cr  = static_cast<const ColumnRefExpr *>(b->rhs.get());
            const LiteralExpr   *lit = static_cast<const LiteralExpr   *>(b->lhs.get());
            if (resolve_col(rel, cr->column) != col_idx) return false;
            *out = cast_literal(lit->raw, rel->columns[col_idx]);
            return true;
        }

        return false;
    }

    /* BETWEEN v AND lo AND hi → lo is the lower bound */
    if (e->kind == Expr::Kind::Between) {
        const BetweenExpr *bw = static_cast<const BetweenExpr *>(e);
        if (!bw->v || bw->v->kind != Expr::Kind::ColumnRef) return false;
        const ColumnRefExpr *cr = static_cast<const ColumnRefExpr *>(bw->v.get());
        if (resolve_col(rel, cr->column) != col_idx) return false;
        if (!bw->lo || bw->lo->kind != Expr::Kind::Literal) return false;
        const LiteralExpr *lit = static_cast<const LiteralExpr *>(bw->lo.get());
        *out = cast_literal(lit->raw, rel->columns[col_idx]);
        return true;
    }

    return false;
}

/* ======================================================================
 * plan_to_ap — Step 7 bridge
 *
 * Translate the planner's PlanNode (path type + cost metadata) into the
 * concrete AccessPath struct the scan loops consume.
 *
 * Point lookups (PK_LOOKUP, INDEX_LOOKUP): key extracted by re-scanning
 * the WHERE tree via find_eq_value — works for any column type including
 * VARCHAR since cast_literal is called with the raw literal string.
 *
 * Range scan (PK_RANGE): lower bound extracted via find_range_lo; if
 * only an upper-bound predicate exists, AP_SCAN is returned so the WHERE
 * filter applies the bound at evaluation time.
 *
 * INDEX_RANGE → AP_SCAN: Phase 1 storage API has no secondary-index range
 * cursor; the full scan + WHERE filter is the correct fallback.
 * ====================================================================== */
static AccessPath plan_to_ap(const PlanNode    &plan,
                              const WhereClause *where,
                              const RelationDef *rel)
{
    AccessPath ap;
    memset(&ap, 0, sizeof(ap));
    ap.kind = AP_SCAN;   /* default: full scan */

    const Expr *root = (where) ? where->root.get() : nullptr;

    switch (plan.path) {

    case ACCESS_FULL_SCAN:
        /* already AP_SCAN */
        break;

    case ACCESS_PK_LOOKUP: {
        Value key;
        memset(&key, 0, sizeof(key));
        if (root && find_eq_value(root, rel, (int)rel->pk_col_idx, &key)) {
            ap.kind = AP_GET_PK;
            ap.key  = key;
        }
        break;
    }

    case ACCESS_PK_RANGE: {
        /* Use scan_from(lo) when a lower bound exists.  An upper-bound-only
         * predicate (e.g. WHERE pk < 100) falls back to AP_SCAN and the
         * WHERE filter stops the scan when the bound is exceeded. */
        Value lo_key;
        memset(&lo_key, 0, sizeof(lo_key));
        if (root && find_range_lo(root, rel, (int)rel->pk_col_idx, &lo_key)) {
            ap.kind = AP_SCAN_FROM;
            ap.key  = lo_key;
        }
        break;
    }

    case ACCESS_INDEX_LOOKUP: {
        Value key;
        memset(&key, 0, sizeof(key));
        if (root && find_eq_value(root, rel, plan.index_col_idx, &key)) {
            ap.kind    = AP_GET_INDEX;
            ap.col_idx = plan.index_col_idx;
            ap.key     = key;
        }
        break;
    }

    case ACCESS_INDEX_RANGE:
        /* Phase 1: no secondary index range scan API.
         * WHERE filter at full-scan time handles the predicate. */
        ap.kind = AP_SCAN;
        break;
    }

    return ap;
}

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
        snprintf(out, cap, "  Error: JOIN is not yet implemented");
        return MYDB_ERR;
    }

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
    int rc = engine_check_access(eng, 0);
    if (rc != MYDB_OK) {
        format_error(rc, out, cap, s->table_name.c_str());
        return rc;
    }

    const RelationDef *rel = engine_find_relation(eng, s->table_name.c_str());
    if (!rel) {
        snprintf(out, cap, "  Error: table '%s' does not exist",
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
                             "  Error: column '%s' does not exist in table '%s'",
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
            snprintf(out, cap, "  Error: SELECT * is not allowed with GROUP BY");
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
    PlanNode   plan    = planner_choose_path(eng, rel, sargs, n_sargs);
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
                while ((row = cursor_next(cur)) != NULL)
                    if (where_matches(s->where_clause.get(), rel, row))
                        agg_accumulate(states, n, s, rel, row);
                cursor_close(cur);
            }
            break;
        }
        default: {
            Cursor *cur = storage_scan(rel_rw);
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
        Row *row = storage_get_by_pk(rel_rw, &ap.key);
        if (row && where_matches(s->where_clause.get(), rel, row))
            all_rows.push_back(*row);
        break;
    }
    case AP_GET_INDEX: {
        Row *row = storage_get_by_index(rel_rw, ap.col_idx, &ap.key);
        if (row && where_matches(s->where_clause.get(), rel, row))
            all_rows.push_back(*row);
        break;
    }
    case AP_SCAN_FROM: {
        Cursor *cur = storage_scan_from(rel_rw, &ap.key);
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
        Cursor *cur = storage_scan(rel_rw);
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
