#pragma once
/*
 * exec_access_path.hpp — shared access path infrastructure.
 *
 * Used by dql.cpp (SELECT) and dml.cpp (UPDATE, DELETE).
 * Translates a WHERE clause into the cheapest storage API call
 * using the cost-based planner.
 *
 * All functions are static inline so this header can be included
 * in multiple translation units without linker conflicts.
 */

#include "AST.hpp"
#include "expr_eval.hpp"
#include "value_cast.hpp"
#include "planner.h"   /* Sarg, PlanNode, AccessPathType, planner_choose_path */

#include <cstring>

/* ------------------------------------------------------------------ */
/*  AccessPath — the concrete storage call chosen by plan_to_ap        */
/* ------------------------------------------------------------------ */
#define AP_SCAN           0   /* storage_scan           — full table scan      */
#define AP_SCAN_FROM      1   /* storage_scan_from      — PK range scan        */
#define AP_GET_PK         2   /* storage_get_by_pk      — PK point lookup      */
#define AP_GET_INDEX      3   /* storage_get_by_index   — index point lookup   */
#define AP_SCAN_BY_INDEX  4   /* storage_scan_by_index  — index range scan     */

struct AccessPath {
    int   kind;       /* AP_* constant above                               */
    Value key;        /* typed key for point lookups and range lower bounds */
    int   col_idx;    /* secondary column index (AP_GET_INDEX / AP_SCAN_BY_INDEX) */
};

/* ------------------------------------------------------------------ */
/*  validate_expr_cols                                                  */
/*                                                                      */
/*  Walk an Expr tree and verify every ColumnRef names a real column   */
/*  in rel. Returns the offending column name on error, NULL on success.*/
/* ------------------------------------------------------------------ */
static inline const char *validate_expr_cols(const Expr        *e,
                                              const RelationDef *rel)
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

/* ------------------------------------------------------------------ */
/*  literal_to_i64                                                      */
/*                                                                      */
/*  Encode a literal string as int64 using the same scheme as          */
/*  ColumnStats min_numeric / max_numeric so the planner can compare   */
/*  values from the WHERE clause against stored statistics.            */
/* ------------------------------------------------------------------ */
static inline int64_t literal_to_i64(const LiteralExpr *lit,
                                      const ColumnDef   &cd)
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
    default:            return 0;   /* VARCHAR — not range-comparable */
    }
}

/* ------------------------------------------------------------------ */
/*  extract_sargs                                                       */
/*                                                                      */
/*  Walk the WHERE expression tree and collect sargable predicates      */
/*  into a flat Sarg[] array the planner can consume.                  */
/*                                                                      */
/*  Rules:                                                              */
/*    AND  — recurse into both branches.                                */
/*    OR   — skipped (disjunctions cannot drive index access).          */
/*    NOT BETWEEN — skipped (leave for WHERE filter).                   */
/*    col OP lit  — sargable when op is =, !=, <, <=, >, >=.           */
/*    BETWEEN     — sargable when v is ColumnRef, lo/hi are Literals.  */
/*    IS NULL / IS NOT NULL — always sargable.                          */
/* ------------------------------------------------------------------ */
static inline void extract_sargs_from_expr(const Expr        *e,
                                            const RelationDef *rel,
                                            Sarg *sargs, int *n, int cap)
{
    if (!e || *n >= cap) return;

    switch (e->kind) {

    case Expr::Kind::Binary: {
        const BinaryExpr *b  = static_cast<const BinaryExpr *>(e);
        const char       *op = b->op.c_str();

        /* AND: both branches contribute sargs independently */
        if (strcmp(op, "AND") == 0) {
            extract_sargs_from_expr(b->lhs.get(), rel, sargs, n, cap);
            extract_sargs_from_expr(b->rhs.get(), rel, sargs, n, cap);
            return;
        }

        /* only comparison operators are sargable */
        if (strcmp(op,"=")  != 0 && strcmp(op,"!=") != 0 &&
            strcmp(op,"<")  != 0 && strcmp(op,"<=") != 0 &&
            strcmp(op,">")  != 0 && strcmp(op,">=") != 0) return;

        /* normalise to  col op lit  (flip if literal is on the left) */
        const Expr *col_e  = nullptr, *lit_e = nullptr;
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

        const ColumnDef   &cd = rel->columns[ci];
        const LiteralExpr *lo = static_cast<const LiteralExpr *>(bw->lo.get());
        const LiteralExpr *hi = static_cast<const LiteralExpr *>(bw->hi.get());

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

/* Entry point: collect all sargable predicates from a WhereClause. */
static inline int extract_sargs(const WhereClause *w,
                                  const RelationDef *rel,
                                  Sarg *sargs, int cap)
{
    int n = 0;
    if (w && w->root)
        extract_sargs_from_expr(w->root.get(), rel, sargs, &n, cap);
    return n;
}

/* ------------------------------------------------------------------ */
/*  find_eq_value                                                       */
/*                                                                      */
/*  Scan an AND-tree for the first (col_idx = literal) predicate.      */
/*  Returns true and writes *out on success.                           */
/*  Used by plan_to_ap to extract the typed key for point lookups.     */
/* ------------------------------------------------------------------ */
static inline bool find_eq_value(const Expr        *e,
                                   const RelationDef *rel,
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

/* ------------------------------------------------------------------ */
/*  find_range_lo                                                       */
/*                                                                      */
/*  Scan an AND-tree for the tightest lower bound on col_idx.          */
/*  Handles >=, >, and BETWEEN.lo.                                     */
/*  When multiple lower bounds exist, the highest one wins.            */
/* ------------------------------------------------------------------ */
static inline bool find_range_lo(const Expr        *e,
                                   const RelationDef *rel,
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
                /* take the tighter (higher) lower bound */
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

        /* lit <= col  or  lit < col  (literal on the left side) */
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

/* ------------------------------------------------------------------ */
/*  plan_to_ap                                                          */
/*                                                                      */
/*  Translate the planner's PlanNode into a concrete AccessPath.       */
/*  Re-walks the WHERE tree to extract the typed Value key that the    */
/*  storage API needs (the planner only stores int64-encoded values).  */
/*                                                                      */
/*  ACCESS_INDEX_RANGE with a lower bound → AP_SCAN_BY_INDEX.         */
/*  Upper-bound-only predicates fall back to AP_SCAN so the WHERE      */
/*  filter applies the bound at scan time.                             */
/* ------------------------------------------------------------------ */
static inline AccessPath plan_to_ap(const PlanNode    &plan,
                                     const WhereClause *where,
                                     const RelationDef *rel)
{
    AccessPath ap;
    memset(&ap, 0, sizeof(ap));
    ap.kind = AP_SCAN;   /* safe default */

    const Expr *root = where ? where->root.get() : nullptr;

    switch (plan.path) {

    case ACCESS_FULL_SCAN:
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

    case ACCESS_INDEX_RANGE: {
        Value lo_key;
        memset(&lo_key, 0, sizeof(lo_key));
        if (root && find_range_lo(root, rel, plan.index_col_idx, &lo_key)) {
            ap.kind    = AP_SCAN_BY_INDEX;
            ap.col_idx = plan.index_col_idx;
            ap.key     = lo_key;
        }
        break;
    }
    }

    return ap;
}
