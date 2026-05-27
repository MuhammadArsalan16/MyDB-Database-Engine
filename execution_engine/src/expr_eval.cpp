/*
 * expr_eval.cpp — evaluate AST expression trees against storage rows.
 *
 * Public API (declared in expr_eval.hpp):
 *   resolve_col    — column name → index in rel->columns[]
 *   eval_expr      — test one row against an Expr tree
 *   where_matches  — test one row against a full WhereClause
 *
 * Internal helpers (static, not exposed):
 *   col_idx_by_name  — raw strcmp scan of rel->columns[]
 *   literal_to_value — parse a LiteralExpr with best-guess type
 *   eval_value       — get a Value from any Expr (ColumnRef or Literal)
 *   compare_values   — type-aware comparison, returns <0 / 0 / >0
 *   like_match       — SQL LIKE pattern matching (% and _)
 *
 * NULL propagation rule:
 *   Any comparison where either operand is NULL returns false.
 *   IS NULL / IS NOT NULL are the only operators that inspect the null flag.
 */

#include "expr_eval.hpp"
#include "value_cast.hpp"

#include <cstring>
#include <cstdlib>

/* ======================================================================
 * Internal helpers
 * ====================================================================== */

/* Linear scan of rel->columns[] by exact column name. */
static int col_idx_by_name(const RelationDef *rel, const char *name)
{
    for (int i = 0; i < (int)rel->num_columns; i++) {
        if (strcmp(rel->columns[i].name, name) == 0) return i;
    }
    return -1;
}

/*
 * Parse a LiteralExpr into a Value without any type hint.
 * Uses the token type to make a best guess:
 *   NUMBER with '.'  → TYPE_DECIMAL (scale=2)
 *   NUMBER without   → TYPE_INT
 *   STRING           → TYPE_VARCHAR
 *   KEYWORD TRUE/FALSE → TYPE_BOOL
 *   KEYWORD NULL     → is_null=1
 */
static Value literal_to_value(const LiteralExpr *lit)
{
    Value v;
    memset(&v, 0, sizeof(v));

    if (lit->type == TokenType::NUMBER) {
        if (lit->raw.find('.') != std::string::npos) {
            /* decimal — build a fake ColumnDef with scale=2 */
            ColumnDef fake;
            memset(&fake, 0, sizeof(fake));
            fake.type  = TYPE_DECIMAL;
            fake.scale = 2;
            return cast_literal(lit->raw, fake);
        } else {
            v.type      = TYPE_INT;
            v.v.int_val = (int32_t)strtol(lit->raw.c_str(), NULL, 10);
        }

    } else if (lit->type == TokenType::STRING) {
        v.type = TYPE_VARCHAR;
        size_t len = lit->raw.size();
        if (len > MAX_VARCHAR_LEN) len = MAX_VARCHAR_LEN;
        v.v.varchar_val.len = (uint16_t)len;
        memcpy(v.v.varchar_val.data, lit->raw.c_str(), len);
        v.v.varchar_val.data[len] = '\0';

    } else if (lit->type == TokenType::KEYWORD) {
        if (lit->raw == "TRUE"  || lit->raw == "true") {
            v.type = TYPE_BOOL; v.v.bool_val = 1;
        } else if (lit->raw == "FALSE" || lit->raw == "false") {
            v.type = TYPE_BOOL; v.v.bool_val = 0;
        } else {
            v.is_null = 1;   /* NULL or unknown keyword */
        }
    }

    return v;
}

/*
 * Get the Value of any expression given a row.
 *
 * hint — if non-NULL and e is a Literal, cast using the hint column's
 *         type (correct type coercion when the other side is a ColumnRef).
 *         Pass NULL when no hint is available.
 */
static Value eval_value(const Expr *e, const RelationDef *rel,
                        const Row *row, const ColumnDef *hint)
{
    Value v;
    memset(&v, 0, sizeof(v));

    if (!e) { v.is_null = 1; return v; }

    if (e->kind == Expr::Kind::ColumnRef) {
        const ColumnRefExpr *cr = static_cast<const ColumnRefExpr *>(e);
        /* strip table prefix if present (e.g. "users.id" → "id") */
        const char *col_name = cr->column.c_str();
        int idx = col_idx_by_name(rel, col_name);
        if (idx < 0) { v.is_null = 1; return v; }
        return row->cols[idx];
    }

    if (e->kind == Expr::Kind::Literal) {
        const LiteralExpr *lit = static_cast<const LiteralExpr *>(e);
        if (hint) return cast_literal(lit->raw, *hint);
        return literal_to_value(lit);
    }

    v.is_null = 1;
    return v;
}

/*
 * Type-aware comparison of two Values.
 * Returns: negative  → a < b
 *          zero      → a == b
 *          positive  → a > b
 *
 * If either value is NULL, returns 1 (non-equal, non-null wins) —
 * callers must check is_null before calling when IS NULL semantics
 * are needed.
 *
 * Cross-type numeric coercion: INT ↔ DECIMAL.
 * All other cross-type comparisons return 0 (treated as unequal by
 * callers via the != path).
 */
int compare_values(const Value *a, const Value *b)
{
    if (a->is_null || b->is_null) return 1;   /* NULL != anything */

    switch (a->type) {

    case TYPE_INT:
        if (b->type == TYPE_INT) {
            return (a->v.int_val > b->v.int_val) - (a->v.int_val < b->v.int_val);
        }
        if (b->type == TYPE_DECIMAL) {
            /* promote a to same scale as b — assume scale=2 for best-guess */
            int64_t av = (int64_t)a->v.int_val * 100;
            return (av > b->v.decimal_val) - (av < b->v.decimal_val);
        }
        return 1;

    case TYPE_DECIMAL:
        if (b->type == TYPE_DECIMAL) {
            return (a->v.decimal_val > b->v.decimal_val)
                 - (a->v.decimal_val < b->v.decimal_val);
        }
        if (b->type == TYPE_INT) {
            int64_t bv = (int64_t)b->v.int_val * 100;
            return (a->v.decimal_val > bv) - (a->v.decimal_val < bv);
        }
        return 1;

    case TYPE_VARCHAR:
        if (b->type == TYPE_VARCHAR)
            return strcmp(a->v.varchar_val.data, b->v.varchar_val.data);
        return 1;

    case TYPE_BOOL:
        return (int)a->v.bool_val - (int)b->v.bool_val;

    case TYPE_ENUM:
        return (int)a->v.enum_val - (int)b->v.enum_val;

    case TYPE_DATE:
        return (a->v.date_val > b->v.date_val)
             - (a->v.date_val < b->v.date_val);

    case TYPE_DATETIME:
        return (a->v.datetime_val > b->v.datetime_val)
             - (a->v.datetime_val < b->v.datetime_val);
    }

    return 0;
}

/*
 * SQL LIKE pattern matching.
 *   %  — matches any sequence of characters (including empty)
 *   _  — matches exactly one character
 *   all other characters match literally (case-sensitive)
 *
 * Returns 1 (match) or 0 (no match).
 */
static int like_match(const char *str, const char *pat)
{
    while (*pat) {
        if (*pat == '%') {
            pat++;
            /* empty string satisfies %, so check current position first */
            do {
                if (like_match(str, pat)) return 1;
            } while (*str++);
            return 0;
        }
        if (*pat == '_') {
            if (!*str) return 0;   /* need at least one char */
            str++; pat++;
        } else {
            if (*str != *pat) return 0;
            str++; pat++;
        }
    }
    return (*str == '\0');
}

/* ======================================================================
 * Public API
 * ====================================================================== */

int resolve_col(const RelationDef *rel, const std::string &name)
{
    /* strip "table." prefix if present */
    size_t dot = name.find('.');
    const char *col_name = (dot != std::string::npos)
                           ? name.c_str() + dot + 1
                           : name.c_str();
    return col_idx_by_name(rel, col_name);
}

bool eval_expr(const Expr *e, const RelationDef *rel, const Row *r)
{
    if (!e) return true;

    switch (e->kind) {

    /* ------------------------------------------------------------------
     * Binary — AND, OR, comparison operators
     * ------------------------------------------------------------------ */
    case Expr::Kind::Binary: {
        const BinaryExpr *bin = static_cast<const BinaryExpr *>(e);
        const char *op = bin->op.c_str();

        /* Logical — short-circuit */
        if (strcmp(op, "AND") == 0)
            return eval_expr(bin->lhs.get(), rel, r)
                && eval_expr(bin->rhs.get(), rel, r);

        if (strcmp(op, "OR") == 0)
            return eval_expr(bin->lhs.get(), rel, r)
                || eval_expr(bin->rhs.get(), rel, r);

        /*
         * Comparison — derive type hint from whichever side is a ColumnRef
         * so the literal on the other side is cast to the right type.
         */
        const ColumnDef *lhint = NULL;
        const ColumnDef *rhint = NULL;

        if (bin->lhs && bin->lhs->kind == Expr::Kind::ColumnRef) {
            const ColumnRefExpr *cr =
                static_cast<const ColumnRefExpr *>(bin->lhs.get());
            int idx = col_idx_by_name(rel, cr->column.c_str());
            if (idx >= 0) rhint = &rel->columns[idx];
        }
        if (bin->rhs && bin->rhs->kind == Expr::Kind::ColumnRef) {
            const ColumnRefExpr *cr =
                static_cast<const ColumnRefExpr *>(bin->rhs.get());
            int idx = col_idx_by_name(rel, cr->column.c_str());
            if (idx >= 0) lhint = &rel->columns[idx];
        }

        Value lv = eval_value(bin->lhs.get(), rel, r, lhint);
        Value rv = eval_value(bin->rhs.get(), rel, r, rhint);

        /* NULL in a comparison → false */
        if (lv.is_null || rv.is_null) return false;

        int cmp = compare_values(&lv, &rv);

        if (strcmp(op, "=")  == 0) return cmp == 0;
        if (strcmp(op, "!=") == 0 ||
            strcmp(op, "<>") == 0) return cmp != 0;
        if (strcmp(op, "<")  == 0) return cmp <  0;
        if (strcmp(op, ">")  == 0) return cmp >  0;
        if (strcmp(op, "<=") == 0) return cmp <= 0;
        if (strcmp(op, ">=") == 0) return cmp >= 0;

        return false;
    }

    /* ------------------------------------------------------------------
     * Unary — NOT
     * ------------------------------------------------------------------ */
    case Expr::Kind::Unary: {
        const UnaryExpr *un = static_cast<const UnaryExpr *>(e);
        if (strcmp(un->op.c_str(), "NOT") == 0)
            return !eval_expr(un->child.get(), rel, r);
        return false;
    }

    /* ------------------------------------------------------------------
     * IS NULL / IS NOT NULL
     * ------------------------------------------------------------------ */
    case Expr::Kind::IsNull: {
        const IsNullExpr *isn = static_cast<const IsNullExpr *>(e);
        Value v = eval_value(isn->child.get(), rel, r, NULL);
        bool is_null = (v.is_null != 0);
        return isn->negated ? !is_null : is_null;
    }

    /* ------------------------------------------------------------------
     * BETWEEN — lo <= v <= hi
     * ------------------------------------------------------------------ */
    case Expr::Kind::Between: {
        const BetweenExpr *bw = static_cast<const BetweenExpr *>(e);

        /* get type hint from the column being tested */
        const ColumnDef *hint = NULL;
        if (bw->v && bw->v->kind == Expr::Kind::ColumnRef) {
            const ColumnRefExpr *cr =
                static_cast<const ColumnRefExpr *>(bw->v.get());
            int idx = col_idx_by_name(rel, cr->column.c_str());
            if (idx >= 0) hint = &rel->columns[idx];
        }

        Value v  = eval_value(bw->v.get(),  rel, r, NULL);
        Value lo = eval_value(bw->lo.get(), rel, r, hint);
        Value hi = eval_value(bw->hi.get(), rel, r, hint);

        if (v.is_null || lo.is_null || hi.is_null) return false;

        bool result = (compare_values(&v, &lo) >= 0)
                   && (compare_values(&v, &hi) <= 0);
        return bw->negated ? !result : result;
    }

    /* ------------------------------------------------------------------
     * IN — v matches any value in the literal list
     * ------------------------------------------------------------------ */
    case Expr::Kind::In: {
        const InExpr *in = static_cast<const InExpr *>(e);

        const ColumnDef *hint = NULL;
        if (in->v && in->v->kind == Expr::Kind::ColumnRef) {
            const ColumnRefExpr *cr =
                static_cast<const ColumnRefExpr *>(in->v.get());
            int idx = col_idx_by_name(rel, cr->column.c_str());
            if (idx >= 0) hint = &rel->columns[idx];
        }

        Value v = eval_value(in->v.get(), rel, r, NULL);
        if (v.is_null) return false;

        bool found = false;
        for (const auto &lit : in->list) {
            Value lv = hint ? cast_literal(lit->raw, *hint)
                            : literal_to_value(lit.get());
            if (compare_values(&v, &lv) == 0) { found = true; break; }
        }
        return in->negated ? !found : found;
    }

    /* ------------------------------------------------------------------
     * LIKE — SQL pattern match (% = any string, _ = one char)
     * ------------------------------------------------------------------ */
    case Expr::Kind::Like: {
        const LikeExpr *lk = static_cast<const LikeExpr *>(e);
        Value v = eval_value(lk->v.get(), rel, r, NULL);
        if (v.is_null || v.type != TYPE_VARCHAR) return false;

        bool result = like_match(v.v.varchar_val.data, lk->pattern.c_str()) != 0;
        return lk->negated ? !result : result;
    }

    /* ------------------------------------------------------------------
     * ColumnRef / Literal at top level — treat as boolean (non-zero = true)
     * (rare in practice, but handle gracefully)
     * ------------------------------------------------------------------ */
    case Expr::Kind::ColumnRef:
    case Expr::Kind::Literal: {
        Value v = eval_value(e, rel, r, NULL);
        if (v.is_null) return false;
        if (v.type == TYPE_BOOL)    return v.v.bool_val != 0;
        if (v.type == TYPE_INT)     return v.v.int_val  != 0;
        return false;
    }

    default:
        return true;
    }
}

bool where_matches(const WhereClause *w, const RelationDef *rel, const Row *r)
{
    if (!w) return true;
    return eval_expr(w->root.get(), rel, r);
}

/* ======================================================================
 * LIKE type validator  (strict: LIKE is only legal on VARCHAR columns)
 * ====================================================================== */

/*
 * Recursively walk an Expr tree and return the first error found:
 *   - LIKE on a column that is not TYPE_VARCHAR → type-error string
 *   - LIKE on an unknown column name            → "unknown column" string
 * Returns nullptr if the tree contains no LIKE problems.
 *
 * The returned pointer is into a static buffer — valid until the next call.
 * Callers use it immediately to populate the output buffer, so this is safe.
 */
static const char *validate_like_expr(const Expr *e, const RelationDef *rel)
{
    if (!e) return nullptr;

    switch (e->kind) {

    case Expr::Kind::Like: {
        const LikeExpr *lk = static_cast<const LikeExpr *>(e);
        /* The LHS of LIKE must be a column reference */
        if (!lk->v || lk->v->kind != Expr::Kind::ColumnRef)
            return nullptr;  /* literal LIKE — unusual but harmless */
        const ColumnRefExpr *cr = static_cast<const ColumnRefExpr *>(lk->v.get());
        int idx = resolve_col(rel, cr->column);
        if (idx < 0) {
            static char ebuf[256];
            snprintf(ebuf, sizeof(ebuf),
                     "unknown column '%s' in LIKE expression",
                     cr->column.c_str());
            return ebuf;
        }
        if (rel->columns[idx].type != TYPE_VARCHAR) {
            static char ebuf[256];
            snprintf(ebuf, sizeof(ebuf),
                     "LIKE requires a VARCHAR column, but '%s' is %s "
                     "(use = or != for non-text columns)",
                     rel->columns[idx].name,
                     rel->columns[idx].type == TYPE_INT      ? "INT"      :
                     rel->columns[idx].type == TYPE_DECIMAL  ? "DECIMAL"  :
                     rel->columns[idx].type == TYPE_BOOL     ? "BOOL"     :
                     rel->columns[idx].type == TYPE_ENUM     ? "ENUM"     :
                     rel->columns[idx].type == TYPE_DATE     ? "DATE"     :
                     rel->columns[idx].type == TYPE_DATETIME ? "DATETIME" : "unknown");
            return ebuf;
        }
        return nullptr;
    }

    case Expr::Kind::Binary: {
        const BinaryExpr *bin = static_cast<const BinaryExpr *>(e);
        const char *err = validate_like_expr(bin->lhs.get(), rel);
        if (err) return err;
        return validate_like_expr(bin->rhs.get(), rel);
    }

    case Expr::Kind::Unary: {
        const UnaryExpr *un = static_cast<const UnaryExpr *>(e);
        return validate_like_expr(un->child.get(), rel);
    }

    case Expr::Kind::Between: {
        const BetweenExpr *bt = static_cast<const BetweenExpr *>(e);
        return validate_like_expr(bt->v.get(), rel);
    }

    case Expr::Kind::In: {
        const InExpr *in = static_cast<const InExpr *>(e);
        return validate_like_expr(in->v.get(), rel);
    }

    default:
        return nullptr;
    }
}

const char *where_validate_likes(const WhereClause *w, const RelationDef *rel)
{
    if (!w) return nullptr;
    return validate_like_expr(w->root.get(), rel);
}
