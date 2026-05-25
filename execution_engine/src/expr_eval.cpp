/*
 * expr_eval.cpp — evaluate AST expression trees against storage rows.
 *
 * Phase 2 implements this file.
 */

#include "expr_eval.hpp"

int resolve_col(const RelationDef * /*rel*/, const std::string & /*name*/)
{
    return -1;   /* stub */
}

bool eval_expr(const Expr * /*e*/, const RelationDef * /*rel*/,
               const Row * /*r*/)
{
    return true;  /* stub — passes every row */
}

bool where_matches(const WhereClause *w, const RelationDef *rel, const Row *r)
{
    if (!w) return true;
    return eval_expr(w->root.get(), rel, r);
}
