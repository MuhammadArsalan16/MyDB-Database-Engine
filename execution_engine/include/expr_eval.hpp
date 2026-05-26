#pragma once
/*
 * expr_eval.hpp — evaluate AST expression trees against storage rows.
 *
 * The parser represents WHERE conditions as a tree of Expr nodes
 * (BinaryExpr for AND/OR/comparisons, UnaryExpr for NOT, IsNullExpr,
 * BetweenExpr, InExpr, LikeExpr).  This module walks that tree and
 * tests whether a given Row satisfies it.
 *
 * Implemented in src/expr_eval.cpp (Phase 2).
 */

extern "C" {
#include "common.h"
#include "relation_def.h"
#include "storage.h"
}
#include "AST.hpp"

/*
 * Find the index of a column in rel->columns[] by name.
 * Strips a leading "table." prefix if present (e.g. "users.id" → "id").
 * Returns -1 if no column with that name exists.
 */
int resolve_col(const RelationDef *rel, const std::string &name);

/*
 * Recursively evaluate an Expr tree against a single row.
 * Returns true  — row satisfies the expression.
 * Returns false — row does not satisfy it, or an operand is NULL
 *                 (NULL propagates to false for all comparisons except
 *                  IS NULL / IS NOT NULL).
 */
bool eval_expr(const Expr *e, const RelationDef *rel, const Row *r);

/*
 * Evaluate a full WhereClause.
 * Returns true (pass) if w is nullptr (no WHERE clause → all rows match).
 * Otherwise delegates to eval_expr on the root expression.
 */
bool where_matches(const WhereClause *w, const RelationDef *rel, const Row *r);

/*
 * Type-aware comparison of two Values.
 * Returns: negative → a < b,  zero → a == b,  positive → a > b.
 *
 * NULL handling: if either value is NULL, returns 1 (treated as not-equal,
 * NULL sorts last — callers that need IS NULL semantics check is_null first).
 *
 * Cross-type coercion: INT ↔ DECIMAL (scale-2 assumed for best-guess).
 * All other cross-type comparisons return 0.
 *
 * Used by ORDER BY sort comparator in dql.cpp.
 */
int compare_values(const Value *a, const Value *b);
