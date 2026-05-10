#ifndef EXPR_H
#define EXPR_H

#include "storage.h"

typedef enum {
    EXPR_COL,
    EXPR_CONST,
    EXPR_EQ
} ExprType;

typedef struct Expr {
    ExprType type;

    int col_idx;        // for column reference
    Value value;        // for constants

    struct Expr *left;
    struct Expr *right;

} Expr;

/* Evaluate expression on a row */
int eval_expr(Expr *expr, Row *row);

#endif