#include <string.h>
#include "expr.h"

/* Compare values */
int value_eq(Value *a, Value *b) {

    if (a->type != b->type) return 0;

    if (a->type == TYPE_INT)
        return a->v.int_val == b->v.int_val;

    if (a->type == TYPE_VARCHAR)
        return strncmp(a->v.varchar_val.data,
                       b->v.varchar_val.data,
                       a->v.varchar_val.len) == 0;

    return 0;
}

/* Evaluate expression */
int eval_expr(Expr *expr, Row *row) {

    if (!expr) return 1;

    switch (expr->type) {

        case EXPR_CONST:
            return 1;

        case EXPR_COL:
            return 1;

        case EXPR_EQ: {
            Value *left = &row->cols[expr->left->col_idx];
            Value *right = &expr->right->value;

            return value_eq(left, right);
        }

        default:
            return 0;
    }
}