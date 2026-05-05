#ifndef NODE_H
#define NODE_H

#include "storage.h"
#include "expr.h"

typedef enum {
    NODE_SEQ_SCAN,
    NODE_INDEX_SCAN,
    NODE_FILTER,
    NODE_INSERT,
    NODE_DELETE,
    NODE_UPDATE
} NodeType;

typedef struct PlanNode {
    NodeType type;

    char table[64];

    struct PlanNode *left;
    struct PlanNode *right;

    int col_idx;
    Value value;

    Row insert_row;
    int set_col_idx;
    Value set_value;

    Expr *filter_expr;

} PlanNode;

/* helper functions */
PlanNode* create_plan_node(NodeType type);
void free_plan(PlanNode *node);

#endif