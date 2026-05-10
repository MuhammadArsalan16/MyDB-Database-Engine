#include <stdlib.h>
#include <string.h>
#include "node.h"

/* Create node */
PlanNode* create_plan_node(NodeType type) {
    PlanNode *node = (PlanNode*)malloc(sizeof(PlanNode));
    memset(node, 0, sizeof(PlanNode));
    node->type = type;
    return node;
}

/* Free plan tree */
void free_plan(PlanNode *node) {
    if (!node) return;

    free_plan(node->left);
    free_plan(node->right);

    free(node);
}