#include <stdlib.h>
#include <string.h>
#include "../../storage_engine/include/storage.h"
#include "plan.h"

PlanNode* create_plan_node(NodeType type) {
    PlanNode *n = (PlanNode*)malloc(sizeof(PlanNode));
    memset(n, 0, sizeof(PlanNode));
    n->type = type;
    return n;
}

void free_plan(PlanNode *node) {
    if (!node) return;
    if (node->left) free_plan(node->left);
    if (node->right) free_plan(node->right);
    free(node);
}

/* SELECT * FROM table WHERE col = value */
PlanNode* build_select_plan(char *table, int col_idx, Value val) {

    PlanNode *scan = create_plan_node(NODE_SEQ_SCAN);
    strcpy(scan->table, table);

    PlanNode *filter = create_plan_node(NODE_FILTER);
    filter->left = scan;
    filter->col_idx = col_idx;
    filter->value = val;

    return filter;
}

/* Optimization: if PK → use INDEX SCAN */
PlanNode* optimize_plan(PlanNode *plan) {

    if (plan->type == NODE_FILTER && plan->left->type == NODE_SEQ_SCAN) {

        // simple rule: if filtering on PK (col_idx == 0)
        if (plan->col_idx == 0) {

            PlanNode *index = create_plan_node(NODE_INDEX_SCAN);
            strcpy(index->table, plan->left->table);
            index->value = plan->value;

            return index;
        }
    }

    return plan;
}