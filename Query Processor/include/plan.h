#ifndef PLAN_H
#define PLAN_H

#include "node.h"

PlanNode* build_select_plan(char *table, int col_idx, Value val);
PlanNode* optimize_plan(PlanNode *plan);

#endif