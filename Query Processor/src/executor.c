#include <stdio.h>
#include <string.h>
#include "../../storage_engine/include/storage.h"
#include "executor.h"

/* Evaluate condition */
int match(Value *a, Value *b) {
    if (a->type != b->type) return 0;

    if (a->type == TYPE_INT)
        return a->v.int_val == b->v.int_val;

    if (a->type == TYPE_VARCHAR)
        return strncmp(a->v.varchar_val.data,
                       b->v.varchar_val.data,
                       a->v.varchar_val.len) == 0;

    return 0;
}

void print_row(const Row *row) {
    printf("Row:");
    for (int i = 0; i < row->num_cols; i++) {
        if (row->cols[i].is_null) {
            printf(" NULL");
        } else {
            switch (row->cols[i].type) {
                case TYPE_INT:
                    printf(" %d", row->cols[i].v.int_val);
                    break;
                case TYPE_VARCHAR:
                    printf(" %.*s", row->cols[i].v.varchar_val.len, row->cols[i].v.varchar_val.data);
                    break;
                case TYPE_BOOL:
                    printf(" %s", row->cols[i].v.bool_val ? "true" : "false");
                    break;
                default:
                    printf(" [unsupported-type]");
            }
        }
    }
    printf("\n");
}

/* SEQ SCAN + FILTER */
int exec_seq_scan_filter(PlanNode *plan) {

    Cursor *cur = storage_scan(plan->left->table);
    Row *row;

    while ((row = cursor_next(cur)) != NULL) {

        if (match(&row->cols[plan->col_idx], &plan->value)) {
            print_row(row);
        }
    }

    cursor_close(cur);
    return 0;
}

/* INDEX SCAN using PK */
int exec_index_scan(PlanNode *plan) {

    Row *row = storage_get_by_pk(plan->table, &plan->value);

    if (row != NULL) {
        printf("Found via INDEX:\n");
        print_row(row);
    } else {
        printf("Not Found\n");
    }

    return 0;
}

int exec_seq_scan(PlanNode *plan) {
    Cursor *cur = storage_scan(plan->table);
    Row *row;

    while ((row = cursor_next(cur)) != NULL) {
        print_row(row);
    }

    cursor_close(cur);
    return 0;
}

/* INSERT */
int exec_insert(PlanNode *plan) {
    return storage_insert(plan->table, &plan->insert_row);
}

/* UPDATE with filter */
int exec_update(PlanNode *plan) {
    Cursor *cur = storage_scan(plan->table);
    Row *row;
    int result = 0;

    while ((row = cursor_next(cur)) != NULL) {
        if (match(&row->cols[plan->col_idx], &plan->value)) {
            Row new_row = *row;
            if (plan->set_col_idx >= 0 && plan->set_col_idx < new_row.num_cols) {
                new_row.cols[plan->set_col_idx] = plan->set_value;
            }
            result = storage_update(plan->table, row->rid, &new_row);
            if (result != 0) break;
        }
    }

    cursor_close(cur);
    return result;
}

/* DELETE with filter */
int exec_delete(PlanNode *plan) {

    Cursor *cur = storage_scan(plan->table);
    Row *row;

    while ((row = cursor_next(cur)) != NULL) {

        if (match(&row->cols[plan->col_idx], &plan->value)) {
            RID rid = row->rid;
            storage_delete(plan->table, rid);
        }
    }

    cursor_close(cur);
    return 0;
}

/* MAIN EXECUTOR */
int execute_plan(PlanNode *plan) {

    switch (plan->type) {

        case NODE_FILTER:
            return exec_seq_scan_filter(plan);

        case NODE_INDEX_SCAN:
            return exec_index_scan(plan);

        case NODE_DELETE:
            return exec_delete(plan);

        case NODE_INSERT:
            return exec_insert(plan);

        case NODE_UPDATE:
            return exec_update(plan);

        default:
            printf("Unsupported plan\n");
            return -1;
    }
}