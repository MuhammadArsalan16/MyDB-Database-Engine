#include "../../storage_engine/include/storage.h"
#include "plan.h"
#include "executor.h"
#include <string.h>

int main() {

    storage_init("./data");

    /* Example: SELECT * FROM users WHERE id = 1 */

    Value v;
    v.type = TYPE_INT;
    v.is_null = 0;
    v.v.int_val = 1;

    PlanNode *plan = build_select_plan("users", 0, v);

    /* Optimize */
    plan = optimize_plan(plan);

    /* Execute */
    execute_plan(plan);

    storage_shutdown();
    return 0;
}