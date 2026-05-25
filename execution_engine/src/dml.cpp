/*
 * dml.cpp — INSERT, UPDATE, DELETE handlers.
 *
 * Phase 4 (INSERT) and Phase 6 (UPDATE, DELETE) fill in the real logic.
 *
 * Every handler MUST follow this auto-commit pattern:
 *
 *   AUTOCOMMIT_BEGIN();
 *   rc = storage_insert(rel, &row);   // or update / delete
 *   AUTOCOMMIT_END(rc);
 *   return rc;
 *
 * The two macros from exec_internal.h handle both modes transparently:
 *   - Explicit txn (user issued BEGIN): macros do nothing, user drives it.
 *   - Auto-commit (no BEGIN): macros open, then commit or rollback.
 */

#include "ast_executor.hpp"
#include "exec_internal.h"
#include "result_fmt.hpp"
#include "value_cast.hpp"
#include "expr_eval.hpp"

#include <cstdio>

int exec_insert(EngineState * /*eng*/,
                const InsertStatement * /*s*/,
                char *out, size_t cap)
{
    /*
     * Phase 4 implementation goes here:
     *
     *   AUTOCOMMIT_BEGIN();
     *   rc = storage_insert(rel, &row);
     *   AUTOCOMMIT_END(rc);
     *   return rc;
     */
    snprintf(out, cap, "not implemented: INSERT");
    return MYDB_ERR;
}

int exec_update(EngineState * /*eng*/,
                const UpdateStatement * /*s*/,
                char *out, size_t cap)
{
    /*
     * Phase 6 implementation goes here:
     *
     *   AUTOCOMMIT_BEGIN();
     *   rc = storage_update(rel, rid, &new_row);
     *   AUTOCOMMIT_END(rc);
     *   return rc;
     */
    snprintf(out, cap, "not implemented: UPDATE");
    return MYDB_ERR;
}

int exec_delete(EngineState * /*eng*/,
                const DeleteStatement * /*s*/,
                char *out, size_t cap)
{
    /*
     * Phase 6 implementation goes here:
     *
     *   AUTOCOMMIT_BEGIN();
     *   rc = storage_delete(rel, row->rid);
     *   AUTOCOMMIT_END(rc);
     *   return rc;
     */
    snprintf(out, cap, "not implemented: DELETE");
    return MYDB_ERR;
}
