/*
 * dml.cpp — INSERT, UPDATE, DELETE handlers.
 *
 * Phase 4 (INSERT) and Phase 6 (UPDATE, DELETE) implement this file.
 */

#include "ast_executor.hpp"
#include "result_fmt.hpp"
#include "value_cast.hpp"
#include "expr_eval.hpp"

#include <cstdio>

int exec_insert(EngineState * /*eng*/,
                const InsertStatement * /*s*/,
                char *out, size_t cap)
{
    std::snprintf(out, cap, "not implemented: INSERT");
    return MYDB_ERR;
}

int exec_update(EngineState * /*eng*/,
                const UpdateStatement * /*s*/,
                char *out, size_t cap)
{
    std::snprintf(out, cap, "not implemented: UPDATE");
    return MYDB_ERR;
}

int exec_delete(EngineState * /*eng*/,
                const DeleteStatement * /*s*/,
                char *out, size_t cap)
{
    std::snprintf(out, cap, "not implemented: DELETE");
    return MYDB_ERR;
}
