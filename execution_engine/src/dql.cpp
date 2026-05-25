/*
 * dql.cpp — SELECT handler.
 *
 * Phase 5 implements this file.
 */

#include "ast_executor.hpp"
#include "result_fmt.hpp"
#include "value_cast.hpp"
#include "expr_eval.hpp"

#include <cstdio>

int exec_select(EngineState * /*eng*/,
                const SelectStatement * /*s*/,
                char *out, size_t cap)
{
    std::snprintf(out, cap, "not implemented: SELECT");
    return MYDB_ERR;
}
