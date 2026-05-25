/*
 * exec_engine_api.cpp — the single extern "C" entry point into the
 * execution engine.
 *
 * Called exclusively by engine_execute_sql() in engine/src/engine.c.
 * Validates pre-conditions, then delegates to exec_dispatch().
 *
 * This file intentionally stays thin.  All real logic lives in
 * dispatch.cpp and the per-statement handlers.
 */

#include "exec_engine_api.h"
#include "ast_executor.hpp"

#include <cstdio>

extern "C" int exec_engine_execute(struct EngineState *eng,
                                   ParserAST          *ast,
                                   char               *result_out,
                                   size_t              result_cap)
{
    /* Basic precondition checks. */
    if (!eng || !ast || !result_out || result_cap == 0)
        return MYDB_ERR;

    if (!eng->logged_in) {
        std::snprintf(result_out, result_cap, "not logged in");
        return MYDB_ERR_PERM;
    }

    const ASTNode *node = parser_ast_node(ast);
    if (!node) {
        std::snprintf(result_out, result_cap, "empty AST");
        return MYDB_ERR;
    }

    return exec_dispatch(eng, node, result_out, result_cap);
}
