/* exec_engine_api.cpp — STUB.
 *
 * Real AST walking is the execution engine team's work. This stub exists
 * so engine_execute_sql() can be wired up end-to-end and tested before
 * the executor is fleshed out. It reports the AST statement type and
 * returns MYDB_OK so the REPL flow can be exercised.
 *
 * Replace the body when ready: walk `node` by ast->type and call into
 * storage_* / engine_* per the documented contract in
 * execution_engine/include/exec_engine_api.h. */

#include "exec_engine_api.h"
#include "parser_api.hpp"   /* parser_ast_node() — walks the AST node */
#include "AST.hpp"

#include <cstdio>

/* Match common.h. The execution engine library deliberately doesn't
 * include common.h to keep its dependency surface minimal — it's a
 * C++ AST walker, not a storage consumer (yet). When the executor
 * starts calling storage_*, link against storage_engine and switch
 * these to MYDB_OK / MYDB_ERR. */
#define EXEC_OK   0
#define EXEC_ERR (-1)

extern "C" int exec_engine_execute(struct EngineState *eng,
                                   ParserAST *ast,
                                   char *result_out, size_t result_cap)
{
    (void)eng;
    if (!result_out || result_cap == 0) return EXEC_ERR;

    const ASTNode *node = parser_ast_node(ast);
    if (!node) {
        std::snprintf(result_out, result_cap,
                      "exec_engine: empty AST");
        return EXEC_ERR;
    }

    std::snprintf(result_out, result_cap,
                  "exec_engine: not implemented "
                  "(received AST, statement type=%d)",
                  static_cast<int>(node->type));
    return EXEC_OK;
}
