#ifndef EXEC_ENGINE_API_H
#define EXEC_ENGINE_API_H

/* C-callable entry point into the execution engine.
 *
 * Sole caller: engine_execute_sql() in engine.c. After parsing, engine
 * hands the AST here to be walked. The execution engine dispatches AST
 * nodes to:
 *   - storage_*       (table DDL/DML/DQL, transactions)
 *   - engine_check_access, engine_find_relation, engine_use_schema, etc.
 *
 * No other module calls this. Bin never sees it.
 *
 * Result format: writes a printable, NUL-terminated status / result
 * string into result_out, truncated to result_cap-1 bytes. The string
 * is what bin's REPL prints back to the user. Format conventions are
 * the execution engine's choice (e.g. "1 row affected", a tabular
 * result set, or an error description).
 *
 * Return codes (mirrors common.h MYDB_*):
 *   MYDB_OK              statement executed; result_out is the answer
 *   MYDB_ERR_PERM        no active session, or auth check failed
 *   MYDB_ERR_NOT_FOUND   referenced relation / schema does not exist
 *   MYDB_ERR_*           other storage/engine error codes
 *
 * The AST handle is borrowed for the duration of the call. The caller
 * (engine) owns it and frees it after this returns. */

#include <stddef.h>

#include "parser_api.h"   /* ParserAST opaque type */

#ifdef __cplusplus
extern "C" {
#endif

/* Forward declaration — full definition is in engine.h. Including
 * engine.h here would create a header cycle (engine.h → exec_engine_api.h
 * → engine.h). Implementation files include engine.h directly. */
struct EngineState;

int exec_engine_execute(struct EngineState *eng,
                        ParserAST *ast,
                        char *result_out, size_t result_cap);

#ifdef __cplusplus
}
#endif

#endif /* EXEC_ENGINE_API_H */
