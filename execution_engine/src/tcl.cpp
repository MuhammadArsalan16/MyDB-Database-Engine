/*
 * tcl.cpp — BEGIN / COMMIT / ROLLBACK handler.
 *
 * Also owns g_in_explicit_txn, the flag that tells DML handlers
 * whether they need to wrap their operation in their own transaction.
 *
 * Phase 1 implements this file.
 */

#include "ast_executor.hpp"

#include <cstdio>

/* True while an explicit BEGIN has been issued and not yet committed
 * or rolled back.  DML handlers read this to decide auto-commit. */
bool g_in_explicit_txn = false;

int exec_tcl(EngineState * /*eng*/, const TransactionStatement *s,
             char *out, size_t cap)
{
    const char *cmd =
        (s->command == TransactionCommand::BEGIN)    ? "BEGIN"    :
        (s->command == TransactionCommand::COMMIT)   ? "COMMIT"   : "ROLLBACK";

    std::snprintf(out, cap, "not implemented: %s", cmd);
    return MYDB_ERR;
}
