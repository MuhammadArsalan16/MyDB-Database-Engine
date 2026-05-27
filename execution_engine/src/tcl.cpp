/*
 * tcl.cpp — BEGIN / COMMIT / ROLLBACK handlers.
 *
 * Also owns g_in_explicit_txn — the flag DML handlers read to decide
 * whether to wrap their operation in their own transaction.
 *
 * Rules enforced here:
 *   BEGIN    — rejected if a transaction is already open.
 *   COMMIT   — rejected if no transaction is active.
 *   ROLLBACK — rejected if no transaction is active.
 *
 * On COMMIT or ROLLBACK failure we still clear g_in_explicit_txn
 * because the transaction is dead regardless of what the storage
 * layer did — leaving the flag set would block every future BEGIN.
 *
 * Design 3 output: "OK  BEGIN/COMMIT/ROLLBACK".
 * Errors:          "  Error: ..."
 */

#include "ast_executor.hpp"

#include <cstdio>

/* Defined here; declared extern in ast_executor.hpp so every other
 * .cpp file that includes ast_executor.hpp can read and test this flag. */
bool g_in_explicit_txn = false;

int exec_tcl(EngineState * /*eng*/, const TransactionStatement *s,
             char *out, size_t cap)
{
    int rc;

    switch (s->command) {

    case TransactionCommand::BEGIN:
        if (g_in_explicit_txn) {
            snprintf(out, cap, "  Error: there is already an open transaction");
            return MYDB_ERR;
        }
        rc = storage_begin();
        if (rc != MYDB_OK) {
            snprintf(out, cap, "  Error: storage_begin failed (rc=%d)", rc);
            return rc;
        }
        g_in_explicit_txn = true;
        snprintf(out, cap, "OK  BEGIN");
        return MYDB_OK;

    case TransactionCommand::COMMIT:
        if (!g_in_explicit_txn) {
            snprintf(out, cap, "  Error: no active transaction");
            return MYDB_ERR_NO_TXN;
        }
        rc = storage_commit();
        g_in_explicit_txn = false;   /* clear even on error — txn is dead */
        if (rc != MYDB_OK) {
            snprintf(out, cap, "  Error: commit failed (rc=%d)", rc);
            return rc;
        }
        snprintf(out, cap, "OK  COMMIT");
        return MYDB_OK;

    case TransactionCommand::ROLLBACK:
        if (!g_in_explicit_txn) {
            snprintf(out, cap, "  Error: no active transaction");
            return MYDB_ERR_NO_TXN;
        }
        rc = storage_rollback();
        g_in_explicit_txn = false;
        if (rc != MYDB_OK) {
            snprintf(out, cap, "  Error: rollback failed (rc=%d)", rc);
            return rc;
        }
        snprintf(out, cap, "OK  ROLLBACK");
        return MYDB_OK;

    default:
        snprintf(out, cap, "  Error: unknown transaction command");
        return MYDB_ERR;
    }
}
