#pragma once
/*
 * exec_internal.h — shared helpers for DML auto-commit.
 *
 * Include this only in implementation .cpp files.
 * Never include it from public headers.
 *
 * Requires: ast_executor.hpp already included (brings in storage.h,
 * common.h, and the extern declaration of g_in_explicit_txn).
 */

/*
 * AUTOCOMMIT_BEGIN / AUTOCOMMIT_END
 * ----------------------------------
 * Every DML handler (INSERT, UPDATE, DELETE) wraps its write operation
 * with these two macros.
 *
 * Two modes:
 *   Explicit txn  — user issued BEGIN, so g_in_explicit_txn is true.
 *                   We do nothing; the user drives commit/rollback.
 *   Auto-commit   — no explicit BEGIN.  We open a transaction before
 *                   the write and close it right after.  Commit on
 *                   success, rollback on failure.
 *
 * Usage pattern in every DML handler:
 *
 *   AUTOCOMMIT_BEGIN(ectx);
 *   rc = pm_insert(ectx->partition, rel, &row);   // or update / delete
 *   AUTOCOMMIT_END(ectx, rc);
 *   return rc;
 *
 * AUTOCOMMIT_END does NOT change rc — the caller keeps it.
 *
 * The transaction boundary is driven through the partition_manager
 * (pm_begin / pm_commit / pm_rollback), which owns the TransactionManager.
 */

/*
 * REQUIRE_LOGIN / REQUIRE_SCHEMA / REQUIRE_PARTITION
 * ---------------------------------------------------
 * Precondition guards used at the top of every handler.
 * Each expands to a complete if-return so it is safe without braces.
 * The per-session flags now live on the Connection inside ExecContext.
 * Requires: `out`, `cap` in scope (the handler's output buffer params).
 */

#define REQUIRE_LOGIN(ectx)                                             \
    if (!(ectx)->conn->logged_in) {                                     \
        snprintf(out, cap, "  Error: not logged in");                   \
        return MYDB_ERR_PERM;                                           \
    }

#define REQUIRE_SCHEMA(ectx)                                                        \
    if (!(ectx)->conn->schema_active) {                                             \
        snprintf(out, cap,                                                          \
                 "  Error: no schema selected — run USE <schema> first");           \
        return MYDB_ERR;                                                            \
    }

#define REQUIRE_PARTITION(ectx)                                            \
    if (!(ectx)->conn->partition_open) {                                    \
        snprintf(out, cap, "  Error: user owns no partition");              \
        return MYDB_ERR_PERM;                                               \
    }

#define AUTOCOMMIT_BEGIN(ectx)              \
    do {                                    \
        if (!g_in_explicit_txn)             \
            pm_begin((ectx)->partition);    \
    } while (0)

#define AUTOCOMMIT_END(ectx, _rc_)                              \
    do {                                                        \
        if (!g_in_explicit_txn) {                               \
            if ((_rc_) == MYDB_OK) pm_commit((ectx)->partition);\
            else                   pm_rollback((ectx)->partition);\
        }                                                       \
    } while (0)
