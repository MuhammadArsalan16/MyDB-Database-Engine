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
 *   AUTOCOMMIT_BEGIN();
 *   rc = storage_insert(rel, &row);   // or update / delete
 *   AUTOCOMMIT_END(rc);
 *   return rc;
 *
 * AUTOCOMMIT_END does NOT change rc — the caller keeps it.
 */

#define AUTOCOMMIT_BEGIN()              \
    do {                                \
        if (!g_in_explicit_txn)         \
            storage_begin();            \
    } while (0)

#define AUTOCOMMIT_END(_rc_)                        \
    do {                                            \
        if (!g_in_explicit_txn) {                   \
            if ((_rc_) == MYDB_OK) storage_commit();\
            else                   storage_rollback();\
        }                                           \
    } while (0)
