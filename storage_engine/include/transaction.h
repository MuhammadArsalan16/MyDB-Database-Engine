#ifndef TRANSACTION_H
#define TRANSACTION_H

#include "common.h"
#include "disk_manager.h"
#include "buffer_pool.h"

/*
 * transaction.h — single-user transaction manager (Phase 1)
 *
 * Strategy (no WAL / no crash recovery):
 *   BEGIN    : assign a new transaction ID, mark active
 *   COMMIT   : flush all dirty pages for every open table to disk
 *   ROLLBACK : evict all pages without flushing — dirty changes are
 *              discarded; on next access the buffer pool reloads the
 *              pre-transaction data from disk
 *
 * The storage API registers each table it opens so the transaction
 * manager knows which (table_id, DiskManager) pairs to flush/evict.
 *
 * Transaction IDs start at 1. ID 0 means "no active transaction".
 * The ID is written into DB_TRX_ID of every row that is inserted or
 * updated (handled by the storage layer, not here).
 */

typedef struct {
    BufferPool  *bp;

    /* Registry of currently open tables */
    int          table_ids[MAX_TABLES];
    DiskManager *dms[MAX_TABLES];
    int          num_open;

    /* Transaction state */
    uint64_t     trx_id;    /* ID assigned at the last BEGIN */
    uint8_t      active;    /* 1 = inside BEGIN...COMMIT/ROLLBACK */
    uint64_t     next_id;   /* monotonic counter — never resets */
} TransactionManager;

/* Initialise. Call once before any other trx_ function. */
void trx_init(TransactionManager *tm, BufferPool *bp);

/*
 * Register an open table. Called by the storage API on table open.
 * Returns MYDB_ERR if table_id is already registered or registry is full.
 */
int trx_register_table(TransactionManager *tm, int table_id, DiskManager *dm);

/*
 * Unregister a table. Called by the storage API on table close.
 * Returns MYDB_ERR_NOT_FOUND if the table was not registered.
 */
int trx_unregister_table(TransactionManager *tm, int table_id);

/*
 * BEGIN a transaction.
 * Returns MYDB_ERR if a transaction is already active.
 */
int trx_begin(TransactionManager *tm);

/*
 * COMMIT: flush all dirty pages for every registered table to disk,
 * then mark the transaction as inactive.
 * Returns MYDB_ERR_NO_TXN if no transaction is active.
 */
int trx_commit(TransactionManager *tm);

/*
 * ROLLBACK: evict all pages for every registered table WITHOUT flushing.
 * Dirty in-memory changes are discarded; disk retains the old data.
 * Returns MYDB_ERR_NO_TXN if no transaction is active.
 */
int trx_rollback(TransactionManager *tm);

/* Return the active transaction ID, or 0 if none is active. */
uint64_t trx_current_id(const TransactionManager *tm);

/* Return 1 if a transaction is currently active, 0 otherwise. */
int trx_is_active(const TransactionManager *tm);

#endif /* TRANSACTION_H */
