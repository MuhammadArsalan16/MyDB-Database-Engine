#include "transaction.h"
#include <string.h>

void trx_init(TransactionManager *tm, BufferPool *bp)
{
    memset(tm, 0, sizeof(TransactionManager));
    tm->bp      = bp;
    tm->next_id = 1;  /* ID 0 is reserved for "no transaction" */
}

int trx_register_table(TransactionManager *tm, int table_id, DiskManager *dm)
{
    if (!tm || !dm) return MYDB_ERR;

    /* reject duplicates */
    for (int i = 0; i < tm->num_open; i++) {
        if (tm->table_ids[i] == table_id) return MYDB_ERR;
    }
    if (tm->num_open >= MAX_TABLES) return MYDB_ERR_FULL;

    tm->table_ids[tm->num_open] = table_id;
    tm->dms[tm->num_open]       = dm;
    tm->num_open++;
    return MYDB_OK;
}

int trx_unregister_table(TransactionManager *tm, int table_id)
{
    if (!tm) return MYDB_ERR;

    for (int i = 0; i < tm->num_open; i++) {
        if (tm->table_ids[i] == table_id) {
            /* shift remaining entries down to fill the gap */
            for (int j = i; j < tm->num_open - 1; j++) {
                tm->table_ids[j] = tm->table_ids[j + 1];
                tm->dms[j]       = tm->dms[j + 1];
            }
            tm->num_open--;
            return MYDB_OK;
        }
    }
    return MYDB_ERR_NOT_FOUND;
}

int trx_begin(TransactionManager *tm)
{
    if (!tm) return MYDB_ERR;
    if (tm->active) return MYDB_ERR;  /* nested transactions not supported */

    tm->trx_id = tm->next_id++;
    tm->active = 1;
    return MYDB_OK;
}

int trx_commit(TransactionManager *tm)
{
    if (!tm) return MYDB_ERR;
    if (!tm->active) return MYDB_ERR_NO_TXN;

    /* write every dirty page for every registered table to disk */
    for (int i = 0; i < tm->num_open; i++) {
        if (bp_flush_table(tm->bp, tm->dms[i], tm->table_ids[i]) != MYDB_OK)
            return MYDB_ERR;
    }

    tm->active = 0;
    return MYDB_OK;
}

int trx_rollback(TransactionManager *tm)
{
    if (!tm) return MYDB_ERR;
    if (!tm->active) return MYDB_ERR_NO_TXN;

    /*
     * Evict all pages for every registered table WITHOUT flushing.
     * bp_evict_table invalidates frames regardless of dirty flag,
     * so uncommitted in-memory changes are simply dropped.
     * The next access will reload the original data from disk.
     */
    for (int i = 0; i < tm->num_open; i++) {
        /* ignore errors — best-effort eviction during rollback */
        bp_evict_table(tm->bp, tm->table_ids[i]);
    }

    tm->active = 0;
    return MYDB_OK;
}

uint64_t trx_current_id(const TransactionManager *tm)
{
    if (!tm || !tm->active) return 0;
    return tm->trx_id;
}

int trx_is_active(const TransactionManager *tm)
{
    if (!tm) return 0;
    return tm->active;
}
