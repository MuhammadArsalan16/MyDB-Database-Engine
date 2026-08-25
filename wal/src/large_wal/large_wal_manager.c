#include "large_wal/large_wal_manager.h"

#include <stdio.h>

int large_wal_manager_init(LargeWalManager *mgr, const char *wal_dir,
                            uint32_t partition_id, WalWorker *worker)
{
    if (!mgr || !wal_dir) return MYDB_ERR;

    mgr->partition_id = partition_id;
    snprintf(mgr->wal_dir, sizeof(mgr->wal_dir), "%s", wal_dir);

    /* Registry first: the pool holds a pointer to it, and registers
     * every segment_no claim_next mints. */
    if (large_wal_registry_init(&mgr->lw_registry) != MYDB_OK)
        return MYDB_ERR;

    if (large_wal_segment_pool_init(&mgr->lw_pool, wal_dir, partition_id,
                                     &mgr->lw_registry) != MYDB_OK) {
        large_wal_registry_shutdown(&mgr->lw_registry);
        return MYDB_ERR;
    }

    if (large_wal_index_open(&mgr->lw_idx, wal_dir) != MYDB_OK) {
        large_wal_registry_shutdown(&mgr->lw_registry);
        large_wal_segment_pool_shutdown(&mgr->lw_pool);
        return MYDB_ERR;
    }

    if (large_wal_state_open(&mgr->lw_state, wal_dir) != MYDB_OK) {
        large_wal_index_close(&mgr->lw_idx);
        large_wal_registry_shutdown(&mgr->lw_registry);
        large_wal_segment_pool_shutdown(&mgr->lw_pool);
        return MYDB_ERR;
    }

    if (large_wal_archiver_init(&mgr->lw_archiver, wal_dir) != MYDB_OK) {
        large_wal_state_close(&mgr->lw_state);
        large_wal_index_close(&mgr->lw_idx);
        large_wal_registry_shutdown(&mgr->lw_registry);
        large_wal_segment_pool_shutdown(&mgr->lw_pool);
        return MYDB_ERR;
    }

    if (large_wal_writer_init(&mgr->lw_writer, &mgr->lw_pool, &mgr->lw_registry,
                               &mgr->lw_idx, &mgr->lw_state, worker) != MYDB_OK) {
        large_wal_archiver_shutdown(&mgr->lw_archiver);
        large_wal_state_close(&mgr->lw_state);
        large_wal_index_close(&mgr->lw_idx);
        large_wal_registry_shutdown(&mgr->lw_registry);
        large_wal_segment_pool_shutdown(&mgr->lw_pool);
        return MYDB_ERR;
    }

    if (large_wal_writer_start(&mgr->lw_writer) != MYDB_OK) {
        large_wal_archiver_shutdown(&mgr->lw_archiver);
        large_wal_state_close(&mgr->lw_state);
        large_wal_index_close(&mgr->lw_idx);
        large_wal_registry_shutdown(&mgr->lw_registry);
        large_wal_segment_pool_shutdown(&mgr->lw_pool);
        return MYDB_ERR;
    }

    return MYDB_OK;
}

int large_wal_manager_shutdown(LargeWalManager *mgr)
{
    if (!mgr) return MYDB_ERR;

    large_wal_writer_stop(&mgr->lw_writer);
    large_wal_archiver_shutdown(&mgr->lw_archiver);
    large_wal_state_close(&mgr->lw_state);
    large_wal_index_close(&mgr->lw_idx);
    large_wal_registry_shutdown(&mgr->lw_registry);
    large_wal_segment_pool_shutdown(&mgr->lw_pool);

    return MYDB_OK;
}
