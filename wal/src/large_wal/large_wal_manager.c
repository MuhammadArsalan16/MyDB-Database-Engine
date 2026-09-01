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

    /* Takes the registry so it can re-register holding-area files left
     * by a previous run (and delete half-written ones) -- see
     * large_wal_archiver.h's init doc comment. Runs BEFORE writer_init,
     * so a resumed segment's registry entry and the holding area's are
     * both in place before anything reads. */
    if (large_wal_archiver_init(&mgr->lw_archiver, wal_dir, &mgr->lw_registry) != MYDB_OK) {
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

    /* The archiver starts LAST, after the writer is fully up.
     * writer_init's find_or_claim_active reads pool->slots[i].header
     * without taking pool->lock, which is safe only while nothing else
     * is running -- so no second thread may exist before that point. */
    if (large_wal_archiver_start(&mgr->lw_archiver, &mgr->lw_pool,
                                  &mgr->lw_registry, &mgr->lw_idx) != MYDB_OK) {
        large_wal_writer_stop(&mgr->lw_writer);
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

    /* Writer first: once it is stopped no new slot can reach LSEG_DONE,
     * so the archiver's final pass sees a settled pool rather than
     * chasing one that is still filling. archiver_shutdown joins the
     * thread (via stop) before anything it touches is torn down.
     *
     * No forced drain beyond that final pass. Slots still LSEG_DONE at
     * exit are picked up next startup -- pool_init reads each slot's
     * self-describing header off disk, and the archiver's first tick
     * handles them. Draining to empty here would make shutdown take an
     * unbounded amount of time for no durability gain: the data is
     * already fsynced in the rotation slot either way. */
    large_wal_writer_stop(&mgr->lw_writer);
    large_wal_archiver_shutdown(&mgr->lw_archiver);
    large_wal_state_close(&mgr->lw_state);
    large_wal_index_close(&mgr->lw_idx);
    large_wal_registry_shutdown(&mgr->lw_registry);
    large_wal_segment_pool_shutdown(&mgr->lw_pool);

    return MYDB_OK;
}
