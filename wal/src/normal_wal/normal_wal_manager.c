#include "normal_wal/normal_wal_manager.h"

int normal_wal_manager_init(NormalWalManager *nwm, const char *wal_dir,
                             uint32_t partition_id, WalWorker *worker)
{
    if (!nwm || !wal_dir) return MYDB_ERR;

    if (wal_ring_buffer_init(&nwm->rb) != MYDB_OK)
        return MYDB_ERR;

    if (wal_segment_pool_init(&nwm->pool, wal_dir, partition_id) != MYDB_OK) {
        wal_ring_buffer_shutdown(&nwm->rb);
        return MYDB_ERR;
    }

    uint32_t slot;
    if (wal_segment_pool_claim_next(&nwm->pool, &slot) != MYDB_OK) {
        wal_segment_pool_shutdown(&nwm->pool);
        wal_ring_buffer_shutdown(&nwm->rb);
        return MYDB_ERR;
    }

    if (wal_flusher_start(&nwm->flusher, &nwm->rb, &nwm->pool, slot, worker) != MYDB_OK) {
        wal_segment_pool_shutdown(&nwm->pool);
        wal_ring_buffer_shutdown(&nwm->rb);
        return MYDB_ERR;
    }

    return MYDB_OK;
}

int normal_wal_manager_shutdown(NormalWalManager *nwm)
{
    if (!nwm) return MYDB_ERR;

    wal_flusher_stop(&nwm->flusher);
    wal_segment_pool_shutdown(&nwm->pool);
    wal_ring_buffer_shutdown(&nwm->rb);

    return MYDB_OK;
}
