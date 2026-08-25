#include "wal_manager.h"

int wal_manager_init(WalManager *wm, const char *wal_dir, uint32_t partition_id)
{
    if (!wm || !wal_dir) return MYDB_ERR;

    if (wal_worker_start(&wm->worker) != MYDB_OK)
        return MYDB_ERR;

    if (normal_wal_manager_init(&wm->nwm, wal_dir, partition_id, &wm->worker) != MYDB_OK) {
        wal_worker_stop(&wm->worker);
        return MYDB_ERR;
    }

    if (large_wal_manager_init(&wm->lwm, wal_dir, partition_id, &wm->worker) != MYDB_OK) {
        normal_wal_manager_shutdown(&wm->nwm);
        wal_worker_stop(&wm->worker);
        return MYDB_ERR;
    }

    return MYDB_OK;
}

int wal_manager_shutdown(WalManager *wm)
{
    if (!wm) return MYDB_ERR;

    large_wal_manager_shutdown(&wm->lwm);
    normal_wal_manager_shutdown(&wm->nwm);
    wal_worker_stop(&wm->worker);

    return MYDB_OK;
}
