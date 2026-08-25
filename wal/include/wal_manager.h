#ifndef WAL_MANAGER_H
#define WAL_MANAGER_H

#include <stdint.h>
#include "common.h"
#include "wal_worker.h"
#include "normal_wal/normal_wal_manager.h"
#include "large_wal/large_wal_manager.h"

/*
 * wal_manager.h — top-level, sibling to normal_wal/ and large_wal/.
 * Deliberately tiny: owns the one shared WalWorker (wal_worker.h) and
 * both subsystems' own managers, and that's it.
 *
 * NOT the "TxnManager -> wal_manager -> normal_wal_manager/
 * large_wal_manager" routing/dispatch layer from the earlier design
 * discussion — deciding which stream a record goes to, and TxnManager
 * integration, are both still fully deferred. This is purely "owns the
 * shared worker + both sub-managers," nothing more.
 *
 * One WalManager per partition (once this is embedded in PartitionCtx —
 * not built here): one shared WalWorker is enough per partition, since
 * the only contention case is normal_wal's Flusher and large_wal's
 * Writer both rolling over at the literal same instant on the same
 * partition — a rare collision that just costs the second one a brief
 * wait, not a correctness issue.
 */

typedef struct {
    WalWorker         worker;
    NormalWalManager  nwm;
    LargeWalManager   lwm;
} WalManager;

/* wal_worker_start -> normal_wal_manager_init(..., &worker) ->
 * large_wal_manager_init(..., &worker), unwinding whatever already
 * succeeded on any failure. */
int wal_manager_init(WalManager *wm, const char *wal_dir, uint32_t partition_id);

/* Reverses init(): large_wal_manager_shutdown, normal_wal_manager_
 * shutdown, worker_stop. Safe on a partially-initialised wm. */
int wal_manager_shutdown(WalManager *wm);

#endif /* WAL_MANAGER_H */
