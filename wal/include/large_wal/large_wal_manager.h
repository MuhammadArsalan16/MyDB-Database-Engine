#ifndef LARGE_WAL_MANAGER_H
#define LARGE_WAL_MANAGER_H

#include <stdint.h>
#include "common.h"
#include "large_wal/large_wal_segment_pool.h"
#include "large_wal/large_wal_registry.h"
#include "large_wal/large_wal_index.h"
#include "large_wal/large_wal_state.h"
#include "large_wal/large_wal_archiver.h"
#include "large_wal/large_wal_writer.h"

/*
 * large_wal_manager.h — owns every large_wal sub-piece for one
 * partition
 *
 * Lifecycle-and-ownership, The real orchestration logic (large_wal_get's index+registry+reassembly work)
 * lives in large_wal_api.c, operating on a LargeWalManager*'s fields
 * directly. No get/write/copy_out/try_free wrappers live
 * here; inventing pass-through functions for calls that already reach
 * their target module in one hop would be exactly the kind of
 * manufactured indirection this design avoids.
 */

typedef struct {
    uint32_t                 partition_id;
    char                     wal_dir[256];

    LargeWalSegmentPool      lw_pool;
    LargeWalRegistry         lw_registry;
    LargeWalIndex            lw_idx;
    LargeWalState            lw_state;
    LargeWalArchiver         lw_archiver;
    LargeWalWriter           lw_writer;
} LargeWalManager;

/* Single entry point:
 * segment_pool_init -> registry_init -> index_open -> state_open ->
 * archiver_init -> writer_init -> writer_start, in order, unwinding
 * whatever already succeeded on any failure. worker (may be NULL) is a
 * pass-through parameter only — forwarded into large_wal_writer_init so
 * the writer's own write()/mark_done() calls can overlap a rollover's
 * two fsyncs (wal_worker.h); not stored as a field on LargeWalManager
 * itself, since nothing at this level ever calls write()/mark_done(). */
int large_wal_manager_init(LargeWalManager *mgr, const char *wal_dir,
                            uint32_t partition_id, WalWorker *worker);

/* Reverses init(): writer_stop, state_close, index_close,
 * registry_shutdown, segment_pool_shutdown. Safe on a
 * partially-initialised mgr. */
int large_wal_manager_shutdown(LargeWalManager *mgr);

#endif /* LARGE_WAL_MANAGER_H */
