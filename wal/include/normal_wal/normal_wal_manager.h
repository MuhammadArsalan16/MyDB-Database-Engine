#ifndef NORMAL_WAL_MANAGER_H
#define NORMAL_WAL_MANAGER_H

#include <stdint.h>
#include "common.h"
#include "normal_wal/wal_ring_buffer.h"
#include "normal_wal/wal_segment_pool.h"
#include "normal_wal/wal_flusher.h"
#include "wal_worker.h"

/*
 * normal_wal_manager.h — owns every normal_wal sub-piece for one
 * partition. Mirrors LargeWalManager's shape exactly
 * (large_wal/large_wal_manager.h): lifecycle-and-ownership only, no
 * business-logic functions here — mostly composition of pieces that
 * already exist (WalRingBuffer, WalSegmentPool, WalFlusher), no new
 * logic.
 *
 * Known, pre-existing, unrelated gap this inherits rather than solves:
 * wal_flusher_start()'s own doc comment already says it takes an
 * already-claimed slot and does no reload/resume logic of its own —
 * normal_wal never got the equivalent of large_wal_writer_init's
 * find_or_claim_active() (scan for an already-ACTIVE slot on restart).
 * So init() here always claims a fresh segment, same limitation
 * WalFlusher already has today.
 */

typedef struct {
    WalRingBuffer  rb;
    WalSegmentPool pool;
    WalFlusher     flusher;
} NormalWalManager;

/* wal_ring_buffer_init -> wal_segment_pool_init -> claim a fresh
 * segment -> wal_flusher_start(..., worker), unwinding whatever already
 * succeeded on any failure. worker (may be NULL) is a pass-through
 * parameter only — forwarded into wal_flusher_start so the Flusher's
 * own write() calls can overlap a rollover's two fsyncs (wal_worker.h);
 * not stored as a field on NormalWalManager itself. */
int normal_wal_manager_init(NormalWalManager *nwm, const char *wal_dir,
                             uint32_t partition_id, WalWorker *worker);

/* Reverses init(): flusher_stop, segment_pool_shutdown,
 * ring_buffer_shutdown. Safe on a partially-initialised nwm. */
int normal_wal_manager_shutdown(NormalWalManager *nwm);

#endif /* NORMAL_WAL_MANAGER_H */
