#ifndef WAL_FLUSHER_H
#define WAL_FLUSHER_H

#include <pthread.h>
#include <stdint.h>
#include "common.h"
#include "normal_wal/wal_ring_buffer.h"
#include "normal_wal/wal_segment_pool.h"
#include "wal_worker.h"

/*
 * wal_flusher.h — the real Flusher thread (MYDB_WAL_DESIGN.md §7). The
 * first genuine background worker thread in this codebase (confirmed via
 * grep -rn pthread_create before this: nothing existed, only
 * pthread_mutex_t data-protection latches in PartitionBuffer).
 *
 * Two-phase drain, matching the design doc's own stated reason for the
 * algorithm's shape: writer threads calling wal_ring_buffer_append()
 * must never block on disk I/O, only on a brief memcpy. Each cycle:
 *   1. wal_ring_buffer_snapshot() — locks the ring briefly, copies
 *      closed frames into this Flusher's own scratch buffer, unlocks.
 *   2. wal_segment_pool_write() against that copy — the slow I/O part,
 *      entirely off the ring's lock.
 *   3. wal_ring_buffer_mark_flushed() once the batch is confirmed durable.
 *
 * Owns the design doc §7 cursor triad faithfully: buf_cursor (ring-side
 * position, reported by snapshot()), seg_cursor (segment-side position,
 * reported by wal_segment_pool_write()), and seg_no (the current
 * segment's own persistent number). seg_slot_index is the one thing not
 * named in the docs — segment addressing is the pool's own internal
 * concern — kept alongside seg_no rather than re-derived every cycle.
 *
 * No ATT/DPT, no real commit-path wiring, no Checkpointer, one Flusher
 * per explicit (WalRingBuffer, WalSegmentPool) pair the caller supplies
 * — still standalone, not wired into PartitionCtx/TransactionManager.
 */

#define WAL_FLUSHER_PERIODIC_MS 20   /* placeholder — same "tunable, not
                                         load-tested" status the design
                                         doc gives every other timing
                                         constant */

typedef struct {
    WalRingBuffer   *rb;
    WalSegmentPool  *pool;
    WalWorker       *worker;   /* not owned; may be NULL — see wal_flusher_start */

    WalCursor  buf_cursor;      /* position in the WAL ring buffer — page_no is
                                    the last-drained ring frame index, lsn is
                                    that frame's own end_lsn, offset always 0
                                    this phase (whole-frame draining only) */
    WalCursor  seg_cursor;      /* position in the WAL segment — page_no/offset
                                    mirror wal_segment_pool_write's own out-
                                    params exactly; lsn is the end_lsn of the
                                    most recently *durably written* frame —
                                    deliberately distinct from buf_cursor.lsn
                                    (which advances at snapshot time, before
                                    I/O; this only advances once a frame is
                                    actually written) */
    uint64_t   seg_no;          /* the current segment's own persistent number
                                    (design doc §7) — refreshed from
                                    pool->slots[seg_slot_index].header.segment_no
                                    after every write, since a write may
                                    auto-roll to a new slot */
    uint32_t   seg_slot_index;  /* which physical pool slot seg_cursor targets */

    uint8_t         *scratch;   /* WAL_RING_BUFFER_SIZE bytes, allocated once at start */

    pthread_t        thread;
    uint8_t          started;
    uint8_t          stop_requested;

    pthread_mutex_t  demand_mutex;
    pthread_cond_t   demand_cond;   /* signaled for demand-wake; timedwait
                                        also covers the periodic trigger */
} WalFlusher;

/* Allocates scratch, seeds seg_cursor/seg_no/seg_slot_index from the
 * already-claimed slot the caller passes (wal_segment_pool_claim_next
 * must have been called first — this function does not claim one
 * itself), starts the thread. worker (may be NULL) is stored and
 * forwarded into every wal_segment_pool_write() call this Flusher makes
 * — see wal_segment_pool.h's own worker parameter doc comment. */
int wal_flusher_start(WalFlusher *f, WalRingBuffer *rb, WalSegmentPool *pool,
                       uint32_t seg_slot_index, WalWorker *worker);

/* Sets stop_requested, signals demand_cond so the thread wakes
 * immediately rather than waiting out its timeout, pthread_join's,
 * frees scratch. Safe to call even if start() was never called
 * (started == 0) or after an earlier stop() — a no-op in both cases. */
int wal_flusher_stop(WalFlusher *f);

/* Demand-wake: pthread_cond_signal(&f->demand_cond) under demand_mutex.
 * The primitive a future commit path calls right after appending its
 * COMMIT record, before waiting on wal_ring_buffer_wait_until_flushed —
 * design doc §11's commit gate. Not wired to any real commit path yet. */
void wal_flusher_signal(WalFlusher *f);

#endif /* WAL_FLUSHER_H */