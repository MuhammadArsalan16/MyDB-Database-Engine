#ifndef LARGE_WAL_WRITER_H
#define LARGE_WAL_WRITER_H

#include <pthread.h>
#include <stdint.h>
#include "common.h"
#include "large_wal/large_wal_segment_pool.h"
#include "large_wal/large_wal_registry.h"
#include "large_wal/large_wal_index.h"
#include "large_wal/large_wal_state.h"
#include "large_wal/large_wal_buffer.h"
#include "wal_worker.h"

/*
 * large_wal_writer.h — the LARGE_WAL Writer thread (MYDB_WAL_
 * IMPLEMENTATION.md Appendix A: "single dedicated thread, packs content
 * into rotation segments, fsyncs, advances large_wal_flush_lsn"). NOT
 * the storage engine's buffer-pool page writer — a different, unrelated
 * component with a similar name.
 *
 * Owns its LargeWalBuffer (per §10.10 — "the staging area the LARGE_WAL
 * Writer packs content into") and its own live cursor
 * (cur_slot_index/cur_page_no), the same "caller owns and persists the
 * cursor across calls" pattern wal_segment_pool_write's own params and
 * WalFlusher's buf_cursor/seg_cursor already use.
 *
 * submit() is a blocking request/response handoff over one mutex+
 * condvar, not a general async queue — the engine's whole execution
 * model is single-statement-in-flight (CLAUDE.md's Concurrency model),
 * and §10.7's Commit Wait Rule is phrased as a wait, not a callback, so
 * at most one large-record write is ever in flight system-wide. This
 * also means no separate large_wal_flush_lsn query API is needed: if
 * submit() returns MYDB_OK, large_wal_flush_lsn has already advanced to
 * at least this record's content_lsn (persisted+fsynced before submit()
 * returns).
 *
 * Per-submit sequence (do_write, large_wal_writer.c): segment-fit check
 * first (roll over before packing, never mid-buffer, closing Phase 4's
 * "segment-fit" open item) -> pack fully into the owned buffer,
 * stamping real LargeWalPageHeaders (closing Phase 4's "header
 * stamping" open item, since the writer -- unlike the buffer alone --
 * knows exactly where these pages are landing) -> one byte-for-byte
 * large_wal_segment_pool_write() call, reused as-is -> index insert ->
 * large_wal_state advance.
 *
 * Also registers every segment it claims with the registry
 * (large_wal_registry_register, large_wal_registry.h) immediately,
 * widening that registry to cover a segment's whole life instead of
 * just its post-copy_out residency (Phase 3's deferred "Structural
 * consequence"). Registers with owns_fd=0 — a rotation slot's fd is
 * the pool's own, closed by the pool's shutdown, never this registry's.
 * Never touches large_wal_archiver at all — the writer only ever
 * needed register(), not anything archiver-specific, so it depends on
 * the registry directly rather than the whole archiver module.
 *
 * Still standalone: dependencies (pool/registry/idx/state) are caller-
 * supplied, not constructed here, matching every phase so far. No
 * wal_manager/large_wal_manager/TxnManager wiring, no per-segment_no
 * locking (see this phase's plan, "Concurrency" section) — this phase's
 * own tests never have a concurrent large_wal_get() caller racing an
 * in-flight submit().
 */

typedef struct {
    LargeWalSegmentPool *pool;      /* not owned */
    LargeWalRegistry      *registry; /* not owned */
    LargeWalIndex            *idx;    /* not owned */
    LargeWalState              *state; /* not owned */
    WalWorker                    *worker; /* not owned; may be NULL — see large_wal_writer_init */

    LargeWalBuffer  buf;   /* OWNED — embedded, per §10.10 */

    /* Live cursor, seeded at init() from whatever's already LSEG_ACTIVE
     * (or a fresh claim if none) — Phase 2's own crash-reload tail-scan
     * already makes data_pages accurate post-crash, so no new recovery
     * logic is needed here. */
    uint32_t  cur_slot_index;
    uint32_t  cur_page_no;
    uint64_t  cur_segment_last_lsn;   /* content_lsn of the last record
                                          written into cur_slot_index —
                                          used as end_lsn if a future
                                          submit closes this segment
                                          early via manual rollover */

    pthread_t        thread;
    pthread_mutex_t  lock;
    pthread_cond_t   cond;
    uint8_t          started;
    uint8_t          stop_requested;

    /* single-slot request/response mailbox — content is a borrowed
     * pointer, valid for the call's duration since submit() blocks */
    uint8_t                request_pending;
    uint8_t                request_done;
    int                      last_result;
    const uint8_t             *req_content;
    uint32_t                    req_total_size;
    uint64_t                     req_content_lsn;
    uint8_t                       req_rec_type;
    LargeWalIndexEntry             req_out_entry;
} LargeWalWriter;

/* Seeds cur_slot_index/cur_page_no from whatever segment is already
 * LSEG_ACTIVE in pool (claiming a fresh one via claim_next() if none
 * is), and registers that segment with registry either way — necessary
 * because LargeWalRegistry is in-memory only and starts empty every
 * process start. Does not start the thread. worker (may be NULL) is
 * stored and forwarded into every large_wal_segment_pool_write()/
 * mark_done() call this writer makes — see large_wal_segment_pool.h's
 * own worker parameter doc comments. */
int large_wal_writer_init(LargeWalWriter *w, LargeWalSegmentPool *pool,
                           LargeWalRegistry *registry, LargeWalIndex *idx,
                           LargeWalState *state, WalWorker *worker);

/* Starts the background thread. init() must have succeeded first. */
int large_wal_writer_start(LargeWalWriter *w);

/* Signals stop, joins the thread. Safe to call even if start() was
 * never called, or after an earlier stop() — a no-op in both cases. */
int large_wal_writer_stop(LargeWalWriter *w);

/* Blocking. Wakes the thread, waits for this one record to be packed,
 * written, indexed, and durably flush_lsn-advanced before returning.
 * *out_entry receives the resulting LargeWalIndexEntry on MYDB_OK.
 * Fails immediately (no hang) if the writer isn't started or is
 * stopping. */
int large_wal_writer_submit(LargeWalWriter *w, const uint8_t *content, uint32_t total_size,
                             uint64_t content_lsn, uint8_t rec_type,
                             LargeWalIndexEntry *out_entry);

#endif /* LARGE_WAL_WRITER_H */
