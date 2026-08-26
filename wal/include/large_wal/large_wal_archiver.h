#ifndef LARGE_WAL_ARCHIVER_H
#define LARGE_WAL_ARCHIVER_H

#include <stdint.h>
#include <pthread.h>
#include "common.h"
#include "large_wal/large_wal_segment_pool.h"
#include "large_wal/large_wal_page.h"
#include "large_wal/large_wal_index.h"
#include "large_wal/large_wal_registry.h"

/*
 * large_wal_archiver.h — moves a filled rotation-pool segment into the
 * holding area, and frees a holding-area copy once both freeing gates
 * clear (MYDB_WAL_IMPLEMENTATION.md §10.1). Genuinely archiver-only
 * work — the (segment_no -> fd) table this used to own moved out to
 * large_wal_registry.h once large_wal_writer started registering into
 * it too (it was never archiver-specific, just archiver-opened by
 * coincidence of build order). copy_out()/try_free() now take an
 * explicit LargeWalRegistry* instead of an implicit arc-owned one.
 *
 * LargeWalArchiver itself shrank to just wal_dir at that point — kept
 * as a real struct rather than eliminated, since it's the natural home
 * for the not-yet-built Archiver thread (Appendix A) to grow into
 * later, the same shape large_wal_writer.h already established for its
 * own thread. That later is now: the thread lives here, and the fields
 * below are what it needed.
 *
 * Gate A (checkpoint_lsn > segment_end_lsn) and Gate B ("every
 * content_lsn in this segment resolved by the Normal WAL Archiver") are
 * both caller-supplied parameters to try_free() — neither a Checkpointer
 * nor a Normal WAL Archiver exists yet to compute them for real.
 *
 * large_wal_get() does NOT live here — it's large_wal's external
 * read-path contract, not archiver-internal plumbing. See
 * large_wal_api.h.
 *
 * ------------------------------------------------------------------
 * Concurrency
 * ------------------------------------------------------------------
 * The note here used to read "none built here". That was true while
 * nothing called copy_out(); it no longer is. This file now owns
 * large_wal's second thread.
 *
 * What the thread does, per tick: wait for a slot to reach LSEG_DONE,
 * copy those slots out, then walk the holding area and try_free() what
 * the gates allow. It takes no lock of its own — the locks it needs
 * already belong to the pool and the registry, and it uses them through
 * copy_out()/try_free(), which were written for exactly this caller.
 *
 * Two rules the thread body must not break:
 *
 *   1. Never hold pool->lock across copy_out(). copy_out goes
 *      reg -> node -> free_slot -> pool, so holding pool->lock first is
 *      the global lock order backwards. The thread reads slot states
 *      via large_wal_segment_pool_slot_info() (which takes and releases
 *      pool->lock itself) and only then calls copy_out.
 *   2. Never take the registry lock directly. copy_out and try_free do
 *      their own acquire/release, and doing it here as well would nest
 *      the registry lock against itself.
 *
 * ONE THREAD, TWO JOBS. Appendix A lists a "LARGE_WAL Archiver
 * (holding-area)" whose stated role is only "monitors
 * wal/large_wal_archival_*, checks Gate A + Gate B, deletes once both
 * clear" — that is try_free alone. The doc never names a thread for
 * copy-out, though 10.1's own 4-slot reasoning requires copy-out to run
 * while the writer keeps writing. Both jobs are done by this one
 * thread, because LargeWalArchiver already owns both functions and
 * neither is expensive enough to deserve a thread of its own. Written
 * down here rather than left as an unexplained difference from the doc.
 */

/* Supplies Gate A and Gate B for one segment.
 *
 * Return MYDB_OK with both out-params filled; anything else means "no
 * answer for this segment right now" and the segment is left alone.
 *
 * This is a hook rather than a real computation because nothing in the
 * codebase can compute either gate yet: Gate A needs a Checkpointer and
 * Gate B needs the Normal WAL Archiver, and neither exists. Inventing a
 * placeholder value here would silently delete segments on a rule
 * nobody chose. */
typedef int (*LargeWalGateFn)(void *ctx, uint64_t segment_no,
                              uint64_t segment_end_lsn,
                              uint64_t *out_checkpoint_lsn,
                              int *out_gate_b_cleared);

/* Default poll interval. The thread is normally woken by mark_done, so
 * this is not how fast it reacts to a full segment — it is how often it
 * re-checks the holding area, whose gates change on their own schedule
 * with nothing to signal on. Also bounds how long stop() waits. */
#define LARGE_WAL_ARCHIVER_POLL_MS  50

typedef struct {
    char wal_dir[256];

    /* Not owned. Set by init (registry) and start (the rest) — the
     * thread needs all four, and storing them beats threading them
     * through a heap-allocated argument struct into pthread_create. */
    LargeWalRegistry    *registry;
    LargeWalSegmentPool *pool;
    LargeWalIndex       *idx;

    LargeWalGateFn  gate_fn;    /* NULL => nothing is ever freed */
    void           *gate_ctx;

    uint32_t         poll_interval_ms;

    pthread_t        thread;
    uint8_t          started;
    uint8_t          stop_requested;   /* read/written under pool->lock,
                                           the same lock the thread's
                                           condvar wait uses */
} LargeWalArchiver;

/* Also scans wal_dir for holding-area files left by a previous run and
 * registers each valid one, because LargeWalRegistry is in-memory and
 * starts empty every process start — without this, a restart leaves
 * perfectly good archived records unresolvable by large_wal_get.
 *
 * A file whose header doesn't validate is unlinked rather than kept.
 * That is not tidying: copy_out opens with O_EXCL, so a half-written
 * file left by a crash mid-copy makes copy_out fail on that segment
 * every single tick, forever — the slot stays LSEG_DONE, the pool never
 * drains, and writers block. Deleting it lets copy_out make a fresh one
 * from the rotation-slot original, which impl doc 10.1 already
 * describes as the correct recovery ("not durable, re-copy from the
 * still-valid rotation-slot original"). copy_out keeps O_EXCL, which
 * still prevents two live copy-outs colliding on one path.
 *
 * reg may be NULL for a caller that only wants copy_out/try_free
 * directly (the pre-thread tests), in which case no scan happens. */
int large_wal_archiver_init(LargeWalArchiver *arc, const char *wal_dir,
                             LargeWalRegistry *reg);
int large_wal_archiver_shutdown(LargeWalArchiver *arc);

/* Installs the Gate A / Gate B provider. Safe to call before start().
 * With no provider installed, try_free is still called every tick but
 * never frees anything — segments leave the rotation pool (which is
 * what unblocks writes) and then accumulate in the holding area
 * indefinitely. That is this phase's known limit, not a bug: it ends
 * when a real Checkpointer and Normal WAL Archiver exist. */
int large_wal_archiver_set_gates(LargeWalArchiver *arc, LargeWalGateFn fn, void *ctx);

/* Starts the background thread. init() must have succeeded first.
 * pool/reg/idx are stored for the thread's own use. */
int large_wal_archiver_start(LargeWalArchiver *arc, LargeWalSegmentPool *pool,
                              LargeWalRegistry *reg, LargeWalIndex *idx);

/* Signals stop, joins the thread. Safe to call even if start() was
 * never called, or after an earlier stop() — a no-op in both cases.
 * Mirrors large_wal_writer_stop exactly. */
int large_wal_archiver_stop(LargeWalArchiver *arc);

/* One pass of the thread's loop, run synchronously on the caller's
 * thread: copy out every LSEG_DONE slot, then walk the holding area and
 * try_free what the gates allow. Public so tests can drive a single
 * deterministic pass without starting a thread — the same reason
 * tail_scan is public. Does no waiting. */
int large_wal_archiver_tick(LargeWalArchiver *arc, LargeWalSegmentPool *pool,
                             LargeWalRegistry *reg, LargeWalIndex *idx);

/* Copy-out: reads the DONE segment's full bytes (via
 * large_wal_segment_pool_read_segment), writes them to
 * wal/large_wal_archival_<segment_no>.mydb with state overwritten to
 * LSEG_ARCHIVING (byte-identical otherwise, per §10.1), fsyncs, THEN
 * (only after that fsync confirms) calls
 * large_wal_segment_pool_free_slot() and registers the new fd in reg
 * (large_wal_registry_register(reg, segment_no, fd, owns_fd=1) —
 * repointing the entry large_wal_writer already registered at claim
 * time, not leaving a stale second one). Returns MYDB_ERR if the slot
 * isn't LSEG_DONE. */
int large_wal_archiver_copy_out(LargeWalArchiver *arc, LargeWalSegmentPool *pool,
                                 LargeWalRegistry *reg, uint32_t slot_index);

/* CRASH RECOVERY — why copy_out is safe to call again on a slot a
 * previous run died inside.
 *
 * A crash can land in two places, and they need opposite responses.
 * copy_out tells them apart by looking at the holding-area file before
 * doing anything (holding_copy_status in the .c):
 *
 *   Crash during copy_out's own 2MB pwrite. The file is short or its
 *   header won't deserialize; the rotation slot is untouched and still
 *   LSEG_DONE. copy_out unlinks the stub and copies again from the
 *   slot. (init's scan deletes such files too, so this is belt and
 *   braces.)
 *
 *   Crash inside free_slot, after the copy was written AND fsynced.
 *   The file is complete; the slot's content pages are partly zeroed
 *   and its header still reads LSEG_DONE, because free_slot never
 *   reached the step that rewrites it. copy_out must NOT copy again --
 *   the half-zeroed slot would overwrite the only surviving copy of
 *   that segment. It skips straight to freeing the slot, repointing the
 *   registry only if the startup scan has not already done so.
 *
 * Without this, that second case wedged the slot permanently: the
 * O_EXCL open would collide with the existing file on every single
 * tick, the slot would stay LSEG_DONE forever, and one of the four
 * rotation slots would be lost for the life of the installation. */

/* Gate A + Gate B, both caller-supplied. If checkpoint_lsn >
 * segment_end_lsn (Gate A) AND gate_b_cleared is true (Gate B): unlinks
 * the holding-area file, closes + removes its fd from reg, and prunes
 * every index entry for that segment_no via
 * large_wal_index_delete_by_segment(). Confirms the holding-area file
 * actually exists on disk before touching anything — reg's entry for
 * segment_no could still be a rotation slot's own live fd if this
 * segment was never actually archived, and closing that would break
 * the live pool. Returns MYDB_OK whether or not the segment actually
 * cleared (out_freed reports which). */
int large_wal_archiver_try_free(LargeWalArchiver *arc, LargeWalRegistry *reg, LargeWalIndex *idx,
                                 uint64_t segment_no, uint64_t segment_end_lsn,
                                 uint64_t checkpoint_lsn, int gate_b_cleared,
                                 int *out_freed);

#endif /* LARGE_WAL_ARCHIVER_H */
