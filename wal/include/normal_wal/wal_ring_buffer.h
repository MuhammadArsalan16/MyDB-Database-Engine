#ifndef WAL_RING_BUFFER_H
#define WAL_RING_BUFFER_H

#include <pthread.h>
#include <stddef.h>
#include <stdint.h>
#include "common.h"
#include "normal_wal/wal_types.h"
#include "normal_wal/wal_page.h"
#include "normal_wal/wal_segment_pool.h"

/*
 * wal_ring_buffer.h — the 16MB WAL ring buffer and txn_append_record()
 * (MYDB_WAL_DESIGN.md §2/§5/§7, MYDB_WAL_IMPLEMENTATION.md §8.6).
 *
 * Ring frames are byte-identical to segment pages (WalPageHeader +
 * packed, multiple small records up to WAL_PAGE_USABLE bytes, same
 * no-spanning-a-page rule) — the same shape wal_segment_pool already
 * uses, just living in memory instead of a file. wal_ring_buffer_append
 * mirrors wal_segment_pool_write's page-filling loop directly: does the
 * current frame have room; if not, close it and start a fresh one.
 *
 * No ATT/DPT here — that's TransactionManager's domain
 * (partition_manager/include/transaction.h), wired in later once this
 * module gets integrated. prev_lsn chaining is therefore the caller's
 * own responsibility this phase: the caller fills in hdr->prev_lsn/
 * txn_id/table_id/page_no/rec_type before calling append(); this module
 * only assigns lsn/total_len/checksum.
 *
 * Draining: this struct owns no "how far has been drained" position of
 * its own (no drain_frame field) — that's the *consumer's* state, not
 * the ring's. wal_ring_buffer_drain() and wal_ring_buffer_snapshot()
 * both take the caller's current drain position as an explicit in/out
 * parameter. A test can own a plain uint32_t for it; the real Flusher
 * (a later addition) owns it as its own buf_cursor.page_no — the ring
 * buffer itself never needs to know or care which.
 *
 * Interface discipline: same as WalSegmentPool (see wal_segment_pool.h)
 * — transparent struct, but external callers go through the functions
 * below only.
 */

#define WAL_RING_BUFFER_SIZE   (16 * 1024 * 1024)                     /* 16MB */
#define WAL_RING_BUFFER_FRAMES (WAL_RING_BUFFER_SIZE / WAL_PAGE_SIZE)  /* 4096 */

/* Conservative usable budget for the ring-full check in append() (see
 * there): current_lsn - flush_lsn approximates bytes of unflushed data
 * in the ring, since LSN is byte-offset-based — but it undercounts by
 * each occupied frame's own WalPageHeader bytes (LSN only counts record
 * bytes) plus any per-frame padding left over when a record doesn't fit
 * a frame's remaining space. Reserving one frame-header's worth of
 * margin per frame keeps the check safely conservative rather than
 * exact — the same "additive safety valve, not load-tested" treatment
 * the WAL docs give comparable backpressure gaps (Appendix B #8/#9). */
#define WAL_RING_BUFFER_USABLE_BUDGET \
    (WAL_RING_BUFFER_SIZE - (WAL_RING_BUFFER_FRAMES * WAL_PAGE_HEADER_SIZE))

/* WalCursor — design doc §7. One shape, reused for both the ring-buffer-
 * side and segment-side position (the design doc's own choice: "WalCursor
 * buf_cursor; WalCursor seg_cursor;"). Defined here rather than in
 * wal_flusher.h specifically so wal_ring_buffer_snapshot can report one
 * without a circular include (wal_flusher.h already includes this header
 * to get WalRingBuffer; the reverse can't also be true). */
typedef struct {
    uint64_t lsn;
    uint32_t page_no;
    uint16_t offset;
} WalCursor;

typedef struct {
    uint8_t       *buf;              /* heap-allocated WAL_RING_BUFFER_SIZE bytes */
    uint64_t       current_lsn;      /* next LSN to assign — byte-offset counter, never persisted */
    uint64_t       flush_lsn;        /* highest LSN confirmed durable in segments */
    uint32_t       write_frame;      /* ring frame currently being filled */
    uint32_t       write_frame_used; /* bytes used so far in write_frame's data area (<= WAL_PAGE_USABLE) */
    WalPageHeader  write_frame_hdr;  /* in-progress header for write_frame; serialized into buf on close */

    pthread_mutex_t lock;         /* protects current_lsn/write_frame/write_frame_used/
                                      write_frame_hdr and buf's write region */
    pthread_mutex_t flush_mutex;  /* protects flush_lsn */
    pthread_cond_t  flush_cond;   /* paired with flush_mutex, for wait_until_flushed() waiters */
} WalRingBuffer;

/* buf is malloc'd (WAL_RING_BUFFER_SIZE bytes); zeroed; write_frame_hdr
 * reset for a fresh frame 0; lock/flush_mutex/flush_cond initialized. */
int  wal_ring_buffer_init(WalRingBuffer *rb);
void wal_ring_buffer_shutdown(WalRingBuffer *rb);

/* Assigns lsn = current_lsn before this call (byte-offset counter:
 * current_lsn advances by hdr->total_len after — InnoDB-style, not a
 * simple +1 counter). Stamps hdr->lsn/total_len/checksum (via
 * wal_record_header_serialize) and packs {hdr, body} into the current
 * ring frame if it fits (write_frame_used + total_len <=
 * WAL_PAGE_USABLE); otherwise closes that frame (finalizes its
 * WalPageHeader into buf: data_len, checksum) and starts a fresh one,
 * same no-spanning rule as segment pages. Updates write_frame_hdr's
 * page_lsn (first record's lsn in this frame, set once) and end_lsn
 * (this record's lsn, updated every append into this frame). The whole
 * body runs under rb->lock.
 *
 * hdr->prev_lsn/txn_id/table_id/page_no/rec_type are the caller's
 * responsibility — not derived here (no ATT this phase).
 *
 * Returns MYDB_ERR if:
 *   - WAL_RECORD_HEADER_SIZE + body_len > WAL_MAX_RECORD_SIZE (LARGE_WAL
 *     redirection — not built yet, out of scope)
 *   - the ring is full: (current_lsn - flush_lsn) + total_len would
 *     exceed WAL_RING_BUFFER_USABLE_BUDGET (backpressure — reachable in
 *     a long run with nothing draining/flushing it). flush_lsn is read
 *     without locking flush_mutex here — same accepted-approximate-read
 *     precedent as wal_ring_buffer_lsn_is_flushed below; this is a soft
 *     capacity check, not a correctness-critical one.
 * On success, *out_lsn is set and MYDB_OK is returned. */
int wal_ring_buffer_append(WalRingBuffer *rb, WalRecordHeader *hdr,
                            const void *body, size_t body_len, uint64_t *out_lsn);

/* Drains every closed frame from *drain_frame up to (but not including)
 * write_frame, writing each whole frame via wal_segment_pool_write.
 * *drain_frame/seg_slot/seg_page_no/seg_offset are all the caller's own
 * state, passed in and updated in place. Advances flush_lsn (to the
 * drained frame's own end_lsn, read back off its finalized header) to
 * match what was actually written. Reads straight from rb->buf during
 * the segment-write I/O itself — fine for a single-threaded caller (a
 * test, or any use with no concurrent append() in flight), but this is
 * NOT what a real Flusher thread should use once one exists (see
 * wal_ring_buffer_snapshot, which copies out under lock first, keeping
 * I/O off the ring's own memory). Returns MYDB_ERR if
 * wal_segment_pool_write fails partway (*drain_frame/flush_lsn reflect
 * whatever succeeded before the failure). */
int wal_ring_buffer_drain(WalRingBuffer *rb, WalSegmentPool *pool,
                           uint32_t *drain_frame,
                           uint32_t *seg_slot, uint32_t *seg_page_no, uint32_t *seg_offset);

/* Locks rb->lock, memcpy's every closed frame (*drain_frame up to, but
 * not including, write_frame) into scratch (caller-owned, must hold >=
 * scratch_frame_cap * WAL_PAGE_SIZE bytes), advances *drain_frame past
 * what was copied, unlocks. Does NOT touch flush_lsn — that only
 * advances once the caller has confirmed the copied frames are actually
 * durable (wal_ring_buffer_mark_flushed, below), since this call
 * finishes before any I/O happens.
 *
 * Sets *out_num_frames (0 if nothing was closed since *drain_frame) and
 * *out_cursor — page_no = the new *drain_frame value after advancing,
 * lsn = the last-copied frame's own end_lsn (read from its
 * just-finalized header, already sitting in scratch — no extra I/O),
 * offset = 0 always this phase (only ever whole closed frames are
 * copied, no partial-frame byte tracking yet). A real Flusher stores
 * whatever this reports as its own buf_cursor (design doc §7). */
int wal_ring_buffer_snapshot(WalRingBuffer *rb, uint32_t *drain_frame,
                              uint8_t *scratch, uint32_t scratch_frame_cap,
                              uint32_t *out_num_frames, WalCursor *out_cursor);

/* Locks flush_mutex, sets rb->flush_lsn = new_flush_lsn (only if it's
 * actually higher — defensive against any out-of-order caller),
 * broadcasts flush_cond, unlocks. Call once per drain cycle, after the
 * segment write(s) for that batch are confirmed durable. */
void wal_ring_buffer_mark_flushed(WalRingBuffer *rb, uint64_t new_flush_lsn);

/* Blocks until rb->flush_lsn >= lsn, via flush_mutex/flush_cond. This is
 * the primitive a future commit path calls after appending its COMMIT
 * record and signaling the Flusher (design doc §11's commit gate) — not
 * wired to any real commit path yet, no transactions exist. */
int wal_ring_buffer_wait_until_flushed(WalRingBuffer *rb, uint64_t lsn);

/* txn_lsn_is_flushed (design doc §2's second storage-engine call) —
 * non-blocking, lock-free read. A torn read of flush_lsn is very
 * unlikely on this platform's word size but not formally guarded;
 * acceptable for this phase, revisit if it ever matters. */
static inline int wal_ring_buffer_lsn_is_flushed(const WalRingBuffer *rb, uint64_t lsn)
{
    return rb->flush_lsn >= lsn;
}

#endif /* WAL_RING_BUFFER_H */
