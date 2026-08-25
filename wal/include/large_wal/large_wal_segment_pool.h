#ifndef LARGE_WAL_SEGMENT_POOL_H
#define LARGE_WAL_SEGMENT_POOL_H

#include <stddef.h>
#include <stdint.h>
#include "common.h"
#include "large_wal/large_wal_segment.h"

/*
 * large_wal_segment_pool.h — the rotation pool of
 * LARGE_WAL_SEGMENT_POOL_SLOTS physical large_wal_<N>.mydb segment
 * files: pre-allocation, round-robin claiming, raw page I/O within a
 * claimed slot, and the crash-reload tail-scan (MYDB_WAL_IMPLEMENTATION.md
 * §10.1). Mirrors normal_wal/wal_segment_pool.h's own original scope
 * exactly — init/reload, claim_next, raw page I/O, finalize, tail_scan —
 * *not* the copy-out-to-holding-area mechanism, the in-memory
 * (segment_no -> fd) table, or LargeWalIndexEntry, all of which are the
 * "archive section" phase's job, not this one's (§10.1/§10.5/§10.6 —
 * a real, separate concern from the rotation pool itself).
 *
 * LargeWalSlot (the in-memory runtime handle — fd + cached header) lives
 * here, not in large_wal_segment.h, which covers only the on-disk
 * LargeWalSegmentHeader *format*. This module is the pool/runtime
 * concern — same split normal_wal already established between
 * wal_segment.h and wal_segment_pool.h.
 *
 * Files live in the SAME wal/ directory normal_wal's own pool already
 * uses, named large_wal_<slot_index>.mydb — not a separate top-level
 * folder (MYDB_WAL_DESIGN.md §6's original layout; an intermediate
 * implementation-doc revision had moved this to a dedicated large_wal/
 * folder with abbreviated lw_ filenames and was itself reverted).
 *
 * Interface discipline: same as WalSegmentPool — transparent struct, but
 * external callers (the LARGE_WAL Writer thread, a later phase; the
 * LARGE_WAL Archiver, the "archive section" phase) go through the
 * large_wal_segment_pool_*() functions below only.
 *
 * Still no ring buffer, no LSN-assignment concept of its own, no
 * TxnManager/PartitionCtx wiring — this pool takes an explicit wal_dir
 * path from its caller, staying fully standalone, same as normal_wal.
 */

#define LARGE_WAL_SEGMENT_POOL_SLOTS      4
#define LARGE_WAL_SEGMENT_FILE_SIZE       (2 * 1024 * 1024)                          /* 2MB */
#define LARGE_WAL_SEGMENT_PAGES_PER_FILE  (LARGE_WAL_SEGMENT_FILE_SIZE / PAGE_SIZE)   /* 128:
     matches design doc's own "2MB / 16KB = 128 pages per segment" exactly. PAGE_SIZE
     (16384, common.h) reused directly — design doc: "Internal pages: 16KB (matches
     data page size)" — the same B+Tree page size, not a new constant. */

/* ------------------------------------------------------------------
 * LargeWalSlot — in-memory only, never persisted. One per physical
 * rotation-pool slot.
 * ------------------------------------------------------------------ */
typedef struct {
    uint8_t                slot_no;   /* 0..LARGE_WAL_SEGMENT_POOL_SLOTS-1 */
    int                    fd;        /* open for process lifetime */
    LargeWalSegmentHeader  header;    /* in-memory cached copy */
} LargeWalSlot;

/* ------------------------------------------------------------------
 * LargeWalSegmentPool — one per partition's large_wal. next_segment_no
 * is the pool-level monotonic counter driving claim_next()'s segment_no
 * assignment — same scheme as WalSegmentPool (one shared counter,
 * assigned at claim time): this phase only exercises
 * LSEG_FREE -> LSEG_ACTIVE, so there's no archiver-driven free-time
 * stamping to rely on instead yet.
 * ------------------------------------------------------------------ */
typedef struct {
    char      wal_dir[256];      /* the SAME wal/ directory normal_wal uses —
                                     large_wal_<N>.mydb files sit alongside wal_<N>.mydb,
                                     not a separate folder */
    uint32_t  partition_id;
    uint64_t  next_segment_no;
    LargeWalSlot slots[LARGE_WAL_SEGMENT_POOL_SLOTS];
} LargeWalSegmentPool;

/* ------------------------------------------------------------------
 * Lifecycle.
 *
 * large_wal_segment_pool_init: mkdir(wal_dir) if missing. For each of
 * the 4 slots: if large_wal_<i>.mydb doesn't exist, create +
 * posix_fallocate() to LARGE_WAL_SEGMENT_FILE_SIZE + write an initial
 * LSEG_FREE header + fsync; if it exists, open + validate + load its
 * header. Reseeds next_segment_no from whatever's on disk. Any slot
 * reloaded in LSEG_ACTIVE state is tail-scanned automatically (its
 * on-disk data_pages is stale by definition while active).
 * ------------------------------------------------------------------ */
int large_wal_segment_pool_init(LargeWalSegmentPool *pool, const char *wal_dir, uint32_t partition_id);
int large_wal_segment_pool_shutdown(LargeWalSegmentPool *pool);

/* LSEG_FREE -> LSEG_ACTIVE: claims slot (next_segment_no %
 * LARGE_WAL_SEGMENT_POOL_SLOTS), stamps its segment_no, rewrites +
 * fsyncs the header. *out_slot_index names the claimed slot. Returns
 * MYDB_ERR if that slot isn't currently LSEG_FREE (expected once all 4
 * slots have been claimed once and none has been freed yet — freeing is
 * the archive-section phase's job). */
int large_wal_segment_pool_claim_next(LargeWalSegmentPool *pool, uint32_t *out_slot_index);

/* Raw page I/O within an already-claimed slot. page_no is 1-based (0 is
 * the header's own page-slot) and must be < LARGE_WAL_SEGMENT_PAGES_PER_FILE.
 * No fsync here — infrastructure, not the durable path (that's the
 * writer-thread phase's concern, once it exists). */
int large_wal_segment_pool_write_page(LargeWalSegmentPool *pool, uint32_t slot_index,
                                       uint32_t page_no, const uint8_t *buf);
int large_wal_segment_pool_read_page(LargeWalSegmentPool *pool, uint32_t slot_index,
                                      uint32_t page_no, uint8_t *out_buf);

/* LSEG_ACTIVE -> LSEG_DONE: stamps the caller-supplied final end_lsn/
 * data_pages, rewrites + fsyncs the header. Does NOT free the slot back
 * to LSEG_FREE or copy anything anywhere — that's the archive-section
 * phase's job (impl doc §10.1's copy-out step). A LSEG_DONE slot just
 * sits there, unusable for a new claim, until that phase exists.
 * Returns MYDB_ERR if the slot isn't currently LSEG_ACTIVE. */
int large_wal_segment_pool_mark_done(LargeWalSegmentPool *pool, uint32_t slot_index,
                                     uint64_t end_lsn, uint32_t data_pages);

/* LSEG_DONE -> LSEG_FREE: called by the archiver only after the
 * holding-area copy's fsync has confirmed (impl doc §10.1's ordering
 * rule — a slot must never be marked FREE/reusable before that, or a
 * segment_no could transiently exist validly in two places). Zeroes
 * segment_no/start_lsn/end_lsn/data_pages, rewrites + fsyncs the
 * header. Returns MYDB_ERR if the slot isn't currently LSEG_DONE. */
int large_wal_segment_pool_free_slot(LargeWalSegmentPool *pool, uint32_t slot_index);

/* Scans page_no = 1, 2, ... in the given slot's file, validating each via
 * large_wal_page_header_deserialize, stopping at the first invalid/
 * unwritten page. *out_data_pages gets the count of valid pages found.
 * Called internally by large_wal_segment_pool_init() for any slot
 * reloaded in LSEG_ACTIVE state; exposed publicly so tests can exercise
 * it directly. */
int large_wal_segment_pool_tail_scan(LargeWalSegmentPool *pool, uint32_t slot_index,
                                      uint32_t *out_data_pages);

/* Reads a whole segment file's raw LARGE_WAL_SEGMENT_FILE_SIZE bytes
 * (header page-slot included) into out_buf in one call — mirrors
 * wal_segment_pool_read_segment. The eventual copy-out-to-holding-area
 * step (archive section phase, impl doc §10.1) needs exactly this kind
 * of whole-file read. out_buf must have room for
 * LARGE_WAL_SEGMENT_FILE_SIZE bytes. */
int large_wal_segment_pool_read_segment(LargeWalSegmentPool *pool, uint32_t slot_index,
                                         uint8_t *out_buf);

/* large_wal_segment_pool_write — mirrors wal_segment_pool_write exactly:
 * a blind byte mover, never parses or constructs LargeWalPageHeader
 * content — the caller's buf already carries whatever headers/LSNs it
 * needs (the eventual LARGE_WAL Writer thread builds these, the same
 * way the Flusher builds ring-buffer frames). Only knows page/segment
 * *geometry* (PAGE_SIZE, LARGE_WAL_SEGMENT_PAGES_PER_FILE).
 *
 * slot_index/page_no/offset are the caller's own cursor position,
 * purely positional — never derived from anything inside buf — passed
 * in and updated in place: offset is the exact byte position within
 * page_no's PAGE_SIZE slot to start writing at. On return,
 * slot_index/page_no/offset name where the *next* call should resume.
 *
 * Splits buf across as many pages as needed purely by byte count. When
 * offset reaches PAGE_SIZE, that page-slot is full: moves to the next
 * page_no, or — if that was the segment's last page-slot —
 * auto-finalizes the segment and claims the next one. The one
 * exception to "never reads header content": right before finalizing,
 * reads the just-filled last page back and copies its own content_lsn
 * field into mark_done()'s end_lsn argument. Unlike normal WAL's
 * page_lsn/end_lsn split (needed because a normal WAL page packs
 * *multiple* small records, so "first" and "last" record LSN can
 * differ), a LargeWalPageHeader carries exactly one LSN per page —
 * content_lsn, the single large record this page is a slice of — so
 * there's no first-vs-last ambiguity to resolve here: content_lsn IS
 * the value. A field copy, not content interpretation. Returns
 * MYDB_ERR if claim_next fails mid-write (pool exhausted).
 *
 * TEMPORARY: fsyncs before returning on every call — same reasoning and
 * removal condition as wal_segment_pool_write's own temporary fsync
 * (design doc §11: no LARGE_WAL Writer thread / group-commit yet to
 * batch many calls behind one fsync). One line to remove once that
 * thread lands. */
int large_wal_segment_pool_write(LargeWalSegmentPool *pool,
                                  uint32_t *slot_index, uint32_t *page_no, uint32_t *offset,
                                  const uint8_t *buf, size_t buf_len);

#endif /* LARGE_WAL_SEGMENT_POOL_H */
