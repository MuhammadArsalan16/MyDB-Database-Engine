#ifndef LARGE_WAL_BUFFER_H
#define LARGE_WAL_BUFFER_H

#include <stdint.h>
#include "common.h"
#include "large_wal/large_wal_page.h"

/*
 * large_wal_buffer.h — the LARGE_WAL Writer's staging buffer
 * (MYDB_WAL_IMPLEMENTATION.md §10.10). Per-PartitionCtx: the writer
 * copies a large record's raw bytes in here before writing it out to
 * the current rotation-pool segment.
 *
 * Deliberately bare: no page-header framing happens here. Only
 * the writer thread — the entity that actually tracks the live segment
 * cursor (current page_no/offset) — can correctly decide when a fresh
 * page boundary starts (stamp a new header) versus mid-page
 * continuation. This module has no visibility into any live
 * LargeWalSegmentPool's cursor state (same minimal-dependency
 * discipline every large_wal phase so far has followed), so it cannot
 * safely pre-pack page-shaped output without assuming an alignment it
 * can't verify. large_wal_segment_pool_write() (large_wal_segment_pool.h)
 * stays a pure blind byte mover either way — it never gains header
 * awareness.
 *
 * page_count is computed here purely to size the buffer (and to reject
 * a record too large for the on-disk format up front) — it's
 * informational for the writer's own header-stamping loop, not used to
 * place any bytes at page-aligned offsets in this module.
 */

#define LARGE_WAL_STATIC_PAGES     4
#define LARGE_WAL_STATIC_BUF_SIZE  (LARGE_WAL_STATIC_PAGES * PAGE_SIZE)              /* 64KB */
#define LARGE_WAL_PAGE_USABLE      (PAGE_SIZE - LARGE_WAL_PAGE_HEADER_ON_DISK_SIZE)  /* 16352 */

typedef struct {
    uint8_t  static_buf[LARGE_WAL_STATIC_BUF_SIZE];  /* always allocated, per §10.10 */
    uint8_t *buf;          /* -> static_buf, or a heap block; bare bytes, no page framing */
    uint32_t total_size;   /* raw content size acquire() was sized for */
    uint32_t page_count;   /* ceil(total_size / LARGE_WAL_PAGE_USABLE) */
    int      is_heap;
} LargeWalBuffer;

/* Computes page_count = ceil(total_size / LARGE_WAL_PAGE_USABLE).
 * Rejects total_size == 0 (nothing to redirect) and page_count > 255
 * (LargeWalIndexEntry.page_count is uint8_t — a real format ceiling,
 * not arbitrary; the page header's own page_index field used to be the
 * other half of this justification, but that field is gone). Points buf at
 * static_buf when page_count <= LARGE_WAL_STATIC_PAGES, else mallocs
 * page_count * PAGE_SIZE. The caller copies the record's raw bytes into
 * buf itself — no packing happens here. */
int large_wal_buffer_acquire(LargeWalBuffer *buf, uint32_t total_size);

/* Frees the heap block if one was allocated. Safe to call on a
 * static-buffer acquire (no-op), and idempotent (safe to call twice). */
void large_wal_buffer_release(LargeWalBuffer *buf);

#endif /* LARGE_WAL_BUFFER_H */
