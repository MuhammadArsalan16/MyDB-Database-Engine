#ifndef BUFFER_POOL_H
#define BUFFER_POOL_H

#include "common.h"
#include "disk_manager.h"

/*
 * buffer_pool.h — LRU page cache
 *
 * Sits between the B+ Tree and disk. All page reads/writes from the
 * B+ Tree go through here. Pages are cached in 64 frames.
 *
 * Usage pattern:
 *   uint8_t *page = bp_fetch_page(bp, dm, table_id, page_no);
 *   // ... read or modify page ...
 *   bp_unpin_page(bp, table_id, page_no, dirty_flag);
 *
 * A page stays in a frame until evicted. It cannot be evicted while
 * pin_count > 0. Always unpin after you are done with a page.
 *
 * NOTE: page_no == 0 is reserved for the FileHeader and must NOT be
 * fetched through the buffer pool. Use disk_read_header() directly.
 */

/* One cache frame holding a single page */
typedef struct {
    uint8_t  data[PAGE_SIZE]; /* raw page bytes */
    uint32_t page_no;         /* which page is loaded here */
    int      table_id;        /* which table this page belongs to */
    uint16_t pin_count;       /* >0 means in use, cannot be evicted */
    uint8_t  is_dirty;        /* 1 = modified since last flush */
    uint8_t  is_valid;        /* 1 = frame holds a real page */
} BPFrame;

/* The buffer pool — 64 frames, LRU eviction */
typedef struct {
    BPFrame  frames[BUFFER_POOL_SIZE];
    /*
     * lru_order[0] = index of the most-recently-used frame (MRU)
     * lru_order[BUFFER_POOL_SIZE-1] = least-recently-used frame (LRU)
     */
    int      lru_order[BUFFER_POOL_SIZE];
    int      num_valid; /* number of frames currently in use */
} BufferPool;

/* ------------------------------------------------------------------ */
/*  Functions                                                           */
/* ------------------------------------------------------------------ */

/* Zero all frames, reset the LRU list. */
void bp_init(BufferPool *bp);

/*
 * Fetch a page. On hit: increments pin_count, moves to MRU, returns
 * pointer to frame data. On miss: evicts the LRU unpinned frame (flushing
 * it first if dirty), loads the page from disk, pins it.
 *
 * Returns NULL if page_no==0 (use disk_read_header directly) or if all
 * frames are pinned and no victim can be found.
 */
uint8_t *bp_fetch_page(BufferPool *bp, DiskManager *dm,
                       int table_id, uint32_t page_no);

/*
 * Unpin a page after use. If dirty=1 the frame is marked dirty and
 * will be written to disk on eviction (or explicit flush).
 */
int bp_unpin_page(BufferPool *bp, int table_id, uint32_t page_no, int dirty);

/* Flush a single dirty page to disk immediately. */
int bp_flush_page(BufferPool *bp, DiskManager *dm, int table_id, uint32_t page_no);

/* Flush all dirty pages belonging to table_id. */
int bp_flush_table(BufferPool *bp, DiskManager *dm, int table_id);

/*
 * Allocate a new page on disk, load it into a frame, pin it, and
 * return a pointer to the frame data. Sets *page_no to the new page number.
 */
uint8_t *bp_alloc_page(BufferPool *bp, DiskManager *dm,
                       int table_id, uint32_t *page_no);

/*
 * Evict all pages for table_id from the pool (used during DROP TABLE).
 * All pages for the table must be unpinned before calling this.
 */
int bp_evict_table(BufferPool *bp, int table_id);

#endif /* BUFFER_POOL_H */
