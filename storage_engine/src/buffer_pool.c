#include "buffer_pool.h"
#include <string.h>

/* ------------------------------------------------------------------ */
/*  LRU list helpers                                                    */
/*                                                                      */
/*  lru_order[] stores frame indices. Index 0 = MRU, last = LRU.      */
/*  Moving a frame to MRU: shift everything left one step.             */
/* ------------------------------------------------------------------ */

/* Move frame_idx to the front (MRU position) of the LRU list. */
static void lru_touch(BufferPool *bp, int frame_idx)
{
    /* Find where frame_idx currently sits */
    int pos = -1;
    for (int i = 0; i < BUFFER_POOL_SIZE; i++) {
        if (bp->lru_order[i] == frame_idx) { pos = i; break; }
    }
    if (pos <= 0) return; /* already MRU or not found */

    /* Shift everything before pos one step to the right */
    for (int i = pos; i > 0; i--)
        bp->lru_order[i] = bp->lru_order[i - 1];

    bp->lru_order[0] = frame_idx;
}

/* Add a newly-used frame to the front of the LRU list. */
static void lru_add(BufferPool *bp, int frame_idx)
{
    /* Shift everyone right to make room at index 0 */
    for (int i = BUFFER_POOL_SIZE - 1; i > 0; i--)
        bp->lru_order[i] = bp->lru_order[i - 1];
    bp->lru_order[0] = frame_idx;
}

/*
 * Find a victim frame for eviction: scan from the LRU end toward MRU,
 * return the first frame with pin_count == 0. Returns -1 if all pinned.
 */
static int lru_find_victim(BufferPool *bp)
{
    for (int i = BUFFER_POOL_SIZE - 1; i >= 0; i--) {
        int fi = bp->lru_order[i];
        if (!bp->frames[fi].is_valid) return fi;       /* empty frame */
        if (bp->frames[fi].pin_count == 0) return fi;  /* unpinned */
    }
    return -1; /* all frames are pinned */
}

/* ------------------------------------------------------------------ */
/*  Find a frame that holds (table_id, page_no). Returns -1 on miss.  */
/* ------------------------------------------------------------------ */
static int find_frame(BufferPool *bp, int table_id, uint32_t page_no)
{
    for (int i = 0; i < BUFFER_POOL_SIZE; i++) {
        if (bp->frames[i].is_valid &&
            bp->frames[i].table_id == table_id &&
            bp->frames[i].page_no  == page_no)
            return i;
    }
    return -1;
}

/* ------------------------------------------------------------------ */
/*  Public API                                                          */
/* ------------------------------------------------------------------ */

void bp_init(BufferPool *bp)
{
    memset(bp, 0, sizeof(BufferPool));
    /* Initialise LRU list to 0,1,2,...,63 (all invalid so order doesn't matter) */
    for (int i = 0; i < BUFFER_POOL_SIZE; i++)
        bp->lru_order[i] = i;
}

uint8_t *bp_fetch_page(BufferPool *bp, DiskManager *dm,
                       int table_id, uint32_t page_no)
{
    /* Refuse to cache page 0 — callers must use disk_read_header directly */
    if (page_no == 0) return NULL;

    /* Cache hit */
    int fi = find_frame(bp, table_id, page_no);
    if (fi >= 0) {
        bp->frames[fi].pin_count++;
        lru_touch(bp, fi);
        return bp->frames[fi].data;
    }

    /* Cache miss — find a victim frame */
    int victim = lru_find_victim(bp);
    if (victim < 0) return NULL; /* all frames pinned */

    BPFrame *f = &bp->frames[victim];

    /* Flush the victim's page to disk if it was modified */
    if (f->is_valid && f->is_dirty) {
        if (disk_write_page(dm, f->page_no, f->data) != MYDB_OK)
            return NULL;
    }

    /* Load the requested page into this frame */
    if (disk_read_page(dm, page_no, f->data) != MYDB_OK)
        return NULL;

    f->page_no   = page_no;
    f->table_id  = table_id;
    f->pin_count = 1;
    f->is_dirty  = 0;
    f->is_valid  = 1;

    if (bp->num_valid < BUFFER_POOL_SIZE) bp->num_valid++;
    lru_touch(bp, victim);   /* promotes victim to MRU */
    lru_add(bp, victim);     /* ensure it is at front */

    return f->data;
}

int bp_unpin_page(BufferPool *bp, int table_id, uint32_t page_no, int dirty)
{
    int fi = find_frame(bp, table_id, page_no);
    if (fi < 0) return MYDB_ERR;
    if (bp->frames[fi].pin_count == 0) return MYDB_ERR;

    bp->frames[fi].pin_count--;
    if (dirty) bp->frames[fi].is_dirty = 1;
    return MYDB_OK;
}

int bp_flush_page(BufferPool *bp, DiskManager *dm, int table_id, uint32_t page_no)
{
    int fi = find_frame(bp, table_id, page_no);
    if (fi < 0) return MYDB_OK; /* not in cache — nothing to flush */

    if (bp->frames[fi].is_dirty) {
        if (disk_write_page(dm, bp->frames[fi].page_no, bp->frames[fi].data) != MYDB_OK)
            return MYDB_ERR;
        bp->frames[fi].is_dirty = 0;
    }
    return MYDB_OK;
}

int bp_flush_table(BufferPool *bp, DiskManager *dm, int table_id)
{
    for (int i = 0; i < BUFFER_POOL_SIZE; i++) {
        if (!bp->frames[i].is_valid) continue;
        if (bp->frames[i].table_id != table_id) continue;
        if (bp->frames[i].is_dirty) {
            if (disk_write_page(dm, bp->frames[i].page_no, bp->frames[i].data) != MYDB_OK)
                return MYDB_ERR;
            bp->frames[i].is_dirty = 0;
        }
    }
    return MYDB_OK;
}

uint8_t *bp_alloc_page(BufferPool *bp, DiskManager *dm,
                       int table_id, uint32_t *page_no)
{
    /* Allocate a new page on disk (disk_alloc_page writes a zeroed page) */
    uint32_t new_pno;
    if (disk_alloc_page(dm, &new_pno) != MYDB_OK) return NULL;

    /* Now load it into a frame through the normal fetch path */
    uint8_t *data = bp_fetch_page(bp, dm, table_id, new_pno);
    if (!data) return NULL;

    *page_no = new_pno;
    return data;
}

int bp_evict_table(BufferPool *bp, int table_id)
{
    for (int i = 0; i < BUFFER_POOL_SIZE; i++) {
        if (!bp->frames[i].is_valid) continue;
        if (bp->frames[i].table_id != table_id) continue;
        if (bp->frames[i].pin_count > 0) return MYDB_ERR; /* still in use */

        /* Evict: just invalidate — caller must flush first */
        memset(&bp->frames[i], 0, sizeof(BPFrame));
    }
    return MYDB_OK;
}
