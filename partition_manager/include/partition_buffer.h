#ifndef PARTITION_BUFFER_H
#define PARTITION_BUFFER_H

#include "schema_file.h"
#include "storage.h"

#include <pthread.h>

#ifdef __cplusplus
extern "C" {
#endif

/*
 * partition_buffer.h — Cache 2.
 *
 * Phase 2 of the PartitionBuffer redesign (PARTITION_BUFFER_DESIGN.md §1-§8):
 * an 8-slot outer LRU of schemas (unchanged count from v2/Phase 1), each
 * holding a real evictable 32-of-64 inner LRU cache of RelationDef pages —
 * not the old flat, fully-resident SchemaFile.defs[] array.
 *
 *   PartitionBuffer
 *     PBOuterSlot[8]        one per cached schema, outer LRU
 *       SchemaFile *sf        header + relations[] directory only now
 *                              (RelationDef pages moved out — see below)
 *       PBFrameDirEntry dir[32]   forward lookup: page_no -> inner[] index
 *       PBInnerFrame inner[32]    the actual cached RelationDef pages
 *       inner_lru_order[32]       inner-cache LRU
 *       latch                     protects dir[]/inner[]/pin_count/eviction
 *                                 for this slot — real pthread_mutex_t,
 *                                 built now even though nothing races under
 *                                 today's single-threaded execution (§7)
 *
 * Outer eviction (pb_get, §7): two-pass per candidate — check every inner
 * frame for pin_count > 0 under the slot's latch (abort this candidate,
 * try the next LRU one, if any is pinned), then flush + close + free.
 * Fail-fast (return NULL) if every outer slot has a pinned inner frame,
 * matching bp_fetch_page's convention — never force-wait (see §7's
 * rationale: under single-threaded execution nothing could ever release
 * the pin we'd be waiting on).
 *
 * Inner lazy-load/eviction (pb_pin_relation, §8): walks the inner LRU from
 * the tail; the first frame that's either empty or unpinned-and-evictable
 * is the target (empty and eviction-victim are the same case, not two —
 * an empty frame is just trivially easy to evict, nothing to flush).
 * Fail-fast if every one of the 32 inner frames is pinned.
 */

#define PARTITION_BUFFER_SLOTS  8
#define PB_INNER_SLOTS          32

/* dir[i] describes inner[i] — index-aligned. Kept separate from PBInnerFrame
 * on purpose: a small, scan-friendly directory (64 bytes total) for the
 * forward "is this page resident, and where" question, versus the heavier
 * per-frame payload (the actual RelationDef) that's only touched once the
 * index is already known from here. See PARTITION_BUFFER_DESIGN.md §4. */
typedef struct {
    uint8_t page_no;       /* which __schema.mydb page — meaningful only if is_resident */
    uint8_t is_resident;
} PBFrameDirEntry;

/* One cached RelationDef page. is_dirty/fpi_pending are part of the final
 * struct shape (locked now per §3/§6) but stay unset/unread until Phase 3
 * (write-back) / WAL integration — nothing wires flush logic against them
 * yet, and nothing should until those phases actually land. */
typedef struct {
    RelationDef def;
    uint8_t     is_dirty;
    uint8_t     fpi_pending;
    uint16_t    pin_count;   /* >0 = in use, cannot be evicted */
} PBInnerFrame;

/* One cached schema. sf is NULL when the slot is empty. */
typedef struct {
    SchemaFile      *sf;
    PBFrameDirEntry  dir[PB_INNER_SLOTS];
    PBInnerFrame     inner[PB_INNER_SLOTS];
    int              inner_lru_order[PB_INNER_SLOTS];  /* [0]=MRU frame idx, [31]=LRU */
    uint8_t          page0_dirty;   /* placeholder, unused until Phase 3 */
    pthread_mutex_t  latch;
} PBOuterSlot;

/* Named struct tag so partition_ctx.h can forward-declare it as
 * `struct PartitionBuffer` without including this header. */
typedef struct PartitionBuffer {
    PBOuterSlot slots[PARTITION_BUFFER_SLOTS];
    int         lru_order[PARTITION_BUFFER_SLOTS]; /* [0]=MRU slot idx, [n_loaded-1]=LRU */
    int         n_loaded;
} PartitionBuffer;

/* ------------------------------------------------------------------ */
/*  Outer slot (Cache 2) — schema-level                                */
/* ------------------------------------------------------------------ */

/* Initialise all slots empty and their latches. Must be called before any
 * other pb_* function. */
int pb_init(PartitionBuffer *pb);

/* Return a pointer to the MRU SchemaFile, or NULL if the cache is empty. */
SchemaFile *pb_active_schema(PartitionBuffer *pb);

/* Return the schema_name of the MRU slot, or NULL if the cache is empty. */
const char *pb_active_schema_name(const PartitionBuffer *pb);

/* Flush all dirty pages in the buffer pool for every cached schema.
 * se is the StorageEngine owned by the same PartitionCtx.
 * Does not close any SchemaFile handles. */
int pb_flush_all(PartitionBuffer *pb, StorageEngine *se);

/* Close all open SchemaFile handles, free them, and destroy every slot's
 * latch. Safe to call on a partially-initialised PartitionBuffer
 * (n_loaded=0 is a no-op for the SchemaFile side; latches are always
 * destroyed since pb_init always initialises them). */
void pb_destroy(PartitionBuffer *pb);

/* Full outer LRU get (§7). Returns the cached or newly-opened SchemaFile
 * for schema_name, marked MRU. On a cache miss with all 8 slots occupied,
 * evicts the LRU slot whose inner frames are all currently unpinned
 * (walking from the LRU end, skipping any slot with a pinned inner frame);
 * returns NULL if every slot has at least one pinned inner frame (no
 * force-wait — see the file header rationale). */
SchemaFile *pb_get(PartitionBuffer *pb,
                   const char *schema_name,
                   const char *schema_path,
                   StorageEngine *se);

/* Return the cached SchemaFile for schema_name (no load, no LRU bump),
 * or NULL if it is not currently cached. */
SchemaFile *pb_find(PartitionBuffer *pb, const char *schema_name);

/* Same as pb_find but returns the full outer slot object (needed to reach
 * the inner cache / latch for pb_pin_relation / pb_unpin_relation), not
 * just the SchemaFile* portion. NULL if schema_name is not cached. */
PBOuterSlot *pb_find_outer_slot(PartitionBuffer *pb, const char *schema_name);

/* Evict schema_name from the cache if present (close handle + free + compact).
 * No-op if not cached. Used when a schema is dropped. */
int pb_remove(PartitionBuffer *pb, const char *schema_name);

/* ------------------------------------------------------------------ */
/*  Inner cache (Cache 2's RelationDef pages) — §8                     */
/* ------------------------------------------------------------------ */

/* Resolve relation_name to a pinned RelationDef* within this already-loaded
 * outer slot — a cache hit bumps LRU and increments pin_count; a miss
 * lazy-loads the def page from disk (evicting the inner-LRU unpinned frame
 * if the 32-frame cache is full). Runs entirely under slot->latch.
 *
 * Returns NULL if relation_name isn't in the directory (relations[]), or
 * if every one of the 32 inner frames is currently pinned (fail-fast — no
 * victim available, matching bp_fetch_page's convention). Every non-NULL
 * return must be matched by exactly one pb_unpin_relation. */
RelationDef *pb_pin_relation(PBOuterSlot *slot, const char *relation_name);

/* Release a pin taken by pb_pin_relation — decrements the owning frame's
 * pin_count (clamped at 0; a no-op if already released). Runs under
 * slot->latch. */
int pb_unpin_relation(PBOuterSlot *slot, const RelationDef *rel);

/* Invalidate any cached inner frame holding page_no, if resident — clears
 * dir[]'s is_resident, nothing else (no flush: the page is being dropped,
 * not evicted for space, so there's nothing meaningful left to write back).
 * Runs under slot->latch. Required after schema_remove_relation frees a
 * page_no for reuse: without this, a later relation assigned the same
 * page_no (schema_file.c's lowest-free-page policy) could be handed back
 * the previous relation's stale cached definition instead of its own. */
void pb_invalidate_page(PBOuterSlot *slot, uint8_t page_no);

#ifdef __cplusplus
}
#endif

#endif /* PARTITION_BUFFER_H */
