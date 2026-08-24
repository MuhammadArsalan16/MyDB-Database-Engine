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

/* One cached RelationDef page. is_dirty is real as of Phase 3 (write-back
 * — see pb_mark_relation_dirty/pb_flush_slot_dirty/pb_discard_slot_dirty
 * below). fpi_pending stays unset/unread until WAL integration — nothing
 * wires it up yet, and nothing should until that phase lands. */
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
    uint8_t          page0_dirty;   /* real as of Phase 3 — see pb_mark_page0_dirty */
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

/* Flush all dirty pages in the buffer pool for every cached schema, and
 * (Phase 3) every outer slot's dirty metadata (page0_dirty + any dirty
 * inner frames, via pb_flush_slot_dirty). se is the StorageEngine owned
 * by the same PartitionCtx. Does not close any SchemaFile handles. Used
 * as the final shutdown safety net (pctx_close) — the per-statement path
 * for metadata is pm_commit/pm_rollback, not this. */
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

/* ------------------------------------------------------------------ */
/*  Write-back (Phase 3)                                                */
/* ------------------------------------------------------------------ */

/* Mark this slot's Page 0 (header + relations[] directory) dirty. Called
 * by pm_api.c after schema_update_stats/schema_bump_relation_pages/
 * schema_bump_relation_rows succeed — those no longer persist internally
 * (Phase 3). Runs under slot->latch. */
void pb_mark_page0_dirty(PBOuterSlot *slot);

/* Mark the inner frame holding rel dirty. Called by pm_api.c after an
 * in-place RelationDef mutation (auto_incr_counter, root_page_no) instead
 * of the old immediate schema_flush_relation call. Uses the same
 * pointer-arithmetic technique pb_unpin_relation already uses internally
 * (def is PBInnerFrame's first member) — exposed here so pm_api.c doesn't
 * need to know PBInnerFrame's layout directly. Runs under slot->latch. */
void pb_mark_relation_dirty(PBOuterSlot *slot, const RelationDef *rel);

/* Flush this slot's dirty state to disk: Page 0 if page0_dirty, every
 * inner frame with is_dirty (via schema_flush_relation). Clears both
 * flags on success. Runs under slot->latch. Used by pm_commit (the
 * active slot only — v3's one-active-schema-at-a-time model means
 * nothing else could have been dirtied) and by pb_flush_all (every slot,
 * shutdown safety net). Also what §7 (outer eviction) and §8 (inner
 * eviction) call internally before reclaiming a dirty slot/frame — same
 * underlying per-frame/page0 flush, not reimplemented per call site. */
int pb_flush_slot_dirty(PBOuterSlot *slot);

/* Discard this slot's dirty state without persisting: Page 0 reloaded
 * from disk (schema_reload_page0, schema_file.h) if page0_dirty, every
 * dirty inner frame marked non-resident (next access reloads fresh from
 * disk — same as an ordinary eviction, just without the flush). Clears
 * both flags. Runs under slot->latch. Used by pm_rollback — the
 * write-back counterpart to storage_evict_all's data-page discard. */
int pb_discard_slot_dirty(PBOuterSlot *slot);

#ifdef __cplusplus
}
#endif

#endif /* PARTITION_BUFFER_H */
