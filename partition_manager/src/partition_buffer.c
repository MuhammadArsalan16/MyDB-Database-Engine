#include "partition_buffer.h"
#include "schema_file.h"
#include "storage.h"

#include <string.h>
#include <stdlib.h>

/* Zero an outer slot's cache state back to "nothing resident" — dir[],
 * inner[] (which also clears pin_count/is_dirty/fpi_pending), and
 * inner_lru_order reset to identity order. Called whenever a slot starts
 * representing a different schema (eviction-and-reuse in pb_get, or
 * pb_remove) — never needed on a slot's first-ever use, since pb_init
 * already leaves it in exactly this state. Does not touch sf or the
 * latch itself. */
static void reset_outer_slot_cache(PBOuterSlot *slot)
{
    memset(slot->dir,   0, sizeof(slot->dir));
    memset(slot->inner, 0, sizeof(slot->inner));
    for (int i = 0; i < PB_INNER_SLOTS; i++) slot->inner_lru_order[i] = i;
}

/* Move frame_idx to the front (MRU) of an outer slot's inner LRU order.
 * Precondition: frame_idx is present in inner_lru_order[0..PB_INNER_SLOTS). */
static void inner_lru_bump(PBOuterSlot *slot, int frame_idx)
{
    int p = 0;
    while (p < PB_INNER_SLOTS && slot->inner_lru_order[p] != frame_idx) p++;
    if (p >= PB_INNER_SLOTS) return;   /* not found — shouldn't happen */
    for (int i = p; i > 0; i--)
        slot->inner_lru_order[i] = slot->inner_lru_order[i - 1];
    slot->inner_lru_order[0] = frame_idx;
}

int pb_init(PartitionBuffer *pb)
{
    if (!pb) return MYDB_ERR;
    memset(pb, 0, sizeof(*pb));   /* all slots[].sf = NULL, n_loaded = 0 */
    for (int i = 0; i < PARTITION_BUFFER_SLOTS; i++) {
        if (pthread_mutex_init(&pb->slots[i].latch, NULL) != 0) {
            for (int j = 0; j < i; j++)
                pthread_mutex_destroy(&pb->slots[j].latch);
            return MYDB_ERR;
        }
        for (int f = 0; f < PB_INNER_SLOTS; f++)
            pb->slots[i].inner_lru_order[f] = f;
    }
    return MYDB_OK;
}

SchemaFile *pb_active_schema(PartitionBuffer *pb)
{
    if (!pb || pb->n_loaded == 0) return NULL;
    return pb->slots[pb->lru_order[0]].sf;
}

const char *pb_active_schema_name(const PartitionBuffer *pb)
{
    if (!pb || pb->n_loaded == 0) return NULL;
    return pb->slots[pb->lru_order[0]].sf->header.schema_name;
}

/* Flush all dirty pages in the buffer pool for every cached schema.
 * se is the StorageEngine owned by the same PartitionCtx.
 * Does not close any SchemaFile handles. */
int pb_flush_all(PartitionBuffer *pb, StorageEngine *se)
{
    if (!pb || pb->n_loaded == 0) return MYDB_OK;
    return storage_flush_all_dirty(se);
}

void pb_destroy(PartitionBuffer *pb)
{
    if (!pb) return;
    for (int i = 0; i < PARTITION_BUFFER_SLOTS; i++) {
        PBOuterSlot *s = &pb->slots[i];
        if (s->sf) {
            if (s->sf->fd > 0) schema_close(s->sf);
            free(s->sf);
            s->sf = NULL;
        }
        pthread_mutex_destroy(&s->latch);
    }
    pb->n_loaded = 0;
}

/* Move slot index `slot` to the front (MRU) of lru_order.
 * Precondition: slot is present in lru_order[0 .. n_loaded). */
static void lru_bump(PartitionBuffer *pb, int slot)
{
    int p = 0;
    while (p < pb->n_loaded && pb->lru_order[p] != slot) p++;
    if (p >= pb->n_loaded) return;        /* not found — shouldn't happen */
    for (int i = p; i > 0; i--)
        pb->lru_order[i] = pb->lru_order[i - 1];
    pb->lru_order[0] = slot;
}

/* Return the cached SchemaFile for schema_name without loading or bumping. */
SchemaFile *pb_find(PartitionBuffer *pb, const char *schema_name)
{
    PBOuterSlot *s = pb_find_outer_slot(pb, schema_name);
    return s ? s->sf : NULL;
}

PBOuterSlot *pb_find_outer_slot(PartitionBuffer *pb, const char *schema_name)
{
    if (!pb || !schema_name) return NULL;
    for (int i = 0; i < PARTITION_BUFFER_SLOTS; i++) {
        if (pb->slots[i].sf &&
            strncmp(pb->slots[i].sf->header.schema_name, schema_name, 32) == 0)
            return &pb->slots[i];
    }
    return NULL;
}

/* Get-or-load the SchemaFile for schema_name, marking it MRU.
 * On a miss with all slots full: §7's two-pass outer eviction — walk the
 * LRU end, first candidate slot with no pinned inner frame is evicted and
 * reused. Returns NULL if every slot has at least one pinned inner frame
 * (fail-fast, no force-wait — see partition_buffer.h's file header). */
SchemaFile *pb_get(PartitionBuffer *pb,
                   const char *schema_name,
                   const char *schema_path,
                   StorageEngine *se)
{
    if (!pb || !schema_name || !schema_path) return NULL;

    /* Cache hit — bump to MRU and return. */
    for (int i = 0; i < PARTITION_BUFFER_SLOTS; i++) {
        if (pb->slots[i].sf &&
            strncmp(pb->slots[i].sf->header.schema_name, schema_name, 32) == 0) {
            lru_bump(pb, i);
            return pb->slots[i].sf;
        }
    }

    /* Miss — open the schema file first, before touching any slot, so a
     * failed open never disturbs existing cache state. */
    SchemaFile *sf = (SchemaFile *)calloc(1, sizeof(SchemaFile));
    if (!sf) return NULL;
    if (schema_open(schema_path, sf) != MYDB_OK) {
        free(sf);
        return NULL;
    }

    int slot;
    if (pb->n_loaded < PARTITION_BUFFER_SLOTS) {
        /* Free slot available — already correctly "nothing resident" from
         * pb_init, no reset needed. */
        slot = -1;
        for (int i = 0; i < PARTITION_BUFFER_SLOTS; i++)
            if (pb->slots[i].sf == NULL) { slot = i; break; }
        for (int i = pb->n_loaded; i > 0; i--)
            pb->lru_order[i] = pb->lru_order[i - 1];
        pb->lru_order[0] = slot;
        pb->n_loaded++;
    } else {
        /* Full — evict. Two-pass per candidate: check every inner frame
         * for a pin under the slot's latch before touching anything; skip
         * to the next LRU candidate if any frame is pinned. */
        int victim = -1;
        for (int p = pb->n_loaded - 1; p >= 0; p--) {
            int candidate = pb->lru_order[p];
            PBOuterSlot *cs = &pb->slots[candidate];
            pthread_mutex_lock(&cs->latch);

            int any_pinned = 0;
            for (int f = 0; f < PB_INNER_SLOTS; f++) {
                if (cs->inner[f].pin_count > 0) { any_pinned = 1; break; }
            }
            if (any_pinned) {
                pthread_mutex_unlock(&cs->latch);
                continue;
            }

            /* Evictable. storage_flush_all_dirty covers BP data pages
             * (existing behaviour, unchanged). Flushing this slot's own
             * dirty inner frames before close is a no-op this phase —
             * write-through means is_dirty is never set yet; Phase 3
             * (write-back) is what gives this step real work to do. */
            storage_flush_all_dirty(se);
            if (cs->sf) {
                if (cs->sf->fd > 0) schema_close(cs->sf);
                free(cs->sf);
                cs->sf = NULL;
            }
            reset_outer_slot_cache(cs);
            pthread_mutex_unlock(&cs->latch);
            victim = candidate;
            break;
        }
        if (victim < 0) {
            /* Every outer slot has a pinned inner frame — fail-fast, same
             * rationale as §7/§8: single-threaded execution means nothing
             * could ever release the pin we'd be waiting on, and under
             * real concurrency this avoids undesigned deadlock/starvation
             * machinery. */
            if (sf->fd > 0) schema_close(sf);
            free(sf);
            return NULL;
        }
        slot = victim;
        lru_bump(pb, slot);
    }

    pb->slots[slot].sf = sf;
    return sf;
}

/* Evict schema_name from the cache if present (close handle, free, reset
 * cache state, compact lru_order). Used when a schema is dropped.
 * No-op if not cached. Does not check for pinned inner frames — callers
 * (pm_drop_schema) already reject dropping the currently active schema,
 * and single-threaded execution means nothing else can be mid-use of a
 * schema that isn't active. */
int pb_remove(PartitionBuffer *pb, const char *schema_name)
{
    if (!pb || !schema_name) return MYDB_ERR;

    int slot = -1;
    for (int i = 0; i < PARTITION_BUFFER_SLOTS; i++) {
        if (pb->slots[i].sf &&
            strncmp(pb->slots[i].sf->header.schema_name, schema_name, 32) == 0) {
            slot = i;
            break;
        }
    }
    if (slot < 0) return MYDB_OK;          /* not cached */

    PBOuterSlot *cs = &pb->slots[slot];
    pthread_mutex_lock(&cs->latch);
    if (cs->sf->fd > 0) schema_close(cs->sf);
    free(cs->sf);
    cs->sf = NULL;
    reset_outer_slot_cache(cs);
    pthread_mutex_unlock(&cs->latch);

    /* Drop slot from lru_order and compact the tail. */
    int p = 0;
    while (p < pb->n_loaded && pb->lru_order[p] != slot) p++;
    for (int i = p; i < pb->n_loaded - 1; i++)
        pb->lru_order[i] = pb->lru_order[i + 1];
    pb->n_loaded--;
    return MYDB_OK;
}

/* ------------------------------------------------------------------ */
/*  Inner cache (§8)                                                    */
/* ------------------------------------------------------------------ */

RelationDef *pb_pin_relation(PBOuterSlot *slot, const char *relation_name)
{
    if (!slot || !slot->sf || !relation_name) return NULL;

    RelationEntry *entry = schema_find_relation_stat(slot->sf, relation_name);
    if (!entry) return NULL;
    uint8_t page_no = entry->page_no;

    pthread_mutex_lock(&slot->latch);

    /* Hit path */
    for (int i = 0; i < PB_INNER_SLOTS; i++) {
        if (slot->dir[i].is_resident && slot->dir[i].page_no == page_no) {
            inner_lru_bump(slot, i);
            slot->inner[i].pin_count++;
            RelationDef *rel = &slot->inner[i].def;
            pthread_mutex_unlock(&slot->latch);
            return rel;
        }
    }

    /* Miss path — walk inner LRU from the tail; the first frame that's
     * either empty or unpinned-and-evictable wins (same case, not two —
     * an empty frame is trivially easy to evict, nothing to flush). */
    int target = -1;
    for (int p = PB_INNER_SLOTS - 1; p >= 0; p--) {
        int i = slot->inner_lru_order[p];
        if (slot->inner[i].pin_count > 0) continue;   /* implies occupied */
        if (slot->dir[i].is_resident) {
            /* Evict. No flush needed this phase — write-through means
             * is_dirty is never set yet (Phase 3 gives this real work). */
            slot->dir[i].is_resident = 0;
        }
        target = i;
        break;
    }

    if (target == -1) {
        pthread_mutex_unlock(&slot->latch);
        return NULL;   /* every inner frame pinned — fail-fast */
    }

    if (schema_load_relation_page(slot->sf, page_no, &slot->inner[target].def)
        != MYDB_OK) {
        pthread_mutex_unlock(&slot->latch);
        return NULL;
    }
    slot->inner[target].is_dirty    = 0;
    slot->inner[target].fpi_pending = 0;
    slot->dir[target].page_no       = page_no;
    slot->dir[target].is_resident   = 1;
    inner_lru_bump(slot, target);
    slot->inner[target].pin_count   = 1;

    RelationDef *rel = &slot->inner[target].def;
    pthread_mutex_unlock(&slot->latch);
    return rel;
}

int pb_unpin_relation(PBOuterSlot *slot, const RelationDef *rel)
{
    if (!slot || !rel) return MYDB_ERR;

    pthread_mutex_lock(&slot->latch);
    /* def is the first member of PBInnerFrame, so this cast recovers the
     * enclosing frame — well-defined in C (a pointer to a structure,
     * suitably converted, points to its initial member, and vice versa). */
    const PBInnerFrame *frame = (const PBInnerFrame *)rel;
    int idx = (int)(frame - slot->inner);
    if (idx < 0 || idx >= PB_INNER_SLOTS) {
        pthread_mutex_unlock(&slot->latch);
        return MYDB_ERR;
    }
    if (slot->inner[idx].pin_count > 0) slot->inner[idx].pin_count--;
    pthread_mutex_unlock(&slot->latch);
    return MYDB_OK;
}

void pb_invalidate_page(PBOuterSlot *slot, uint8_t page_no)
{
    if (!slot) return;
    pthread_mutex_lock(&slot->latch);
    for (int i = 0; i < PB_INNER_SLOTS; i++) {
        if (slot->dir[i].is_resident && slot->dir[i].page_no == page_no) {
            slot->dir[i].is_resident = 0;
            break;
        }
    }
    pthread_mutex_unlock(&slot->latch);
}
