#include "partition_buffer.h"
#include "schema_file.h"
#include "storage.h"

#include <string.h>
#include <stdlib.h>

int pb_init(PartitionBuffer *pb)
{
    if (!pb) return MYDB_ERR;
    memset(pb, 0, sizeof(*pb));   /* all slots[] = NULL, n_loaded = 0 */
    return MYDB_OK;
}

SchemaFile *pb_active_schema(PartitionBuffer *pb)
{
    if (!pb || pb->n_loaded == 0) return NULL;
    return pb->slots[pb->lru_order[0]];   /* pointer, not address-of */
}

const char *pb_active_schema_name(const PartitionBuffer *pb)
{
    if (!pb || pb->n_loaded == 0) return NULL;
    return pb->slots[pb->lru_order[0]]->header.schema_name;
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
    /* Only iterate slots tracked in lru_order — safe on zero-initialized
     * state where n_loaded=0. */
    for (int i = 0; i < pb->n_loaded; i++) {
        int slot = pb->lru_order[i];
        SchemaFile *sf = pb->slots[slot];
        if (sf) {
            if (sf->fd > 0) schema_close(sf);
            free(sf);
            pb->slots[slot] = NULL;
        }
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
    if (!pb || !schema_name) return NULL;
    for (int i = 0; i < PARTITION_BUFFER_SLOTS; i++) {
        if (pb->slots[i] &&
            strncmp(pb->slots[i]->header.schema_name, schema_name, 32) == 0)
            return pb->slots[i];
    }
    return NULL;
}

/* Get-or-load the SchemaFile for schema_name, marking it MRU.
 * On a miss with all slots full, evicts the LRU slot (flush dirty pages,
 * close the handle, free) and reuses it. */
SchemaFile *pb_get(PartitionBuffer *pb,
                   const char *schema_name,
                   const char *schema_path,
                   StorageEngine *se)
{
    if (!pb || !schema_name || !schema_path) return NULL;

    /* Cache hit — bump to MRU and return. */
    for (int i = 0; i < PARTITION_BUFFER_SLOTS; i++) {
        if (pb->slots[i] &&
            strncmp(pb->slots[i]->header.schema_name, schema_name, 32) == 0) {
            lru_bump(pb, i);
            return pb->slots[i];
        }
    }

    /* Miss — open the schema file. */
    SchemaFile *sf = (SchemaFile *)calloc(1, sizeof(SchemaFile));
    if (!sf) return NULL;
    if (schema_open(schema_path, sf) != MYDB_OK) {
        free(sf);
        return NULL;
    }

    int slot;
    if (pb->n_loaded < PARTITION_BUFFER_SLOTS) {
        /* Free slot available. */
        slot = -1;
        for (int i = 0; i < PARTITION_BUFFER_SLOTS; i++)
            if (pb->slots[i] == NULL) { slot = i; break; }
        /* Shift lru_order right, insert new slot at MRU. */
        for (int i = pb->n_loaded; i > 0; i--)
            pb->lru_order[i] = pb->lru_order[i - 1];
        pb->lru_order[0] = slot;
        pb->n_loaded++;
    } else {
        /* Full — evict the LRU slot (tail of lru_order). */
        slot = pb->lru_order[pb->n_loaded - 1];
        if (pb->slots[slot]) {
            storage_flush_all_dirty(se);   /* persist dirty pages before forget */
            if (pb->slots[slot]->fd > 0) schema_close(pb->slots[slot]);
            free(pb->slots[slot]);
            pb->slots[slot] = NULL;
        }
        lru_bump(pb, slot);                /* LRU slot becomes MRU */
    }

    pb->slots[slot] = sf;
    return sf;
}

/* Evict schema_name from the cache if present (close handle, free, compact).
 * Used when a schema is dropped.  No-op if not cached. */
int pb_remove(PartitionBuffer *pb, const char *schema_name)
{
    if (!pb || !schema_name) return MYDB_ERR;

    int slot = -1;
    for (int i = 0; i < PARTITION_BUFFER_SLOTS; i++) {
        if (pb->slots[i] &&
            strncmp(pb->slots[i]->header.schema_name, schema_name, 32) == 0) {
            slot = i;
            break;
        }
    }
    if (slot < 0) return MYDB_OK;          /* not cached */

    if (pb->slots[slot]->fd > 0) schema_close(pb->slots[slot]);
    free(pb->slots[slot]);
    pb->slots[slot] = NULL;

    /* Drop slot from lru_order and compact the tail. */
    int p = 0;
    while (p < pb->n_loaded && pb->lru_order[p] != slot) p++;
    for (int i = p; i < pb->n_loaded - 1; i++)
        pb->lru_order[i] = pb->lru_order[i + 1];
    pb->n_loaded--;
    return MYDB_OK;
}
