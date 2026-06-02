#ifndef PARTITION_BUFFER_H
#define PARTITION_BUFFER_H

#include "schema_file.h"
#include "storage.h"

#ifdef __cplusplus
extern "C" {
#endif

/*
 * partition_buffer.h — Cache 2: LRU cache of open SchemaFile handles.
 *
 * In v2, EngineState held a single active_schema slot.  USE forced a
 * flush-and-close of the old schema before opening the new one.
 *
 * PartitionBuffer replaces that with an 8-slot LRU cache.  Connections
 * on the same partition switching between schemas pay no file-open cost
 * after the first access.
 *
 * Eviction policy (Phase 3 — pb_get):
 *   On a cache miss when all slots are full, the LRU slot is evicted:
 *     1. storage_flush_all_dirty(se) for that schema's tables in BufferPool
 *     2. schema_close() on the SchemaFile handle
 *     3. The slot is reused for the new schema.
 *
 * lru_order[0] = index of MRU slot, lru_order[n_loaded-1] = LRU slot.
 * Empty slots (sf == NULL) are not tracked in lru_order.
 *
 * Phase 2: single-slot usage only — pb_get is a stub (Phase 3).
 * Engine directly places opened SchemaFile* into slot 0 via
 * pctx_open_schema(); pb_get with full LRU eviction lands in Phase 3.
 */

#define PARTITION_BUFFER_SLOTS  8

/* Named struct tag so partition_ctx.h can forward-declare it as
 * `struct PartitionBuffer` without including this header. */
typedef struct PartitionBuffer {
    SchemaFile *slots[PARTITION_BUFFER_SLOTS];   /* NULL = empty; heap-allocated */
    int         lru_order[PARTITION_BUFFER_SLOTS]; /* indices into slots[] */
    int         n_loaded;
} PartitionBuffer;

/* Initialise all slots to NULL.  Must be called before any other pb_* function. */
int pb_init(PartitionBuffer *pb);

/* Return a pointer to the MRU SchemaFile, or NULL if the cache is empty. */
SchemaFile *pb_active_schema(PartitionBuffer *pb);

/* Return the schema_name of the MRU slot, or NULL if the cache is empty. */
const char *pb_active_schema_name(const PartitionBuffer *pb);

/* Flush all dirty pages in the buffer pool for every cached schema.
 * se is the StorageEngine owned by the same PartitionCtx.
 * Does not close any SchemaFile handles. */
int pb_flush_all(PartitionBuffer *pb, StorageEngine *se);

/* Close all open SchemaFile handles and free heap-allocated SchemaFile objects.
 * Safe to call on a partially-initialised PartitionBuffer (n_loaded=0 is a
 * no-op). */
void pb_destroy(PartitionBuffer *pb);

/* Full LRU get — Phase 3.
 * Returns the cached or newly-opened SchemaFile for schema_name.
 * Evicts the LRU slot (calling storage_flush_all_dirty + schema_close) if
 * all PARTITION_BUFFER_SLOTS are occupied. */
SchemaFile *pb_get(PartitionBuffer *pb,
                   const char *schema_name,
                   const char *schema_path,
                   StorageEngine *se);

#ifdef __cplusplus
}
#endif

#endif /* PARTITION_BUFFER_H */
