#ifndef PARTITION_CTX_H
#define PARTITION_CTX_H

#include <stdint.h>
#include "common.h"
#include "partition.h"
#include "transaction.h"
#include "schema_file.h"
#include "storage.h"
#include "partition_buffer.h"

#ifdef __cplusplus
extern "C" {
#endif

/*
 * partition_ctx.h — per-partition runtime context (v3).
 *
 * PartitionCtx owns everything that belongs to one active partition:
 *   - StorageEngine storage : stateless I/O layer (BufferPool + OpenTableCache)
 *   - Catalog        catalog : Cache 1 — __catalog.mydb (quota, schema list)
 *   - PartitionBuffer*       : Cache 2 — LRU of open SchemaFile handles
 *   - TransactionManager     : promoted from StorageState; WAL owner post-WAL
 *   - SubConnPool            : slice of the engine's master ConnectionPool
 *
 * One PartitionCtx exists per active partition.
 * Phase 1: one slot (PartitionCtx*) in EngineState.
 * Future: EngineState holds PartitionCtx* array up to MAX_PARTITIONS,
 *         one instance per simultaneously active partition.
 *
 * Engine lifecycle:
 *   ctx = pctx_init(partition_id, path)  → heap-alloc + init storage + catalog
 *   pctx_open_catalog(ctx)               → open __catalog.mydb
 *   pctx_open_schema(ctx, name, path)    → open schema into Cache 2
 *   pctx_close(ctx)                      → flush, close schema cache + catalog,
 *                                          shutdown storage
 *   free(ctx)                            → caller frees the PartitionCtx
 */

/* ------------------------------------------------------------------
 * PartitionCtx
 *
 * The partition is connection-agnostic: it does NOT track which
 * connections use it.  The engine owns connection→partition routing and
 * maintains n_refs (incremented when a connection attaches to this
 * partition, decremented when it detaches); the engine evicts the
 * PartitionCtx when n_refs reaches 0.
 * ------------------------------------------------------------------ */
typedef struct PartitionCtx {
    uint32_t         partition_id;
    char             partition_path[256];

    /* Active schema for the currently-executing statement.  The connection
     * owns its current schema; the engine projects it here per statement
     * (via pctx_open_schema) because one partition is shared by all of its
     * connections.  Read only through pctx_active_schema[_name]() — pm code
     * never touches this field directly. */
    char             current_schema_name[32];

    /* v3: StorageEngine is embedded here (one per partition, no global singleton).
     * storage_init(&ctx->storage) is called by pctx_init.
     * All storage_* calls pass &ctx->storage explicitly. */
    StorageEngine    storage;

    Catalog          catalog;       /* Cache 1: __catalog.mydb */

    /* Cache 2: heap-allocated PartitionBuffer (LRU of SchemaFile handles).
     * Allocated by pctx_init, freed by pctx_close. */
    PartitionBuffer *schema_cache;

    TransactionManager txn_mgr;    /* promoted from StorageState; WAL owner post-WAL */

    /* Number of connections currently attached to this partition.  Owned and
     * maintained by the engine; the partition itself never reads it.  When it
     * reaches 0 the engine flushes + closes + frees this PartitionCtx. */
    int              n_refs;
} PartitionCtx;


/* ------------------------------------------------------------------
 * Lifecycle
 * ------------------------------------------------------------------ */

/* Allocate and zero-initialise a PartitionCtx on the heap.
 * Calls storage_init(&ctx->storage) and heap-allocates the PartitionBuffer.
 * Does NOT open the catalog — call pctx_open_catalog() next.
 *
 * Returns a heap-allocated PartitionCtx* on success, NULL on OOM or error.
 * Caller frees with:  pctx_close(ctx);  free(ctx); */
PartitionCtx *pctx_init(uint32_t partition_id, const char *partition_path);

/* Open __catalog.mydb into ctx->catalog.
 * Returns MYDB_OK or MYDB_ERR_* on I/O failure. */
int pctx_open_catalog(PartitionCtx *ctx);

/* Flush all dirty pages (via ctx->storage), close all SchemaFile handles,
 * free the PartitionBuffer, close the catalog, and call storage_shutdown.
 * Safe to call on a partially-initialised ctx. */
int pctx_close(PartitionCtx *ctx);


/* ------------------------------------------------------------------
 * Schema cache accessors (Cache 2)
 * The engine uses these instead of touching schema_cache directly.
 * ------------------------------------------------------------------ */

/* Open schema_name from schema_path into the PartitionBuffer.
 * Phase 2: single slot (slot 0); no LRU eviction yet (Phase 3 adds pb_get).
 * Calls storage_set_context() on ctx->storage so path-building in
 * storage.c reflects the new active schema.
 * Returns the opened SchemaFile* on success, NULL on I/O error. */
SchemaFile *pctx_open_schema(PartitionCtx *ctx,
                              const char *schema_name,
                              const char *schema_path);

/* Mark "no schema active" for this partition.  Does NOT evict from Cache 2;
 * the schema stays open for fast re-activation. */
int pctx_deactivate_schema(PartitionCtx *ctx);

/* Evict a dropped schema from Cache 2 (close handle + free).  Clears the
 * active marker if it named that schema.  No-op if not cached. */
int pctx_evict_schema(PartitionCtx *ctx, const char *schema_name);

/* Return the active SchemaFile*, or NULL if no schema is open. */
SchemaFile *pctx_active_schema(PartitionCtx *ctx);

/* Return the active schema's name, or NULL if no schema is open. */
const char *pctx_active_schema_name(const PartitionCtx *ctx);

/* Debug-only leak check for the Phase 1 pin/release discipline
 * (PARTITION_BUFFER_DESIGN.md, pm_find_relation_const()/pm_release_relation()):
 * returns 1 if every SchemaFile currently held in this partition's schema
 * cache has pin_count[i] == 0 for every slot, 0 if any slot is still
 * pinned. Intended for `assert(pctx_debug_no_pinned_relations(ctx))` right
 * after a statement finishes — a failure here means some pm_find_relation_const()
 * call site is missing its matching release (a RelationGuard going out of
 * scope, or a direct pm_release_relation() call). Safe to call with a NULL
 * or partially-initialised ctx (returns 1 — nothing to leak). */
int pctx_debug_no_pinned_relations(const PartitionCtx *ctx);

#ifdef __cplusplus
}
#endif

#endif /* PARTITION_CTX_H */
