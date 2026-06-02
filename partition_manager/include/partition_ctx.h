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
 * SubConnPool — slice of the engine's master ConnectionPool.
 *
 * Holds Connection* pointers into EngineState.conn_pool.conns[].
 * The Connection objects live in the master pool; this sub-pool only
 * holds references.  A connection is registered here on login and
 * removed on logout / connection close.
 *
 * Phase 1: capacity = 1.
 * ------------------------------------------------------------------ */
#define MAX_SUBCONN  1   /* Phase 1 */

/* Forward declaration — full definition in engine/include/connection.h.
 * partition_ctx.h does not include connection.h to avoid a header cycle
 * (engine.h includes both).  Implementation files include both. */
struct Connection;

typedef struct {
    struct Connection  *conns[MAX_SUBCONN];
    int                 n_active;
} SubConnPool;


/* ------------------------------------------------------------------
 * PartitionCtx
 * ------------------------------------------------------------------ */
typedef struct PartitionCtx {
    uint32_t         partition_id;
    char             partition_path[256];

    /* Temporary bridge field — removed in Phase 3 when
     * pctx_active_schema_name() replaces all direct reads. */
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

    SubConnPool      sub_pool;     /* references into engine's master pool */
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

/* Register / remove a Connection* in the sub-pool. */
int pctx_add_conn(PartitionCtx *ctx, struct Connection *conn);
int pctx_remove_conn(PartitionCtx *ctx, struct Connection *conn);


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

/* Flush dirty pages and close the currently active schema (if any).
 * Clears current_schema_name and calls storage_clear_schema(). */
int pctx_deactivate_schema(PartitionCtx *ctx);

/* Return the active SchemaFile*, or NULL if no schema is open. */
SchemaFile *pctx_active_schema(PartitionCtx *ctx);

/* Return the active schema's name, or NULL if no schema is open. */
const char *pctx_active_schema_name(const PartitionCtx *ctx);

#ifdef __cplusplus
}
#endif

#endif /* PARTITION_CTX_H */
