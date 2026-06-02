#include "partition_ctx.h"
#include "partition_buffer.h"
#include "partition.h"
#include "transaction.h"
#include "storage.h"
#include "common.h"

#include <string.h>
#include <stdio.h>
#include <stdlib.h>

/* ------------------------------------------------------------------ */
/*  Lifecycle                                                           */
/* ------------------------------------------------------------------ */

PartitionCtx *pctx_init(uint32_t partition_id, const char *partition_path)
{
    if (!partition_path) return NULL;

    PartitionCtx *ctx = (PartitionCtx *)calloc(1, sizeof(PartitionCtx));
    if (!ctx) return NULL;

    ctx->catalog.fd   = -1;
    ctx->partition_id = partition_id;
    strncpy(ctx->partition_path, partition_path,
            sizeof(ctx->partition_path) - 1);

    /* Initialise the embedded StorageEngine (one per PartitionCtx). */
    if (storage_init(&ctx->storage) != MYDB_OK) {
        free(ctx);
        return NULL;
    }

    /* Initialise the TransactionManager with the storage engine's BufferPool.
     * storage_init must run first so ctx->storage.bp is ready. */
    trx_init(&ctx->txn_mgr, &ctx->storage.bp);

    /* Heap-allocate the PartitionBuffer (Cache 2 — LRU SchemaFile cache). */
    ctx->schema_cache = (PartitionBuffer *)calloc(1, sizeof(PartitionBuffer));
    if (!ctx->schema_cache) {
        storage_shutdown(&ctx->storage);
        free(ctx);
        return NULL;
    }
    pb_init(ctx->schema_cache);

    return ctx;
}

int pctx_open_catalog(PartitionCtx *ctx)
{
    if (!ctx) return MYDB_ERR;
    char cat_path[512];
    int n = snprintf(cat_path, sizeof(cat_path),
                     "%s/__catalog.mydb", ctx->partition_path);
    if (n < 0 || (size_t)n >= sizeof(cat_path)) return MYDB_ERR;
    return cat_open(cat_path, &ctx->catalog);
}

int pctx_close(PartitionCtx *ctx)
{
    if (!ctx) return MYDB_OK;

    /* storage_shutdown must precede pb_destroy — it flushes dirty pages
     * for all open tables before we close their SchemaFile handles. */
    storage_shutdown(&ctx->storage);

    if (ctx->schema_cache) {
        /* pb_flush_all is a safety net; storage_shutdown above already
         * flushed everything. */
        pb_flush_all(ctx->schema_cache, &ctx->storage);
        pb_destroy(ctx->schema_cache);
        free(ctx->schema_cache);
        ctx->schema_cache = NULL;
    }

    if (ctx->catalog.fd > 0)
        cat_close(&ctx->catalog);

    return MYDB_OK;
}

int pctx_add_conn(PartitionCtx *ctx, struct Connection *conn)
{
    if (!ctx || !conn) return MYDB_ERR;
    if (ctx->sub_pool.n_active >= MAX_SUBCONN) return MYDB_ERR_FULL;
    ctx->sub_pool.conns[ctx->sub_pool.n_active++] = conn;
    return MYDB_OK;
}

int pctx_remove_conn(PartitionCtx *ctx, struct Connection *conn)
{
    if (!ctx || !conn) return MYDB_ERR;
    for (int i = 0; i < ctx->sub_pool.n_active; i++) {
        if (ctx->sub_pool.conns[i] == conn) {
            ctx->sub_pool.conns[i] =
                ctx->sub_pool.conns[--ctx->sub_pool.n_active];
            ctx->sub_pool.conns[ctx->sub_pool.n_active] = NULL;
            return MYDB_OK;
        }
    }
    return MYDB_ERR_NOT_FOUND;
}

/* ------------------------------------------------------------------ */
/*  Schema cache accessors                                              */
/* ------------------------------------------------------------------ */

SchemaFile *pctx_active_schema(PartitionCtx *ctx)
{
    if (!ctx || !ctx->schema_cache) return NULL;
    return pb_active_schema(ctx->schema_cache);
}

const char *pctx_active_schema_name(const PartitionCtx *ctx)
{
    if (!ctx || !ctx->schema_cache) return NULL;
    return pb_active_schema_name(ctx->schema_cache);
}

SchemaFile *pctx_open_schema(PartitionCtx *ctx,
                              const char *schema_name,
                              const char *schema_path)
{
    if (!ctx || !schema_name || !schema_path) return NULL;

    /* Close any currently active schema (Phase 2: single slot). */
    pctx_deactivate_schema(ctx);

    /* Allocate a SchemaFile on the heap and open it. */
    SchemaFile *sf = (SchemaFile *)calloc(1, sizeof(SchemaFile));
    if (!sf) return NULL;

    if (schema_open(schema_path, sf) != MYDB_OK) {
        free(sf);
        return NULL;
    }

    /* Place in slot 0 of the PartitionBuffer.
     * Phase 3 replaces this with pb_get() and full LRU eviction. */
    ctx->schema_cache->slots[0]     = sf;
    ctx->schema_cache->lru_order[0] = 0;
    ctx->schema_cache->n_loaded     = 1;

    strncpy(ctx->current_schema_name, schema_name,
            sizeof(ctx->current_schema_name) - 1);
    ctx->current_schema_name[sizeof(ctx->current_schema_name) - 1] = '\0';

    /* Notify the storage engine of the new filesystem context so that
     * build_path() inside storage.c produces correct table file paths. */
    storage_set_context(&ctx->storage, ctx->partition_path, schema_name);

    return sf;
}

int pctx_deactivate_schema(PartitionCtx *ctx)
{
    if (!ctx || !ctx->schema_cache) return MYDB_OK;

    PartitionBuffer *pb = ctx->schema_cache;
    if (pb->n_loaded == 0) return MYDB_OK;

    SchemaFile *sf = pb->slots[pb->lru_order[0]];
    if (sf) {
        if (sf->fd > 0) schema_close(sf);
        free(sf);
        pb->slots[pb->lru_order[0]] = NULL;
    }
    pb->n_loaded = 0;

    ctx->current_schema_name[0] = '\0';
    storage_clear_schema(&ctx->storage);
    return MYDB_OK;
}
