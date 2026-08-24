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

    ctx->partition_id = partition_id;
    strncpy(ctx->partition_path, partition_path,
            sizeof(ctx->partition_path) - 1);

    /* Initialise the embedded StorageEngine (one per PartitionCtx). */
    if (storage_init(&ctx->storage) != MYDB_OK) {
        free(ctx);
        return NULL;
    }

    /* Storage only needs the partition root once — the per-relation schema
     * travels in rel->owner_schema, so there is no active-schema context. */
    storage_set_context(&ctx->storage, partition_path);

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
    return pb_open_catalog(ctx->schema_cache, cat_path);
}

Catalog *pctx_catalog(PartitionCtx *ctx)
{
    if (!ctx) return NULL;
    return pb_catalog(ctx->schema_cache);
}

int pctx_close(PartitionCtx *ctx)
{
    if (!ctx) return MYDB_OK;

    /* storage_shutdown must precede pb_destroy — it flushes dirty pages
     * for all open tables before we close their SchemaFile handles. */
    storage_shutdown(&ctx->storage);

    if (ctx->schema_cache) {
        /* pb_flush_all is a safety net; storage_shutdown above already
         * flushed everything. pb_destroy closes PB[0]'s Catalog too
         * (Phase 4) — no separate cat_close needed. */
        pb_flush_all(ctx->schema_cache, &ctx->storage);
        pb_destroy(ctx->schema_cache);
        free(ctx->schema_cache);
        ctx->schema_cache = NULL;
    }

    return MYDB_OK;
}

/* ------------------------------------------------------------------ */
/*  Schema cache accessors                                              */
/* ------------------------------------------------------------------ */

/* The active schema is the one named by ctx->current_schema_name, looked up
 * in Cache 2.  It is NOT simply the MRU slot — switching active schema does
 * not require it to be most-recently-used. */
SchemaFile *pctx_active_schema(PartitionCtx *ctx)
{
    if (!ctx || !ctx->schema_cache) return NULL;
    if (ctx->current_schema_name[0] == '\0') return NULL;
    return pb_find(ctx->schema_cache, ctx->current_schema_name);
}

const char *pctx_active_schema_name(const PartitionCtx *ctx)
{
    if (!ctx || ctx->current_schema_name[0] == '\0') return NULL;
    return ctx->current_schema_name;
}

/* Make schema_name the active schema, loading it into Cache 2 if needed.
 * Multiple schemas stay open simultaneously (LRU); a switch is a cache
 * bump, not a close/reopen.  Storage holds no schema state — each storage
 * call carries its schema in rel->owner_schema. */
SchemaFile *pctx_open_schema(PartitionCtx *ctx,
                              const char *schema_name,
                              const char *schema_path)
{
    if (!ctx || !schema_name || !schema_path) return NULL;

    SchemaFile *sf = pb_get(ctx->schema_cache, schema_name,
                            schema_path, &ctx->storage);
    if (!sf) return NULL;

    strncpy(ctx->current_schema_name, schema_name,
            sizeof(ctx->current_schema_name) - 1);
    ctx->current_schema_name[sizeof(ctx->current_schema_name) - 1] = '\0';

    return sf;
}

/* Mark "no schema active" for this partition.  Does NOT evict from Cache 2 —
 * the schema stays open for fast re-activation.  (Eviction happens via LRU
 * pressure, pctx_evict_schema on DROP, or pctx_close on shutdown.) */
int pctx_deactivate_schema(PartitionCtx *ctx)
{
    if (!ctx) return MYDB_OK;
    ctx->current_schema_name[0] = '\0';
    return MYDB_OK;
}

/* Evict a dropped schema from Cache 2 so no stale handle to a deleted file
 * lingers.  Clears the active marker if it named that schema. */
int pctx_evict_schema(PartitionCtx *ctx, const char *schema_name)
{
    if (!ctx || !ctx->schema_cache || !schema_name) return MYDB_ERR;
    if (strncmp(ctx->current_schema_name, schema_name,
                sizeof(ctx->current_schema_name)) == 0)
        ctx->current_schema_name[0] = '\0';
    return pb_remove(ctx->schema_cache, schema_name);
}

/* Return the active outer slot (PBOuterSlot — dir[]/inner[]/pin_count/latch),
 * or NULL if no schema is open. This is the Phase 2 companion to
 * pctx_active_schema(): most callers still only need the SchemaFile*
 * portion (relations[] directory, header) and keep using that; only the
 * pin/release path (pm_find_relation, pm_release_relation) needs to reach
 * the inner cache, which is why this is a separate accessor rather than a
 * change to pctx_active_schema's return type. */
PBOuterSlot *pctx_active_outer_slot(PartitionCtx *ctx)
{
    if (!ctx || !ctx->schema_cache) return NULL;
    if (ctx->current_schema_name[0] == '\0') return NULL;
    return pb_find_outer_slot(ctx->schema_cache, ctx->current_schema_name);
}

/* Debug-only Phase 1/2 pin/release leak check — see partition_ctx.h. Walks
 * every occupied outer slot (not just the active schema) since a pin taken
 * against a schema that was active earlier in the same statement, then
 * switched away from, would otherwise go unchecked. */
int pctx_debug_no_pinned_relations(const PartitionCtx *ctx)
{
    if (!ctx || !ctx->schema_cache) return 1;

    for (int i = 0; i < PARTITION_BUFFER_SLOTS; i++) {
        const PBOuterSlot *s = &ctx->schema_cache->slots[i];
        if (!s->sf) continue;
        for (int j = 0; j < PB_INNER_SLOTS; j++) {
            if (s->inner[j].pin_count != 0) return 0;
        }
    }
    return 1;
}
