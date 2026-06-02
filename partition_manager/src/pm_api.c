/*
 * pm_api.c — partition_manager public API.
 *
 * This file contains all session-aware logic that lived in the v2
 * storage.c but does not belong in a stateless storage engine:
 *
 *   - Schema DDL   (pm_create_schema, pm_drop_schema)
 *   - Table DDL    (pm_create_table, pm_drop_table, pm_add_index)
 *   - DML          (pm_insert, pm_update, pm_delete)
 *   - DQL wrappers (pm_scan, pm_get_by_pk, ...)
 *   - TCL          (pm_begin, pm_commit, pm_rollback)
 *   - Statistics   (pm_analyze_table)
 *   - Helpers      (pm_find_relation, pm_find_relation_const)
 *
 * Every pm_* function receives a PartitionCtx* which carries:
 *   ctx->storage    — StorageEngine (stateless I/O, one per partition)
 *   ctx->catalog    — Catalog (__catalog.mydb, quota + schema list)
 *   ctx->txn_mgr    — TransactionManager (begin/commit/rollback)
 *   pctx_active_schema(ctx) → SchemaFile* (__schema.mydb, RelationDef pages)
 *
 * The execution engine never calls storage_* directly; it calls pm_*.
 */

#include "pm_api.h"
#include "partition_ctx.h"
#include "partition.h"
#include "schema_file.h"
#include "transaction.h"
#include "stats.h"
#include "storage.h"
#include "common.h"
#include "btree.h"

#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <errno.h>
#include <sys/stat.h>
#include <sys/types.h>

/* ------------------------------------------------------------------ */
/*  Private helpers                                                     */
/* ------------------------------------------------------------------ */

/* Return the index of the column named col_name in r, or -1. */
static int find_col_idx(const RelationDef *r, const char *col_name)
{
    for (int i = 0; i < r->num_columns; i++)
        if (strncmp(r->columns[i].name, col_name, MAX_COLUMN_NAME) == 0)
            return i;
    return -1;
}

/* Compare two Values of the same type via their encoded key form. */
static int value_compare(const Value *a, const Value *b)
{
    uint8_t  ab[MAX_VARCHAR_LEN + 2], bb[MAX_VARCHAR_LEN + 2];
    uint16_t alen = btree_key_encode(a, ab);
    uint16_t blen = btree_key_encode(b, bb);
    return btree_key_compare(ab, alen, bb, blen, a->type);
}

/* ------------------------------------------------------------------ */
/*  Quota helpers                                                       */
/*                                                                      */
/*  Pre-check quota before allocating pages.  Returns MYDB_ERR_FULL    */
/*  if the partition would exceed its quota.  Analysts (no partition)  */
/*  pass through unconditionally — they have no quota to enforce.      */
/* ------------------------------------------------------------------ */

static int quota_headroom(PartitionCtx *ctx, uint32_t headroom_pages)
{
    Catalog *c = &ctx->catalog;
    if (c->fd < 0) return MYDB_OK;   /* analyst: no quota to enforce */
    if (c->header.used_bytes > c->header.quota_bytes) return MYDB_ERR_FULL;
    uint64_t need = (uint64_t)headroom_pages * PAGE_SIZE;
    if (c->header.quota_bytes - c->header.used_bytes < need)
        return MYDB_ERR_FULL;
    return MYDB_OK;
}

#define DML_QUOTA_HEADROOM_PAGES 4

/* ------------------------------------------------------------------ */
/*  Quota reconciliation after DML                                      */
/*                                                                      */
/*  A successful DML may have grown the relation file via B+ tree       */
/*  splits.  The pre/post difference in storage_table_page_count tells */
/*  us by how much.  We bump RelationEntry.num_pages so __schema.mydb  */
/*  stays the persisted source of truth, and call cat_track_alloc so   */
/*  __catalog.mydb's used_bytes stays consistent with disk consumption. */
/*                                                                      */
/*  Note: tree_height update requires access to the BTree handle inside */
/*  StorageEngine (not exposed in Phase 2).  It will be added via a    */
/*  storage_table_tree_height() accessor in a future phase.            */
/* ------------------------------------------------------------------ */

static void reconcile_growth(PartitionCtx *ctx, const char *relation_name,
                              uint32_t pages_before)
{
    const char *schema = pctx_active_schema_name(ctx);
    uint32_t pages_after =
        storage_table_page_count(&ctx->storage, schema, relation_name);
    if (pages_after == pages_before) return;
    int32_t delta = (int32_t)(pages_after - pages_before);

    SchemaFile *sf = pctx_active_schema(ctx);
    if (sf) {
        schema_bump_relation_pages(sf, relation_name, delta);
        cat_track_alloc(&ctx->catalog, (int64_t)delta * PAGE_SIZE);
    }
}

/* ------------------------------------------------------------------ */
/*  Auto-commit helpers                                                 */
/*                                                                      */
/*  Every DML (INSERT, UPDATE, DELETE) wraps its storage write with     */
/*  autocommit_begin / autocommit_end.                                  */
/*                                                                      */
/*  Two modes:                                                          */
/*    Explicit txn  — user issued BEGIN (trx_is_active returns 1).     */
/*                    We do nothing; the user drives commit/rollback.   */
/*    Auto-commit   — no explicit BEGIN.  We open a transaction before  */
/*                    the write and close it right after.  Commit on    */
/*                    success, rollback on failure.                     */
/*                                                                      */
/*  Returns 1 if we started the transaction (auto-commit mode),        */
/*  0 if the caller is inside an explicit transaction.                  */
/* ------------------------------------------------------------------ */

static int autocommit_begin(PartitionCtx *ctx)
{
    if (trx_is_active(&ctx->txn_mgr)) return 0;
    trx_begin(&ctx->txn_mgr);
    return 1;
}

static void autocommit_end(PartitionCtx *ctx, int auto_txn, int rc)
{
    if (!auto_txn) return;
    if (rc == MYDB_OK) pm_commit(ctx);
    else               pm_rollback(ctx);
}

/* ------------------------------------------------------------------ */
/*  FK constraint helpers                                               */
/* ------------------------------------------------------------------ */

/* On INSERT / UPDATE: verify every FK value in row exists in the
 * referenced relation's clustered index. NULL FK values are skipped
 * (NULL satisfies any FK constraint). */
static int fk_check_ref_exists(PartitionCtx *ctx,
                                const RelationDef *r, const Row *row)
{
    for (int i = 0; i < r->num_foreign_keys; i++) {
        const ForeignKey *fk = &r->foreign_keys[i];

        int fk_col = find_col_idx(r, fk->column_name);
        if (fk_col < 0) return MYDB_ERR;

        if (row->cols[fk_col].is_null) continue;

        RelationDef *ref = pm_find_relation(ctx, fk->ref_relation_name);
        if (!ref) return MYDB_ERR_FK_VIOLATION;

        Value fk_val = row->cols[fk_col];
        Row *ref_row = storage_get_by_pk(&ctx->storage, ref, &fk_val);
        if (!ref_row) return MYDB_ERR_FK_VIOLATION;
    }
    return MYDB_OK;
}

/* Forward declaration — fk_apply_on_delete calls pm_delete / pm_update. */
static int fk_apply_on_delete(PartitionCtx *ctx,
                               const RelationDef *r, const Value *pk_val);

/* fk_check_not_referenced — pure RESTRICT check used by the UPDATE path.
 * Returns MYDB_ERR_FK_VIOLATION if any referencing row exists. */
static int fk_check_not_referenced(PartitionCtx *ctx,
                                    const RelationDef *r, const Value *pk_val)
{
    const char *pk_col_name = r->columns[r->pk_col_idx].name;
    SchemaFile *sf = pctx_active_schema(ctx);
    if (!sf) return MYDB_ERR;

    for (int i = 0; i < MAX_RELATIONS_PER_SCHEMA; i++) {
        if (!sf->relations[i].is_valid) continue;
        RelationDef *ref_rel = &sf->defs[i];

        for (int j = 0; j < ref_rel->num_foreign_keys; j++) {
            const ForeignKey *fk = &ref_rel->foreign_keys[j];

            if (strncmp(fk->ref_relation_name, r->relation_name,
                        MAX_TABLE_NAME) != 0) continue;
            if (strncmp(fk->ref_column_name, pk_col_name,
                        MAX_COLUMN_NAME) != 0) continue;

            int fk_col = find_col_idx(ref_rel, fk->column_name);
            if (fk_col < 0) continue;

            Cursor *cur = storage_scan(&ctx->storage, ref_rel);
            if (!cur) return MYDB_ERR;

            Row *row;
            while ((row = cursor_next(cur)) != NULL) {
                if (value_compare(&row->cols[fk_col], pk_val) == 0) {
                    cursor_close(cur);
                    return MYDB_ERR_FK_VIOLATION;
                }
            }
            cursor_close(cur);
        }
    }
    return MYDB_OK;
}

/* (FK_ACTION_MAX_ROWS removed — RID collection now uses a heap-allocated
 * growing array, so there is no per-operation row limit.) */

/*
 * fk_apply_on_delete — enforce ON DELETE actions for every FK that
 * references relation `r` at primary key `pk_val`.
 *
 * RESTRICT (0) : return MYDB_ERR_FK_VIOLATION if any referencing row exists.
 * CASCADE  (1) : delete all referencing rows (recursive — may cascade further).
 * SET_NULL (2) : set the FK column to NULL in all referencing rows.
 *
 * Called only from pm_delete. The UPDATE path always uses
 * fk_check_not_referenced (RESTRICT-only) because ON UPDATE actions
 * are out of scope for Phase 1.
 */
static int fk_apply_on_delete(PartitionCtx *ctx,
                               const RelationDef *r, const Value *pk_val)
{
    const char *pk_col_name = r->columns[r->pk_col_idx].name;
    SchemaFile *sf = pctx_active_schema(ctx);
    if (!sf) return MYDB_ERR;

    for (int i = 0; i < MAX_RELATIONS_PER_SCHEMA; i++) {
        if (!sf->relations[i].is_valid) continue;
        RelationDef *ref_rel = &sf->defs[i];

        for (int j = 0; j < ref_rel->num_foreign_keys; j++) {
            const ForeignKey *fk = &ref_rel->foreign_keys[j];

            if (strncmp(fk->ref_relation_name, r->relation_name,
                        MAX_TABLE_NAME) != 0) continue;
            if (strncmp(fk->ref_column_name, pk_col_name,
                        MAX_COLUMN_NAME) != 0) continue;

            int fk_col = find_col_idx(ref_rel, fk->column_name);
            if (fk_col < 0) continue;

            uint8_t action = fk->on_delete_action;

            /* ---- RESTRICT: scan and fail on first match ---- */
            if (action == FK_ON_DELETE_RESTRICT) {
                Cursor *cur = storage_scan(&ctx->storage, ref_rel);
                if (!cur) return MYDB_ERR;

                Row *row;
                while ((row = cursor_next(cur)) != NULL) {
                    if (value_compare(&row->cols[fk_col], pk_val) == 0) {
                        cursor_close(cur);
                        return MYDB_ERR_FK_VIOLATION;
                    }
                }
                cursor_close(cur);
                continue;
            }

            /* ---- CASCADE / SET_NULL: collect matching RIDs first,
             *     then apply the action outside the scan loop to avoid
             *     cursor invalidation while the table is being mutated.
             *
             *     Heap-allocated growing array — no arbitrary row cap.
             *     Starts at 64, doubles on overflow.  Freed before every
             *     return path (success or failure). ---- */
            int   rid_cap = 64;
            int   nfound  = 0;
            RID  *rids    = (RID *)malloc((size_t)rid_cap * sizeof(RID));
            if (!rids) return MYDB_ERR;

            Cursor *cur = storage_scan(&ctx->storage, ref_rel);
            if (!cur) { free(rids); return MYDB_ERR; }

            Row *row;
            while ((row = cursor_next(cur)) != NULL) {
                if (value_compare(&row->cols[fk_col], pk_val) != 0) continue;

                /* Grow the array if full */
                if (nfound == rid_cap) {
                    rid_cap *= 2;
                    RID *tmp = (RID *)realloc(rids,
                                              (size_t)rid_cap * sizeof(RID));
                    if (!tmp) {
                        cursor_close(cur);
                        free(rids);
                        return MYDB_ERR;
                    }
                    rids = tmp;
                }
                rids[nfound++] = row->rid;
            }
            cursor_close(cur);

            /* Apply the action for every collected RID.
             * Use fk_rc + break so rids is always freed before returning. */
            int fk_rc = MYDB_OK;
            for (int k = 0; k < nfound; k++) {
                if (action == FK_ON_DELETE_CASCADE) {
                    /* Recursive: pm_delete may itself cascade further */
                    fk_rc = pm_delete(ctx, ref_rel, rids[k]);
                    if (fk_rc != MYDB_OK) break;
                } else {
                    /* SET_NULL: re-fetch the row, zero the FK column, update */
                    Row *old_row = storage_get_by_rid(&ctx->storage,
                                                       ref_rel, rids[k]);
                    if (!old_row) { fk_rc = MYDB_ERR; break; }

                    Row updated_row = *old_row;   /* copy */
                    updated_row.cols[fk_col].is_null = 1;

                    fk_rc = pm_update(ctx, ref_rel, rids[k], &updated_row);
                    if (fk_rc != MYDB_OK) break;
                }
            }
            free(rids);
            if (fk_rc != MYDB_OK) return fk_rc;
        }
    }
    return MYDB_OK;
}

/* ------------------------------------------------------------------ */
/*  Schema lookup                                                       */
/* ------------------------------------------------------------------ */

RelationDef *pm_find_relation(PartitionCtx *ctx, const char *relation_name)
{
    if (!ctx || !relation_name) return NULL;
    SchemaFile *sf = pctx_active_schema(ctx);
    if (!sf) return NULL;
    return schema_find_relation(sf, relation_name);
}

const RelationDef *pm_find_relation_const(PartitionCtx *ctx,
                                           const char *relation_name)
{
    return (const RelationDef *)pm_find_relation(ctx, relation_name);
}

/* ------------------------------------------------------------------ */
/*  Schema DDL                                                          */
/* ------------------------------------------------------------------ */

int pm_create_schema(PartitionCtx *ctx, const char *name)
{
    if (!ctx || !name || name[0] == '\0') return MYDB_ERR;
    if (strlen(name) >= 32) return MYDB_ERR;

    /* Owner check: only partition owners can create schemas. Analysts
     * (no partition) get MYDB_ERR_PERM here. */
    if (ctx->catalog.fd < 0) return MYDB_ERR_PERM;

    /* Reject duplicates against the partition catalog. */
    if (cat_find_schema(&ctx->catalog, name) != NULL)
        return MYDB_ERR_DUPLICATE;

    /* Build <partition>/<name>/ and <partition>/<name>/__schema.mydb. */
    char dir[512], path[512];
    int n = snprintf(dir, sizeof(dir), "%s/%s", ctx->partition_path, name);
    if (n < 0 || (size_t)n >= sizeof(dir)) return MYDB_ERR;
    n = snprintf(path, sizeof(path), "%s/__schema.mydb", dir);
    if (n < 0 || (size_t)n >= sizeof(path)) return MYDB_ERR;

    /* mkdir; tolerate EEXIST only if it's already a directory. */
    if (mkdir(dir, 0755) != 0) {
        struct stat st;
        if (errno != EEXIST || stat(dir, &st) != 0 || !S_ISDIR(st.st_mode))
            return MYDB_ERR;
    }

    SchemaFile sf;
    int rc = schema_create(path, ctx->catalog.header.partition_id, name, &sf);
    if (rc != MYDB_OK) {
        rmdir(dir);   /* best-effort cleanup if dir was newly created */
        return rc;
    }
    schema_close(&sf);

    rc = cat_add_schema(&ctx->catalog, name);
    if (rc != MYDB_OK) {
        unlink(path);
        rmdir(dir);
        return rc;
    }
    return MYDB_OK;
}

int pm_drop_schema(PartitionCtx *ctx, const char *name)
{
    if (!ctx || !name || name[0] == '\0') return MYDB_ERR;
    if (ctx->catalog.fd < 0) return MYDB_ERR_PERM;

    /* Reject dropping the currently active schema — caller must USE
     * another database (or none) before dropping this one. */
    const char *active = pctx_active_schema_name(ctx);
    if (active && strncmp(active, name, 32) == 0)
        return MYDB_ERR;   /* exec_drop_database checks and surfaces a message */

    /* Schema must be registered in the partition catalog. */
    if (cat_find_schema(&ctx->catalog, name) == NULL)
        return MYDB_ERR_NOT_FOUND;

    /* If the schema is cached in Cache 2, evict it now so we don't leave a
     * stale SchemaFile handle pointing at files we're about to delete. */
    pctx_evict_schema(ctx, name);

    /* Build schema directory path. */
    char schema_dir[512], schema_file_path[512];
    int n = snprintf(schema_dir, sizeof(schema_dir), "%s/%s",
                     ctx->partition_path, name);
    if (n < 0 || (size_t)n >= sizeof(schema_dir)) return MYDB_ERR;
    n = snprintf(schema_file_path, sizeof(schema_file_path),
                 "%s/__schema.mydb", schema_dir);
    if (n < 0 || (size_t)n >= sizeof(schema_file_path)) return MYDB_ERR;

    /* Open the schema file to enumerate relation files. */
    SchemaFile sf;
    int rc = schema_open(schema_file_path, &sf);
    if (rc != MYDB_OK) return rc;

    /* Compute bytes used so we can credit them back to the quota. */
    uint64_t freed_bytes = schema_compute_size_bytes(&sf);

    /* Close any cached open-table handles for relations in this schema.
     * Since we only have one active schema at a time and we rejected the
     * active case above, no open tables should be from this schema — but
     * close defensively anyway via storage_drop_table. */
    for (int i = 0; i < MAX_RELATIONS_PER_SCHEMA; i++) {
        if (!sf.relations[i].is_valid) continue;

        /* Close the table handle if open (storage_drop_table closes + unlinks). */
        storage_drop_table(&ctx->storage, name, sf.relations[i].relation_name);
    }

    schema_close(&sf);

    /* The schema's optimizer-stats file is NOT here anymore (v3): it lives in
     * the engine-owned pool under system_schema/stats/ and is removed by the
     * executor via sb_remove() after this call.  partition_manager has no
     * visibility into the StatsBuffer, so nothing to unlink in the schema dir. */

    /* Delete the schema metadata file. */
    unlink(schema_file_path);

    /* Remove the now-empty schema directory. */
    rmdir(schema_dir);

    /* Credit freed data pages back to the partition quota. */
    if (freed_bytes > 0)
        cat_track_alloc(&ctx->catalog, -(int64_t)freed_bytes);

    /* Remove the schema slot from __catalog.mydb. */
    return cat_remove_schema(&ctx->catalog, name);
}

/* ------------------------------------------------------------------ */
/*  Table DDL                                                           */
/* ------------------------------------------------------------------ */

int pm_create_table(PartitionCtx *ctx, RelationDef *rel)
{
    if (!ctx || !rel) return MYDB_ERR;
    if (!pctx_active_schema_name(ctx)) return MYDB_ERR_PERM;

    SchemaFile *sf = pctx_active_schema(ctx);
    if (!sf) return MYDB_ERR_PERM;

    if (schema_find_relation(sf, rel->relation_name) != NULL)
        return MYDB_ERR_DUPLICATE;

    /* Pre-check quota: clustered root + one root per secondary index. */
    uint32_t need_pages = 1u + rel->num_secondary_indexes;
    if (quota_headroom(ctx, need_pages) != MYDB_OK) return MYDB_ERR_FULL;

    /* Stamp the owning schema on the freshly-built rel so storage can build
     * its path (this rel is not yet in the SchemaFile, so it was not stamped
     * at load time).  schema_add_relation re-stamps the persisted copy. */
    strncpy(rel->owner_schema, sf->header.schema_name,
            sizeof(rel->owner_schema) - 1);

    /* Create the relation file and allocate its B+ tree root pages.
     * storage_create_table fills in rel->root_page_no. */
    int rc = storage_create_table(&ctx->storage, rel);
    if (rc != MYDB_OK) return rc;

    /* Persist the relation in the active schema. schema_add_relation
     * takes a snapshot of *rel into its defs[] slot — subsequent
     * mutations (auto_incr_counter, etc.) flow through schema_flush_relation. */
    rc = schema_add_relation(sf, rel);
    if (rc != MYDB_OK) return rc;

    /* schema_add_relation already sets tree_height = 1 for the new
     * relation.  Bump num_pages for the root pages just allocated. */
    schema_bump_relation_pages(sf, rel->relation_name, (int32_t)need_pages);

    /* Credit the allocated pages to the partition quota. */
    cat_track_alloc(&ctx->catalog, (int64_t)need_pages * PAGE_SIZE);

    /* Keep the partition catalog's SchemaEntry.num_relations in sync so
     * DESCRIBE PARTITION shows the correct table count. */
    SchemaEntry *se = cat_find_schema(&ctx->catalog, pctx_active_schema_name(ctx));
    if (se) {
        se->num_relations++;
        cat_save(&ctx->catalog);
    }

    return MYDB_OK;
}

int pm_drop_table(PartitionCtx *ctx, RelationDef *rel)
{
    if (!ctx || !rel) return MYDB_ERR;
    if (!pctx_active_schema_name(ctx)) return MYDB_ERR_PERM;

    SchemaFile *sf = pctx_active_schema(ctx);
    if (!sf) return MYDB_ERR_PERM;

    /* Reclaim the partition quota for the file's pages before deleting. */
    RelationEntry *e = schema_find_relation_stat(sf, rel->relation_name);
    if (e && e->num_pages > 0)
        cat_track_alloc(&ctx->catalog, -(int64_t)e->num_pages * PAGE_SIZE);

    int rc = storage_drop_table(&ctx->storage, sf->header.schema_name,
                                rel->relation_name);
    if (rc != MYDB_OK) return rc;

    rc = schema_remove_relation(sf, rel->relation_name);
    if (rc != MYDB_OK) return rc;

    /* Keep the partition catalog's SchemaEntry.num_relations in sync. */
    SchemaEntry *se = cat_find_schema(&ctx->catalog, pctx_active_schema_name(ctx));
    if (se && se->num_relations > 0) {
        se->num_relations--;
        cat_save(&ctx->catalog);
    }

    return MYDB_OK;
}

int pm_add_index(PartitionCtx *ctx, RelationDef *rel, int col_idx)
{
    if (!ctx || !rel) return MYDB_ERR;
    if (!pctx_active_schema_name(ctx)) return MYDB_ERR_PERM;

    SchemaFile *sf = pctx_active_schema(ctx);
    if (!sf) return MYDB_ERR_PERM;

    /* Work through the authoritative in-schema copy */
    RelationDef *r = schema_find_relation(sf, rel->relation_name);
    if (!r) return MYDB_ERR_NOT_FOUND;

    if (quota_headroom(ctx, 1) != MYDB_OK) return MYDB_ERR_FULL;

    /* Allocate root page, wire up BTree handle, and backfill all existing
     * rows into the new secondary index.  storage_add_index mutates r
     * in-place (sets secondary_root_page_no, increments num_secondary_indexes). */
    int rc = storage_add_index(&ctx->storage, r, col_idx);
    if (rc != MYDB_OK) return rc;

    /* Persist the updated RelationDef before backfill so a crash after
     * a partial backfill leaves the index visible (and rebuildable). */
    rc = schema_flush_relation(sf, r->relation_name);
    if (rc != MYDB_OK) return rc;

    schema_bump_relation_pages(sf, r->relation_name, 1);
    cat_track_alloc(&ctx->catalog, (int64_t)PAGE_SIZE);

    return MYDB_OK;
}

/* ------------------------------------------------------------------ */
/*  TCL                                                                 */
/* ------------------------------------------------------------------ */

int pm_begin(PartitionCtx *ctx)
{
    if (!ctx) return MYDB_ERR;
    return trx_begin(&ctx->txn_mgr);
}

int pm_commit(PartitionCtx *ctx)
{
    if (!ctx) return MYDB_ERR;

    /* Flush all dirty pages to disk before acknowledging the commit. */
    storage_flush_all_dirty(&ctx->storage);

    /* Fsync the active schema file so RelationDef mutations (row counts,
     * auto_incr_counter, num_pages) survive a crash. */
    SchemaFile *sf = pctx_active_schema(ctx);
    if (sf && sf->fd >= 0)
        fsync(sf->fd);

    /* Reset transaction state (trx_commit is a no-op for table flushing
     * in Phase 2 since no tables are registered with the TransactionManager;
     * the flush was done above via storage_flush_all_dirty). */
    return trx_commit(&ctx->txn_mgr);
}

int pm_rollback(PartitionCtx *ctx)
{
    if (!ctx) return MYDB_ERR;

    /* Evict all pages without flushing — dirty changes are discarded;
     * on next access the buffer pool reloads pre-transaction data from disk. */
    storage_evict_all(&ctx->storage);

    return trx_rollback(&ctx->txn_mgr);
}

/* ------------------------------------------------------------------ */
/*  DML — INSERT                                                        */
/* ------------------------------------------------------------------ */

int pm_insert(PartitionCtx *ctx, RelationDef *rel, Row *row)
{
    if (!ctx || !rel || !row) return MYDB_ERR;

    /* Read the writable RelationDef from the active schema — caller's
     * pointer may be a parser-side const view. */
    RelationDef *r = pm_find_relation(ctx, rel->relation_name);
    if (!r) return MYDB_ERR_NOT_FOUND;

    if (quota_headroom(ctx, DML_QUOTA_HEADROOM_PAGES) != MYDB_OK)
        return MYDB_ERR_FULL;

    int auto_txn = autocommit_begin(ctx);

    /* NOT NULL */
    for (int i = 0; i < r->num_columns; i++) {
        if (r->columns[i].is_not_null && !r->columns[i].is_auto_increment) {
            if (row->cols[i].is_null) {
                autocommit_end(ctx, auto_txn, MYDB_ERR);
                return MYDB_ERR_NULL_VIOLATION;
            }
        }
    }

    /* FK referential integrity */
    int fk_rc = fk_check_ref_exists(ctx, r, row);
    if (fk_rc != MYDB_OK) {
        autocommit_end(ctx, auto_txn, fk_rc);
        return fk_rc;
    }

    /* AUTO_INCREMENT */
    int pk = r->pk_col_idx;
    if (r->columns[pk].is_auto_increment) {
        if (row->cols[pk].is_null || row->cols[pk].v.int_val == 0) {
            row->cols[pk].type      = TYPE_INT;
            row->cols[pk].is_null   = 0;
            row->cols[pk].v.int_val = (int32_t)r->auto_incr_counter;
            r->auto_incr_counter++;
        }
    }

    uint32_t pages_before = storage_table_page_count(&ctx->storage,
                                                       r->owner_schema,
                                                       r->relation_name);

    RID rid;
    int rc = storage_insert(&ctx->storage, r, row,
                             trx_current_id(&ctx->txn_mgr), &rid);
    if (rc != MYDB_OK) {
        reconcile_growth(ctx, rel->relation_name, pages_before);
        autocommit_end(ctx, auto_txn, rc);
        return rc;
    }

    reconcile_growth(ctx, rel->relation_name, pages_before);

    SchemaFile *sf = pctx_active_schema(ctx);
    if (sf) {
        schema_bump_relation_rows(sf, rel->relation_name, 1);
        if (r->columns[pk].is_auto_increment)
            schema_flush_relation(sf, rel->relation_name);
    }

    autocommit_end(ctx, auto_txn, MYDB_OK);
    return MYDB_OK;
}

/* ------------------------------------------------------------------ */
/*  DML — DELETE                                                        */
/* ------------------------------------------------------------------ */

int pm_delete(PartitionCtx *ctx, RelationDef *rel, RID rid)
{
    if (!ctx || !rel) return MYDB_ERR;

    RelationDef *r = pm_find_relation(ctx, rel->relation_name);
    if (!r) return MYDB_ERR_NOT_FOUND;

    int auto_txn = autocommit_begin(ctx);

    /* Read the row to get its PK for FK referential integrity checks. */
    Row *old_row = storage_get_by_rid(&ctx->storage, r, rid);
    if (!old_row) {
        autocommit_end(ctx, auto_txn, MYDB_ERR);
        return MYDB_ERR;
    }
    Value pk_val = old_row->cols[r->pk_col_idx];

    /* FK referential integrity — apply ON DELETE action (RESTRICT / CASCADE
     * / SET_NULL) for every FK that references this row. */
    int fk_rc = fk_apply_on_delete(ctx, r, &pk_val);
    if (fk_rc != MYDB_OK) {
        autocommit_end(ctx, auto_txn, fk_rc);
        return fk_rc;
    }

    int rc = storage_delete(&ctx->storage, r, rid);

    if (rc == MYDB_OK) {
        SchemaFile *sf = pctx_active_schema(ctx);
        if (sf) schema_bump_relation_rows(sf, rel->relation_name, -1);
    }

    autocommit_end(ctx, auto_txn, rc);
    return rc;
}

/* ------------------------------------------------------------------ */
/*  DML — UPDATE                                                        */
/* ------------------------------------------------------------------ */

int pm_update(PartitionCtx *ctx, RelationDef *rel, RID rid, Row *new_row)
{
    if (!ctx || !rel || !new_row) return MYDB_ERR;

    RelationDef *r = pm_find_relation(ctx, rel->relation_name);
    if (!r) return MYDB_ERR_NOT_FOUND;

    if (quota_headroom(ctx, DML_QUOTA_HEADROOM_PAGES) != MYDB_OK)
        return MYDB_ERR_FULL;

    int auto_txn = autocommit_begin(ctx);

    /* NOT NULL */
    for (int i = 0; i < r->num_columns; i++) {
        if (r->columns[i].is_not_null && new_row->cols[i].is_null) {
            autocommit_end(ctx, auto_txn, MYDB_ERR);
            return MYDB_ERR_NULL_VIOLATION;
        }
    }

    /* Read the old row to check PK changes and FK constraints. */
    Row *old_row = storage_get_by_rid(&ctx->storage, r, rid);
    if (!old_row) {
        autocommit_end(ctx, auto_txn, MYDB_ERR);
        return MYDB_ERR;
    }
    Value old_pk = old_row->cols[r->pk_col_idx];

    /* FK checks: new row's FK values must reference existing rows;
     * if the PK changes, the old PK must not be referenced by others. */
    {
        int fk_rc = fk_check_ref_exists(ctx, r, new_row);
        if (fk_rc != MYDB_OK) {
            autocommit_end(ctx, auto_txn, fk_rc);
            return fk_rc;
        }
    }

    const Value *new_pk = &new_row->cols[r->pk_col_idx];
    int pk_changed = (value_compare(&old_pk, new_pk) != 0);
    if (pk_changed) {
        int fk_rc = fk_check_not_referenced(ctx, r, &old_pk);
        if (fk_rc != MYDB_OK) {
            autocommit_end(ctx, auto_txn, fk_rc);
            return fk_rc;
        }
    }

    uint32_t pages_before = storage_table_page_count(&ctx->storage,
                                                       r->owner_schema,
                                                       r->relation_name);

    int rc = storage_update(&ctx->storage, r, rid, new_row,
                             trx_current_id(&ctx->txn_mgr));

    reconcile_growth(ctx, rel->relation_name, pages_before);
    autocommit_end(ctx, auto_txn, rc);
    return rc;
}

/* ------------------------------------------------------------------ */
/*  DQL                                                                 */
/* ------------------------------------------------------------------ */

Row *pm_get_by_pk(PartitionCtx *ctx, RelationDef *rel, Value *pk)
{
    if (!ctx || !rel || !pk) return NULL;
    RelationDef *r = pm_find_relation(ctx, rel->relation_name);
    if (!r) return NULL;
    return storage_get_by_pk(&ctx->storage, r, pk);
}

Row *pm_get_by_index(PartitionCtx *ctx, RelationDef *rel,
                     int col_idx, Value *key)
{
    if (!ctx || !rel || !key) return NULL;
    RelationDef *r = pm_find_relation(ctx, rel->relation_name);
    if (!r) return NULL;
    return storage_get_by_index(&ctx->storage, r, col_idx, key);
}

Cursor *pm_scan(PartitionCtx *ctx, RelationDef *rel)
{
    if (!ctx || !rel) return NULL;
    RelationDef *r = pm_find_relation(ctx, rel->relation_name);
    if (!r) return NULL;
    return storage_scan(&ctx->storage, r);
}

Cursor *pm_scan_from(PartitionCtx *ctx, RelationDef *rel, Value *lo)
{
    if (!ctx || !rel || !lo) return NULL;
    RelationDef *r = pm_find_relation(ctx, rel->relation_name);
    if (!r) return NULL;
    return storage_scan_from(&ctx->storage, r, lo);
}

Cursor *pm_scan_by_index(PartitionCtx *ctx, RelationDef *rel,
                          int col_idx, Value *lo)
{
    if (!ctx || !rel) return NULL;
    RelationDef *r = pm_find_relation(ctx, rel->relation_name);
    if (!r) return NULL;
    return storage_scan_by_index(&ctx->storage, r, col_idx, lo);
}

/* ======================================================================
 * pm_analyze_table
 *
 * Full table scan → compute per-column statistics → write to StatsFile.
 *
 * For every column (including PK) we always collect:
 *   total_rows, num_nulls, num_distinct, min_numeric, max_numeric.
 *
 * For non-VARCHAR columns we additionally decide between:
 *   MCV       — num_distinct ≤ STATS_MAX_ENTRIES, or type is BOOL/ENUM.
 *               Store all distinct values sorted by frequency (desc).
 *   HISTOGRAM — num_distinct > STATS_MAX_ENTRIES AND type is numeric/date.
 *               Build STATS_MAX_ENTRIES equi-height buckets.
 *   (none)    — VARCHAR: only scalar stats stored; no MCV or histogram.
 *
 * The internal hash map tracks up to ANALYZE_HM_LIMIT = 256 distinct
 * values.  Beyond that limit the map stops accepting new keys but
 * existing counts keep growing.  For high-cardinality columns the
 * histogram is built from the sampled 256 values; selectivity estimates
 * will be approximate but not wrong in a dangerous direction.
 * ====================================================================== */

/* ------------------------------------------------------------------ */
/*  Per-column scratch types (file-scope to avoid C99 VLA issues)     */
/* ------------------------------------------------------------------ */

#define ANALYZE_HM_CAP    512    /* open-addressing capacity (power-of-2)  */
#define ANALYZE_HM_MASK  (ANALYZE_HM_CAP - 1)
#define ANALYZE_HM_LIMIT  256    /* stop adding new keys beyond this many   */

typedef struct {
    int64_t  key;
    uint32_t count;
    uint8_t  used;
    uint8_t  _pad[3];
} AnalyzeSlot;   /* 16 B */

typedef struct {
    AnalyzeSlot slots[ANALYZE_HM_CAP];  /* 512 × 16 B = 8 KB */
    int64_t  min_val;
    int64_t  max_val;
    uint32_t null_count;
    uint32_t distinct_count;     /* unique values seen (capped at ANALYZE_HM_LIMIT) */
    uint8_t  hm_full;            /* 1 once distinct_count hit the limit */
    uint8_t  has_any;            /* 1 once we've seen at least one non-null value */
    uint8_t  skip_blob;          /* 1 for VARCHAR (no MCV/histogram) */
    uint8_t  _pad;
} ColAnalysis;   /* ~8 KB per column */

/* ------------------------------------------------------------------ */
/*  (key, count) pair used during sort                                 */
/* ------------------------------------------------------------------ */
typedef struct { int64_t key; uint32_t count; } KVPair;

static int kv_cmp_key(const void *a, const void *b)
{
    const KVPair *ka = a, *kb = b;
    if (ka->key < kb->key) return -1;
    if (ka->key > kb->key) return  1;
    return 0;
}
static int kv_cmp_freq(const void *a, const void *b)
{
    const KVPair *ka = a, *kb = b;
    /* descending frequency */
    if (ka->count > kb->count) return -1;
    if (ka->count < kb->count) return  1;
    return 0;
}

/* ------------------------------------------------------------------ */
/*  Encode a Value as int64 for the hash map / stats struct           */
/* ------------------------------------------------------------------ */
static int64_t val_to_i64(const Value *v)
{
    switch (v->type) {
        case TYPE_INT:      return (int64_t)v->v.int_val;
        case TYPE_DECIMAL:  return v->v.decimal_val;
        case TYPE_DATE:     return (int64_t)v->v.date_val;
        case TYPE_DATETIME: return v->v.datetime_val;
        case TYPE_BOOL:     return (int64_t)v->v.bool_val;
        case TYPE_ENUM:     return (int64_t)v->v.enum_val;
        case TYPE_VARCHAR:  return 0;  /* not used */
    }
    return 0;
}

/* ------------------------------------------------------------------ */
/*  Hash map insert — FNV-1a-ish mixing on 64-bit key                 */
/* ------------------------------------------------------------------ */
static void hm_insert(ColAnalysis *ca, int64_t key)
{
    uint32_t h = (uint32_t)(((uint64_t)key ^ ((uint64_t)key >> 32))
                             & ANALYZE_HM_MASK);
    for (int t = 0; t < ANALYZE_HM_CAP; t++) {
        AnalyzeSlot *s = &ca->slots[h];
        if (!s->used) {
            if (ca->hm_full) return;    /* limit reached, drop new key */
            s->key   = key;
            s->count = 1;
            s->used  = 1;
            ca->distinct_count++;
            if (ca->distinct_count >= ANALYZE_HM_LIMIT)
                ca->hm_full = 1;
            return;
        }
        if (s->key == key) { s->count++; return; }
        h = (h + 1) & ANALYZE_HM_MASK;
    }
}

/* ------------------------------------------------------------------ */
/*  Extract live entries from a hash map into a flat KVPair array     */
/*  Returns count of entries written.                                 */
/* ------------------------------------------------------------------ */
static int hm_extract(const ColAnalysis *ca, KVPair *out, int max_out)
{
    int n = 0;
    for (int i = 0; i < ANALYZE_HM_CAP && n < max_out; i++) {
        if (ca->slots[i].used) {
            out[n].key   = ca->slots[i].key;
            out[n].count = ca->slots[i].count;
            n++;
        }
    }
    return n;
}

/* ------------------------------------------------------------------ */
/*  pm_analyze_table                                                   */
/* ------------------------------------------------------------------ */
int pm_analyze_table(PartitionCtx *ctx, RelationDef *rel,
                     StatsFile *sf, int slot_idx)
{
    if (!ctx || !rel) return MYDB_ERR;
    if (!sf) return MYDB_OK;   /* no stats file yet — silently skip */

    /* Read-only operation: scan using pm_scan so we go through the
     * pm layer's relation lookup. */

    /* Blank this relation's stats page in memory. */
    stats_reset_relation(sf, slot_idx);

    /* ----------------------------------------------------------------
     * Phase 1: full table scan.
     *
     * We allocate one ColAnalysis per column on the stack.  Each entry
     * is ~8 KB; 32 columns = ~256 KB — well within Linux's 8 MB default.
     * ---------------------------------------------------------------- */
    ColAnalysis ca[MAX_COLUMNS];
    memset(ca, 0, sizeof(ca));

    /* Mark VARCHAR columns so the inner loop can skip hash-map work. */
    for (int i = 0; i < rel->num_columns; i++)
        if (rel->columns[i].type == TYPE_VARCHAR)
            ca[i].skip_blob = 1;

    uint32_t total_rows = 0;

    Cursor *cur = pm_scan(ctx, rel);
    if (cur) {
        Row *row;
        while ((row = cursor_next(cur)) != NULL) {
            total_rows++;
            for (int i = 0; i < rel->num_columns; i++) {
                const Value *v = &row->cols[i];

                if (v->is_null) {
                    ca[i].null_count++;
                    continue;
                }

                /* Update min / max. */
                int64_t enc = val_to_i64(v);
                if (!ca[i].has_any) {
                    ca[i].min_val = enc;
                    ca[i].max_val = enc;
                    ca[i].has_any = 1;
                } else {
                    if (enc < ca[i].min_val) ca[i].min_val = enc;
                    if (enc > ca[i].max_val) ca[i].max_val = enc;
                }

                if (!ca[i].skip_blob)
                    hm_insert(&ca[i], enc);
            }
        }
        cursor_close(cur);
    }

    /* ----------------------------------------------------------------
     * Phase 2: write per-column stats.
     * ---------------------------------------------------------------- */
    for (int i = 0; i < rel->num_columns; i++) {
        ColumnStats *cs = &sf->pages[slot_idx].cols[i];

        cs->has_stats    = 1;
        cs->total_rows   = total_rows;
        cs->num_nulls    = ca[i].null_count;
        cs->num_distinct = ca[i].distinct_count;
        cs->min_numeric  = ca[i].min_val;
        cs->max_numeric  = ca[i].max_val;

        if (total_rows == 0 || ca[i].skip_blob) {
            /* VARCHAR or empty table: scalar stats only. */
            cs->stats_type = STATS_TYPE_NONE;
            continue;
        }

        /* Decide: MCV or histogram?
         *
         * BOOL and ENUM always get MCV — their cardinality is tiny.
         * For other types: MCV when we can store all distinct values
         * exactly (num_distinct ≤ STATS_MAX_ENTRIES); histogram otherwise. */
        DataType dtype = rel->columns[i].type;
        int want_mcv = (dtype == TYPE_BOOL || dtype == TYPE_ENUM)
                       || (ca[i].distinct_count <= STATS_MAX_ENTRIES);

        /* Extract all live (key, count) pairs from the hash map. */
        KVPair pairs[ANALYZE_HM_LIMIT];
        int    npairs = hm_extract(&ca[i], pairs, ANALYZE_HM_LIMIT);

        if (npairs == 0) {
            cs->stats_type = STATS_TYPE_NONE;
            continue;
        }

        if (want_mcv) {
            /* Sort by frequency descending so the planner can short-circuit
             * the MCV scan as soon as accumulated probability exceeds the
             * predicate value. */
            qsort(pairs, (size_t)npairs, sizeof(KVPair), kv_cmp_freq);

            MCVEntry entries[STATS_MAX_ENTRIES];
            int      nentries = (npairs < STATS_MAX_ENTRIES)
                                 ? npairs : STATS_MAX_ENTRIES;
            for (int j = 0; j < nentries; j++) {
                entries[j].value     = pairs[j].key;
                entries[j].frequency = pairs[j].count;
                entries[j].pad       = 0;
            }
            /* Ignore MYDB_ERR_FULL — the page still gets saved with
             * whatever columns fit. */
            stats_write_mcv(sf, slot_idx, i, entries, (uint16_t)nentries);

        } else {
            /* Equi-height histogram.
             *
             * Sort pairs by key (ascending) then divide into
             * STATS_MAX_ENTRIES buckets of roughly equal cumulative count. */
            qsort(pairs, (size_t)npairs, sizeof(KVPair), kv_cmp_key);

            /* Sum of counts from sampled values (may be < total_rows if
             * we hit ANALYZE_HM_LIMIT and stopped tracking new keys). */
            uint64_t total_in_hm = 0;
            for (int j = 0; j < npairs; j++)
                total_in_hm += pairs[j].count;

            int nbuckets = (npairs < STATS_MAX_ENTRIES)
                            ? npairs : STATS_MAX_ENTRIES;
            uint64_t target = (total_in_hm + (uint64_t)nbuckets - 1)
                              / (uint64_t)nbuckets;  /* ceil */

            HistBucket buckets[STATS_MAX_ENTRIES];
            int        nb = 0;
            uint64_t   accum = 0;

            for (int j = 0; j < npairs && nb < STATS_MAX_ENTRIES; j++) {
                accum += pairs[j].count;
                /* Close the bucket on the last pair OR when accumulated
                 * count meets the target. */
                int is_last = (j == npairs - 1);
                if (accum >= target || is_last) {
                    buckets[nb].upper_bound = pairs[j].key;
                    buckets[nb].row_count   = (uint32_t)(accum > UINT32_MAX
                                                          ? UINT32_MAX : accum);
                    buckets[nb].pad = 0;
                    nb++;
                    accum = 0;
                }
            }

            stats_write_hist(sf, slot_idx, i, buckets, (uint16_t)nb);
        }
    }

    /* Persist the stats page to the StatsFile. */
    return stats_save_relation(sf, slot_idx);
}
