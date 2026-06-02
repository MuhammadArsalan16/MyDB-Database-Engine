#ifndef PM_API_H
#define PM_API_H

/*
 * pm_api.h — partition_manager public API for the execution engine.
 *
 * These functions are the wrappers the execution engine calls instead of
 * the old storage_* functions.  Each pm_* function:
 *
 *   1. Validates context (schema active, ownership, quota headroom).
 *   2. Performs constraint checking (FK, NOT NULL, UNIQUE, AUTO_INCREMENT).
 *   3. Calls the stateless storage_* functions via ctx->storage.
 *   4. Updates SchemaFile stats (row count, page count, auto_incr_counter).
 *   5. Updates the Catalog quota via cat_track_alloc.
 *
 * The execution engine (execution_engine/) includes this header and passes
 * ectx->partition as the PartitionCtx*.  It never includes storage.h or
 * calls storage_* functions directly.
 *
 * Cursor operations (cursor_next, cursor_close) are defined in storage.h
 * and remain unchanged — they are pure B+ tree operations with no session
 * state, so the execution engine may call them directly.
 */

#include "partition_ctx.h"
#include "stats.h"      /* StatsFile — for pm_analyze_table */

#ifdef __cplusplus
extern "C" {
#endif

/* ------------------------------------------------------------------ */
/*  Schema DDL                                                          */
/* ------------------------------------------------------------------ */

/* Create a new schema in ctx's partition.
 * Checks ownership, rejects duplicates, creates the directory and
 * __schema.mydb, registers the schema in __catalog.mydb. */
int pm_create_schema(PartitionCtx *ctx, const char *name);

/* Drop a schema from ctx's partition.
 * Rejects if name is the currently active schema.
 * Enumerates relations, unlinks data files, deletes __schema.mydb,
 * rmdir's the schema directory, credits freed bytes to the quota. */
int pm_drop_schema(PartitionCtx *ctx, const char *name);

/* ------------------------------------------------------------------ */
/*  Table DDL                                                           */
/* ------------------------------------------------------------------ */

/* Create a new relation.
 * Quota pre-check, calls storage_create_table (fills root_page_no),
 * schema_add_relation, updates SchemaEntry.num_relations + cat quota. */
int pm_create_table(PartitionCtx *ctx, RelationDef *rel);

/* Drop a relation.
 * Reclaims quota, calls storage_drop_table, schema_remove_relation,
 * updates SchemaEntry.num_relations. */
int pm_drop_table(PartitionCtx *ctx, RelationDef *rel);

/* Add a secondary index on col_idx of an existing table.
 * Quota pre-check, calls storage_add_index (backfills existing rows),
 * schema_flush_relation, tracks quota delta. */
int pm_add_index(PartitionCtx *ctx, RelationDef *rel, int col_idx);

/* ------------------------------------------------------------------ */
/*  DML                                                                 */
/* ------------------------------------------------------------------ */

/* Insert a row.
 * NOT NULL check, FK check, AUTO_INCREMENT fill-in, auto-commit wrapper,
 * storage_insert, schema_bump_relation_rows, auto_incr persist, quota. */
int pm_insert(PartitionCtx *ctx, RelationDef *rel, Row *row);

/* Update the row at rid with new_row.
 * NOT NULL check, FK check, auto-commit, storage_update, quota. */
int pm_update(PartitionCtx *ctx, RelationDef *rel, RID rid, Row *new_row);

/* Delete the row at rid.
 * FK ON DELETE check (RESTRICT / CASCADE / SET_NULL), auto-commit,
 * storage_delete, schema_bump_relation_rows. */
int pm_delete(PartitionCtx *ctx, RelationDef *rel, RID rid);

/* ------------------------------------------------------------------ */
/*  DQL                                                                 */
/* ------------------------------------------------------------------ */

/* Fetch a single row by primary key. Returns NULL on miss. */
Row *pm_get_by_pk(PartitionCtx *ctx, RelationDef *rel, Value *pk);

/* Fetch a single row by secondary (UNIQUE) index. Returns NULL on miss. */
Row *pm_get_by_index(PartitionCtx *ctx, RelationDef *rel,
                     int col_idx, Value *key);

/* Open a full-table scan cursor.  Close with cursor_close(). */
Cursor *pm_scan(PartitionCtx *ctx, RelationDef *rel);

/* Open a PK-range scan cursor starting at the first key >= lo. */
Cursor *pm_scan_from(PartitionCtx *ctx, RelationDef *rel, Value *lo);

/* Open a secondary-index scan cursor starting at the first key >= lo.
 * Pass lo = NULL to start from the leftmost key. */
Cursor *pm_scan_by_index(PartitionCtx *ctx, RelationDef *rel,
                          int col_idx, Value *lo);

/* ------------------------------------------------------------------ */
/*  TCL                                                                 */
/* ------------------------------------------------------------------ */

int pm_begin(PartitionCtx *ctx);
int pm_commit(PartitionCtx *ctx);
int pm_rollback(PartitionCtx *ctx);

/* ------------------------------------------------------------------ */
/*  Statistics                                                          */
/* ------------------------------------------------------------------ */

/*
 * Full table scan → compute per-column statistics → write to sf.
 * sf must be an open StatsFile handle (from the engine's StatsBuffer,
 * passed via ExecContext.stats).  If sf is NULL the call is a no-op.
 * slot_idx is the relation's slot in the SchemaFile relations[] array.
 */
int pm_analyze_table(PartitionCtx *ctx, RelationDef *rel,
                     StatsFile *sf, int slot_idx);

/* ------------------------------------------------------------------ */
/*  Schema / relation lookup helpers                                    */
/* ------------------------------------------------------------------ */

/* Return the writable RelationDef* for relation_name from the active
 * SchemaFile, or NULL if no schema is active or the relation doesn't
 * exist.  This is the pm-layer replacement for engine_find_relation. */
RelationDef *pm_find_relation(PartitionCtx *ctx, const char *relation_name);

/* Return the read-only (const) RelationDef* — for the execution engine's
 * parse-time lookup path which must not mutate the schema. */
const RelationDef *pm_find_relation_const(PartitionCtx *ctx,
                                           const char *relation_name);

#ifdef __cplusplus
}
#endif

#endif /* PM_API_H */
