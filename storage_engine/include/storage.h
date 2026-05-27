#ifndef STORAGE_H
#define STORAGE_H

#include "common.h"
#include "relation_def.h"
#include "transaction.h"
#include "btree.h"

#ifdef __cplusplus
extern "C" {
#endif

/*
 * storage.h — public interface for the execution engine
 *
 * Phase 9 rewrite: storage is now session-aware. All DDL / DML / DQL
 * functions take a `RelationDef *` instead of a table-name string;
 * the parser is responsible for validating the identifier at parse
 * time (looking it up via engine_find_relation) and handing storage
 * a validated pointer.
 *
 * Usage:
 *   storage_init(&eng);          // once after engine_login + engine_use_schema
 *   storage_begin();
 *   storage_insert(rel, &row);
 *   storage_commit();
 *   storage_shutdown();          // once before engine_close
 *
 * All functions return MYDB_OK (0) on success, negative error codes on failure.
 * storage_get_by_pk and cursor_next return NULL on miss / end-of-scan.
 */

/* Forward declaration — full definition lives in engine.h. Storage
 * keeps a pointer to the engine session for filesystem paths, the
 * active SchemaFile (Cache 2) and the active partition catalog. */
struct EngineState;

/* ------------------------------------------------------------------ */
/*  Row — one table row (forward-declared in common.h)                  */
/* ------------------------------------------------------------------ */
struct Row {
    uint8_t  num_cols;
    Value    cols[MAX_COLUMNS];
    RID      rid;   /* set by storage_get_by_pk and cursor_next */
};

/* ------------------------------------------------------------------ */
/*  Engine lifecycle                                                     */
/* ------------------------------------------------------------------ */

/* Initialise the storage runtime against an open engine session.
 * The caller must already have called engine_init + engine_login.
 * `engine_use_schema` may be called before OR after storage_init —
 * storage resolves the active schema lazily on each call. */
int storage_init(struct EngineState *eng);

/* Commit any open transaction, flush everything, close all files. */
int storage_shutdown(void);

/* Flush every dirty page in the buffer pool to disk without closing any
 * table or tearing down the runtime. Called by engine_use_schema before
 * swapping the active schema so the old schema's pages are durable. */
int storage_flush_all_dirty(void);

/* ------------------------------------------------------------------ */
/*  DDL                                                                 */
/* ------------------------------------------------------------------ */

/*
 * Create a new schema in the current partition.
 *   1. Owner check: caller must own the active partition.
 *   2. Reject duplicates against the partition catalog.
 *   3. mkdir <partition>/<name>/, write its __schema.mydb, register in
 *      __catalog.mydb.
 *
 * Returns MYDB_ERR_PERM      if the caller does not own a partition.
 *         MYDB_ERR_DUPLICATE if a schema with that name already exists.
 *         MYDB_ERR_FULL      if the catalog has no free schema slots.
 */
int storage_create_schema(const char *name);

/*
 * Drop a schema from the current partition.
 *   1. Rejects if name is the currently active schema (USE another first).
 *   2. Opens __schema.mydb to enumerate all relation files.
 *   3. unlinks each <relation>.mydb and __stats.mydb (if present).
 *   4. unlinks __schema.mydb, then rmdir the schema directory.
 *   5. Credits freed bytes back via cat_track_alloc (quota update).
 *   6. Removes the slot from __catalog.mydb via cat_remove_schema.
 *
 * Returns MYDB_ERR_NOT_FOUND if no such schema exists.
 *         MYDB_ERR           if the schema is currently active.
 */
int storage_drop_schema(const char *name);

/*
 * Create a new relation in the active schema. `rel` is mutated:
 *   rel->root_page_no and rel->secondary_root_page_no[] are filled in
 *   by this function.
 * Persists the RelationDef into __schema.mydb and creates the
 * <relation>.mydb data file under the active partition.
 *
 * Returns MYDB_ERR_DUPLICATE if a relation with that name already exists.
 */
int storage_create_table(RelationDef *rel);

/* Drop the relation: deletes its data file and removes its slot from
 * __schema.mydb. */
int storage_drop_table(RelationDef *rel);

/*
 * Add a secondary index on column col_idx of an existing table.
 * Works for both UNIQUE (is_secondary=1) and non-unique (is_secondary=2)
 * columns — the B-tree type is derived from rel->columns[col_idx].is_unique.
 *
 * Allocates a new root page, backfills all existing rows, then persists
 * the updated RelationDef into __schema.mydb.
 *
 * Returns MYDB_ERR_DUPLICATE if col_idx is already indexed.
 * Returns MYDB_ERR_FULL      if MAX_SECONDARY_IDX is reached or quota exceeded.
 */
int storage_add_index(RelationDef *rel, int col_idx);

/* ------------------------------------------------------------------ */
/*  DML                                                                 */
/* ------------------------------------------------------------------ */

/*
 * Insert a row.
 * - NOT NULL columns must have non-null values in row->cols[].
 * - AUTO_INCREMENT PK: set row->cols[pk_idx] to is_null=1 or int_val=0
 *   and the engine fills in the next counter value.
 * - UNIQUE columns: returns MYDB_ERR_DUPLICATE on violation.
 */
int storage_insert(RelationDef *rel, Row *row);

/*
 * Update the row identified by rid with the values in new_row.
 * Internally: delete old record + insert new record.
 * rid is obtained from row->rid returned by storage_get_by_pk / cursor_next.
 */
int storage_update(RelationDef *rel, RID rid, Row *new_row);

/* Delete the row identified by rid. */
int storage_delete(RelationDef *rel, RID rid);

/* ------------------------------------------------------------------ */
/*  DQL                                                                 */
/* ------------------------------------------------------------------ */

/*
 * Fetch a single row by primary key.
 * Returns a pointer to an internal static Row, or NULL if not found.
 * The pointer is valid until the next storage_get_by_pk call.
 */
Row *storage_get_by_pk(RelationDef *rel, Value *pk);

/*
 * Fetch a single row by a secondary (UNIQUE) index.
 * col_idx is the column position in rel->columns[] that has is_unique=1.
 * Descends the secondary B+ tree to get the RID, then fetches the full
 * row from the clustered tree in one page read.
 * Returns a pointer to an internal static Row, or NULL if not found /
 * col_idx has no secondary index / permission denied.
 * The pointer is valid until the next storage_get_by_index call.
 */
Row *storage_get_by_index(RelationDef *rel, int col_idx, Value *key);

/*
 * Open a scan cursor on a secondary (UNIQUE) index, positioned at the
 * first key >= lo.  Pass lo = NULL to start from the leftmost key.
 *
 * cursor_next() returns full rows fetched from the clustered index
 * (same Row * as a clustered scan — RID, all columns present).
 * The caller applies any upper bound by comparing the indexed column
 * value from each returned row and breaking when it exceeds the
 * desired range — the same pattern as storage_scan_from() for PK ranges.
 *
 * Returns NULL if col_idx has no secondary index, on permission
 * failure, or on internal error. Close with cursor_close().
 */
Cursor *storage_scan_by_index(RelationDef *rel, int col_idx, Value *lo);

/*
 * Open a full-table scan cursor. NULL on error.
 * The cursor must be closed with cursor_close().
 */
Cursor *storage_scan(RelationDef *rel);

/*
 * Open a scan cursor positioned at the first row whose primary key is >= lo.
 * The cursor walks forward in key order from there; the caller applies any
 * upper bound by comparing each row's PK and stopping when it exceeds the
 * desired range. NULL on error. Close with cursor_close().
 */
Cursor *storage_scan_from(RelationDef *rel, Value *lo);

/* Advance the cursor and return a pointer to the next row, or NULL at
 * end-of-scan. The pointed-to Row is owned by the cursor. */
Row *cursor_next(Cursor *cursor);

/* Close a scan cursor and free its resources. */
void cursor_close(Cursor *cursor);

/* ------------------------------------------------------------------ */
/*  TCL                                                                 */
/* ------------------------------------------------------------------ */

int storage_begin(void);
int storage_commit(void);
int storage_rollback(void);

/* ------------------------------------------------------------------ */
/*  Statistics collection (for the cost-based planner)                */
/* ------------------------------------------------------------------ */

/*
 * Scan the entire clustered B+ tree and recompute per-column statistics
 * for the CBO.  Writes the results to __stats.mydb in the active schema
 * directory (creates the file if it does not exist yet).
 *
 * Called by the execution engine when the user issues:
 *   ANALYZE TABLE <table_name>;
 *
 * Column statistics collected:
 *   - total_rows, num_nulls, num_distinct (NDV), min, max
 *   - MCV (Most Common Values) when num_distinct ≤ STATS_MAX_ENTRIES,
 *     or when the column is BOOL/ENUM regardless of cardinality.
 *   - Equi-height histogram (STATS_MAX_ENTRIES buckets) otherwise.
 *   - VARCHAR columns get only scalar stats (no MCV/histogram).
 *
 * Returns MYDB_OK on success.  Returns MYDB_ERR_PERM if the caller
 * does not have read access to the active schema.
 */
int storage_analyze_table(RelationDef *rel);

#ifdef __cplusplus
}
#endif

#endif /* STORAGE_H */
