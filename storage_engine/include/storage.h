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
 * Open a full-table scan cursor. NULL on error.
 * The cursor must be closed with cursor_close().
 */
Cursor *storage_scan(RelationDef *rel);

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

#ifdef __cplusplus
}
#endif

#endif /* STORAGE_H */
