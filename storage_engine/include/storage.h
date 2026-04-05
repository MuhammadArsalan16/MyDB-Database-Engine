#ifndef STORAGE_H
#define STORAGE_H

#include "common.h"
#include "schema.h"
#include "transaction.h"
#include "btree.h"

/*
 * storage.h — public interface for the execution engine
 *
 * This is the only header the execution engine needs to include.
 * It ties together disk manager, buffer pool, B+ Tree, schema catalog,
 * and transaction manager into a single unified API.
 *
 * Usage:
 *   storage_init("/path/to/data/dir");   // once at startup
 *   storage_begin();
 *   storage_insert("users", &row);
 *   storage_commit();
 *   storage_shutdown();                  // once at exit
 *
 * All functions return MYDB_OK (0) on success, negative error codes on failure.
 * storage_get_by_pk and cursor_next return NULL on miss / end-of-scan.
 */

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

/*
 * Initialise the storage engine: open the schema catalog in data_dir,
 * set up the buffer pool and transaction manager.
 * Must be called once before any other storage_* function.
 */
int storage_init(const char *data_dir);

/* Commit any open transaction, flush everything, close all files. */
int storage_shutdown(void);

/* ------------------------------------------------------------------ */
/*  DDL                                                                 */
/* ------------------------------------------------------------------ */

/*
 * Create a new table.  schema->root_page_no and secondary_root_page_no[]
 * are set by this function; the caller should not fill them in.
 * Returns MYDB_ERR_DUPLICATE if a table with that name already exists.
 */
int storage_create_table(const char *name, Schema *schema);

/* Delete a table's data file and remove it from the catalog. */
int storage_drop_table(const char *name);

/* ------------------------------------------------------------------ */
/*  DML                                                                 */
/* ------------------------------------------------------------------ */

/*
 * Insert a row.
 * - NOT NULL columns must have non-null values in row->cols[].
 * - AUTO_INCREMENT PK: set row->cols[pk_idx] to is_null=1 or int_val=0
 *   and the engine will fill in the next counter value.
 * - UNIQUE columns: returns MYDB_ERR_DUPLICATE on violation.
 */
int storage_insert(const char *table, Row *row);

/*
 * Update the row identified by rid with the values in new_row.
 * Internally: delete old record + insert new record.
 * rid is obtained from row->rid returned by storage_get_by_pk / cursor_next.
 */
int storage_update(const char *table, RID rid, Row *new_row);

/* Delete the row identified by rid. */
int storage_delete(const char *table, RID rid);

/* ------------------------------------------------------------------ */
/*  DQL                                                                 */
/* ------------------------------------------------------------------ */

/*
 * Fetch a single row by primary key.
 * Returns a pointer to an internal static Row, or NULL if not found.
 * The pointer is valid until the next storage_get_by_pk call.
 */
Row *storage_get_by_pk(const char *table, Value *pk);

/*
 * Open a full-table scan cursor.
 * Returns NULL on error (table not found, etc.).
 * The cursor must be closed with cursor_close() when done.
 */
Cursor *storage_scan(const char *table);

/*
 * Advance the cursor and return a pointer to the next row.
 * Returns NULL when there are no more rows.
 * The pointed-to Row is owned by the cursor; do not free it.
 */
Row *cursor_next(Cursor *cursor);

/* Close a scan cursor and free its resources. */
void cursor_close(Cursor *cursor);

/* ------------------------------------------------------------------ */
/*  TCL                                                                 */
/* ------------------------------------------------------------------ */

int storage_begin(void);
int storage_commit(void);
int storage_rollback(void);

#endif /* STORAGE_H */
