#ifndef STORAGE_H
#define STORAGE_H

#include "common.h"
#include "relation_def.h"
#include "btree.h"
#include "disk_manager.h"
#include "buffer_pool.h"

#ifdef __cplusplus
extern "C" {
#endif

/*
 * storage.h — stateless storage engine (v3).
 *
 * StorageEngine is a plain struct: one instance per PartitionCtx, owned
 * by PartitionCtx (partition_manager).  There is NO global singleton.
 *
 * Responsibilities (storage_engine only):
 *   - Buffer pool (LRU frame cache)
 *   - Open-table cache (DiskManager + BTree handles)
 *   - Raw B+ tree insert / delete / search
 *   - Path building from the filesystem context stored in the struct
 *
 * NOT in storage_engine (moved to partition_manager):
 *   - Catalog (quota, schema registry)
 *   - SchemaFile (RelationDef persistence)
 *   - TransactionManager (begin / commit / rollback)
 *   - StatsFile (optimizer statistics)
 *   - FK constraint checks
 *   - NOT NULL / UNIQUE validation
 *   - Schema-stat updates (num_rows, num_pages, auto_incr_counter)
 *
 * Usage (called by partition_manager):
 *   StorageEngine se;
 *   storage_init(&se);
 *   storage_set_context(&se, partition_path, schema_name);
 *   storage_insert(&se, rel, row, trx_id, &rid);
 *   storage_shutdown(&se);
 */

/* ------------------------------------------------------------------ */
/*  One entry in the open-table cache                                   */
/* ------------------------------------------------------------------ */
typedef struct {
    char        name[MAX_TABLE_NAME];
    int         id;             /* table_id used with the buffer pool */
    DiskManager dm;
    BTree       clustered;
    BTree       secondary[MAX_SECONDARY_IDX];
    int         is_open;
} OpenTable;

/* ------------------------------------------------------------------ */
/*  StorageEngine — one per PartitionCtx, no global singleton          */
/* ------------------------------------------------------------------ */
typedef struct StorageEngine {
    /* Filesystem context — set by storage_set_context() when the
     * active schema changes.  partition_manager owns these strings. */
    char        partition_path[256];
    char        current_schema_name[32];

    BufferPool  bp;
    OpenTable   open_tables[MAX_TABLES];
    int         num_open;
    int         next_table_id;   /* monotonic counter for buffer-pool table IDs */
    int         initialized;
    DiskManager *last_written_dm; /* track for fsync on commit */
} StorageEngine;

/* ------------------------------------------------------------------ */
/*  Row — one table row                                                 */
/* ------------------------------------------------------------------ */
struct Row {
    uint8_t  num_cols;
    Value    cols[MAX_COLUMNS];
    RID      rid;   /* set by storage_get_by_pk and cursor_next */
};

/* ------------------------------------------------------------------ */
/*  Lifecycle                                                           */
/* ------------------------------------------------------------------ */

/* Initialise the storage engine: zero state, init buffer pool.
 * Call once per PartitionCtx after allocating it. */
int storage_init(StorageEngine *se);

/* Flush all dirty pages and close all open table handles.
 * Call before freeing the PartitionCtx. */
int storage_shutdown(StorageEngine *se);

/* Flush every dirty page without closing tables.
 * Called by partition_manager before evicting a schema from Cache 2. */
int storage_flush_all_dirty(StorageEngine *se);

/* Evict all pages from the buffer pool without flushing (used on ROLLBACK). */
int storage_evict_all(StorageEngine *se);

/* ------------------------------------------------------------------ */
/*  Context management                                                  */
/* ------------------------------------------------------------------ */

/* Set the active filesystem context.  Called by partition_manager after
 * opening a schema (pctx_open_schema) so that path-building inside
 * storage.c uses the correct partition + schema directories. */
void storage_set_context(StorageEngine *se,
                         const char *partition_path,
                         const char *schema_name);

/* Clear the active schema name (called on deactivate / schema switch). */
void storage_clear_schema(StorageEngine *se);

/* ------------------------------------------------------------------ */
/*  DDL — pure I/O, no schema/catalog side-effects                     */
/* ------------------------------------------------------------------ */

/*
 * Create a new relation file and allocate its B+ tree root page(s).
 * Fills in rel->root_page_no and rel->secondary_root_page_no[].
 * Does NOT register in SchemaFile or update Catalog — that is the
 * caller's (partition_manager's) responsibility.
 *
 * Returns MYDB_ERR_FULL if the page allocation fails (disk full).
 */
int storage_create_table(StorageEngine *se, RelationDef *rel);

/*
 * Close the open handle for table_name (if cached) and unlink its file.
 * Does NOT touch SchemaFile or Catalog.
 */
int storage_drop_table(StorageEngine *se, const char *table_name);

/*
 * Allocate a secondary-index root page, initialise its B+ tree, and
 * backfill all existing clustered rows into it.
 * Fills in rel->secondary_root_page_no[rel->num_secondary_indexes - 1].
 * Caller must have already incremented rel->num_secondary_indexes and
 * set rel->secondary_col_idx[new_slot] before calling.
 * Does NOT flush/persist the updated RelationDef — caller does that.
 */
int storage_add_index(StorageEngine *se, RelationDef *rel, int col_idx);

/* ------------------------------------------------------------------ */
/*  DML — pure B+ tree operations, no FK/schema/catalog side-effects   */
/*                                                                      */
/*  trx_id is stamped into DB_TRX_ID of every affected record.         */
/*  All constraint checking (FK, NOT NULL, UNIQUE) is the caller's job.*/
/* ------------------------------------------------------------------ */

int storage_insert(StorageEngine *se, RelationDef *rel,
                   Row *row, uint64_t trx_id, RID *rid_out);

int storage_delete(StorageEngine *se, RelationDef *rel, RID rid);

int storage_update(StorageEngine *se, RelationDef *rel,
                   RID rid, Row *new_row, uint64_t trx_id);

/* ------------------------------------------------------------------ */
/*  DQL — pure B+ tree lookups and scans                               */
/* ------------------------------------------------------------------ */

Row    *storage_get_by_pk(StorageEngine *se, RelationDef *rel, Value *pk);

Row    *storage_get_by_index(StorageEngine *se, RelationDef *rel,
                              int col_idx, Value *key);

Cursor *storage_scan(StorageEngine *se, RelationDef *rel);

Cursor *storage_scan_from(StorageEngine *se, RelationDef *rel, Value *lo);

Cursor *storage_scan_by_index(StorageEngine *se, RelationDef *rel,
                               int col_idx, Value *lo);

Row  *cursor_next(Cursor *cursor);
void  cursor_close(Cursor *cursor);

/* Fetch a full row by its Record ID.
 * Used by partition_manager for FK constraint resolution (reading the
 * old row before update/delete to check referencing rows).
 * Returns a pointer to an internal static Row, or NULL on error. */
Row *storage_get_by_rid(StorageEngine *se, RelationDef *rel, RID rid);

/* ------------------------------------------------------------------ */
/*  Page-count query (used by partition_manager to track quota delta)  */
/* ------------------------------------------------------------------ */

/* Return the current on-disk page count for the named table, or 0 if
 * the table is not open (caller should open it first). */
uint32_t storage_table_page_count(StorageEngine *se, const char *table_name);

#ifdef __cplusplus
}
#endif

#endif /* STORAGE_H */
