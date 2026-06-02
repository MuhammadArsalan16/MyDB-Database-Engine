#ifndef SCHEMA_FILE_H
#define SCHEMA_FILE_H

#include "common.h"
#include "relation_def.h"

/* ------------------------------------------------------------------ */
/*  __schema.mydb — schema definition file                            */
/*                                                                    */
/*  One file per schema, ~1 MB fixed (SCHEMA_FILE_PAGES x PAGE_SIZE). */
/*                                                                    */
/*    Page 0          : SchemaHeader + RelationEntry slot directory   */
/*                      + FNV-1a checksum trailer                     */
/*    Pages 1..64     : one full serialized RelationDef per page,     */
/*                      allocated on demand                           */
/*                                                                    */
/*  Loaded into Cache 2 on USE schema_name. Direct pread/pwrite —     */
/*  bypasses the buffer pool entirely (consistent with other engine   */
/*  metadata files).                                                  */
/* ------------------------------------------------------------------ */

/* SchemaHeader — Page 0 fixed prefix (72 B per design doc §8.1) */
typedef struct {
    uint32_t partition_id;            /* which partition owns this schema */
    char     schema_name[32];
    uint64_t created_at;              /* YYYYMMDDHHmmSS */
    uint64_t last_modified;           /* refreshed on every Page 0 save */
    uint64_t size_bytes;              /* computed at load, NOT persisted */
    uint8_t  num_relations;           /* count of valid slots */
} SchemaHeader;

/* RelationEntry — slot directory entry (56 B per design doc §8.2) */
typedef struct {
    char     relation_name[32];
    uint8_t  is_valid;                /* 1 = occupied, 0 = empty */
    uint8_t  page_no;                 /* 1..SCHEMA_FILE_PAGES-1 */
    uint8_t  num_columns;
    uint32_t num_rows;                /* maintained by INSERT/DELETE callers */
    uint32_t num_pages;               /* persisted source of truth for size */
    uint8_t  tree_height;             /* B+ tree height (1 = root only); CBO cost input */
    uint8_t  reserved;               /* reserved for future CBO fields */
} RelationEntry;

/* In-memory schema file representation. relations[i] and defs[i] are
 * parallel arrays — defs[i] is meaningful only when relations[i].is_valid. */
typedef struct {
    SchemaHeader  header;
    RelationEntry relations[MAX_RELATIONS_PER_SCHEMA];
    RelationDef   defs[MAX_RELATIONS_PER_SCHEMA];
    int           fd;                 /* open fd; -1 when closed */
    char          path[256];
} SchemaFile;

/* ------------------------------------------------------------------ */
/*  Lifecycle                                                          */
/* ------------------------------------------------------------------ */

/* Create a new __schema.mydb. Fails if the file already exists.
 * Pre-allocates SCHEMA_FILE_SIZE zeroed bytes and writes Page 0. */
int schema_create(const char *path, uint32_t partition_id,
                  const char *schema_name, SchemaFile *out);

/* Open an existing __schema.mydb. Verifies magic, version, file type,
 * and Page 0 checksum. Loads the slot directory and every valid
 * relation's RelationDef page into memory. */
int schema_open(const char *path, SchemaFile *out);

/* Close the file descriptor. Caller is responsible for saving any
 * unsaved changes; every public mutator already saves. */
int schema_close(SchemaFile *sf);

/* Refresh last_modified, repack Page 0, recompute checksum, pwrite,
 * fsync. Use after editing header/relations[] without going through
 * a higher-level mutator. */
int schema_save_page0(SchemaFile *sf);

/* ------------------------------------------------------------------ */
/*  RelationDef CRUD                                                   */
/* ------------------------------------------------------------------ */

/* Add a relation: picks the lowest free slot in relations[] and the
 * lowest free def page (1..SCHEMA_FILE_PAGES-1), serializes def to that
 * page, marks the slot valid, persists the def page and Page 0.
 * Rejects duplicate relation_name. Returns MYDB_ERR_FULL when no slot
 * is free. */
int schema_add_relation(SchemaFile *sf, const RelationDef *def);

/* Remove a relation by name: zeroes the def page on disk, clears the
 * slot, persists Page 0. Returns MYDB_ERR_NOT_FOUND if absent. */
int schema_remove_relation(SchemaFile *sf, const char *relation_name);

/* Linear lookups. Pointers are valid for the lifetime of *sf, NULL if
 * no valid slot matches. */
RelationDef   *schema_find_relation     (SchemaFile *sf, const char *relation_name);
RelationEntry *schema_find_relation_stat(SchemaFile *sf, const char *relation_name);

/* Re-serialize and persist the def page for an existing relation.
 * Used when a running relation's auto_incr_counter or root_page_no
 * changes. Does NOT touch Page 0. */
int schema_flush_relation(SchemaFile *sf, const char *relation_name);

/* Update optimizer-stat fields (num_rows, num_pages, tree_height) on
 * the relation's slot and persist Page 0. */
int schema_update_stats(SchemaFile *sf, const char *relation_name,
                        uint32_t num_rows, uint32_t num_pages,
                        uint8_t tree_height);

/* Increment (delta>0) or decrement (delta<0) the persisted num_pages
 * counter for one relation slot and persist Page 0. Refuses to drive
 * the counter below 0. Returns MYDB_ERR_NOT_FOUND if no valid slot
 * matches `relation_name`.
 *
 * Used by storage.c after every successful partition_alloc_page on a
 * user relation (phase 9 — closes the deferral from phase 6's
 * single-responsibility split). */
int schema_bump_relation_pages(SchemaFile *sf, const char *relation_name,
                               int32_t delta);

/* Increment (delta>0) or decrement (delta<0) the persisted num_rows
 * counter for one relation slot and persist Page 0. Refuses to drive
 * the counter below 0. Returns MYDB_ERR_NOT_FOUND if no valid slot
 * matches `relation_name`.
 *
 * Called by storage.c after every successful storage_insert (+1) and
 * storage_delete (-1) to keep num_rows accurate for the CBO. */
int schema_bump_relation_rows(SchemaFile *sf, const char *relation_name,
                              int32_t delta);

/* Sum of (entry.num_pages * PAGE_SIZE) across valid slots. Computed
 * fresh — never read from disk. */
uint64_t schema_compute_size_bytes(const SchemaFile *sf);

#endif /* SCHEMA_FILE_H */
