#ifndef PARTITION_H
#define PARTITION_H

#include "common.h"
#include "disk_manager.h"

/* ------------------------------------------------------------------ */
/*  __catalog.mydb — partition catalog                                */
/*                                                                    */
/*  One file per partition, 4 KB fixed. Holds the partition's:        */
/*    - identity (partition_id, owner_id)                             */
/*    - quota state (quota_bytes, used_bytes — used_bytes is the      */
/*      single persisted source of truth for storage consumption)     */
/*    - schema directory (up to MAX_SCHEMAS_PER_PARTITION names)      */
/*                                                                    */
/*  Loaded into Cache 1 on user login and stays resident.             */
/*  Direct pread/pwrite — does NOT go through the buffer pool.        */
/* ------------------------------------------------------------------ */

typedef struct {
    uint32_t partition_id;     /* this catalog's partition */
    uint32_t owner_id;         /* FK -> system_schema.users.user_id */
    uint64_t created_at;       /* YYYYMMDDHHmmSS */
    uint64_t last_modified;    /* refreshed on every cat_save */
    uint64_t quota_bytes;      /* maximum storage allowed */
    uint64_t used_bytes;       /* current consumption — persisted authoritatively */
    uint8_t  num_schemas;      /* count of valid schema slots */
    uint32_t next_table_id;    /* durable per-partition counter; next value handed
                                 * out by cat_alloc_table_id(). Never reused, even
                                 * across restarts — the WAL-addressing property
                                 * a per-process counter couldn't provide. */
    uint32_t next_schema_id;   /* durable per-partition counter; next value handed
                                 * out by cat_alloc_schema_id(). Same never-reused
                                 * guarantee as next_table_id. */
} CatalogHeader;

typedef struct {
    char     schema_name[32];
    uint8_t  num_relations;    /* updated by schema_file.c when relations come/go */
    uint8_t  is_valid;         /* 1 = occupied, 0 = empty */
    uint32_t schema_id;        /* persistent identity; mirrors the schema file's
                                 * own SchemaHeader.schema_id. Catalog-level copy
                                 * — read here, not by opening __schema.mydb. */
} SchemaEntry;

typedef struct {
    CatalogHeader header;
    SchemaEntry   schemas[MAX_SCHEMAS_PER_PARTITION];
    int           fd;
    char          path[256];
} Catalog;

/* Create a new __catalog.mydb. Fails if the file already exists. */
int cat_create(const char *path, uint32_t partition_id, uint32_t owner_id,
               uint64_t quota_bytes, Catalog *out);

/* Open an existing catalog. Verifies magic, version, file type and
 * trailer checksum. */
int cat_open(const char *path, Catalog *out);

/* Close the file descriptor. Caller is responsible for cat_save'ing
 * any unsaved changes; every public mutator already saves. */
int cat_close(Catalog *cat);

/* Refresh last_modified, repack, recompute checksum, pwrite, fsync. */
int cat_save(Catalog *cat);

/* Add a schema slot, stamping the already-allocated schema_id (from
 * cat_alloc_schema_id) into the new entry. Rejects duplicates. Returns
 * MYDB_ERR_FULL when all MAX_SCHEMAS_PER_PARTITION slots are valid.
 * Persists. */
int cat_add_schema(Catalog *cat, const char *schema_name, uint32_t schema_id);

/* Mark a schema slot empty. Does NOT delete <partition>/<schema>/
 * on disk — that's a higher-layer DROP DATABASE concern. */
int cat_remove_schema(Catalog *cat, const char *schema_name);

/* Update used_bytes by delta_bytes (signed: + on alloc, - on free).
 * Returns MYDB_ERR_FULL if a positive delta would exceed quota_bytes,
 * MYDB_ERR if a negative delta would underflow. Persists on success. */
int cat_track_alloc(Catalog *cat, int64_t delta_bytes);

/* Linear lookup. Returns NULL if no valid match. Pointer is valid
 * for the lifetime of *cat. */
SchemaEntry *cat_find_schema(Catalog *cat, const char *schema_name);

/* Hand out the next persistent table_id and persist the advanced counter
 * before returning it — durable and never reused, even across restarts.
 * Called once per CREATE TABLE, by pm_create_table, before
 * storage_create_table stamps the id into the relation's own file header. */
int cat_alloc_table_id(Catalog *cat, uint32_t *out_id);

/* Hand out the next persistent schema_id and persist the advanced counter
 * before returning it — same durability guarantee as cat_alloc_table_id.
 * Called once per CREATE SCHEMA, by pm_create_schema, before schema_create
 * stamps the id into the schema file's own header and cat_add_schema
 * stamps it into this schema's catalog-level SchemaEntry. */
int cat_alloc_schema_id(Catalog *cat, uint32_t *out_id);

/* ------------------------------------------------------------------ */
/*  Quota-aware page allocation                                       */
/* ------------------------------------------------------------------ */

/* Allocate one new page in a user relation file under partition policy.
 * Single responsibility: thread disk_alloc_page through the partition's
 * policy gates (ownership + quota).
 *
 *   1. Ownership: cat->owner_id == current_user_id. Returns
 *      MYDB_ERR_PERM on mismatch — cheapest reject, before quota math.
 *   2. Quota pre-check: cat->used_bytes + PAGE_SIZE <= cat->quota_bytes.
 *      Returns MYDB_ERR_FULL on exceedance — no disk I/O wasted.
 *   3. disk_alloc_page(dm, &pno) appends one page to the relation file.
 *   4. cat_track_alloc(cat, +PAGE_SIZE) bumps used_bytes and persists
 *      __catalog.mydb. If catalog persistence fails after the disk page
 *      was written, the file growth is left in place (v1 has no per-page
 *      free path) and used_bytes is stale-low; the inconsistency is
 *      reconciled at next startup against RelationEntry.num_pages
 *      (design doc §7 implementation note).
 *   5. *out_pno receives the new page number.
 *
 * Engine metadata files (__database.mydb, __catalog.mydb, __schema.mydb,
 * and the system_schema flat files) bypass this wrapper and call
 * disk_alloc_page directly, so their growth does not consume partition
 * quota and is not subject to per-user ownership.
 *
 * `current_user_id` comes from EngineState.current_user_id at the call
 * site; the storage layer threads it through but does not look it up.
 *
 * Phase deferrals:
 *   - RelationEntry.num_pages bump  → phase 9 (storage layer) */
int partition_alloc_page(Catalog *cat, DiskManager *dm,
                         uint32_t current_user_id, uint32_t *out_pno);

#endif /* PARTITION_H */
