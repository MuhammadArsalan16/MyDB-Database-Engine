#include "storage.h"
#include "engine.h"
#include "page.h"
#include "buffer_pool.h"

#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <errno.h>
#include <sys/stat.h>
#include <sys/types.h>

/* ------------------------------------------------------------------ */
/*  Internal types                                                       */
/* ------------------------------------------------------------------ */

/* One entry in the open-table registry. Storage caches DiskManager +
 * BTree handles per relation_name so a single SQL session doesn't
 * reopen the file on every call. */
typedef struct {
    char        name[MAX_TABLE_NAME];
    int         id;             /* table_id used with the buffer pool */
    DiskManager dm;
    BTree       clustered;
    BTree       secondary[MAX_SECONDARY_IDX];
    int         is_open;
} OpenTable;

/*
 * StorageScan — wraps a BTree cursor with relation context.
 * storage_scan() returns (Cursor *)scan; cursor_next/close cast it back.
 * struct Cursor MUST be the first member so the cast is valid.
 */
typedef struct {
    Cursor       btree_cur;     /* must be first */
    OpenTable   *ot;
    RelationDef *rel;
    Row          current_row;
    uint8_t      rec_buf[PAGE_SIZE];
    int          sec_idx;       /* -1 = clustered scan; >=0 = secondary index scan */
} StorageScan;

/* Singleton storage state (one engine per process) */
typedef struct {
    EngineState        *eng;             /* set by storage_init */
    BufferPool          bp;
    TransactionManager  trx;
    OpenTable           open_tables[MAX_TABLES];
    int                 num_open;
    int                 next_table_id;   /* monotonic counter for table IDs */
    int                 initialized;
} StorageState;

/*
 * File-static singleton — deliberate exception to the "no globals" rule.
 * A storage engine is inherently a single shared resource per process.
 */
static StorageState g;

/* ------------------------------------------------------------------ */
/*  Helpers                                                             */
/* ------------------------------------------------------------------ */

/* Build the on-disk path for a relation file:
 *   <partition_path>/<schema_name>/<relation>.mydb  */
static int build_path(char *out, size_t outlen, const char *relation_name)
{
    int n = snprintf(out, outlen, "%s/%s/%s.mydb",
                     g.eng->current_partition_path,
                     g.eng->current_schema_name,
                     relation_name);
    return (n < 0 || (size_t)n >= outlen) ? MYDB_ERR : MYDB_OK;
}

/* Find an already-open table by relation name; NULL if not open. */
static OpenTable *find_open(const char *name)
{
    for (int i = 0; i < MAX_TABLES; i++) {
        if (g.open_tables[i].is_open &&
            strcmp(g.open_tables[i].name, name) == 0)
            return &g.open_tables[i];
    }
    return NULL;
}

/* Open a relation lazily: load DiskManager + BTree handles, register
 * the table_id with the trx manager. The fresh writable RelationDef
 * pointer comes from the active schema (storage is the single writer
 * for auto_incr_counter / root_page_no, so we always read through
 * the active SchemaFile rather than trusting the caller's copy). */
static OpenTable *open_table(const char *name)
{
    OpenTable *ot = find_open(name);
    if (ot) return ot;

    RelationDef *r = schema_find_relation(&g.eng->active_schema, name);
    if (!r) return NULL;

    int slot = -1;
    for (int i = 0; i < MAX_TABLES; i++) {
        if (!g.open_tables[i].is_open) { slot = i; break; }
    }
    if (slot < 0) return NULL;

    ot = &g.open_tables[slot];
    memset(ot, 0, sizeof(OpenTable));

    char path[512];
    if (build_path(path, sizeof(path), name) != MYDB_OK) return NULL;

    if (disk_open(&ot->dm, path) != MYDB_OK) return NULL;

    strncpy(ot->name, name, MAX_TABLE_NAME - 1);
    ot->id = g.next_table_id++;

    /* clustered B+ tree */
    btree_init(&ot->clustered, &g.bp, &ot->dm, ot->id,
               r->root_page_no,
               r->columns[r->pk_col_idx].type, 0);

    /* secondary B+ trees — unique cols use is_secondary=1 (rejects duplicates),
     * non-unique (INDEXED) cols use is_secondary=2 (allows duplicates). */
    for (int i = 0; i < r->num_secondary_indexes; i++) {
        int     ci     = r->secondary_col_idx[i];
        uint8_t is_sec = r->columns[ci].is_unique ? 1 : 2;
        btree_init(&ot->secondary[i], &g.bp, &ot->dm, ot->id,
                   r->secondary_root_page_no[i],
                   r->columns[ci].type, is_sec);
    }

    ot->is_open = 1;
    g.num_open++;

    trx_register_table(&g.trx, ot->id, &ot->dm);
    return ot;
}

static int close_table(OpenTable *ot)
{
    if (!ot || !ot->is_open) return MYDB_OK;
    bp_flush_table(&g.bp, ot->id);
    bp_evict_table(&g.bp, ot->id);
    trx_unregister_table(&g.trx, ot->id);
    disk_close(&ot->dm);
    ot->is_open = 0;
    g.num_open--;
    return MYDB_OK;
}

/* ------------------------------------------------------------------ */
/*  Row value serialization                                             */
/*                                                                      */
/*  Value bytes layout:                                                 */
/*    [DB_TRX_ID:   6B LE]                                             */
/*    [DB_ROLL_PTR: 7B,  all zeros in Phase 1]                         */
/*    [null_bitmap: 4B,  bit i = 1 if col i is NULL]                   */
/*    [col[0] ... col[n-1] serialized based on type]                   */
/*                                                                      */
/*  Column sizes: INT=4, DECIMAL=8, VARCHAR=2+len, ENUM=1,             */
/*                BOOL=1, DATE=4, DATETIME=8.                           */
/*  NULL columns: write zero bytes of the appropriate size.            */
/* ------------------------------------------------------------------ */

static void write_col(const Value *v, const ColumnDef *col, uint8_t *out)
{
    if (v->is_null) {
        memset(out, 0, relation_col_size(col));
        return;
    }
    switch (col->type) {
        case TYPE_INT:
            memcpy(out, &v->v.int_val, 4);
            break;
        case TYPE_DECIMAL:
            memcpy(out, &v->v.decimal_val, 8);
            break;
        case TYPE_VARCHAR: {
            uint16_t len = v->v.varchar_val.len;
            if (len > col->max_len) len = col->max_len;
            memcpy(out, &len, 2);
            memcpy(out + 2, v->v.varchar_val.data, len);
            break;
        }
        case TYPE_ENUM:   out[0] = v->v.enum_val;  break;
        case TYPE_BOOL:   out[0] = v->v.bool_val;  break;
        case TYPE_DATE:   memcpy(out, &v->v.date_val, 4); break;
        case TYPE_DATETIME: memcpy(out, &v->v.datetime_val, 8); break;
    }
}

static void read_col(const uint8_t *in, const ColumnDef *col, Value *v)
{
    memset(v, 0, sizeof(Value));
    v->type = col->type;
    switch (col->type) {
        case TYPE_INT:
            memcpy(&v->v.int_val, in, 4);
            break;
        case TYPE_DECIMAL:
            memcpy(&v->v.decimal_val, in, 8);
            break;
        case TYPE_VARCHAR: {
            uint16_t len;
            memcpy(&len, in, 2);
            v->v.varchar_val.len = len;
            memcpy(v->v.varchar_val.data, in + 2, len);
            break;
        }
        case TYPE_ENUM:     v->v.enum_val = in[0]; break;
        case TYPE_BOOL:     v->v.bool_val = in[0]; break;
        case TYPE_DATE:     memcpy(&v->v.date_val, in, 4); break;
        case TYPE_DATETIME: memcpy(&v->v.datetime_val, in, 8); break;
    }
}

/* Serialize hidden cols + null bitmap + user cols into out[].
   Returns number of bytes written. */
static uint16_t serialize_row_value(const RelationDef *r, const Row *row,
                                    uint64_t trx_id, uint8_t *out)
{
    uint16_t off = 0;

    memcpy(out + off, &trx_id, 6);   /* DB_TRX_ID */
    off += 6;
    memset(out + off, 0, 7);          /* DB_ROLL_PTR (zeros in Phase 1) */
    off += 7;

    /* null bitmap (4 bytes = 32 bits, one per column) */
    uint32_t null_bits = 0;
    for (int i = 0; i < r->num_columns; i++) {
        if (row->cols[i].is_null)
            null_bits |= (1u << i);
    }
    memcpy(out + off, &null_bits, 4);
    off += 4;

    for (int i = 0; i < r->num_columns; i++) {
        uint16_t csz = relation_col_size(&r->columns[i]);
        write_col(&row->cols[i], &r->columns[i], out + off);
        off += csz;
    }

    return off;
}

static void deserialize_row_value(const uint8_t *val, uint16_t vlen,
                                   const RelationDef *r, Row *row)
{
    (void)vlen;
    uint16_t off = 0;
    off += 13;    /* skip DB_TRX_ID (6) + DB_ROLL_PTR (7) */

    uint32_t null_bits;
    memcpy(&null_bits, val + off, 4);
    off += 4;

    row->num_cols = r->num_columns;
    for (int i = 0; i < r->num_columns; i++) {
        uint16_t csz = relation_col_size(&r->columns[i]);
        read_col(val + off, &r->columns[i], &row->cols[i]);
        row->cols[i].is_null = (null_bits >> i) & 1;
        off += csz;
    }
}

/* ------------------------------------------------------------------ */
/*  Clustered record building                                           */
/*  Format: [klen:2BE][key_bytes][vlen:2BE][val_bytes]                 */
/* ------------------------------------------------------------------ */

static uint16_t build_clustered_record(const RelationDef *r, const Row *row,
                                        uint64_t trx_id, uint8_t *out)
{
    const Value *pk_val = &row->cols[r->pk_col_idx];
    uint8_t  key_buf[MAX_VARCHAR_LEN + 2];
    uint16_t klen = btree_key_encode(pk_val, key_buf);

    uint8_t  val_buf[PAGE_SIZE];
    uint16_t vlen = serialize_row_value(r, row, trx_id, val_buf);

    uint16_t off = 0;
    out[off++] = (uint8_t)(klen >> 8);
    out[off++] = (uint8_t)(klen & 0xFF);
    memcpy(out + off, key_buf, klen);  off += klen;
    out[off++] = (uint8_t)(vlen >> 8);
    out[off++] = (uint8_t)(vlen & 0xFF);
    memcpy(out + off, val_buf, vlen);  off += vlen;
    return off;
}

/* ------------------------------------------------------------------ */
/*  Secondary index record                                              */
/*  Format: [klen:2BE][key_bytes][page_no:4LE][slot_no:2LE]            */
/* ------------------------------------------------------------------ */

static uint16_t build_secondary_record(const Value *key, RID rid, uint8_t *out)
{
    uint8_t  key_buf[MAX_VARCHAR_LEN + 2];
    uint16_t klen = btree_key_encode(key, key_buf);

    uint16_t off = 0;
    out[off++] = (uint8_t)(klen >> 8);
    out[off++] = (uint8_t)(klen & 0xFF);
    memcpy(out + off, key_buf, klen);  off += klen;
    memcpy(out + off, &rid.page_no, 4);  off += 4;
    memcpy(out + off, &rid.slot_no, 2);  off += 2;
    return off;
}

/* Compare two Values of the same type via their encoded key form. */
static int value_compare(const Value *a, const Value *b)
{
    uint8_t  ab[MAX_VARCHAR_LEN + 2], bb[MAX_VARCHAR_LEN + 2];
    uint16_t alen = btree_key_encode(a, ab);
    uint16_t blen = btree_key_encode(b, bb);
    return btree_key_compare(ab, alen, bb, blen, a->type);
}

static void record_get_pk(const uint8_t *rec, DataType pk_type, Value *out_pk)
{
    uint16_t klen = ((uint16_t)rec[0] << 8) | rec[1];
    btree_key_decode(rec + 2, klen, pk_type, out_pk);
}

/* ------------------------------------------------------------------ */
/*  Quota reconciliation after DML                                      */
/*                                                                      */
/*  Step 2 of the phase 9 plan: a successful DML may have grown the     */
/*  relation file via btree splits. The pre/post difference in          */
/*  dm.num_pages tells us by how much. We bump RelationEntry.num_pages  */
/*  so __schema.mydb stays the persisted source of truth, and call      */
/*  cat_track_alloc so __catalog.mydb's used_bytes stays consistent     */
/*  with the partition's actual disk consumption.                       */
/*                                                                      */
/*  Note: this is a coarse, post-hoc reconciliation. v1 has no per-page */
/*  free path, so we never drive deltas negative on user relations      */
/*  outside DROP TABLE (which uses disk_destroy + cat_track_alloc       */
/*  directly). Pre-check quota at the entry to DML to avoid growing     */
/*  past the quota; this reconciliation closes the loop afterwards.    */
/* ------------------------------------------------------------------ */

static void reconcile_growth(OpenTable *ot, const char *relation_name,
                             uint32_t pages_before)
{
    uint32_t pages_after = ot->dm.num_pages;
    if (pages_after == pages_before) return;
    int32_t delta = (int32_t)(pages_after - pages_before);

    /* If the clustered B+ tree grew taller, sync tree_height in RAM
     * before schema_bump_relation_pages calls schema_save_page0, so
     * both changes are flushed in the same write. */
    uint8_t h = btree_compute_height(&ot->clustered);
    RelationEntry *e = schema_find_relation_stat(&g.eng->active_schema,
                                                  relation_name);
    if (e && h > e->tree_height)
        e->tree_height = h;

    schema_bump_relation_pages(&g.eng->active_schema, relation_name, delta);
    cat_track_alloc(&g.eng->active_catalog, (int64_t)delta * PAGE_SIZE);
}

/* Quota pre-check: we don't know exactly how many pages a DML will
 * allocate (b-tree splits depend on key size and tree shape), but a
 * conservative upper bound of 4 pages catches the common "1 leaf
 * split + maybe 1 internal split + maybe 1 root growth" pattern.
 * Returns MYDB_OK if at least `headroom_pages` pages still fit under
 * the partition quota, MYDB_ERR_FULL otherwise. */
static int quota_headroom(uint32_t headroom_pages)
{
    Catalog *c = &g.eng->active_catalog;
    if (c->header.used_bytes > c->header.quota_bytes) return MYDB_ERR_FULL;
    uint64_t need = (uint64_t)headroom_pages * PAGE_SIZE;
    if (c->header.quota_bytes - c->header.used_bytes < need) return MYDB_ERR_FULL;
    return MYDB_OK;
}

#define DML_QUOTA_HEADROOM_PAGES 4

/* ------------------------------------------------------------------ */
/*  Engine lifecycle                                                    */
/* ------------------------------------------------------------------ */

int storage_init(EngineState *eng)
{
    if (!eng) return MYDB_ERR;
    if (g.initialized) return MYDB_OK;

    memset(&g, 0, sizeof(g));
    g.eng           = eng;
    g.next_table_id = 1;

    bp_init(&g.bp);
    trx_init(&g.trx, &g.bp);

    g.initialized = 1;
    return MYDB_OK;
}

int storage_shutdown(void)
{
    if (!g.initialized) return MYDB_OK;

    if (trx_is_active(&g.trx)) trx_commit(&g.trx);

    for (int i = 0; i < MAX_TABLES; i++) {
        if (g.open_tables[i].is_open)
            close_table(&g.open_tables[i]);
    }

    g.eng         = NULL;
    g.initialized = 0;
    return MYDB_OK;
}

int storage_flush_all_dirty(void)
{
    if (!g.initialized) return MYDB_OK;
    return bp_flush_dirty_all(&g.bp);
}

/* ------------------------------------------------------------------ */
/*  DDL                                                                 */
/* ------------------------------------------------------------------ */

int storage_create_schema(const char *name)
{
    if (!g.initialized || !name || name[0] == '\0') return MYDB_ERR;
    if (strlen(name) >= sizeof(g.eng->current_schema_name)) return MYDB_ERR;

    /* Owner check: only partition owners can create schemas. Analysts
     * (no partition) get MYDB_ERR_PERM here. */
    if (!g.eng->partition_open) return MYDB_ERR_PERM;

    /* Reject duplicates against the partition catalog. */
    if (cat_find_schema(&g.eng->active_catalog, name) != NULL)
        return MYDB_ERR_DUPLICATE;

    /* Build <partition>/<name>/ and <partition>/<name>/__schema.mydb. */
    char dir[256], path[256];
    int n = snprintf(dir, sizeof(dir), "%s/%s",
                     g.eng->current_partition_path, name);
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
    int rc = schema_create(path, g.eng->current_partition_id, name, &sf);
    if (rc != MYDB_OK) {
        rmdir(dir);   /* best-effort cleanup if dir was newly created */
        return rc;
    }
    schema_close(&sf);

    rc = cat_add_schema(&g.eng->active_catalog, name);
    if (rc != MYDB_OK) {
        unlink(path);
        rmdir(dir);
        return rc;
    }
    return MYDB_OK;
}

int storage_create_table(RelationDef *rel)
{
    if (!g.initialized || !rel) return MYDB_ERR;
    if (!g.eng->schema_active) return MYDB_ERR_PERM;
    if (schema_find_relation(&g.eng->active_schema, rel->relation_name) != NULL)
        return MYDB_ERR_DUPLICATE;

    /* Pre-check quota: clustered root + one root per secondary index. */
    uint32_t need_pages = 1u + rel->num_secondary_indexes;
    if (quota_headroom(need_pages) != MYDB_OK) return MYDB_ERR_FULL;

    rel->root_page_no = INVALID_PAGE;

    char path[512];
    if (build_path(path, sizeof(path), rel->relation_name) != MYDB_OK)
        return MYDB_ERR;

    DiskManager dm;
    if (disk_create(&dm, path) != MYDB_OK) return MYDB_ERR;

    /* Allocate the clustered root through partition_alloc_page so the
     * partition quota counter reflects the growth. partition_alloc_page
     * grows the file on disk; bp_fetch_page brings it into a frame so
     * we can initialise it. */
    uint32_t root_pno;
    int rc = partition_alloc_page(&g.eng->active_catalog, &dm,
                                  g.eng->current_user_id, &root_pno);
    if (rc != MYDB_OK) { disk_close(&dm); return rc; }

    uint8_t *root_page = bp_fetch_page(&g.bp, &dm, 0 /*temp id*/, root_pno);
    if (!root_page) { disk_close(&dm); return MYDB_ERR; }
    page_init(root_page, root_pno, PAGE_TYPE_DATA);
    bp_unpin_page(&g.bp, 0, root_pno, 1);
    bp_flush_table(&g.bp, 0);
    bp_evict_table(&g.bp, 0);

    rel->root_page_no = root_pno;

    /* Same pattern for each secondary index root. */
    for (int i = 0; i < rel->num_secondary_indexes; i++) {
        uint32_t sec_pno;
        rc = partition_alloc_page(&g.eng->active_catalog, &dm,
                                  g.eng->current_user_id, &sec_pno);
        if (rc != MYDB_OK) { disk_close(&dm); return rc; }

        uint8_t *sp = bp_fetch_page(&g.bp, &dm, 0, sec_pno);
        if (!sp) { disk_close(&dm); return MYDB_ERR; }
        page_init(sp, sec_pno, PAGE_TYPE_DATA);
        bp_unpin_page(&g.bp, 0, sec_pno, 1);
        bp_flush_table(&g.bp, 0);
        bp_evict_table(&g.bp, 0);
        rel->secondary_root_page_no[i] = sec_pno;
    }

    disk_close(&dm);

    /* Persist the relation in the active schema. schema_add_relation
     * takes a snapshot of *rel into its defs[] slot — subsequent
     * mutations (auto_incr_counter, etc.) flow through schema_flush_
     * relation. */
    rc = schema_add_relation(&g.eng->active_schema, rel);
    if (rc != MYDB_OK) return rc;

    /* schema_add_relation already sets tree_height = 1 for the new
     * relation.  Bump num_pages for the root pages we just allocated
     * (partition_alloc_page already bumped used_bytes). */
    schema_bump_relation_pages(&g.eng->active_schema, rel->relation_name,
                               (int32_t)need_pages);
    return MYDB_OK;
}

int storage_drop_table(RelationDef *rel)
{
    if (!g.initialized || !rel) return MYDB_ERR;
    if (!g.eng->schema_active) return MYDB_ERR_PERM;

    OpenTable *ot = find_open(rel->relation_name);
    if (ot) close_table(ot);

    /* Reclaim the partition quota for the file's pages before deleting. */
    RelationEntry *e = schema_find_relation_stat(&g.eng->active_schema,
                                                  rel->relation_name);
    if (e && e->num_pages > 0) {
        cat_track_alloc(&g.eng->active_catalog,
                        -(int64_t)e->num_pages * PAGE_SIZE);
    }

    char path[512];
    if (build_path(path, sizeof(path), rel->relation_name) != MYDB_OK)
        return MYDB_ERR;
    disk_destroy(path);

    return schema_remove_relation(&g.eng->active_schema, rel->relation_name);
}

int storage_add_index(RelationDef *rel, int col_idx)
{
    if (!g.initialized || !rel) return MYDB_ERR;
    if (!g.eng->schema_active) return MYDB_ERR_PERM;
    if (engine_check_access(g.eng, 1) != MYDB_OK) return MYDB_ERR_PERM;

    /* Work through the authoritative in-schema copy */
    RelationDef *r = schema_find_relation(&g.eng->active_schema,
                                          rel->relation_name);
    if (!r) return MYDB_ERR_NOT_FOUND;

    if (col_idx < 0 || col_idx >= r->num_columns) return MYDB_ERR;

    /* Reject if this column is already indexed */
    for (int i = 0; i < r->num_secondary_indexes; i++) {
        if (r->secondary_col_idx[i] == (uint8_t)col_idx)
            return MYDB_ERR_DUPLICATE;
    }
    if (r->num_secondary_indexes >= MAX_SECONDARY_IDX) return MYDB_ERR_FULL;
    if (quota_headroom(1) != MYDB_OK) return MYDB_ERR_FULL;

    /* Open (or reuse) the table handle */
    OpenTable *ot = open_table(r->relation_name);
    if (!ot) return MYDB_ERR;

    /* Allocate a new root page for the secondary B-tree */
    uint32_t sec_pno;
    int rc = partition_alloc_page(&g.eng->active_catalog, &ot->dm,
                                  g.eng->current_user_id, &sec_pno);
    if (rc != MYDB_OK) return rc;

    uint8_t *sp = bp_fetch_page(&g.bp, &ot->dm, ot->id, sec_pno);
    if (!sp) return MYDB_ERR;
    page_init(sp, sec_pno, PAGE_TYPE_INDEX);
    bp_unpin_page(&g.bp, ot->id, sec_pno, 1);

    /* Register the new index in the in-memory RelationDef */
    int idx_slot = r->num_secondary_indexes;
    r->secondary_col_idx[idx_slot]      = (uint8_t)col_idx;
    r->secondary_root_page_no[idx_slot] = sec_pno;
    r->num_secondary_indexes++;

    /* Wire up the BTree handle in the open-table slot */
    uint8_t is_sec = r->columns[col_idx].is_unique ? 1 : 2;
    btree_init(&ot->secondary[idx_slot], &g.bp, &ot->dm, ot->id,
               sec_pno, r->columns[col_idx].type, is_sec);

    /* Persist the updated RelationDef before backfill so a crash after
     * a partial backfill leaves the index visible (and rebuildable). */
    rc = schema_flush_relation(&g.eng->active_schema, r->relation_name);
    if (rc != MYDB_OK) return rc;
    schema_bump_relation_pages(&g.eng->active_schema, r->relation_name, 1);

    /* Backfill: scan every existing row and insert into the new secondary
     * B-tree.  We collect the full row from the clustered leaf and use the
     * cursor's last_page_no / last_slot as the RID to store in the leaf. */
    Cursor btree_cur;
    if (btree_cursor_open(&ot->clustered, &btree_cur) != MYDB_OK)
        return MYDB_ERR;

    uint8_t  rec_buf[PAGE_SIZE];
    uint16_t rec_len;
    while (btree_cursor_next(&btree_cur, rec_buf, &rec_len) == MYDB_OK) {
        uint16_t klen = ((uint16_t)rec_buf[0] << 8) | rec_buf[1];
        uint16_t voff = 2 + klen + 2;

        Row row;
        memset(&row, 0, sizeof(row));
        deserialize_row_value(rec_buf + voff, rec_len - voff, r, &row);

        /* Skip NULL values — nulls are not indexed */
        if (row.cols[col_idx].is_null) continue;

        RID rid = { btree_cur.last_page_no, btree_cur.last_slot };

        uint8_t  srec[PAGE_SIZE];
        uint16_t slen = build_secondary_record(&row.cols[col_idx], rid, srec);

        rc = btree_insert(&ot->secondary[idx_slot],
                          &row.cols[col_idx], srec, slen, NULL);
        if (rc != MYDB_OK && rc != MYDB_ERR_DUPLICATE) {
            btree_cursor_close(&btree_cur);
            return rc;
        }
    }
    btree_cursor_close(&btree_cur);

    return MYDB_OK;
}

/* ------------------------------------------------------------------ */
/*  Transaction delegation                                              */
/* ------------------------------------------------------------------ */

int storage_begin(void)    { return trx_begin(&g.trx); }
int storage_commit(void)   { return trx_commit(&g.trx); }
int storage_rollback(void) { return trx_rollback(&g.trx); }

/* ------------------------------------------------------------------ */
/*  FK constraint helpers                                               */
/* ------------------------------------------------------------------ */

/* Return the index of the column named col_name in r, or -1. */
static int find_col_idx(const RelationDef *r, const char *col_name)
{
    for (int i = 0; i < r->num_columns; i++)
        if (strncmp(r->columns[i].name, col_name, MAX_COLUMN_NAME) == 0)
            return i;
    return -1;
}

/* On INSERT / UPDATE: verify every FK value in row exists in the
 * referenced relation's clustered index. NULL FK values are skipped
 * (NULL satisfies any FK constraint). */
static int fk_check_ref_exists(const RelationDef *r, const Row *row)
{
    for (int i = 0; i < r->num_foreign_keys; i++) {
        const ForeignKey *fk = &r->foreign_keys[i];

        int fk_col = find_col_idx(r, fk->column_name);
        if (fk_col < 0) return MYDB_ERR;

        if (row->cols[fk_col].is_null) continue;

        RelationDef *ref = schema_find_relation(&g.eng->active_schema,
                                               fk->ref_relation_name);
        if (!ref) return MYDB_ERR_FK_VIOLATION;

        OpenTable *ref_ot = open_table(ref->relation_name);
        if (!ref_ot) return MYDB_ERR;

        Value fk_val = row->cols[fk_col];
        BTreeSearchResult res;
        if (btree_search(&ref_ot->clustered, &fk_val, &res) != MYDB_OK || !res.found)
            return MYDB_ERR_FK_VIOLATION;
    }
    return MYDB_OK;
}

/* Forward declaration — defined after the FK helpers. */
static int read_record_by_rid(OpenTable *ot, RID rid,
                               uint8_t *out, uint16_t *out_len);

/* fk_check_not_referenced — pure RESTRICT check used by the UPDATE path.
 * Returns MYDB_ERR_FK_VIOLATION if any referencing row exists. */
static int fk_check_not_referenced(const RelationDef *r, const Value *pk_val)
{
    const char *pk_col_name = r->columns[r->pk_col_idx].name;
    SchemaFile *sf = &g.eng->active_schema;

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

            OpenTable *ref_ot = open_table(ref_rel->relation_name);
            if (!ref_ot) return MYDB_ERR;

            Cursor btree_cur;
            if (btree_cursor_open(&ref_ot->clustered, &btree_cur) != MYDB_OK)
                return MYDB_ERR;

            uint8_t  rec_buf[PAGE_SIZE];
            uint16_t rec_len;
            while (btree_cursor_next(&btree_cur, rec_buf, &rec_len) == MYDB_OK) {
                uint16_t klen = ((uint16_t)rec_buf[0] << 8) | rec_buf[1];
                uint16_t voff = 2 + klen + 2;
                Row row;
                memset(&row, 0, sizeof(row));
                deserialize_row_value(rec_buf + voff, rec_len - voff,
                                      ref_rel, &row);
                if (value_compare(&row.cols[fk_col], pk_val) == 0) {
                    btree_cursor_close(&btree_cur);
                    return MYDB_ERR_FK_VIOLATION;
                }
            }
            btree_cursor_close(&btree_cur);
        }
    }
    return MYDB_OK;
}

/* Maximum referencing rows processed per CASCADE / SET_NULL pass.
 * We collect all matching RIDs before mutating to avoid cursor
 * invalidation while the table is being modified. */
#define FK_ACTION_MAX_ROWS 256

/*
 * fk_apply_on_delete — enforce ON DELETE actions for every FK that
 * references relation `r` at primary key `pk_val`.
 *
 * RESTRICT (0) : return MYDB_ERR_FK_VIOLATION if any referencing row exists.
 * CASCADE  (1) : delete all referencing rows (recursive — may cascade further).
 * SET_NULL (2) : set the FK column to NULL in all referencing rows.
 *
 * Called only from storage_delete. The UPDATE path always uses
 * fk_check_not_referenced (RESTRICT-only) because ON UPDATE actions
 * are out of scope for Phase 1.
 */
static int fk_apply_on_delete(const RelationDef *r, const Value *pk_val)
{
    const char *pk_col_name = r->columns[r->pk_col_idx].name;
    SchemaFile *sf = &g.eng->active_schema;

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

            OpenTable *ref_ot = open_table(ref_rel->relation_name);
            if (!ref_ot) return MYDB_ERR;

            /* ---- RESTRICT: scan and fail on first match ---- */
            if (action == FK_ON_DELETE_RESTRICT) {
                Cursor btree_cur;
                if (btree_cursor_open(&ref_ot->clustered, &btree_cur) != MYDB_OK)
                    return MYDB_ERR;

                uint8_t  rec_buf[PAGE_SIZE];
                uint16_t rec_len;
                while (btree_cursor_next(&btree_cur, rec_buf, &rec_len) == MYDB_OK) {
                    uint16_t klen = ((uint16_t)rec_buf[0] << 8) | rec_buf[1];
                    uint16_t voff = 2 + klen + 2;
                    Row row;
                    memset(&row, 0, sizeof(row));
                    deserialize_row_value(rec_buf + voff, rec_len - voff,
                                          ref_rel, &row);
                    if (value_compare(&row.cols[fk_col], pk_val) == 0) {
                        btree_cursor_close(&btree_cur);
                        return MYDB_ERR_FK_VIOLATION;
                    }
                }
                btree_cursor_close(&btree_cur);
                continue;
            }

            /* ---- CASCADE / SET_NULL: collect matching RIDs first,
             *     then apply the action outside the scan loop to avoid
             *     cursor invalidation while the table is being mutated. ---- */
            RID rids[FK_ACTION_MAX_ROWS];
            int nfound = 0;

            Cursor btree_cur;
            if (btree_cursor_open(&ref_ot->clustered, &btree_cur) != MYDB_OK)
                return MYDB_ERR;

            uint8_t  rec_buf[PAGE_SIZE];
            uint16_t rec_len;
            while (btree_cursor_next(&btree_cur, rec_buf, &rec_len) == MYDB_OK) {
                uint16_t klen = ((uint16_t)rec_buf[0] << 8) | rec_buf[1];
                uint16_t voff = 2 + klen + 2;
                Row row;
                memset(&row, 0, sizeof(row));
                deserialize_row_value(rec_buf + voff, rec_len - voff,
                                      ref_rel, &row);

                if (value_compare(&row.cols[fk_col], pk_val) != 0) continue;

                if (nfound >= FK_ACTION_MAX_ROWS) {
                    btree_cursor_close(&btree_cur);
                    return MYDB_ERR;   /* Phase 1 batch limit exceeded */
                }
                rids[nfound].page_no = btree_cur.last_page_no;
                rids[nfound].slot_no = btree_cur.last_slot;
                nfound++;
            }
            btree_cursor_close(&btree_cur);

            /* Apply the action for every collected RID */
            for (int k = 0; k < nfound; k++) {
                if (action == FK_ON_DELETE_CASCADE) {
                    /* Recursive: storage_delete may itself cascade further */
                    int rc = storage_delete(ref_rel, rids[k]);
                    if (rc != MYDB_OK) return rc;
                } else {
                    /* SET_NULL: re-fetch the row, zero the FK column, update */
                    ref_ot = open_table(ref_rel->relation_name);
                    if (!ref_ot) return MYDB_ERR;

                    uint8_t  r_buf[PAGE_SIZE];
                    uint16_t r_len;
                    if (read_record_by_rid(ref_ot, rids[k], r_buf, &r_len) != MYDB_OK)
                        return MYDB_ERR;

                    uint16_t klen = ((uint16_t)r_buf[0] << 8) | r_buf[1];
                    uint16_t voff = 2 + klen + 2;
                    Row updated_row;
                    memset(&updated_row, 0, sizeof(updated_row));
                    deserialize_row_value(r_buf + voff, r_len - voff,
                                          ref_rel, &updated_row);
                    updated_row.cols[fk_col].is_null = 1;

                    int rc = storage_update(ref_rel, rids[k], &updated_row);
                    if (rc != MYDB_OK) return rc;
                }
            }
        }
    }
    return MYDB_OK;
}


/* ------------------------------------------------------------------ */
/*  DML — INSERT                                                        */
/* ------------------------------------------------------------------ */

int storage_insert(RelationDef *rel, Row *row)
{
    if (!g.initialized || !rel || !row) return MYDB_ERR;
    if (engine_check_access(g.eng, 1) != MYDB_OK) return MYDB_ERR_PERM;

    /* Read the writable RelationDef from the active schema — caller's
     * pointer may be a parser-side const view. */
    RelationDef *r = schema_find_relation(&g.eng->active_schema,
                                          rel->relation_name);
    if (!r) return MYDB_ERR_NOT_FOUND;

    OpenTable *ot = open_table(rel->relation_name);
    if (!ot) return MYDB_ERR;

    if (quota_headroom(DML_QUOTA_HEADROOM_PAGES) != MYDB_OK)
        return MYDB_ERR_FULL;

    int auto_txn = !trx_is_active(&g.trx);
    if (auto_txn) trx_begin(&g.trx);

    /* NOT NULL */
    for (int i = 0; i < r->num_columns; i++) {
        if (r->columns[i].is_not_null && !r->columns[i].is_auto_increment) {
            if (row->cols[i].is_null) {
                if (auto_txn) trx_rollback(&g.trx);
                return MYDB_ERR_NULL_VIOLATION;
            }
        }
    }

    /* FK referential integrity */
    int fk_rc = fk_check_ref_exists(r, row);
    if (fk_rc != MYDB_OK) {
        if (auto_txn) trx_rollback(&g.trx);
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

    uint32_t pages_before = ot->dm.num_pages;

    uint8_t  rec_buf[PAGE_SIZE];
    uint16_t rec_len = build_clustered_record(r, row, trx_current_id(&g.trx),
                                              rec_buf);

    RID rid;
    int rc = btree_insert(&ot->clustered, &row->cols[pk], rec_buf, rec_len, &rid);
    if (rc != MYDB_OK) {
        reconcile_growth(ot, rel->relation_name, pages_before);
        if (auto_txn) trx_rollback(&g.trx);
        return rc;
    }

    for (int i = 0; i < r->num_secondary_indexes; i++) {
        int ci = r->secondary_col_idx[i];
        uint8_t  srec[MAX_VARCHAR_LEN + 2 + 4 + 2];
        uint16_t slen = build_secondary_record(&row->cols[ci], rid, srec);

        rc = btree_insert(&ot->secondary[i], &row->cols[ci], srec, slen, NULL);
        if (rc != MYDB_OK) {
            reconcile_growth(ot, rel->relation_name, pages_before);
            if (auto_txn) trx_rollback(&g.trx);
            return rc;
        }
    }

    reconcile_growth(ot, rel->relation_name, pages_before);
    schema_bump_relation_rows(&g.eng->active_schema, rel->relation_name, 1);

    if (r->columns[pk].is_auto_increment) {
        schema_flush_relation(&g.eng->active_schema, rel->relation_name);
    }

    if (auto_txn) trx_commit(&g.trx);
    return MYDB_OK;
}

/* ------------------------------------------------------------------ */
/*  Read a raw clustered record by RID                                  */
/* ------------------------------------------------------------------ */

static int read_record_by_rid(OpenTable *ot, RID rid,
                               uint8_t *rec_out, uint16_t *len_out)
{
    uint8_t *page = bp_fetch_page(&g.bp, &ot->dm, ot->id, rid.page_no);
    if (!page) return MYDB_ERR;

    uint16_t data_off, data_sz;
    int rc = page_get_record(page, rid.slot_no, &data_off, &data_sz);
    if (rc != MYDB_OK) {
        bp_unpin_page(&g.bp, ot->id, rid.page_no, 0);
        return rc;
    }

    memcpy(rec_out, page + data_off, data_sz);
    *len_out = data_sz;
    bp_unpin_page(&g.bp, ot->id, rid.page_no, 0);
    return MYDB_OK;
}

/* ------------------------------------------------------------------ */
/*  DML — DELETE                                                        */
/* ------------------------------------------------------------------ */

int storage_delete(RelationDef *rel, RID rid)
{
    if (!g.initialized || !rel) return MYDB_ERR;
    if (engine_check_access(g.eng, 1) != MYDB_OK) return MYDB_ERR_PERM;

    RelationDef *r = schema_find_relation(&g.eng->active_schema,
                                          rel->relation_name);
    if (!r) return MYDB_ERR_NOT_FOUND;

    OpenTable *ot = open_table(rel->relation_name);
    if (!ot) return MYDB_ERR;

    int auto_txn = !trx_is_active(&g.trx);
    if (auto_txn) trx_begin(&g.trx);

    uint8_t  rec[PAGE_SIZE];
    uint16_t rec_len;
    if (read_record_by_rid(ot, rid, rec, &rec_len) != MYDB_OK) {
        if (auto_txn) trx_rollback(&g.trx);
        return MYDB_ERR;
    }

    Value pk_val;
    record_get_pk(rec, r->columns[r->pk_col_idx].type, &pk_val);

    /* FK referential integrity — apply ON DELETE action (RESTRICT / CASCADE
     * / SET_NULL) for every FK that references this row. */
    int fk_rc = fk_apply_on_delete(r, &pk_val);
    if (fk_rc != MYDB_OK) {
        if (auto_txn) trx_rollback(&g.trx);
        return fk_rc;
    }

    uint16_t klen = ((uint16_t)rec[0] << 8) | rec[1];
    uint16_t voff = 2 + klen + 2;
    Row row;
    memset(&row, 0, sizeof(row));
    deserialize_row_value(rec + voff, rec_len - voff, r, &row);

    for (int i = 0; i < r->num_secondary_indexes; i++) {
        int ci = r->secondary_col_idx[i];
        btree_delete(&ot->secondary[i], &row.cols[ci]);
    }

    int rc = btree_delete(&ot->clustered, &pk_val);
    if (rc == MYDB_OK)
        schema_bump_relation_rows(&g.eng->active_schema, rel->relation_name, -1);
    if (auto_txn) {
        if (rc == MYDB_OK) trx_commit(&g.trx); else trx_rollback(&g.trx);
    }
    return rc;
}

/* ------------------------------------------------------------------ */
/*  DML — UPDATE                                                        */
/* ------------------------------------------------------------------ */

int storage_update(RelationDef *rel, RID rid, Row *new_row)
{
    if (!g.initialized || !rel || !new_row) return MYDB_ERR;
    if (engine_check_access(g.eng, 1) != MYDB_OK) return MYDB_ERR_PERM;

    RelationDef *r = schema_find_relation(&g.eng->active_schema,
                                          rel->relation_name);
    if (!r) return MYDB_ERR_NOT_FOUND;

    OpenTable *ot = open_table(rel->relation_name);
    if (!ot) return MYDB_ERR;

    if (quota_headroom(DML_QUOTA_HEADROOM_PAGES) != MYDB_OK)
        return MYDB_ERR_FULL;

    int auto_txn = !trx_is_active(&g.trx);
    if (auto_txn) trx_begin(&g.trx);

    /* NOT NULL */
    for (int i = 0; i < r->num_columns; i++) {
        if (r->columns[i].is_not_null && new_row->cols[i].is_null) {
            if (auto_txn) trx_rollback(&g.trx);
            return MYDB_ERR_NULL_VIOLATION;
        }
    }

    uint8_t  old_rec[PAGE_SIZE];
    uint16_t old_rec_len;
    if (read_record_by_rid(ot, rid, old_rec, &old_rec_len) != MYDB_OK) {
        if (auto_txn) trx_rollback(&g.trx);
        return MYDB_ERR;
    }

    Value old_pk;
    record_get_pk(old_rec, r->columns[r->pk_col_idx].type, &old_pk);

    uint16_t klen = ((uint16_t)old_rec[0] << 8) | old_rec[1];
    uint16_t voff = 2 + klen + 2;
    Row old_row;
    memset(&old_row, 0, sizeof(old_row));
    deserialize_row_value(old_rec + voff, old_rec_len - voff, r, &old_row);

    /* FK checks: new row's FK values must reference existing rows;
     * if the PK changes, the old PK must not be referenced by others. */
    {
        int fk_rc = fk_check_ref_exists(r, new_row);
        if (fk_rc != MYDB_OK) {
            if (auto_txn) trx_rollback(&g.trx);
            return fk_rc;
        }
    }

    /*
     * Validate-before-delete: if any UNIQUE key changes to a value that
     * already exists for another live row, fail the update without
     * touching the existing data.
     */
    const Value *new_pk = &new_row->cols[r->pk_col_idx];
    int pk_changed = (value_compare(&old_pk, new_pk) != 0);
    if (pk_changed) {
        int fk_rc = fk_check_not_referenced(r, &old_pk);
        if (fk_rc != MYDB_OK) {
            if (auto_txn) trx_rollback(&g.trx);
            return fk_rc;
        }
    }
    if (pk_changed) {
        BTreeSearchResult res;
        if (btree_search(&ot->clustered, new_pk, &res) == MYDB_OK && res.found) {
            if (auto_txn) trx_rollback(&g.trx);
            return MYDB_ERR_DUPLICATE;
        }
    }

    int sec_changed[MAX_SECONDARY_IDX] = {0};
    for (int i = 0; i < r->num_secondary_indexes; i++) {
        int ci = r->secondary_col_idx[i];
        sec_changed[i] = (value_compare(&old_row.cols[ci], &new_row->cols[ci]) != 0);
        if (sec_changed[i]) {
            BTreeSearchResult res;
            if (btree_search(&ot->secondary[i], &new_row->cols[ci], &res) == MYDB_OK
                && res.found) {
                if (auto_txn) trx_rollback(&g.trx);
                return MYDB_ERR_DUPLICATE;
            }
        }
    }

    uint32_t pages_before = ot->dm.num_pages;

    for (int i = 0; i < r->num_secondary_indexes; i++) {
        if (!sec_changed[i]) continue;
        int ci = r->secondary_col_idx[i];
        btree_delete(&ot->secondary[i], &old_row.cols[ci]);
    }
    btree_delete(&ot->clustered, &old_pk);

    uint8_t  new_rec[PAGE_SIZE];
    uint16_t new_rec_len = build_clustered_record(r, new_row,
                                                   trx_current_id(&g.trx), new_rec);
    RID new_rid;
    int rc = btree_insert(&ot->clustered, new_pk,
                          new_rec, new_rec_len, &new_rid);
    if (rc != MYDB_OK) {
        reconcile_growth(ot, rel->relation_name, pages_before);
        if (auto_txn) trx_rollback(&g.trx);
        return rc;
    }

    for (int i = 0; i < r->num_secondary_indexes; i++) {
        if (!sec_changed[i]) continue;
        int ci = r->secondary_col_idx[i];
        uint8_t  srec[MAX_VARCHAR_LEN + 2 + 4 + 2];
        uint16_t slen = build_secondary_record(&new_row->cols[ci], new_rid, srec);
        btree_insert(&ot->secondary[i], &new_row->cols[ci], srec, slen, NULL);
    }

    reconcile_growth(ot, rel->relation_name, pages_before);

    if (auto_txn) trx_commit(&g.trx);
    return MYDB_OK;
}

/* ------------------------------------------------------------------ */
/*  DQL — storage_get_by_index                                         */
/* ------------------------------------------------------------------ */

Row *storage_get_by_index(RelationDef *rel, int col_idx, Value *key)
{
    if (!g.initialized || !rel || !key) return NULL;
    if (engine_check_access(g.eng, 0) != MYDB_OK) return NULL;

    RelationDef *r = schema_find_relation(&g.eng->active_schema,
                                          rel->relation_name);
    if (!r) return NULL;

    /* Find which secondary index covers col_idx */
    int sec_idx = -1;
    for (int i = 0; i < r->num_secondary_indexes; i++) {
        if (r->secondary_col_idx[i] == (uint8_t)col_idx) {
            sec_idx = i;
            break;
        }
    }
    if (sec_idx < 0) return NULL;   /* col has no secondary index */

    OpenTable *ot = open_table(rel->relation_name);
    if (!ot) return NULL;

    /* Step 1: descend secondary tree → secondary record {klen, key, page_no, slot_no} */
    BTreeSearchResult sec_res;
    if (btree_search(&ot->secondary[sec_idx], key, &sec_res) != MYDB_OK) return NULL;
    if (!sec_res.found) return NULL;

    uint8_t *sec_page = bp_fetch_page(&g.bp, &ot->dm, ot->id, sec_res.page_no);
    if (!sec_page) return NULL;

    uint16_t doff, dsz;
    if (page_get_record(sec_page, sec_res.slot_no, &doff, &dsz) != MYDB_OK) {
        bp_unpin_page(&g.bp, ot->id, sec_res.page_no, 0);
        return NULL;
    }

    /* Parse the RID out of the secondary record: [klen:2][key:klen][page_no:4][slot_no:2] */
    const uint8_t *srec = sec_page + doff;
    uint16_t klen = ((uint16_t)srec[0] << 8) | srec[1];
    RID rid;
    memcpy(&rid.page_no, srec + 2 + klen,     4);
    memcpy(&rid.slot_no, srec + 2 + klen + 4, 2);
    bp_unpin_page(&g.bp, ot->id, sec_res.page_no, 0);

    /* Step 2: fetch full row from clustered tree using the RID */
    uint8_t *clust_page = bp_fetch_page(&g.bp, &ot->dm, ot->id, rid.page_no);
    if (!clust_page) return NULL;

    uint16_t cdoff, cdsz;
    if (page_get_record(clust_page, rid.slot_no, &cdoff, &cdsz) != MYDB_OK) {
        bp_unpin_page(&g.bp, ot->id, rid.page_no, 0);
        return NULL;
    }

    uint8_t rec[PAGE_SIZE];
    memcpy(rec, clust_page + cdoff, cdsz);
    bp_unpin_page(&g.bp, ot->id, rid.page_no, 0);

    uint16_t cklen = ((uint16_t)rec[0] << 8) | rec[1];
    uint16_t voff  = 2 + cklen + 2;

    static Row result;
    memset(&result, 0, sizeof(result));
    deserialize_row_value(rec + voff, cdsz - voff, r, &result);
    result.rid = rid;
    return &result;
}

/* ------------------------------------------------------------------ */
/*  DQL — storage_scan_by_index                                        */
/* ------------------------------------------------------------------ */

Cursor *storage_scan_by_index(RelationDef *rel, int col_idx, Value *lo)
{
    if (!g.initialized || !rel) return NULL;
    if (engine_check_access(g.eng, 0) != MYDB_OK) return NULL;

    RelationDef *r = schema_find_relation(&g.eng->active_schema,
                                          rel->relation_name);
    if (!r) return NULL;

    /* Find the secondary index that covers col_idx. */
    int sec_idx = -1;
    for (int i = 0; i < r->num_secondary_indexes; i++) {
        if (r->secondary_col_idx[i] == (uint8_t)col_idx) {
            sec_idx = i;
            break;
        }
    }
    if (sec_idx < 0) return NULL;   /* col has no secondary index */

    OpenTable *ot = open_table(rel->relation_name);
    if (!ot) return NULL;

    StorageScan *sc = (StorageScan *)malloc(sizeof(StorageScan));
    if (!sc) return NULL;

    memset(sc, 0, sizeof(StorageScan));
    sc->ot      = ot;
    sc->rel     = r;
    sc->sec_idx = sec_idx;

    int rc = lo
        ? btree_cursor_open_at(&ot->secondary[sec_idx], lo, &sc->btree_cur)
        : btree_cursor_open   (&ot->secondary[sec_idx], &sc->btree_cur);
    if (rc != MYDB_OK) { free(sc); return NULL; }

    return (Cursor *)sc;
}

/* ------------------------------------------------------------------ */
/*  DQL — storage_get_by_pk                                            */
/* ------------------------------------------------------------------ */

Row *storage_get_by_pk(RelationDef *rel, Value *pk)
{
    if (!g.initialized || !rel || !pk) return NULL;
    if (engine_check_access(g.eng, 0) != MYDB_OK) return NULL;

    RelationDef *r = schema_find_relation(&g.eng->active_schema,
                                          rel->relation_name);
    if (!r) return NULL;

    OpenTable *ot = open_table(rel->relation_name);
    if (!ot) return NULL;

    BTreeSearchResult res;
    if (btree_search(&ot->clustered, pk, &res) != MYDB_OK) return NULL;
    if (!res.found) return NULL;

    uint8_t *page = bp_fetch_page(&g.bp, &ot->dm, ot->id, res.page_no);
    if (!page) return NULL;

    uint16_t data_off, data_sz;
    if (page_get_record(page, res.slot_no, &data_off, &data_sz) != MYDB_OK) {
        bp_unpin_page(&g.bp, ot->id, res.page_no, 0);
        return NULL;
    }

    uint8_t rec[PAGE_SIZE];
    memcpy(rec, page + data_off, data_sz);
    bp_unpin_page(&g.bp, ot->id, res.page_no, 0);

    uint16_t klen = ((uint16_t)rec[0] << 8) | rec[1];
    uint16_t voff = 2 + klen + 2;

    static Row result;
    memset(&result, 0, sizeof(result));
    deserialize_row_value(rec + voff, data_sz - voff, r, &result);
    result.rid.page_no = res.page_no;
    result.rid.slot_no = res.slot_no;
    return &result;
}

/* ------------------------------------------------------------------ */
/*  DQL — scan cursor                                                   */
/* ------------------------------------------------------------------ */

Cursor *storage_scan(RelationDef *rel)
{
    if (!g.initialized || !rel) return NULL;
    if (engine_check_access(g.eng, 0) != MYDB_OK) return NULL;

    RelationDef *r = schema_find_relation(&g.eng->active_schema,
                                          rel->relation_name);
    if (!r) return NULL;

    OpenTable *ot = open_table(rel->relation_name);
    if (!ot) return NULL;

    StorageScan *sc = (StorageScan *)malloc(sizeof(StorageScan));
    if (!sc) return NULL;

    memset(sc, 0, sizeof(StorageScan));
    sc->ot      = ot;
    sc->rel     = r;
    sc->sec_idx = -1;   /* clustered scan */

    if (btree_cursor_open(&ot->clustered, &sc->btree_cur) != MYDB_OK) {
        free(sc);
        return NULL;
    }

    return (Cursor *)sc;
}

Cursor *storage_scan_from(RelationDef *rel, Value *lo)
{
    if (!g.initialized || !rel || !lo) return NULL;
    if (engine_check_access(g.eng, 0) != MYDB_OK) return NULL;

    RelationDef *r = schema_find_relation(&g.eng->active_schema,
                                          rel->relation_name);
    if (!r) return NULL;

    OpenTable *ot = open_table(rel->relation_name);
    if (!ot) return NULL;

    StorageScan *sc = (StorageScan *)malloc(sizeof(StorageScan));
    if (!sc) return NULL;

    memset(sc, 0, sizeof(StorageScan));
    sc->ot      = ot;
    sc->rel     = r;
    sc->sec_idx = -1;   /* clustered scan */

    if (btree_cursor_open_at(&ot->clustered, lo, &sc->btree_cur) != MYDB_OK) {
        free(sc);
        return NULL;
    }

    return (Cursor *)sc;
}

Row *cursor_next(Cursor *cur)
{
    if (!cur) return NULL;
    StorageScan *sc = (StorageScan *)cur;

    if (sc->sec_idx < 0) {
        /* ---- Clustered scan ---- */
        uint16_t len;
        if (btree_cursor_next(&sc->btree_cur, sc->rec_buf, &len) != MYDB_OK)
            return NULL;

        uint16_t klen = ((uint16_t)sc->rec_buf[0] << 8) | sc->rec_buf[1];
        uint16_t voff = 2 + klen + 2;

        memset(&sc->current_row, 0, sizeof(Row));
        deserialize_row_value(sc->rec_buf + voff, len - voff,
                              sc->rel, &sc->current_row);
        sc->current_row.rid.page_no = sc->btree_cur.last_page_no;
        sc->current_row.rid.slot_no = sc->btree_cur.last_slot;
        return &sc->current_row;
    }

    /* ---- Secondary index scan ----
     *
     * Step 1: advance the secondary index cursor to get the next
     *         secondary record: [klen:2BE][key:klen][page_no:4LE][slot_no:2LE]
     * Step 2: parse the RID out of the secondary record.
     * Step 3: fetch the full row from the clustered index using that RID.
     */
    uint16_t sec_len;
    if (btree_cursor_next(&sc->btree_cur, sc->rec_buf, &sec_len) != MYDB_OK)
        return NULL;

    uint16_t klen = ((uint16_t)sc->rec_buf[0] << 8) | sc->rec_buf[1];
    RID rid;
    memcpy(&rid.page_no, sc->rec_buf + 2 + klen,     4);
    memcpy(&rid.slot_no, sc->rec_buf + 2 + klen + 4, 2);

    uint8_t *clust_page = bp_fetch_page(&g.bp, &sc->ot->dm,
                                         sc->ot->id, rid.page_no);
    if (!clust_page) return NULL;

    uint16_t doff, dsz;
    if (page_get_record(clust_page, rid.slot_no, &doff, &dsz) != MYDB_OK) {
        bp_unpin_page(&g.bp, sc->ot->id, rid.page_no, 0);
        return NULL;
    }

    uint8_t clust_rec[PAGE_SIZE];
    memcpy(clust_rec, clust_page + doff, dsz);
    bp_unpin_page(&g.bp, sc->ot->id, rid.page_no, 0);

    uint16_t cklen = ((uint16_t)clust_rec[0] << 8) | clust_rec[1];
    uint16_t voff  = 2 + cklen + 2;

    memset(&sc->current_row, 0, sizeof(Row));
    deserialize_row_value(clust_rec + voff, dsz - voff,
                          sc->rel, &sc->current_row);
    sc->current_row.rid = rid;
    return &sc->current_row;
}

void cursor_close(Cursor *cur)
{
    if (!cur) return;
    StorageScan *sc = (StorageScan *)cur;
    btree_cursor_close(&sc->btree_cur);
    free(sc);
}
