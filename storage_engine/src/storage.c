#include "storage.h"
#include "page.h"

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

/*
 * v3: StorageEngine is no longer a file-static singleton.  One instance
 * lives inside each PartitionCtx (partition_manager).  All functions
 * receive StorageEngine *se explicitly so the storage layer is stateless
 * and supports one engine instance per active partition.
 *
 * Removed from StorageEngine (v2 → v3):
 *   - EngineState *eng         — replaced by filesystem context fields
 *   - TransactionManager trx   — promoted to PartitionCtx (partition_manager)
 *
 * Removed from this file (moved to partition_manager):
 *   - FK constraint helpers    (fk_check_ref_exists, fk_apply_on_delete, etc.)
 *   - Quota helpers            (quota_headroom, reconcile_growth)
 *   - Schema/catalog updates   (schema_bump_*, cat_track_alloc, etc.)
 *   - storage_create_schema / storage_drop_schema
 *   - storage_analyze_table
 *   - storage_begin / commit / rollback
 */

/* ------------------------------------------------------------------ */
/*  Helpers                                                             */
/* ------------------------------------------------------------------ */

/* Build the on-disk path for a relation file:
 *   <partition_path>/<schema_name>/<relation>.mydb
 *
 * v3: uses se->partition_path (set once at init) and the schema_name the
 * caller passes — which comes from rel->owner_schema, never from any
 * active-schema state on the engine. */
static int build_path(StorageEngine *se, char *out, size_t outlen,
                      const char *schema_name, const char *relation_name)
{
    if (!schema_name || schema_name[0] == '\0') return MYDB_ERR;
    int n = snprintf(out, outlen, "%s/%s/%s.mydb",
                     se->partition_path,
                     schema_name,
                     relation_name);
    return (n < 0 || (size_t)n >= outlen) ? MYDB_ERR : MYDB_OK;
}

/* Find an already-open table by (schema_name, relation name); NULL if not
 * open.  Both must match — two schemas may hold same-named tables. */
static OpenTable *find_open(StorageEngine *se,
                            const char *schema_name, const char *name)
{
    if (!schema_name) return NULL;
    for (int i = 0; i < MAX_TABLES; i++) {
        if (se->open_tables[i].is_open &&
            strcmp(se->open_tables[i].name, name) == 0 &&
            strcmp(se->open_tables[i].schema_name, schema_name) == 0)
            return &se->open_tables[i];
    }
    return NULL;
}

/* Open a relation lazily: load DiskManager + BTree handles.
 *
 * v3: rel is passed directly from the caller (partition_manager has
 * already resolved it from the active SchemaFile).  No TransactionManager
 * registration here — transaction lifecycle is partition_manager's
 * responsibility. */
static OpenTable *open_table(StorageEngine *se, const RelationDef *rel)
{
    OpenTable *ot = find_open(se, rel->owner_schema, rel->relation_name);
    if (ot) return ot;

    int slot = -1;
    for (int i = 0; i < MAX_TABLES; i++) {
        if (!se->open_tables[i].is_open) { slot = i; break; }
    }
    if (slot < 0) return NULL;

    ot = &se->open_tables[slot];
    memset(ot, 0, sizeof(OpenTable));

    char path[512];
    if (build_path(se, path, sizeof(path),
                   rel->owner_schema, rel->relation_name) != MYDB_OK)
        return NULL;

    if (disk_open(&ot->dm, path) != MYDB_OK) return NULL;

    strncpy(ot->name, rel->relation_name, MAX_TABLE_NAME - 1);
    strncpy(ot->schema_name, rel->owner_schema, sizeof(ot->schema_name) - 1);
    ot->id = se->next_table_id++;

    /* clustered B+ tree */
    btree_init(&ot->clustered, &se->bp, &ot->dm, ot->id,
               rel->root_page_no,
               rel->columns[rel->pk_col_idx].type, 0);

    /* secondary B+ trees — unique cols use is_secondary=1 (rejects duplicates),
     * non-unique (INDEXED) cols use is_secondary=2 (allows duplicates). */
    for (int i = 0; i < rel->num_secondary_indexes; i++) {
        int     ci     = rel->secondary_col_idx[i];
        uint8_t is_sec = rel->columns[ci].is_unique ? 1 : 2;
        btree_init(&ot->secondary[i], &se->bp, &ot->dm, ot->id,
                   rel->secondary_root_page_no[i],
                   rel->columns[ci].type, is_sec);
    }

    ot->is_open = 1;
    se->num_open++;
    return ot;
}

static int close_table(StorageEngine *se, OpenTable *ot)
{
    if (!ot || !ot->is_open) return MYDB_OK;
    bp_flush_table(&se->bp, ot->id);
    bp_evict_table(&se->bp, ot->id);
    disk_close(&ot->dm);
    ot->is_open = 0;
    se->num_open--;
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
/*  Storage engine lifecycle                                            */
/* ------------------------------------------------------------------ */

int storage_init(StorageEngine *se)
{
    if (!se) return MYDB_ERR;
    if (se->initialized) return MYDB_OK;

    memset(se, 0, sizeof(*se));
    se->next_table_id = 1;

    bp_init(&se->bp);

    se->initialized = 1;
    return MYDB_OK;
}

int storage_shutdown(StorageEngine *se)
{
    if (!se || !se->initialized) return MYDB_OK;

    for (int i = 0; i < MAX_TABLES; i++) {
        if (se->open_tables[i].is_open)
            close_table(se, &se->open_tables[i]);
    }

    se->initialized = 0;
    return MYDB_OK;
}

int storage_flush_all_dirty(StorageEngine *se)
{
    if (!se || !se->initialized) return MYDB_OK;
    return bp_flush_dirty_all(&se->bp);
}

/* Evict all pages from the buffer pool without flushing.
 * Used by partition_manager on ROLLBACK to discard in-memory dirty
 * changes — the disk retains the pre-transaction data. */
int storage_evict_all(StorageEngine *se)
{
    if (!se || !se->initialized) return MYDB_OK;
    for (int i = 0; i < MAX_TABLES; i++) {
        if (se->open_tables[i].is_open)
            bp_evict_table(&se->bp, se->open_tables[i].id);
    }
    return MYDB_OK;
}

/* ------------------------------------------------------------------ */
/*  Context management                                                  */
/* ------------------------------------------------------------------ */

/* Set the partition root path.  Called once by partition_manager at
 * PartitionCtx init.  The per-relation schema comes from rel->owner_schema
 * on each call, so the storage layer holds no active-schema state. */
void storage_set_context(StorageEngine *se,
                         const char *partition_path)
{
    if (!se || !partition_path) return;
    strncpy(se->partition_path, partition_path,
            sizeof(se->partition_path) - 1);
}

/* ------------------------------------------------------------------ */
/*  DDL                                                                 */
/* ------------------------------------------------------------------ */

/*
 * Create a new relation file and allocate its B+ tree root page(s).
 * Fills in rel->root_page_no and rel->secondary_root_page_no[].
 *
 * v3: no quota checking and no SchemaFile/Catalog side-effects here.
 * partition_manager is responsible for:
 *   - Checking quota headroom before calling.
 *   - Calling schema_add_relation() after this returns.
 *   - Updating cat_track_alloc() with (pages_allocated * PAGE_SIZE).
 *
 * Pre-check quota: clustered root + one root per secondary index.
 */
int storage_create_table(StorageEngine *se, RelationDef *rel)
{
    if (!se || !se->initialized || !rel) return MYDB_ERR;
    if (rel->owner_schema[0] == '\0') return MYDB_ERR_PERM;

    rel->root_page_no = INVALID_PAGE;

    char path[512];
    if (build_path(se, path, sizeof(path),
                   rel->owner_schema, rel->relation_name) != MYDB_OK)
        return MYDB_ERR;

    DiskManager dm;
    if (disk_create(&dm, path) != MYDB_OK) return MYDB_ERR;

    if (rel->columns[rel->pk_col_idx].is_auto_increment) {
        FileHeader fh;
        disk_read_header(&dm, &fh);
        fh.flags |= FILEHDR_FLAG_AUTO_INCREMENT;
        disk_write_header(&dm, &fh);
    }

    /* Allocate the clustered root page via disk_alloc_page (raw allocator,
     * no quota tracking — partition_manager tracks the quota delta).
     * bp_fetch_page brings the new page into a frame so we can initialise it. */
    uint32_t root_pno;
    int rc = disk_alloc_page(&dm, &root_pno);
    if (rc != MYDB_OK) { disk_close(&dm); return rc; }

    uint8_t *root_page = bp_fetch_page(&se->bp, &dm, 0 /*temp id*/, root_pno);
    if (!root_page) { disk_close(&dm); return MYDB_ERR; }
    page_init(root_page, root_pno, PAGE_TYPE_DATA);
    bp_unpin_page(&se->bp, 0, root_pno, 1);
    bp_flush_table(&se->bp, 0);
    bp_evict_table(&se->bp, 0);

    rel->root_page_no = root_pno;

    /* Same pattern for each secondary index root. */
    for (int i = 0; i < rel->num_secondary_indexes; i++) {
        uint32_t sec_pno;
        rc = disk_alloc_page(&dm, &sec_pno);
        if (rc != MYDB_OK) { disk_close(&dm); return rc; }

        uint8_t *sp = bp_fetch_page(&se->bp, &dm, 0, sec_pno);
        if (!sp) { disk_close(&dm); return MYDB_ERR; }
        page_init(sp, sec_pno, PAGE_TYPE_DATA);
        bp_unpin_page(&se->bp, 0, sec_pno, 1);
        bp_flush_table(&se->bp, 0);
        bp_evict_table(&se->bp, 0);
        rel->secondary_root_page_no[i] = sec_pno;
    }

    disk_close(&dm);
    return MYDB_OK;
}

/*
 * Close the open handle for (schema_name, table_name) if cached and unlink
 * its file.
 *
 * v3: no SchemaFile or Catalog side-effects.  partition_manager calls
 * schema_remove_relation() and cat_track_alloc() around this call.
 */
int storage_drop_table(StorageEngine *se,
                       const char *schema_name, const char *table_name)
{
    if (!se || !se->initialized || !schema_name || !table_name)
        return MYDB_ERR;

    OpenTable *ot = find_open(se, schema_name, table_name);
    if (ot) close_table(se, ot);

    char path[512];
    if (build_path(se, path, sizeof(path), schema_name, table_name) != MYDB_OK)
        return MYDB_ERR;
    disk_destroy(path);
    return MYDB_OK;
}

/*
 * Allocate a secondary-index root page, initialise its B+ tree, and
 * backfill all existing clustered rows into it.
 *
 * Caller (partition_manager) must:
 *   - Pass the WRITABLE RelationDef* (from SchemaFile) — this function
 *     mutates rel->secondary_col_idx, rel->secondary_root_page_no, and
 *     rel->num_secondary_indexes in place.
 *   - Call schema_flush_relation() after this returns to persist the
 *     updated RelationDef.
 *   - Call schema_bump_relation_pages() and cat_track_alloc() for quota.
 *
 * v3: no quota checking, no schema flush, no catalog update here.
 */
int storage_add_index(StorageEngine *se, RelationDef *rel, int col_idx)
{
    if (!se || !se->initialized || !rel) return MYDB_ERR;
    if (rel->owner_schema[0] == '\0') return MYDB_ERR_PERM;

    if (col_idx < 0 || col_idx >= rel->num_columns) return MYDB_ERR;

    /* Reject if this column is already indexed */
    for (int i = 0; i < rel->num_secondary_indexes; i++) {
        if (rel->secondary_col_idx[i] == (uint8_t)col_idx)
            return MYDB_ERR_DUPLICATE;
    }
    if (rel->num_secondary_indexes >= MAX_SECONDARY_IDX) return MYDB_ERR_FULL;

    /* Open (or reuse) the table handle */
    OpenTable *ot = open_table(se, rel);
    if (!ot) return MYDB_ERR;

    /* Allocate a new root page for the secondary B-tree */
    uint32_t sec_pno;
    int rc = disk_alloc_page(&ot->dm, &sec_pno);
    if (rc != MYDB_OK) return rc;

    uint8_t *sp = bp_fetch_page(&se->bp, &ot->dm, ot->id, sec_pno);
    if (!sp) return MYDB_ERR;
    page_init(sp, sec_pno, PAGE_TYPE_INDEX);
    bp_unpin_page(&se->bp, ot->id, sec_pno, 1);

    /* Register the new index in the in-memory RelationDef */
    int idx_slot = rel->num_secondary_indexes;
    rel->secondary_col_idx[idx_slot]      = (uint8_t)col_idx;
    rel->secondary_root_page_no[idx_slot] = sec_pno;
    rel->num_secondary_indexes++;

    /* Wire up the BTree handle in the open-table slot */
    uint8_t is_sec = rel->columns[col_idx].is_unique ? 1 : 2;
    btree_init(&ot->secondary[idx_slot], &se->bp, &ot->dm, ot->id,
               sec_pno, rel->columns[col_idx].type, is_sec);

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
        deserialize_row_value(rec_buf + voff, rec_len - voff, rel, &row);

        /* Skip NULL values — nulls are not indexed */
        if (row.cols[col_idx].is_null) continue;

        RID rid = { btree_cur.last_page_no, btree_cur.last_slot };

        /* Secondary record: 2B klen + key bytes + 4B page_no + 2B slot_no.
         * Maximum key size is MAX_VARCHAR_LEN, so the tight bound is
         * MAX_VARCHAR_LEN+2+4+2 = 158 B — no need for a full PAGE_SIZE buffer. */
        uint8_t  srec[MAX_VARCHAR_LEN + 2 + 4 + 2];
        uint16_t slen = build_secondary_record(&row.cols[col_idx], rid, srec);

        rc = btree_insert(&ot->secondary[idx_slot],
                          &row.cols[col_idx], srec, slen, NULL);
        if (rc != MYDB_OK && rc != MYDB_ERR_DUPLICATE) {
            btree_cursor_close(&btree_cur);
            return rc;
        }
    }
    btree_cursor_close(&btree_cur);

    rel->secondary_root_page_no[idx_slot] = ot->secondary[idx_slot].root_page_no;

    return MYDB_OK;
}

/* ------------------------------------------------------------------ */
/*  Read a raw clustered record by RID                                  */
/* ------------------------------------------------------------------ */

static int read_record_by_rid(StorageEngine *se, OpenTable *ot, RID rid,
                               uint8_t *rec_out, uint16_t *len_out)
{
    uint8_t *page = bp_fetch_page(&se->bp, &ot->dm, ot->id, rid.page_no);
    if (!page) return MYDB_ERR;

    uint16_t data_off, data_sz;
    int rc = page_get_record(page, rid.slot_no, &data_off, &data_sz);
    if (rc != MYDB_OK) {
        bp_unpin_page(&se->bp, ot->id, rid.page_no, 0);
        return rc;
    }

    memcpy(rec_out, page + data_off, data_sz);
    *len_out = data_sz;
    bp_unpin_page(&se->bp, ot->id, rid.page_no, 0);
    return MYDB_OK;
}

/* ------------------------------------------------------------------ */
/*  DML — INSERT                                                        */
/*                                                                      */
/*  v3: All constraint checking (NOT NULL, FK, UNIQUE), quota checks,  */
/*  AUTO_INCREMENT management, schema stat updates, and transaction     */
/*  management have moved to partition_manager (pm_insert).            */
/*  This function is a pure B+ tree insert — it takes a fully-formed   */
/*  row and trx_id and writes it to disk.                              */
/* ------------------------------------------------------------------ */

int storage_insert(StorageEngine *se, RelationDef *rel,
                   Row *row, uint64_t trx_id, RID *rid_out)
{
    if (!se || !se->initialized || !rel || !row) return MYDB_ERR;

    OpenTable *ot = open_table(se, rel);
    if (!ot) return MYDB_ERR;

    int pk = rel->pk_col_idx;

    uint8_t  rec_buf[PAGE_SIZE];
    uint16_t rec_len = build_clustered_record(rel, row, trx_id, rec_buf);

    RID rid;
    int rc = btree_insert(&ot->clustered, &row->cols[pk], rec_buf, rec_len, &rid);
    if (rc != MYDB_OK) return rc;
    rel->root_page_no = ot->clustered.root_page_no;

    for (int i = 0; i < rel->num_secondary_indexes; i++) {
        int ci = rel->secondary_col_idx[i];
        uint8_t  srec[MAX_VARCHAR_LEN + 2 + 4 + 2];
        uint16_t slen = build_secondary_record(&row->cols[ci], rid, srec);

        rc = btree_insert(&ot->secondary[i], &row->cols[ci], srec, slen, NULL);
        if (rc != MYDB_OK) return rc;
        rel->secondary_root_page_no[i] = ot->secondary[i].root_page_no;
    }

    se->last_written_dm = &ot->dm;
    if (rid_out) *rid_out = rid;
    return MYDB_OK;
}

/* ------------------------------------------------------------------ */
/*  DML — DELETE                                                        */
/*                                                                      */
/*  v3: FK referential integrity (RESTRICT / CASCADE / SET_NULL) and   */
/*  schema row-count updates have moved to partition_manager.          */
/*  This function performs the pure B+ tree deletion.                  */
/* ------------------------------------------------------------------ */

int storage_delete(StorageEngine *se, RelationDef *rel, RID rid)
{
    if (!se || !se->initialized || !rel) return MYDB_ERR;

    OpenTable *ot = open_table(se, rel);
    if (!ot) return MYDB_ERR;

    uint8_t  rec[PAGE_SIZE];
    uint16_t rec_len;
    if (read_record_by_rid(se, ot, rid, rec, &rec_len) != MYDB_OK)
        return MYDB_ERR;

    Value pk_val;
    record_get_pk(rec, rel->columns[rel->pk_col_idx].type, &pk_val);

    uint16_t klen = ((uint16_t)rec[0] << 8) | rec[1];
    uint16_t voff = 2 + klen + 2;
    Row row;
    memset(&row, 0, sizeof(row));
    deserialize_row_value(rec + voff, rec_len - voff, rel, &row);

    for (int i = 0; i < rel->num_secondary_indexes; i++) {
        int ci = rel->secondary_col_idx[i];
        btree_delete(&ot->secondary[i], &row.cols[ci]);
    }

    return btree_delete(&ot->clustered, &pk_val);
}

/* ------------------------------------------------------------------ */
/*  DML — UPDATE                                                        */
/*                                                                      */
/*  v3: Validate-before-delete (UNIQUE / FK checks) and schema updates */
/*  have moved to partition_manager.  This function performs the pure  */
/*  delete-old + insert-new B+ tree operation.                         */
/* ------------------------------------------------------------------ */

int storage_update(StorageEngine *se, RelationDef *rel,
                   RID rid, Row *new_row, uint64_t trx_id)
{
    if (!se || !se->initialized || !rel || !new_row) return MYDB_ERR;

    OpenTable *ot = open_table(se, rel);
    if (!ot) return MYDB_ERR;

    uint8_t  old_rec[PAGE_SIZE];
    uint16_t old_rec_len;
    if (read_record_by_rid(se, ot, rid, old_rec, &old_rec_len) != MYDB_OK)
        return MYDB_ERR;

    Value old_pk;
    record_get_pk(old_rec, rel->columns[rel->pk_col_idx].type, &old_pk);

    uint16_t klen = ((uint16_t)old_rec[0] << 8) | old_rec[1];
    uint16_t voff = 2 + klen + 2;
    Row old_row;
    memset(&old_row, 0, sizeof(old_row));
    deserialize_row_value(old_rec + voff, old_rec_len - voff, rel, &old_row);

    /*
     * Validate-before-delete: if any UNIQUE key changes to a value that
     * already exists for another live row, fail the update without
     * touching the existing data.
     * v3: FK and PK duplicate checks are done by partition_manager before
     * calling here; only secondary UNIQUE conflicts are checked at this
     * level because storage owns the secondary index state.
     */
    const Value *new_pk = &new_row->cols[rel->pk_col_idx];
    int pk_changed = (value_compare(&old_pk, new_pk) != 0);
    if (pk_changed) {
        BTreeSearchResult res;
        if (btree_search(&ot->clustered, new_pk, &res) == MYDB_OK && res.found)
            return MYDB_ERR_DUPLICATE;
    }

    int sec_changed[MAX_SECONDARY_IDX] = {0};
    for (int i = 0; i < rel->num_secondary_indexes; i++) {
        int ci = rel->secondary_col_idx[i];
        sec_changed[i] = (value_compare(&old_row.cols[ci], &new_row->cols[ci]) != 0);
        if (sec_changed[i]) {
            BTreeSearchResult res;
            if (btree_search(&ot->secondary[i], &new_row->cols[ci], &res) == MYDB_OK
                && res.found)
                return MYDB_ERR_DUPLICATE;
        }
    }

    for (int i = 0; i < rel->num_secondary_indexes; i++) {
        if (!sec_changed[i]) continue;
        int ci = rel->secondary_col_idx[i];
        btree_delete(&ot->secondary[i], &old_row.cols[ci]);
    }
    btree_delete(&ot->clustered, &old_pk);

    uint8_t  new_rec[PAGE_SIZE];
    uint16_t new_rec_len = build_clustered_record(rel, new_row, trx_id, new_rec);
    RID new_rid;
    int rc = btree_insert(&ot->clustered, new_pk, new_rec, new_rec_len, &new_rid);
    if (rc != MYDB_OK) return rc;
    rel->root_page_no = ot->clustered.root_page_no;

    for (int i = 0; i < rel->num_secondary_indexes; i++) {
        if (!sec_changed[i]) continue;
        int ci = rel->secondary_col_idx[i];
        uint8_t  srec[MAX_VARCHAR_LEN + 2 + 4 + 2];
        uint16_t slen = build_secondary_record(&new_row->cols[ci], new_rid, srec);
        rc = btree_insert(&ot->secondary[i], &new_row->cols[ci], srec, slen, NULL);
        if (rc != MYDB_OK) return rc;
        rel->secondary_root_page_no[i] = ot->secondary[i].root_page_no;
    }

    se->last_written_dm = &ot->dm;
    return MYDB_OK;
}

/* ------------------------------------------------------------------ */
/*  DQL — storage_get_by_index                                         */
/* ------------------------------------------------------------------ */

Row *storage_get_by_index(StorageEngine *se, RelationDef *rel,
                           int col_idx, Value *key)
{
    if (!se || !se->initialized || !rel || !key) return NULL;

    /* Find which secondary index covers col_idx */
    int sec_idx = -1;
    for (int i = 0; i < rel->num_secondary_indexes; i++) {
        if (rel->secondary_col_idx[i] == (uint8_t)col_idx) {
            sec_idx = i;
            break;
        }
    }
    if (sec_idx < 0) return NULL;   /* col has no secondary index */

    OpenTable *ot = open_table(se, rel);
    if (!ot) return NULL;

    /* Step 1: descend secondary tree → secondary record {klen, key, page_no, slot_no} */
    BTreeSearchResult sec_res;
    if (btree_search(&ot->secondary[sec_idx], key, &sec_res) != MYDB_OK) return NULL;
    if (!sec_res.found) return NULL;

    uint8_t *sec_page = bp_fetch_page(&se->bp, &ot->dm, ot->id, sec_res.page_no);
    if (!sec_page) return NULL;

    uint16_t doff, dsz;
    if (page_get_record(sec_page, sec_res.slot_no, &doff, &dsz) != MYDB_OK) {
        bp_unpin_page(&se->bp, ot->id, sec_res.page_no, 0);
        return NULL;
    }

    /* Parse the RID out of the secondary record: [klen:2][key:klen][page_no:4][slot_no:2] */
    const uint8_t *srec = sec_page + doff;
    uint16_t klen = ((uint16_t)srec[0] << 8) | srec[1];
    RID rid;
    memcpy(&rid.page_no, srec + 2 + klen,     4);
    memcpy(&rid.slot_no, srec + 2 + klen + 4, 2);
    bp_unpin_page(&se->bp, ot->id, sec_res.page_no, 0);

    /* Step 2: fetch full row from clustered tree using the RID */
    uint8_t *clust_page = bp_fetch_page(&se->bp, &ot->dm, ot->id, rid.page_no);
    if (!clust_page) return NULL;

    uint16_t cdoff, cdsz;
    if (page_get_record(clust_page, rid.slot_no, &cdoff, &cdsz) != MYDB_OK) {
        bp_unpin_page(&se->bp, ot->id, rid.page_no, 0);
        return NULL;
    }

    uint8_t rec[PAGE_SIZE];
    memcpy(rec, clust_page + cdoff, cdsz);
    bp_unpin_page(&se->bp, ot->id, rid.page_no, 0);

    uint16_t cklen = ((uint16_t)rec[0] << 8) | rec[1];
    uint16_t voff  = 2 + cklen + 2;

    static Row result;
    memset(&result, 0, sizeof(result));
    deserialize_row_value(rec + voff, cdsz - voff, rel, &result);
    result.rid = rid;
    return &result;
}

/* ------------------------------------------------------------------ */
/*  DQL — storage_scan_by_index                                        */
/* ------------------------------------------------------------------ */

Cursor *storage_scan_by_index(StorageEngine *se, RelationDef *rel,
                               int col_idx, Value *lo)
{
    if (!se || !se->initialized || !rel) return NULL;

    /* Find the secondary index that covers col_idx. */
    int sec_idx = -1;
    for (int i = 0; i < rel->num_secondary_indexes; i++) {
        if (rel->secondary_col_idx[i] == (uint8_t)col_idx) {
            sec_idx = i;
            break;
        }
    }
    if (sec_idx < 0) return NULL;   /* col has no secondary index */

    OpenTable *ot = open_table(se, rel);
    if (!ot) return NULL;

    StorageScan *sc = (StorageScan *)malloc(sizeof(StorageScan));
    if (!sc) return NULL;

    memset(sc, 0, sizeof(StorageScan));
    sc->ot      = ot;
    sc->rel     = rel;
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

Row *storage_get_by_pk(StorageEngine *se, RelationDef *rel, Value *pk)
{
    if (!se || !se->initialized || !rel || !pk) return NULL;

    OpenTable *ot = open_table(se, rel);
    if (!ot) return NULL;

    BTreeSearchResult res;
    if (btree_search(&ot->clustered, pk, &res) != MYDB_OK) return NULL;
    if (!res.found) return NULL;

    uint8_t *page = bp_fetch_page(&se->bp, &ot->dm, ot->id, res.page_no);
    if (!page) return NULL;

    uint16_t data_off, data_sz;
    if (page_get_record(page, res.slot_no, &data_off, &data_sz) != MYDB_OK) {
        bp_unpin_page(&se->bp, ot->id, res.page_no, 0);
        return NULL;
    }

    uint8_t rec[PAGE_SIZE];
    memcpy(rec, page + data_off, data_sz);
    bp_unpin_page(&se->bp, ot->id, res.page_no, 0);

    uint16_t klen = ((uint16_t)rec[0] << 8) | rec[1];
    uint16_t voff = 2 + klen + 2;

    static Row result;
    memset(&result, 0, sizeof(result));
    deserialize_row_value(rec + voff, data_sz - voff, rel, &result);
    result.rid.page_no = res.page_no;
    result.rid.slot_no = res.slot_no;
    return &result;
}

/* ------------------------------------------------------------------ */
/*  DQL — scan cursor                                                   */
/* ------------------------------------------------------------------ */

Cursor *storage_scan(StorageEngine *se, RelationDef *rel)
{
    if (!se || !se->initialized || !rel) return NULL;

    OpenTable *ot = open_table(se, rel);
    if (!ot) return NULL;

    StorageScan *sc = (StorageScan *)malloc(sizeof(StorageScan));
    if (!sc) return NULL;

    memset(sc, 0, sizeof(StorageScan));
    sc->ot      = ot;
    sc->rel     = rel;
    sc->sec_idx = -1;   /* clustered scan */

    if (btree_cursor_open(&ot->clustered, &sc->btree_cur) != MYDB_OK) {
        free(sc);
        return NULL;
    }

    return (Cursor *)sc;
}

Cursor *storage_scan_from(StorageEngine *se, RelationDef *rel, Value *lo)
{
    if (!se || !se->initialized || !rel || !lo) return NULL;

    OpenTable *ot = open_table(se, rel);
    if (!ot) return NULL;

    StorageScan *sc = (StorageScan *)malloc(sizeof(StorageScan));
    if (!sc) return NULL;

    memset(sc, 0, sizeof(StorageScan));
    sc->ot      = ot;
    sc->rel     = rel;
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

    /* Fetch the full row from the clustered index using the RID.
     * The BufferPool pointer is accessible via the clustered BTree handle
     * stored in the OpenTable (sc->ot->clustered.bp). */
    BufferPool *bp = sc->ot->clustered.bp;
    uint8_t *clust_page = bp_fetch_page(bp, &sc->ot->dm,
                                         sc->ot->id, rid.page_no);
    if (!clust_page) return NULL;

    uint16_t doff, dsz;
    if (page_get_record(clust_page, rid.slot_no, &doff, &dsz) != MYDB_OK) {
        bp_unpin_page(bp, sc->ot->id, rid.page_no, 0);
        return NULL;
    }

    uint8_t clust_rec[PAGE_SIZE];
    memcpy(clust_rec, clust_page + doff, dsz);
    bp_unpin_page(bp, sc->ot->id, rid.page_no, 0);

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

/* Fetch a full row by its Record ID.
 * Used by partition_manager for FK constraint resolution. */
Row *storage_get_by_rid(StorageEngine *se, RelationDef *rel, RID rid)
{
    if (!se || !rel) return NULL;
    OpenTable *ot = open_table(se, rel);
    if (!ot) return NULL;

    uint8_t  rec[PAGE_SIZE];
    uint16_t rec_len;
    if (read_record_by_rid(se, ot, rid, rec, &rec_len) != MYDB_OK) return NULL;

    uint16_t klen = ((uint16_t)rec[0] << 8) | rec[1];
    uint16_t voff = 2 + klen + 2;

    static Row result;
    memset(&result, 0, sizeof(result));
    deserialize_row_value(rec + voff, rec_len - voff, rel, &result);
    result.rid = rid;
    return &result;
}

/* ------------------------------------------------------------------ */
/*  Page-count query                                                    */
/*                                                                      */
/*  partition_manager calls this before and after DDL/DML operations   */
/*  to compute the page delta for quota tracking:                      */
/*    pages_before = storage_table_page_count(se, name);              */
/*    storage_insert(se, rel, row, trx_id, &rid);                     */
/*    delta = storage_table_page_count(se, name) - pages_before;      */
/*    cat_track_alloc(catalog, delta * PAGE_SIZE);                     */
/* ------------------------------------------------------------------ */

uint32_t storage_table_page_count(StorageEngine *se,
                                  const char *schema_name,
                                  const char *table_name)
{
    if (!se || !schema_name || !table_name) return 0;
    OpenTable *ot = find_open(se, schema_name, table_name);
    if (!ot) return 0;
    return ot->dm.num_pages;
}
