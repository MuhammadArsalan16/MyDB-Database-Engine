#include "storage.h"
#include "page.h"

#include <string.h>
#include <stdio.h>
#include <stdlib.h>

/* ------------------------------------------------------------------ */
/*  Internal types                                                       */
/* ------------------------------------------------------------------ */

/* One entry in the open-table registry */
typedef struct {
    char        name[MAX_TABLE_NAME];
    int         id;             /* table_id used with the buffer pool */
    DiskManager dm;
    BTree       clustered;
    BTree       secondary[MAX_SECONDARY_IDX];
    int         is_open;
} OpenTable;

/*
 * StorageScan — wraps a BTree cursor with schema context.
 * storage_scan() returns (Cursor *)scan; cursor_next/close cast it back.
 * struct Cursor MUST be the first member so the cast is valid.
 */
typedef struct {
    Cursor    btree_cur;    /* must be first */
    OpenTable *ot;
    Schema    *schema;
    Row       current_row;
    uint8_t   rec_buf[PAGE_SIZE];
} StorageScan;

/* Singleton storage state (one engine per process) */
typedef struct {
    SchemaCatalog      catalog;
    BufferPool         bp;
    TransactionManager trx;
    OpenTable          open_tables[MAX_TABLES];
    int                num_open;
    int                next_table_id;  /* monotonic counter for table IDs */
    char               data_dir[256];
    int                initialized;
} StorageState;

/*
 * File-static singleton — deliberate exception to the "no globals" rule.
 * A storage engine is inherently a single shared resource per process.
 */
static StorageState g;

/* ------------------------------------------------------------------ */
/*  Helpers                                                             */
/* ------------------------------------------------------------------ */

static void build_path(char *out, size_t outlen,
                        const char *dir, const char *name)
{
    snprintf(out, outlen, "%s/%s.mydb", dir, name);
}

/* Find an already-open table by name; returns NULL if not open. */
static OpenTable *find_open(const char *name)
{
    for (int i = 0; i < MAX_TABLES; i++) {
        if (g.open_tables[i].is_open &&
            strcmp(g.open_tables[i].name, name) == 0)
            return &g.open_tables[i];
    }
    return NULL;
}

/* Open a table (load DiskManager + BTree handles), register with trx. */
static OpenTable *open_table(const char *name)
{
    OpenTable *ot = find_open(name);
    if (ot) return ot;  /* already open */

    Schema *s = schema_get(&g.catalog, name);
    if (!s) return NULL;

    /* find a free slot */
    int slot = -1;
    for (int i = 0; i < MAX_TABLES; i++) {
        if (!g.open_tables[i].is_open) { slot = i; break; }
    }
    if (slot < 0) return NULL;

    ot = &g.open_tables[slot];
    memset(ot, 0, sizeof(OpenTable));

    char path[512];
    build_path(path, sizeof(path), g.data_dir, name);

    if (disk_open(&ot->dm, path) != MYDB_OK) return NULL;

    strncpy(ot->name, name, MAX_TABLE_NAME - 1);
    ot->id = g.next_table_id++;

    /* initialise clustered B+ Tree */
    btree_init(&ot->clustered, &g.bp, &ot->dm, ot->id,
               s->root_page_no,
               s->columns[s->pk_col_idx].type, 0);

    /* initialise secondary B+ Trees */
    for (int i = 0; i < s->num_secondary_indexes; i++) {
        int ci = s->secondary_col_idx[i];
        btree_init(&ot->secondary[i], &g.bp, &ot->dm, ot->id,
                   s->secondary_root_page_no[i],
                   s->columns[ci].type, 1);
    }

    ot->is_open = 1;
    g.num_open++;

    trx_register_table(&g.trx, ot->id, &ot->dm);
    return ot;
}

static int close_table(OpenTable *ot)
{
    if (!ot || !ot->is_open) return MYDB_OK;
    bp_flush_table(&g.bp, &ot->dm, ot->id);
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
        memset(out, 0, schema_col_size(col));
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
static uint16_t serialize_row_value(const Schema *s, const Row *row,
                                    uint64_t trx_id, uint8_t *out)
{
    uint16_t off = 0;

    /* DB_TRX_ID (6 bytes LE) */
    memcpy(out + off, &trx_id, 6);
    off += 6;

    /* DB_ROLL_PTR (7 bytes, zeros in Phase 1) */
    memset(out + off, 0, 7);
    off += 7;

    /* null bitmap (4 bytes = 32 bits, one per column) */
    uint32_t null_bits = 0;
    for (int i = 0; i < s->num_columns; i++) {
        if (row->cols[i].is_null)
            null_bits |= (1u << i);
    }
    memcpy(out + off, &null_bits, 4);
    off += 4;

    /* column values */
    for (int i = 0; i < s->num_columns; i++) {
        uint16_t csz = schema_col_size(&s->columns[i]);
        write_col(&row->cols[i], &s->columns[i], out + off);
        off += csz;
    }

    return off;
}

/* Deserialize value bytes back into a Row. */
static void deserialize_row_value(const uint8_t *val, uint16_t vlen,
                                   const Schema *s, Row *row)
{
    (void)vlen;
    uint16_t off = 0;

    /* skip DB_TRX_ID (6) + DB_ROLL_PTR (7) */
    off += 13;

    /* null bitmap */
    uint32_t null_bits;
    memcpy(&null_bits, val + off, 4);
    off += 4;

    row->num_cols = s->num_columns;
    for (int i = 0; i < s->num_columns; i++) {
        uint16_t csz = schema_col_size(&s->columns[i]);
        read_col(val + off, &s->columns[i], &row->cols[i]);
        row->cols[i].is_null = (null_bits >> i) & 1;
        off += csz;
    }
}

/* ------------------------------------------------------------------ */
/*  Clustered record building                                           */
/*  Format: [klen:2BE][key_bytes][vlen:2BE][val_bytes]                 */
/* ------------------------------------------------------------------ */

static uint16_t build_clustered_record(const Schema *s, const Row *row,
                                        uint64_t trx_id, uint8_t *out)
{
    /* key = serialized PK column */
    const Value *pk_val = &row->cols[s->pk_col_idx];
    uint8_t  key_buf[MAX_VARCHAR_LEN + 2];
    uint16_t klen = btree_key_encode(pk_val, key_buf);

    /* value = hidden + null bitmap + user columns */
    uint8_t  val_buf[PAGE_SIZE];
    uint16_t vlen = serialize_row_value(s, row, trx_id, val_buf);

    /* assemble: [klen:2BE][key][vlen:2BE][val] */
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

/* ------------------------------------------------------------------ */
/*  Parse a clustered record to extract the PK value                   */
/* ------------------------------------------------------------------ */

static void record_get_pk(const uint8_t *rec, DataType pk_type, Value *out_pk)
{
    uint16_t klen = ((uint16_t)rec[0] << 8) | rec[1];
    btree_key_decode(rec + 2, klen, pk_type, out_pk);
}

/* ------------------------------------------------------------------ */
/*  Find the page-directory slot that holds data at data_off.          */
/* ------------------------------------------------------------------ */

static int find_slot_for_doff(OpenTable *ot, uint32_t page_no, uint16_t data_off)
{
    uint8_t *page = bp_fetch_page(&g.bp, &ot->dm, ot->id, page_no);
    if (!page) return -1;

    uint16_t n = page_dir_count(page);
    int slot = -1;
    for (uint16_t i = 0; i < n; i++) {
        if (page_dir_get(page, i) == data_off) { slot = (int)i; break; }
    }
    bp_unpin_page(&g.bp, ot->id, page_no, 0);
    return slot;
}

/* ------------------------------------------------------------------ */
/*  Engine lifecycle                                                    */
/* ------------------------------------------------------------------ */

int storage_init(const char *data_dir)
{
    if (!data_dir) return MYDB_ERR;
    if (g.initialized) return MYDB_OK;

    memset(&g, 0, sizeof(g));
    strncpy(g.data_dir, data_dir, sizeof(g.data_dir) - 1);
    g.next_table_id = 1;

    bp_init(&g.bp);
    trx_init(&g.trx, &g.bp);

    if (schema_catalog_open(&g.catalog, data_dir) != MYDB_OK) return MYDB_ERR;

    g.initialized = 1;
    return MYDB_OK;
}

int storage_shutdown(void)
{
    if (!g.initialized) return MYDB_OK;

    /* commit any open transaction */
    if (trx_is_active(&g.trx)) trx_commit(&g.trx);

    /* close all open tables */
    for (int i = 0; i < MAX_TABLES; i++) {
        if (g.open_tables[i].is_open)
            close_table(&g.open_tables[i]);
    }

    schema_catalog_close(&g.catalog);
    g.initialized = 0;
    return MYDB_OK;
}

/* ------------------------------------------------------------------ */
/*  DDL                                                                 */
/* ------------------------------------------------------------------ */

int storage_create_table(const char *name, Schema *schema)
{
    if (!g.initialized || !name || !schema) return MYDB_ERR;
    if (schema_get(&g.catalog, name) != NULL) return MYDB_ERR_DUPLICATE;

    strncpy(schema->table_name, name, MAX_TABLE_NAME - 1);
    schema->root_page_no = INVALID_PAGE;

    /* create the .mydb file */
    char path[512];
    build_path(path, sizeof(path), g.data_dir, name);

    DiskManager dm;
    if (disk_create(&dm, path) != MYDB_OK) return MYDB_ERR;

    /* alloc + init root page for clustered index (page 1) */
    uint32_t root_pno;
    uint8_t *root_page = bp_alloc_page(&g.bp, &dm, 0 /*temp id*/, &root_pno);
    if (!root_page) { disk_close(&dm); return MYDB_ERR; }
    page_init(root_page, root_pno, PAGE_TYPE_DATA);
    bp_unpin_page(&g.bp, 0, root_pno, 1);
    bp_flush_table(&g.bp, &dm, 0);
    bp_evict_table(&g.bp, 0);

    schema->root_page_no = root_pno;

    /* alloc + init a root page for each secondary index */
    for (int i = 0; i < schema->num_secondary_indexes; i++) {
        uint32_t sec_pno;
        uint8_t *sp = bp_alloc_page(&g.bp, &dm, 0, &sec_pno);
        if (!sp) { disk_close(&dm); return MYDB_ERR; }
        page_init(sp, sec_pno, PAGE_TYPE_DATA);
        bp_unpin_page(&g.bp, 0, sec_pno, 1);
        bp_flush_table(&g.bp, &dm, 0);
        bp_evict_table(&g.bp, 0);
        schema->secondary_root_page_no[i] = sec_pno;
    }

    disk_close(&dm);

    /* add to catalog */
    if (schema_add(&g.catalog, schema) != MYDB_OK) return MYDB_ERR;

    return MYDB_OK;
}

int storage_drop_table(const char *name)
{
    if (!g.initialized || !name) return MYDB_ERR;

    /* close the table if it is currently open */
    OpenTable *ot = find_open(name);
    if (ot) close_table(ot);

    /* delete the data file */
    char path[512];
    build_path(path, sizeof(path), g.data_dir, name);
    disk_destroy(path);

    return schema_remove(&g.catalog, name);
}

/* ------------------------------------------------------------------ */
/*  Transaction delegation                                              */
/* ------------------------------------------------------------------ */

int storage_begin(void)    { return trx_begin(&g.trx); }
int storage_commit(void)   { return trx_commit(&g.trx); }
int storage_rollback(void) { return trx_rollback(&g.trx); }

/* ------------------------------------------------------------------ */
/*  DML — INSERT                                                        */
/* ------------------------------------------------------------------ */

int storage_insert(const char *table, Row *row)
{
    if (!g.initialized || !table || !row) return MYDB_ERR;

    Schema *s = schema_get(&g.catalog, table);
    if (!s) return MYDB_ERR_NOT_FOUND;

    OpenTable *ot = open_table(table);
    if (!ot) return MYDB_ERR;

    /* implicit transaction: begin if none active */
    int auto_txn = !trx_is_active(&g.trx);
    if (auto_txn) trx_begin(&g.trx);

    /* validate NOT NULL */
    for (int i = 0; i < s->num_columns; i++) {
        if (s->columns[i].is_not_null && !s->columns[i].is_auto_increment) {
            if (row->cols[i].is_null) {
                if (auto_txn) trx_rollback(&g.trx);
                return MYDB_ERR_NULL_VIOLATION;
            }
        }
    }

    /* handle AUTO_INCREMENT */
    int pk = s->pk_col_idx;
    if (s->columns[pk].is_auto_increment) {
        /* use the counter when the caller passes null or 0 */
        if (row->cols[pk].is_null || row->cols[pk].v.int_val == 0) {
            row->cols[pk].type       = TYPE_INT;
            row->cols[pk].is_null    = 0;
            row->cols[pk].v.int_val  = (int32_t)s->auto_incr_counter;
            s->auto_incr_counter++;
        }
    }

    /* build and insert the clustered record */
    uint8_t rec_buf[PAGE_SIZE];
    uint16_t rec_len = build_clustered_record(s, row, trx_current_id(&g.trx), rec_buf);

    RID rid;
    int rc = btree_insert(&ot->clustered, &row->cols[pk], rec_buf, rec_len, &rid);
    if (rc != MYDB_OK) {
        if (auto_txn) trx_rollback(&g.trx);
        return rc;
    }

    /* insert into secondary indexes */
    for (int i = 0; i < s->num_secondary_indexes; i++) {
        int ci = s->secondary_col_idx[i];
        uint8_t srec[MAX_VARCHAR_LEN + 2 + 4 + 2];
        uint16_t slen = build_secondary_record(&row->cols[ci], rid, srec);

        rc = btree_insert(&ot->secondary[i], &row->cols[ci], srec, slen, NULL);
        if (rc != MYDB_OK) {
            if (auto_txn) trx_rollback(&g.trx);
            return rc;
        }
    }

    /* persist updated auto_incr counter if it changed */
    if (s->columns[pk].is_auto_increment) {
        int idx = -1;
        for (int i = 0; i < g.catalog.num_tables; i++) {
            if (strcmp(g.catalog.tables[i].table_name, table) == 0) { idx = i; break; }
        }
        if (idx >= 0) schema_flush(&g.catalog, idx);
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

int storage_delete(const char *table, RID rid)
{
    if (!g.initialized || !table) return MYDB_ERR;

    Schema *s = schema_get(&g.catalog, table);
    if (!s) return MYDB_ERR_NOT_FOUND;

    OpenTable *ot = open_table(table);
    if (!ot) return MYDB_ERR;

    int auto_txn = !trx_is_active(&g.trx);
    if (auto_txn) trx_begin(&g.trx);

    /* read the record to get the PK and secondary key values */
    uint8_t rec[PAGE_SIZE];
    uint16_t rec_len;
    if (read_record_by_rid(ot, rid, rec, &rec_len) != MYDB_OK) {
        if (auto_txn) trx_rollback(&g.trx);
        return MYDB_ERR;
    }

    /* extract PK */
    Value pk_val;
    record_get_pk(rec, s->columns[s->pk_col_idx].type, &pk_val);

    /* deserialize full row to get secondary key values */
    uint16_t klen = ((uint16_t)rec[0] << 8) | rec[1];
    uint16_t voff = 2 + klen + 2;
    Row row;
    memset(&row, 0, sizeof(row));
    deserialize_row_value(rec + voff, rec_len - voff, s, &row);

    /* delete from secondary indexes first */
    for (int i = 0; i < s->num_secondary_indexes; i++) {
        int ci = s->secondary_col_idx[i];
        btree_delete(&ot->secondary[i], &row.cols[ci]);
    }

    /* delete from clustered index */
    int rc = btree_delete(&ot->clustered, &pk_val);
    if (auto_txn) {
        if (rc == MYDB_OK) trx_commit(&g.trx); else trx_rollback(&g.trx);
    }
    return rc;
}

/* ------------------------------------------------------------------ */
/*  DML — UPDATE                                                        */
/* ------------------------------------------------------------------ */

int storage_update(const char *table, RID rid, Row *new_row)
{
    if (!g.initialized || !table || !new_row) return MYDB_ERR;

    Schema *s = schema_get(&g.catalog, table);
    if (!s) return MYDB_ERR_NOT_FOUND;

    OpenTable *ot = open_table(table);
    if (!ot) return MYDB_ERR;

    int auto_txn = !trx_is_active(&g.trx);
    if (auto_txn) trx_begin(&g.trx);

    /* NOT NULL check on new values */
    for (int i = 0; i < s->num_columns; i++) {
        if (s->columns[i].is_not_null && new_row->cols[i].is_null) {
            if (auto_txn) trx_rollback(&g.trx);
            return MYDB_ERR_NULL_VIOLATION;
        }
    }

    /* read old record to get old secondary key values */
    uint8_t old_rec[PAGE_SIZE];
    uint16_t old_rec_len;
    if (read_record_by_rid(ot, rid, old_rec, &old_rec_len) != MYDB_OK) {
        if (auto_txn) trx_rollback(&g.trx);
        return MYDB_ERR;
    }

    Value old_pk;
    record_get_pk(old_rec, s->columns[s->pk_col_idx].type, &old_pk);

    uint16_t klen = ((uint16_t)old_rec[0] << 8) | old_rec[1];
    uint16_t voff = 2 + klen + 2;
    Row old_row;
    memset(&old_row, 0, sizeof(old_row));
    deserialize_row_value(old_rec + voff, old_rec_len - voff, s, &old_row);

    /* delete old record from clustered + secondary indexes */
    for (int i = 0; i < s->num_secondary_indexes; i++) {
        int ci = s->secondary_col_idx[i];
        btree_delete(&ot->secondary[i], &old_row.cols[ci]);
    }
    btree_delete(&ot->clustered, &old_pk);

    /* insert new record */
    uint8_t new_rec[PAGE_SIZE];
    uint16_t new_rec_len = build_clustered_record(s, new_row,
                                                   trx_current_id(&g.trx), new_rec);
    RID new_rid;
    int rc = btree_insert(&ot->clustered, &new_row->cols[s->pk_col_idx],
                          new_rec, new_rec_len, &new_rid);
    if (rc != MYDB_OK) {
        if (auto_txn) trx_rollback(&g.trx);
        return rc;
    }

    for (int i = 0; i < s->num_secondary_indexes; i++) {
        int ci = s->secondary_col_idx[i];
        uint8_t srec[MAX_VARCHAR_LEN + 2 + 4 + 2];
        uint16_t slen = build_secondary_record(&new_row->cols[ci], new_rid, srec);
        btree_insert(&ot->secondary[i], &new_row->cols[ci], srec, slen, NULL);
    }

    if (auto_txn) trx_commit(&g.trx);
    return MYDB_OK;
}

/* ------------------------------------------------------------------ */
/*  DQL — storage_get_by_pk                                            */
/* ------------------------------------------------------------------ */

Row *storage_get_by_pk(const char *table, Value *pk)
{
    if (!g.initialized || !table || !pk) return NULL;

    Schema *s = schema_get(&g.catalog, table);
    if (!s) return NULL;

    OpenTable *ot = open_table(table);
    if (!ot) return NULL;

    BTreeSearchResult res;
    if (btree_search(&ot->clustered, pk, &res) != MYDB_OK) return NULL;
    if (!res.found) return NULL;

    /* fetch the leaf page and read the record */
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

    /* deserialize value portion */
    uint16_t klen = ((uint16_t)rec[0] << 8) | rec[1];
    uint16_t voff = 2 + klen + 2;

    static Row result;
    memset(&result, 0, sizeof(result));
    deserialize_row_value(rec + voff, data_sz - voff, s, &result);
    result.rid.page_no = res.page_no;
    result.rid.slot_no = res.slot_no;
    return &result;
}

/* ------------------------------------------------------------------ */
/*  DQL — scan cursor                                                   */
/* ------------------------------------------------------------------ */

Cursor *storage_scan(const char *table)
{
    if (!g.initialized || !table) return NULL;

    Schema *s = schema_get(&g.catalog, table);
    if (!s) return NULL;

    OpenTable *ot = open_table(table);
    if (!ot) return NULL;

    StorageScan *sc = (StorageScan *)malloc(sizeof(StorageScan));
    if (!sc) return NULL;

    memset(sc, 0, sizeof(StorageScan));
    sc->ot     = ot;
    sc->schema = s;

    if (btree_cursor_open(&ot->clustered, &sc->btree_cur) != MYDB_OK) {
        free(sc);
        return NULL;
    }

    return (Cursor *)sc;
}

Row *cursor_next(Cursor *cur)
{
    if (!cur) return NULL;
    StorageScan *sc = (StorageScan *)cur;

    uint16_t len;
    if (btree_cursor_next(&sc->btree_cur, sc->rec_buf, &len) != MYDB_OK)
        return NULL;

    /* parse: [klen:2BE][key][vlen:2BE][val] */
    uint16_t klen = ((uint16_t)sc->rec_buf[0] << 8) | sc->rec_buf[1];
    uint16_t voff = 2 + klen + 2;

    memset(&sc->current_row, 0, sizeof(Row));
    deserialize_row_value(sc->rec_buf + voff, len - voff, sc->schema, &sc->current_row);

    /* set RID from the position btree_cursor_next recorded */
    sc->current_row.rid.page_no = sc->btree_cur.last_page_no;
    int slot = find_slot_for_doff(sc->ot,
                                   sc->btree_cur.last_page_no,
                                   sc->btree_cur.last_data_off);
    sc->current_row.rid.slot_no = (slot >= 0) ? (uint16_t)slot : 0;

    return &sc->current_row;
}

void cursor_close(Cursor *cur)
{
    if (!cur) return;
    StorageScan *sc = (StorageScan *)cur;
    btree_cursor_close(&sc->btree_cur);
    free(sc);
}
