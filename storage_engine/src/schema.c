#include "schema.h"

#include <string.h>
#include <stdio.h>
#include <stdlib.h>

/* ------------------------------------------------------------------ */
/*  On-disk schema page format (all integers little-endian)            */
/*                                                                      */
/*  Offset  Size  Field                                                 */
/*  0       4     SCHEMA_PAGE_MAGIC                                     */
/*  4       64    table_name                                            */
/*  68      1     num_columns                                           */
/*  69      1     pk_col_idx                                            */
/*  70      1     num_foreign_keys                                      */
/*  71      1     num_secondary_indexes                                 */
/*  72      4     auto_incr_counter                                     */
/*  76      4     root_page_no                                          */
/*  80      8     secondary_col_idx[8]                                  */
/*  88      32    secondary_root_page_no[8]   (4 bytes each)            */
/*  120     ...   columns  (variable — see below)                       */
/*  ...     ...   foreign keys  (256 bytes each)                        */
/*                                                                      */
/*  Per column:                                                         */
/*    64    name                                                         */
/*    1     type (DataType)                                             */
/*    2     max_len                                                      */
/*    1     scale                                                        */
/*    1     flags  bit0=not_null bit1=pk bit2=unique                    */
/*                 bit3=auto_incr bit4=has_default                      */
/*    1     default_is_null                                             */
/*    152   default_data  (raw: INT=4B, DECIMAL/DATETIME=8B,            */
/*                         VARCHAR=2B len + 150B data,                  */
/*                         ENUM/BOOL/DATE use first 1-4 bytes)          */
/*    1     num_enum_values                                             */
/*    num_enum_values * MAX_ENUM_STR_LEN   enum strings                */
/*                                                                      */
/*  Per FK (256 bytes fixed):                                           */
/*    64    constraint_name                                              */
/*    64    column_name                                                  */
/*    64    ref_table_name                                               */
/*    64    ref_column_name                                              */
/* ------------------------------------------------------------------ */

#define DEFAULT_DATA_SIZE  152   /* 2-byte len + 150 bytes data covers VARCHAR */

/* ------------------------------------------------------------------ */
/*  Little-endian read/write helpers                                    */
/* ------------------------------------------------------------------ */

static void put_u8(uint8_t *buf, uint16_t *off, uint8_t v)
{
    buf[(*off)++] = v;
}

static void put_u16(uint8_t *buf, uint16_t *off, uint16_t v)
{
    buf[*off]     = (uint8_t)(v & 0xFF);
    buf[*off + 1] = (uint8_t)(v >> 8);
    *off += 2;
}

static void put_u32(uint8_t *buf, uint16_t *off, uint32_t v)
{
    buf[*off]     = (uint8_t)(v & 0xFF);
    buf[*off + 1] = (uint8_t)((v >>  8) & 0xFF);
    buf[*off + 2] = (uint8_t)((v >> 16) & 0xFF);
    buf[*off + 3] = (uint8_t)((v >> 24) & 0xFF);
    *off += 4;
}

static void put_bytes(uint8_t *buf, uint16_t *off, const void *src, uint16_t len)
{
    memcpy(buf + *off, src, len);
    *off += len;
}

static uint8_t get_u8(const uint8_t *buf, uint16_t *off)
{
    return buf[(*off)++];
}

static uint16_t get_u16(const uint8_t *buf, uint16_t *off)
{
    uint16_t v = (uint16_t)buf[*off] | ((uint16_t)buf[*off + 1] << 8);
    *off += 2;
    return v;
}

static uint32_t get_u32(const uint8_t *buf, uint16_t *off)
{
    uint32_t v = (uint32_t) buf[*off]
               | ((uint32_t)buf[*off + 1] <<  8)
               | ((uint32_t)buf[*off + 2] << 16)
               | ((uint32_t)buf[*off + 3] << 24);
    *off += 4;
    return v;
}

static void get_bytes(const uint8_t *buf, uint16_t *off, void *dst, uint16_t len)
{
    memcpy(dst, buf + *off, len);
    *off += len;
}

/* ------------------------------------------------------------------ */
/*  Default value serialization                                         */
/* ------------------------------------------------------------------ */

/* Write the default_value union into a fixed DEFAULT_DATA_SIZE byte block. */
static void serialize_default(const ColumnDef *col, uint8_t *out)
{
    memset(out, 0, DEFAULT_DATA_SIZE);
    if (!col->has_default || col->default_value.is_null) return;

    const Value *v = &col->default_value;
    switch (col->type) {
        case TYPE_INT:
            memcpy(out, &v->v.int_val, 4);
            break;
        case TYPE_DECIMAL:
            memcpy(out, &v->v.decimal_val, 8);
            break;
        case TYPE_VARCHAR: {
            uint16_t len = v->v.varchar_val.len;
            memcpy(out, &len, 2);
            memcpy(out + 2, v->v.varchar_val.data, len);
            break;
        }
        case TYPE_ENUM:
            out[0] = v->v.enum_val;
            break;
        case TYPE_BOOL:
            out[0] = v->v.bool_val;
            break;
        case TYPE_DATE:
            memcpy(out, &v->v.date_val, 4);
            break;
        case TYPE_DATETIME:
            memcpy(out, &v->v.datetime_val, 8);
            break;
    }
}

/* Read the default_value from a DEFAULT_DATA_SIZE byte block back into col. */
static void deserialize_default(ColumnDef *col, const uint8_t *in)
{
    Value *v = &col->default_value;
    memset(v, 0, sizeof(Value));
    v->type    = col->type;
    v->is_null = col->default_value.is_null;  /* is_null already set by caller */

    if (!col->has_default || v->is_null) return;

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
        case TYPE_ENUM:
            v->v.enum_val = in[0];
            break;
        case TYPE_BOOL:
            v->v.bool_val = in[0];
            break;
        case TYPE_DATE:
            memcpy(&v->v.date_val, in, 4);
            break;
        case TYPE_DATETIME:
            memcpy(&v->v.datetime_val, in, 8);
            break;
    }
}

/* ------------------------------------------------------------------ */
/*  Schema → page buffer serialization                                  */
/* ------------------------------------------------------------------ */

/*
 * Serialize s into buf (PAGE_SIZE bytes).
 * Returns the number of bytes written, or 0 on overflow.
 */
static uint16_t schema_serialize(const Schema *s, uint8_t *buf)
{
    memset(buf, 0, PAGE_SIZE);
    uint16_t off = 0;

    /* --- fixed header (120 bytes) --- */
    put_u32(buf, &off, SCHEMA_PAGE_MAGIC);
    put_bytes(buf, &off, s->table_name, MAX_TABLE_NAME);
    put_u8(buf, &off, s->num_columns);
    put_u8(buf, &off, s->pk_col_idx);
    put_u8(buf, &off, s->num_foreign_keys);
    put_u8(buf, &off, s->num_secondary_indexes);
    put_u32(buf, &off, s->auto_incr_counter);
    put_u32(buf, &off, s->root_page_no);

    for (int i = 0; i < MAX_SECONDARY_IDX; i++)
        put_u8(buf, &off, s->secondary_col_idx[i]);
    for (int i = 0; i < MAX_SECONDARY_IDX; i++)
        put_u32(buf, &off, s->secondary_root_page_no[i]);

    /* --- columns (variable: 222 bytes + enum data per column) --- */
    for (int i = 0; i < s->num_columns; i++) {
        const ColumnDef *c = &s->columns[i];

        put_bytes(buf, &off, c->name, MAX_COLUMN_NAME);
        put_u8(buf, &off, (uint8_t)c->type);
        put_u16(buf, &off, c->max_len);
        put_u8(buf, &off, c->scale);

        /* pack 5 boolean flags into one byte */
        uint8_t flags = (uint8_t)(
            (c->is_not_null       & 1)       |
            ((c->is_primary_key   & 1) << 1) |
            ((c->is_unique        & 1) << 2) |
            ((c->is_auto_increment& 1) << 3) |
            ((c->has_default      & 1) << 4)
        );
        put_u8(buf, &off, flags);
        put_u8(buf, &off, c->default_value.is_null);

        /* default value data */
        uint8_t def_data[DEFAULT_DATA_SIZE];
        serialize_default(c, def_data);
        put_bytes(buf, &off, def_data, DEFAULT_DATA_SIZE);

        /* enum values (only meaningful when type == TYPE_ENUM) */
        put_u8(buf, &off, c->num_enum_values);
        for (int j = 0; j < c->num_enum_values; j++)
            put_bytes(buf, &off, c->enum_values[j], MAX_ENUM_STR_LEN);

        /* overflow check */
        if (off > PAGE_SIZE - 256) {
            /* not enough room left for foreign keys — schema too large */
            return 0;
        }
    }

    /* --- foreign keys (256 bytes each, fixed) --- */
    for (int i = 0; i < s->num_foreign_keys; i++) {
        const ForeignKey *fk = &s->foreign_keys[i];
        put_bytes(buf, &off, fk->constraint_name, MAX_COLUMN_NAME);
        put_bytes(buf, &off, fk->column_name,     MAX_COLUMN_NAME);
        put_bytes(buf, &off, fk->ref_table_name,  MAX_TABLE_NAME);
        put_bytes(buf, &off, fk->ref_column_name, MAX_COLUMN_NAME);

        if (off > PAGE_SIZE) return 0;
    }

    return off;
}

/* ------------------------------------------------------------------ */
/*  Page buffer → Schema deserialization                                */
/* ------------------------------------------------------------------ */

static int schema_deserialize(Schema *s, const uint8_t *buf)
{
    uint16_t off = 0;

    uint32_t magic = get_u32(buf, &off);
    if (magic != SCHEMA_PAGE_MAGIC) return MYDB_ERR;

    get_bytes(buf, &off, s->table_name, MAX_TABLE_NAME);
    s->num_columns          = get_u8(buf, &off);
    s->pk_col_idx           = get_u8(buf, &off);
    s->num_foreign_keys     = get_u8(buf, &off);
    s->num_secondary_indexes= get_u8(buf, &off);
    s->auto_incr_counter    = get_u32(buf, &off);
    s->root_page_no         = get_u32(buf, &off);

    for (int i = 0; i < MAX_SECONDARY_IDX; i++)
        s->secondary_col_idx[i] = get_u8(buf, &off);
    for (int i = 0; i < MAX_SECONDARY_IDX; i++)
        s->secondary_root_page_no[i] = get_u32(buf, &off);

    /* --- columns --- */
    for (int i = 0; i < s->num_columns; i++) {
        ColumnDef *c = &s->columns[i];

        get_bytes(buf, &off, c->name, MAX_COLUMN_NAME);
        c->type    = (DataType)get_u8(buf, &off);
        c->max_len = get_u16(buf, &off);
        c->scale   = get_u8(buf, &off);

        uint8_t flags         = get_u8(buf, &off);
        c->is_not_null        = (flags >> 0) & 1;
        c->is_primary_key     = (flags >> 1) & 1;
        c->is_unique          = (flags >> 2) & 1;
        c->is_auto_increment  = (flags >> 3) & 1;
        c->has_default        = (flags >> 4) & 1;

        c->default_value.is_null = get_u8(buf, &off);

        uint8_t def_data[DEFAULT_DATA_SIZE];
        get_bytes(buf, &off, def_data, DEFAULT_DATA_SIZE);
        deserialize_default(c, def_data);

        c->num_enum_values = get_u8(buf, &off);
        for (int j = 0; j < c->num_enum_values; j++)
            get_bytes(buf, &off, c->enum_values[j], MAX_ENUM_STR_LEN);
    }

    /* --- foreign keys --- */
    for (int i = 0; i < s->num_foreign_keys; i++) {
        ForeignKey *fk = &s->foreign_keys[i];
        get_bytes(buf, &off, fk->constraint_name, MAX_COLUMN_NAME);
        get_bytes(buf, &off, fk->column_name,     MAX_COLUMN_NAME);
        get_bytes(buf, &off, fk->ref_table_name,  MAX_TABLE_NAME);
        get_bytes(buf, &off, fk->ref_column_name, MAX_COLUMN_NAME);
    }

    return MYDB_OK;
}

/* ------------------------------------------------------------------ */
/*  Catalog lifecycle                                                   */
/* ------------------------------------------------------------------ */

int schema_catalog_open(SchemaCatalog *cat, const char *dir)
{
    if (!cat || !dir) return MYDB_ERR;

    memset(cat, 0, sizeof(SchemaCatalog));

    /* build path: dir + "/" + CATALOG_FILENAME */
    char path[512];
    snprintf(path, sizeof(path), "%s/%s", dir, CATALOG_FILENAME);

    /* try opening an existing catalog; create one if it doesn't exist */
    int created = 0;
    if (disk_open(&cat->catalog_dm, path) != MYDB_OK) {
        if (disk_create(&cat->catalog_dm, path) != MYDB_OK) return MYDB_ERR;
        created = 1;
    }

    if (created) {
        /* pre-allocate MAX_TABLES empty schema slot pages */
        uint8_t zero_page[PAGE_SIZE];
        memset(zero_page, 0, PAGE_SIZE);
        for (int i = 0; i < MAX_TABLES; i++) {
            uint32_t new_page_no;
            if (disk_alloc_page(&cat->catalog_dm, &new_page_no) != MYDB_OK)
                return MYDB_ERR;
            if (disk_write_page(&cat->catalog_dm, new_page_no, zero_page) != MYDB_OK)
                return MYDB_ERR;
        }
    } else {
        /* load all valid schemas from existing catalog */
        uint8_t page_buf[PAGE_SIZE];
        for (int slot = 0; slot < MAX_TABLES; slot++) {
            uint32_t page_no = (uint32_t)(slot + 1);  /* slot 0 → page 1, etc. */

            /* page might not exist yet if the file was partially initialized */
            if (page_no >= cat->catalog_dm.num_pages) break;

            if (disk_read_page(&cat->catalog_dm, page_no, page_buf) != MYDB_OK)
                continue;

            /* check magic — skip empty slots */
            uint32_t magic = (uint32_t)page_buf[0]
                           | ((uint32_t)page_buf[1] <<  8)
                           | ((uint32_t)page_buf[2] << 16)
                           | ((uint32_t)page_buf[3] << 24);
            if (magic != SCHEMA_PAGE_MAGIC) continue;

            int idx = cat->num_tables;
            if (schema_deserialize(&cat->tables[idx], page_buf) == MYDB_OK) {
                cat->slot[idx] = (uint8_t)slot;
                cat->num_tables++;
            }
        }
    }

    cat->is_loaded = 1;
    return MYDB_OK;
}

int schema_catalog_close(SchemaCatalog *cat)
{
    if (!cat || !cat->is_loaded) return MYDB_OK;
    disk_close(&cat->catalog_dm);
    cat->is_loaded = 0;
    return MYDB_OK;
}

/* ------------------------------------------------------------------ */
/*  Schema CRUD                                                         */
/* ------------------------------------------------------------------ */

int schema_add(SchemaCatalog *cat, const Schema *s)
{
    if (!cat || !s || !cat->is_loaded) return MYDB_ERR;

    /* reject duplicates */
    if (schema_get(cat, s->table_name) != NULL) return MYDB_ERR_DUPLICATE;

    if (cat->num_tables >= MAX_TABLES) return MYDB_ERR_FULL;

    /* find the first empty slot (a page with no SCHEMA_PAGE_MAGIC) */
    uint8_t used[MAX_TABLES];
    memset(used, 0, sizeof(used));
    for (int i = 0; i < cat->num_tables; i++)
        used[cat->slot[i]] = 1;

    int free_slot = -1;
    for (int i = 0; i < MAX_TABLES; i++) {
        if (!used[i]) { free_slot = i; break; }
    }
    if (free_slot < 0) return MYDB_ERR_FULL;

    /* copy into in-memory catalog */
    int idx = cat->num_tables;
    cat->tables[idx] = *s;
    cat->slot[idx]   = (uint8_t)free_slot;
    cat->num_tables++;

    /* persist to disk */
    return schema_flush(cat, idx);
}

int schema_remove(SchemaCatalog *cat, const char *table_name)
{
    if (!cat || !table_name || !cat->is_loaded) return MYDB_ERR;

    /* find in-memory index */
    int found = -1;
    for (int i = 0; i < cat->num_tables; i++) {
        if (strcmp(cat->tables[i].table_name, table_name) == 0) {
            found = i;
            break;
        }
    }
    if (found < 0) return MYDB_ERR_NOT_FOUND;

    /* zero out the catalog page (clears the magic → marks slot as empty) */
    uint8_t zero_page[PAGE_SIZE];
    memset(zero_page, 0, PAGE_SIZE);
    uint32_t page_no = (uint32_t)(cat->slot[found] + 1);
    if (disk_write_page(&cat->catalog_dm, page_no, zero_page) != MYDB_OK)
        return MYDB_ERR;

    /* compact the in-memory array by shifting remaining entries down */
    for (int i = found; i < cat->num_tables - 1; i++) {
        cat->tables[i] = cat->tables[i + 1];
        cat->slot[i]   = cat->slot[i + 1];
    }
    cat->num_tables--;

    return MYDB_OK;
}

Schema *schema_get(SchemaCatalog *cat, const char *table_name)
{
    if (!cat || !table_name || !cat->is_loaded) return NULL;

    for (int i = 0; i < cat->num_tables; i++) {
        if (strcmp(cat->tables[i].table_name, table_name) == 0)
            return &cat->tables[i];
    }
    return NULL;
}

int schema_flush(SchemaCatalog *cat, int idx)
{
    if (!cat || idx < 0 || idx >= cat->num_tables) return MYDB_ERR;

    uint8_t buf[PAGE_SIZE];
    if (schema_serialize(&cat->tables[idx], buf) == 0) return MYDB_ERR;

    uint32_t page_no = (uint32_t)(cat->slot[idx] + 1);
    return disk_write_page(&cat->catalog_dm, page_no, buf);
}

/* ------------------------------------------------------------------ */
/*  Row / column sizing                                                 */
/* ------------------------------------------------------------------ */

uint16_t schema_col_size(const ColumnDef *col)
{
    switch (col->type) {
        case TYPE_INT:      return 4;
        case TYPE_DECIMAL:  return 8;
        case TYPE_VARCHAR:  return (uint16_t)(2 + col->max_len); /* 2-byte length prefix */
        case TYPE_ENUM:     return 1;
        case TYPE_BOOL:     return 1;
        case TYPE_DATE:     return 4;
        case TYPE_DATETIME: return 8;
        default:            return 0;
    }
}

uint32_t schema_row_size(const Schema *s)
{
    uint32_t total = 0;
    for (int i = 0; i < s->num_columns; i++)
        total += schema_col_size(&s->columns[i]);
    return total;
}
