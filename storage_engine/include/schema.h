#ifndef SCHEMA_H
#define SCHEMA_H

#include "common.h"
#include "disk_manager.h"

/* ------------------------------------------------------------------ */
/*  Schema-level constants                                              */
/* ------------------------------------------------------------------ */
#define MAX_FOREIGN_KEYS    8       /* max FK constraints per table */
#define MAX_SECONDARY_IDX   8       /* max UNIQUE columns → secondary B+ Trees */
#define SCHEMA_PAGE_MAGIC   0x53434D41u  /* "SCMA" — marks a valid schema slot */
#define CATALOG_FILENAME    "__catalog.mydb"

/* ------------------------------------------------------------------ */
/*  ColumnDef — full definition of one column                          */
/*  (forward-declared as ColumnDef in common.h)                        */
/* ------------------------------------------------------------------ */
struct ColumnDef {
    char      name[MAX_COLUMN_NAME];

    DataType  type;
    uint16_t  max_len;      /* VARCHAR: max chars allowed; DECIMAL: total digits */
    uint8_t   scale;        /* DECIMAL only: digits after decimal point */

    /* column constraints */
    uint8_t   is_not_null;
    uint8_t   is_primary_key;
    uint8_t   is_unique;
    uint8_t   is_auto_increment;

    /* DEFAULT value */
    uint8_t   has_default;
    Value     default_value;    /* valid only when has_default == 1 */

    /* ENUM value list — only used when type == TYPE_ENUM */
    uint8_t   num_enum_values;
    char      enum_values[MAX_ENUM_VALUES][MAX_ENUM_STR_LEN];
};

/* ------------------------------------------------------------------ */
/*  ForeignKey — one FK constraint on a table                          */
/* ------------------------------------------------------------------ */
typedef struct {
    char constraint_name[MAX_COLUMN_NAME];  /* optional user-given name */
    char column_name[MAX_COLUMN_NAME];      /* FK column in this table */
    char ref_table_name[MAX_TABLE_NAME];    /* referenced table */
    char ref_column_name[MAX_COLUMN_NAME];  /* referenced column */
} ForeignKey;

/* ------------------------------------------------------------------ */
/*  Schema — full definition of one table                              */
/*  (forward-declared as Schema in common.h)                           */
/* ------------------------------------------------------------------ */
struct Schema {
    char       table_name[MAX_TABLE_NAME];

    uint8_t    num_columns;
    ColumnDef  columns[MAX_COLUMNS];
    uint8_t    pk_col_idx;      /* index into columns[] for the primary key */

    uint8_t    num_foreign_keys;
    ForeignKey foreign_keys[MAX_FOREIGN_KEYS];

    uint32_t   auto_incr_counter;   /* next value for AUTO_INCREMENT columns */
    uint32_t   root_page_no;        /* root of the clustered B+ Tree */

    /* one secondary B+ Tree per UNIQUE (non-PK) column */
    uint8_t    num_secondary_indexes;
    uint8_t    secondary_col_idx[MAX_SECONDARY_IDX];        /* which columns */
    uint32_t   secondary_root_page_no[MAX_SECONDARY_IDX];   /* their root pages */
};

/* ------------------------------------------------------------------ */
/*  SchemaCatalog — in-memory catalog of all known tables              */
/*                                                                      */
/*  Backed by __catalog.mydb:                                           */
/*    Page 0          : standard FileHeader                             */
/*    Pages 1–MAX_TABLES : schema slots (one per page)                  */
/*  A slot with magic == SCHEMA_PAGE_MAGIC is occupied; 0 == empty.    */
/* ------------------------------------------------------------------ */
typedef struct {
    Schema      tables[MAX_TABLES];
    uint8_t     slot[MAX_TABLES];   /* which catalog page slot each table occupies */
    uint8_t     num_tables;
    DiskManager catalog_dm;
    uint8_t     is_loaded;
} SchemaCatalog;

/* ------------------------------------------------------------------ */
/*  Catalog lifecycle                                                   */
/* ------------------------------------------------------------------ */

/*
 * Open (or create) the catalog file in dir.
 * Loads all existing schemas into memory.
 * Creates __catalog.mydb with MAX_TABLES empty slots if it doesn't exist.
 */
int schema_catalog_open(SchemaCatalog *cat, const char *dir);

/* Flush everything to disk and close the catalog file. */
int schema_catalog_close(SchemaCatalog *cat);

/* ------------------------------------------------------------------ */
/*  Schema CRUD                                                         */
/* ------------------------------------------------------------------ */

/*
 * Add a new schema and persist it immediately.
 * Returns MYDB_ERR_DUPLICATE if a table with that name already exists.
 * Returns MYDB_ERR_FULL if MAX_TABLES is reached.
 */
int schema_add(SchemaCatalog *cat, const Schema *s);

/*
 * Remove a schema by table name (zeroes its catalog slot on disk).
 * Returns MYDB_ERR_NOT_FOUND if the table doesn't exist.
 */
int schema_remove(SchemaCatalog *cat, const char *table_name);

/*
 * Look up a schema by table name.
 * Returns a pointer into cat->tables[], or NULL if not found.
 */
Schema *schema_get(SchemaCatalog *cat, const char *table_name);

/*
 * Persist the schema at in-memory index idx back to its catalog page.
 * Used when a running table's auto_incr_counter or root_page_no changes.
 */
int schema_flush(SchemaCatalog *cat, int idx);

/* ------------------------------------------------------------------ */
/*  Row / column sizing helpers                                         */
/* ------------------------------------------------------------------ */

/* Storage bytes for one value of this column type (used during row serialization). */
uint16_t schema_col_size(const ColumnDef *col);

/* Total storage bytes for one full row (sum of all column sizes, no hidden cols). */
uint32_t schema_row_size(const Schema *s);

#endif /* SCHEMA_H */
