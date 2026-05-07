#ifndef RELATION_DEF_H
#define RELATION_DEF_H

#include "common.h"

#ifdef __cplusplus
extern "C" {
#endif

/* ------------------------------------------------------------------ */
/*  RelationDef — full definition of one table (relation).            */
/*                                                                    */
/*  Canonical struct describing a relation's columns, primary key,    */
/*  foreign keys, secondary indexes, and the root pages of its B+     */
/*  trees. Every layer that needs to interpret a row reads this.      */
/*                                                                    */
/*  Self-contained: depends only on common.h, so the parser /         */
/*  execution engine include it (via engine.h) without pulling in     */
/*  storage internals.                                                */
/* ------------------------------------------------------------------ */

#define MAX_FOREIGN_KEYS    8   /* max FK constraints per relation */
#define MAX_SECONDARY_IDX   8   /* max UNIQUE columns → secondary B+ trees */

/* ------------------------------------------------------------------ */
/*  ColumnDef — full definition of one column                         */
/*  (forward-declared as ColumnDef in common.h)                       */
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
/*  ForeignKey — one FK constraint on a relation                      */
/* ------------------------------------------------------------------ */
typedef struct {
    char constraint_name[MAX_COLUMN_NAME];  /* optional user-given name */
    char column_name[MAX_COLUMN_NAME];      /* FK column in this relation */
    char ref_table_name[MAX_TABLE_NAME];    /* referenced relation */
    char ref_column_name[MAX_COLUMN_NAME];  /* referenced column */
} ForeignKey;

/* ------------------------------------------------------------------ */
/*  RelationDef — table definition                                    */
/*  (forward-declared as RelationDef in common.h)                     */
/* ------------------------------------------------------------------ */
struct RelationDef {
    char       relation_name[MAX_TABLE_NAME];

    uint8_t    num_columns;
    ColumnDef  columns[MAX_COLUMNS];
    uint8_t    pk_col_idx;          /* index into columns[] for the primary key */

    uint8_t    num_foreign_keys;
    ForeignKey foreign_keys[MAX_FOREIGN_KEYS];

    uint32_t   auto_incr_counter;   /* next value for AUTO_INCREMENT columns */
    uint32_t   root_page_no;        /* root of the clustered B+ tree */

    /* one secondary B+ tree per UNIQUE (non-PK) column */
    uint8_t    num_secondary_indexes;
    uint8_t    secondary_col_idx[MAX_SECONDARY_IDX];        /* which columns */
    uint32_t   secondary_root_page_no[MAX_SECONDARY_IDX];   /* their root pages */
};

/* ------------------------------------------------------------------ */
/*  Sizing helpers — pure functions over ColumnDef / RelationDef.     */
/*  Used by the row serializer in storage.c. Lifted here in phase 9   */
/*  from the retired v1 schema.c so they survive after the v1 catalog */
/*  module retires.                                                   */
/* ------------------------------------------------------------------ */

/* Storage bytes for one value of this column type. */
uint16_t relation_col_size(const ColumnDef *col);

/* Total storage bytes for one full row (sum of all column sizes —
 * does NOT include hidden DB_TRX_ID / DB_ROLL_PTR / null bitmap). */
uint32_t relation_row_size(const RelationDef *r);

#ifdef __cplusplus
}
#endif

#endif /* RELATION_DEF_H */
