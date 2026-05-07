#include "relation_def.h"

/* Lifted from the retired v1 schema.c in phase 9. The on-disk byte
 * layout for each type is fixed by common.h's storage size table; this
 * helper returns the same numbers callers depended on previously. */

uint16_t relation_col_size(const ColumnDef *col)
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

uint32_t relation_row_size(const RelationDef *r)
{
    uint32_t total = 0;
    for (int i = 0; i < r->num_columns; i++)
        total += relation_col_size(&r->columns[i]);
    return total;
}
