#ifndef BTREE_H
#define BTREE_H

#include "common.h"
#include "page.h"
#include "buffer_pool.h"

/*
 * btree.h — B+ Tree index
 *
 * Every table has a clustered index keyed on its primary key. Leaf
 * pages of the clustered index store the full serialised row. Internal
 * (non-leaf) pages store (separator_key, child_page_no) pairs.
 *
 * Tables with UNIQUE columns get a secondary index per unique column.
 * Leaf pages of a secondary index store (key_value, RID) pairs that
 * point back into the clustered index.
 *
 * On-disk record format — clustered leaf:
 *   [key_len:2B][key_bytes][value_len:2B][value_bytes]
 *
 * On-disk record format — internal node:
 *   [key_len:2B][key_bytes][child_page_no:4B]
 *
 * On-disk record format — secondary leaf:
 *   [key_len:2B][key_bytes][page_no:4B][slot_no:2B]
 *
 * Key serialisation (Value → bytes):
 *   INT      : 4 bytes big-endian int32  (sign-bit flipped for correct sort)
 *   DECIMAL  : 8 bytes big-endian int64  (sign-bit flipped)
 *   VARCHAR  : 2B length + raw bytes
 *   BOOL     : 1 byte
 *   ENUM     : 1 byte
 *   DATE     : 4 bytes big-endian int32
 *   DATETIME : 8 bytes big-endian int64
 */

/* ------------------------------------------------------------------ */
/*  Key — serialised form of a Value for storage and comparison        */
/* ------------------------------------------------------------------ */
typedef struct {
    uint8_t  data[MAX_VARCHAR_LEN + 2]; /* largest possible key: VARCHAR(150) + 2B len */
    uint16_t len;
} BTreeKey;

/* ------------------------------------------------------------------ */
/*  BTree handle — one per index (clustered or secondary)             */
/* ------------------------------------------------------------------ */
typedef struct {
    BufferPool  *bp;
    DiskManager *dm;
    int          table_id;
    uint32_t     root_page_no;  /* INVALID_PAGE when tree is empty */
    DataType     key_type;
    uint8_t      is_secondary;  /* 0 = clustered, 1 = secondary */
} BTree;

/* ------------------------------------------------------------------ */
/*  Cursor — iterator for sequential scans                             */
/*                                                                      */
/*  We walk the key-order linked list (infimum → r1 → r2 → supremum)  */
/*  rather than the page directory (which is in insertion order).      */
/* ------------------------------------------------------------------ */
struct Cursor {
    BTree   *tree;
    uint32_t page_no;       /* current leaf page */
    uint16_t next_data_off; /* data offset of the NEXT record to return
                               (in the current page's linked list) */
    uint8_t  done;          /* 1 = no more records */
    /* set by btree_cursor_next to the position of the last returned record */
    uint32_t last_page_no;
    uint16_t last_data_off;
};

/* ------------------------------------------------------------------ */
/*  Search result                                                       */
/* ------------------------------------------------------------------ */
typedef struct {
    uint32_t page_no;
    uint16_t slot_no;
    uint8_t  found;     /* 1 if key matched exactly */
} BTreeSearchResult;

/* ------------------------------------------------------------------ */
/*  Key serialisation helpers (used by btree and schema/storage layers) */
/* ------------------------------------------------------------------ */

/* Serialise a Value into a byte key. Returns the number of bytes written. */
uint16_t btree_key_encode(const Value *v, uint8_t *out);

/* Deserialise a byte key back into a Value. */
void btree_key_decode(const uint8_t *data, uint16_t len, DataType type, Value *out);

/* Compare two encoded keys of the same type. Returns <0, 0, >0. */
int btree_key_compare(const uint8_t *a, uint16_t alen,
                      const uint8_t *b, uint16_t blen,
                      DataType type);

/* ------------------------------------------------------------------ */
/*  BTree operations                                                    */
/* ------------------------------------------------------------------ */

/* Initialise a BTree handle (does not create any pages). */
void btree_init(BTree *bt, BufferPool *bp, DiskManager *dm,
                int table_id, uint32_t root_page_no,
                DataType key_type, uint8_t is_secondary);

/*
 * Search for key in the tree.
 * result->found == 1 if an exact match was found.
 * result->page_no / slot_no point to the leaf position (match or insertion point).
 */
int btree_search(BTree *bt, const Value *key, BTreeSearchResult *result);

/*
 * Insert a record into the tree.
 *   key        : the index key
 *   record     : the full record payload (key already included)
 *   record_len : byte length of record
 *   out_rid    : set to the RID of the inserted record
 *
 * Returns MYDB_ERR_DUPLICATE if the key already exists.
 * Handles leaf splits and internal-node splits recursively.
 */
int btree_insert(BTree *bt, const Value *key,
                 const uint8_t *record, uint16_t record_len,
                 RID *out_rid);

/*
 * Delete the record with the given key (exact match required).
 * Uses lazy deletion — marks the record as deleted in the page.
 * Returns MYDB_ERR_NOT_FOUND if key does not exist.
 */
int btree_delete(BTree *bt, const Value *key);

/* Open a cursor positioned at the leftmost (smallest key) leaf record. */
int btree_cursor_open(BTree *bt, Cursor *cur);

/*
 * Advance the cursor to the next live record.
 * Fills data_out (must be PAGE_SIZE bytes) and sets *len to the record size.
 * Returns MYDB_OK, or MYDB_ERR_NOT_FOUND when exhausted.
 */
int btree_cursor_next(Cursor *cur, uint8_t *data_out, uint16_t *len);

/* Close the cursor, unpinning any held pages. */
void btree_cursor_close(Cursor *cur);

#endif /* BTREE_H */
