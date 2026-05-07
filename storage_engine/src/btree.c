#include "btree.h"
#include <string.h>
#include <stdlib.h>

/* ================================================================== */
/*  Key encode / decode / compare                                     */
/* ================================================================== */

/*
 * For correct sort order in a byte-comparison index, signed integers
 * need the sign bit flipped before storing big-endian.
 * e.g.  -1 (0x80000001 after flip? No: -1 = 0xFFFFFFFF, flip → 0x7FFFFFFF)
 *        0 (0x00000000, flip → 0x80000000)
 *       +1 (0x00000001, flip → 0x80000001)
 * This gives correct byte-lexicographic order for signed integers.
 */

static void put_be32(uint8_t *dst, uint32_t v) {
    dst[0]=(uint8_t)(v>>24); dst[1]=(uint8_t)(v>>16);
    dst[2]=(uint8_t)(v>>8);  dst[3]=(uint8_t)v;
}
static void put_be64(uint8_t *dst, uint64_t v) {
    put_be32(dst,(uint32_t)(v>>32)); put_be32(dst+4,(uint32_t)v);
}
static uint32_t get_be32(const uint8_t *src) {
    return ((uint32_t)src[0]<<24)|((uint32_t)src[1]<<16)|((uint32_t)src[2]<<8)|src[3];
}
static uint64_t get_be64(const uint8_t *src) {
    return ((uint64_t)get_be32(src)<<32)|get_be32(src+4);
}

uint16_t btree_key_encode(const Value *v, uint8_t *out)
{
    switch (v->type) {
    case TYPE_INT: {
        /* Flip sign bit for correct sort order */
        uint32_t u = (uint32_t)v->v.int_val ^ 0x80000000u;
        put_be32(out, u);
        return 4;
    }
    case TYPE_DECIMAL: {
        uint64_t u = (uint64_t)v->v.decimal_val ^ 0x8000000000000000ull;
        put_be64(out, u);
        return 8;
    }
    case TYPE_VARCHAR: {
        /* 2-byte length prefix + raw bytes */
        uint16_t len = v->v.varchar_val.len;
        out[0] = (uint8_t)(len >> 8);
        out[1] = (uint8_t)len;
        memcpy(out + 2, v->v.varchar_val.data, len);
        return (uint16_t)(2 + len);
    }
    case TYPE_BOOL:
        out[0] = v->v.bool_val;
        return 1;
    case TYPE_ENUM:
        out[0] = v->v.enum_val;
        return 1;
    case TYPE_DATE: {
        uint32_t u = (uint32_t)v->v.date_val ^ 0x80000000u;
        put_be32(out, u);
        return 4;
    }
    case TYPE_DATETIME: {
        uint64_t u = (uint64_t)v->v.datetime_val ^ 0x8000000000000000ull;
        put_be64(out, u);
        return 8;
    }
    }
    return 0;
}

void btree_key_decode(const uint8_t *data, uint16_t len, DataType type, Value *out)
{
    out->type    = type;
    out->is_null = 0;
    switch (type) {
    case TYPE_INT:
        out->v.int_val = (int32_t)(get_be32(data) ^ 0x80000000u);
        break;
    case TYPE_DECIMAL:
        out->v.decimal_val = (int64_t)(get_be64(data) ^ 0x8000000000000000ull);
        break;
    case TYPE_VARCHAR: {
        uint16_t slen = ((uint16_t)data[0] << 8) | data[1];
        out->v.varchar_val.len = slen;
        memcpy(out->v.varchar_val.data, data + 2, slen);
        /* Buffer is exactly MAX_VARCHAR_LEN bytes; only NUL-terminate if
         * there is room. Callers must rely on .len for length, not strlen. */
        if (slen < MAX_VARCHAR_LEN)
            out->v.varchar_val.data[slen] = '\0';
        break;
    }
    case TYPE_BOOL:
        out->v.bool_val = data[0];
        break;
    case TYPE_ENUM:
        out->v.enum_val = data[0];
        break;
    case TYPE_DATE:
        out->v.date_val = (int32_t)(get_be32(data) ^ 0x80000000u);
        break;
    case TYPE_DATETIME:
        out->v.datetime_val = (int64_t)(get_be64(data) ^ 0x8000000000000000ull);
        break;
    }
    (void)len;
}

int btree_key_compare(const uint8_t *a, uint16_t alen,
                      const uint8_t *b, uint16_t blen,
                      DataType type)
{
    /* For fixed-size types, a straight byte comparison works because we
       encoded with the sign bit flipped and big-endian byte order.       */
    if (type != TYPE_VARCHAR) {
        uint16_t clen = alen < blen ? alen : blen;
        int r = memcmp(a, b, clen);
        if (r != 0) return r;
        return (int)alen - (int)blen;
    }

    /* VARCHAR: compare by content first, then by length */
    uint16_t la = ((uint16_t)a[0] << 8) | a[1];
    uint16_t lb = ((uint16_t)b[0] << 8) | b[1];
    uint16_t clen = la < lb ? la : lb;
    int r = memcmp(a + 2, b + 2, clen);
    if (r != 0) return r;
    return (int)la - (int)lb;
}

/* ================================================================== */
/*  On-disk record helpers                                            */
/*                                                                    */
/*  Clustered leaf record layout:                                     */
/*    [key_len:2B][key_bytes:key_len][val_len:2B][val_bytes:val_len]  */
/*                                                                    */
/*  Internal node record layout:                                      */
/*    [key_len:2B][key_bytes:key_len][child_page_no:4B]               */
/*                                                                    */
/*  Secondary leaf record layout:                                     */
/*    [key_len:2B][key_bytes:key_len][page_no:4B][slot_no:2B]         */
/* ================================================================== */

/* Extract the encoded key from a record stored in a page. */
static void record_get_key(const uint8_t *rec, uint8_t *key_out, uint16_t *key_len)
{
    *key_len = ((uint16_t)rec[0] << 8) | rec[1];
    memcpy(key_out, rec + 2, *key_len);
}

/* For internal nodes: get the child page number at the end of the record. */
static uint32_t record_get_child(const uint8_t *rec)
{
    uint16_t klen = ((uint16_t)rec[0] << 8) | rec[1];
    return get_be32(rec + 2 + klen);
}

/* Build an internal-node record: [key_len:2][key][child:4] */
static uint16_t build_internal_record(const uint8_t *key, uint16_t klen,
                                       uint32_t child, uint8_t *out)
{
    out[0] = (uint8_t)(klen >> 8);
    out[1] = (uint8_t)klen;
    memcpy(out + 2, key, klen);
    put_be32(out + 2 + klen, child);
    return (uint16_t)(2 + klen + 4);
}

/* ================================================================== */
/*  BTree initialise                                                  */
/* ================================================================== */

void btree_init(BTree *bt, BufferPool *bp, DiskManager *dm,
                int table_id, uint32_t root_page_no,
                DataType key_type, uint8_t is_secondary)
{
    bt->bp           = bp;
    bt->dm           = dm;
    bt->table_id     = table_id;
    bt->root_page_no = root_page_no;
    bt->key_type     = key_type;
    bt->is_secondary = is_secondary;
}

/* ================================================================== */
/*  Tree traversal helpers                                            */
/* ================================================================== */

/*
 * Search a leaf page for an encoded key.
 *
 * Iterates the directory in key order. With the directory maintained
 * in key order, this is a straight scan (could be binary-searched as
 * a future perf optimization).
 *
 *   found_out = 1, slot_out = matching slot           — exact match
 *   found_out = 0, slot_out = first slot whose key > key (insertion point);
 *                  num_dir_slots if all slots' keys are < key
 */
static void leaf_search(const uint8_t *page, const uint8_t *key, uint16_t klen,
                        DataType type, uint16_t *slot_out, uint8_t *found_out)
{
    uint16_t n = page_dir_count(page);
    *found_out = 0;
    *slot_out  = n;

    for (uint16_t i = 0; i < n; i++) {
        uint16_t doff = page_dir_get(page, i);

        uint8_t  rk[MAX_VARCHAR_LEN + 2];
        uint16_t rklen;
        record_get_key(page + doff, rk, &rklen);

        int cmp = btree_key_compare(rk, rklen, key, klen, type);
        if (cmp == 0) { *slot_out = i; *found_out = 1; return; }
        if (cmp >  0) { *slot_out = i; return; }
    }
}

/*
 * Find the slot index in a leaf or internal page where a record with
 * the given key should be inserted (first slot whose key >= search key,
 * or num_dir_slots if all are <).
 */
static uint16_t find_insertion_slot(const uint8_t *page,
                                    const uint8_t *key, uint16_t klen,
                                    DataType type)
{
    uint16_t n = page_dir_count(page);
    for (uint16_t i = 0; i < n; i++) {
        uint16_t doff = page_dir_get(page, i);

        uint8_t  rk[MAX_VARCHAR_LEN + 2];
        uint16_t rklen;
        record_get_key(page + doff, rk, &rklen);

        if (btree_key_compare(rk, rklen, key, klen, type) >= 0)
            return i;
    }
    return n;
}

/*
 * Search an internal page for the child page to follow for a given key.
 * Internal records are ordered by separator key in the directory; we
 * follow the rightmost separator <= search key, or the leftmost child
 * (stored in prev_page) if all separators are > key.
 */
static uint32_t internal_search(const uint8_t *page, const uint8_t *key,
                                  uint16_t klen, DataType type)
{
    uint16_t n = page_dir_count(page);

    PageHeader hdr;
    page_read_header(page, &hdr);
    uint32_t child = hdr.prev_page; /* leftmost child page */

    for (uint16_t i = 0; i < n; i++) {
        uint16_t doff = page_dir_get(page, i);

        uint8_t  rk[MAX_VARCHAR_LEN + 2];
        uint16_t rklen;
        record_get_key(page + doff, rk, &rklen);

        int cmp = btree_key_compare(rk, rklen, key, klen, type);
        if (cmp > 0) break;
        child = record_get_child(page + doff);
    }
    return child;
}

/* ================================================================== */
/*  btree_search                                                      */
/* ================================================================== */

int btree_search(BTree *bt, const Value *key, BTreeSearchResult *result)
{
    result->found = 0;

    if (bt->root_page_no == INVALID_PAGE) {
        result->page_no = INVALID_PAGE;
        result->slot_no = 0;
        return MYDB_OK;
    }

    /* Encode the key once */
    uint8_t  enc_key[MAX_VARCHAR_LEN + 2];
    uint16_t enc_len = btree_key_encode(key, enc_key);

    uint32_t cur_pno = bt->root_page_no;

    /* Descend from root to leaf */
    while (1) {
        uint8_t *page = bp_fetch_page(bt->bp, bt->dm, bt->table_id, cur_pno);
        if (!page) return MYDB_ERR;

        PageHeader hdr;
        page_read_header(page, &hdr);

        if (hdr.page_type == PAGE_TYPE_DATA) {
            /* Leaf page — do the final search */
            uint16_t slot;
            uint8_t  found;
            leaf_search(page, enc_key, enc_len, bt->key_type, &slot, &found);
            bp_unpin_page(bt->bp, bt->table_id, cur_pno, 0);

            result->page_no = cur_pno;
            result->slot_no = slot;
            result->found   = found;
            return MYDB_OK;
        }

        /* Internal page — find child to follow */
        uint32_t child = internal_search(page, enc_key, enc_len, bt->key_type);
        bp_unpin_page(bt->bp, bt->table_id, cur_pno, 0);
        cur_pno = child;
    }
}

/* ================================================================== */
/*  Split helpers                                                     */
/*                                                                    */
/*  When a leaf is full we:                                           */
/*    1. Allocate a new right page                                    */
/*    2. Move the upper half of records to the new page               */
/*    3. Return the separator key (first key of new right page) and   */
/*       the new right page number to the caller so it can insert     */
/*       the separator into the parent.                               */
/* ================================================================== */

/*
 * Split leaf page `left_pno`. Records are split at the median.
 * Returns the separator key + new right page number.
 * The caller is responsible for updating the parent.
 */
static int leaf_split(BTree *bt, uint32_t left_pno,
                      uint8_t *sep_key_out, uint16_t *sep_klen_out,
                      uint32_t *right_pno_out)
{
    uint8_t *left = bp_fetch_page(bt->bp, bt->dm, bt->table_id, left_pno);
    if (!left) return MYDB_ERR;

    /*
     * Collect all live records into a scratch buffer BEFORE we call
     * page_init (which zeroes the page and would invalidate any pointer
     * we kept into it).
     *
     * We store: [rec_size:2B][rec_bytes...] sequentially in scratch[].
     */
    uint8_t  scratch[PAGE_SIZE];
    uint16_t scratch_off = 0;
    uint16_t rec_off[2048];   /* offset of each record inside scratch */
    uint16_t rec_sz[2048];
    uint16_t live_count = 0;

    uint16_t n = page_dir_count(left);
    for (uint16_t i = 0; i < n && live_count < 2048; i++) {
        uint16_t doff, dsz;
        if (page_get_record(left, i, &doff, &dsz) == MYDB_OK) {
            rec_off[live_count]  = scratch_off;
            rec_sz[live_count]   = dsz;
            memcpy(scratch + scratch_off, left + doff, dsz);
            scratch_off = (uint16_t)(scratch_off + dsz);
            live_count++;
        }
    }

    uint16_t mid = live_count / 2;

    /* Save prev_page before page_init zeros everything */
    PageHeader lhdr_saved;
    page_read_header(left, &lhdr_saved);
    uint32_t orig_prev = lhdr_saved.prev_page;
    uint32_t orig_next = lhdr_saved.next_page;

    /* Allocate new right page */
    uint32_t right_pno;
    uint8_t *right = bp_alloc_page(bt->bp, bt->dm, bt->table_id, &right_pno);
    if (!right) { bp_unpin_page(bt->bp, bt->table_id, left_pno, 0); return MYDB_ERR; }
    page_init(right, right_pno, PAGE_TYPE_DATA);

    /* Re-initialise left (this zeroes it; that is fine — we have copies in scratch) */
    page_init(left, left_pno, PAGE_TYPE_DATA);

    /* Restore leaf-chain pointers:  orig_prev ← left ↔ right → orig_next */
    PageHeader lhdr, rhdr;
    page_read_header(left, &lhdr);
    page_read_header(right, &rhdr);
    lhdr.prev_page = orig_prev;
    lhdr.next_page = right_pno;
    rhdr.prev_page = left_pno;
    rhdr.next_page = orig_next;
    page_write_header(left,  &lhdr);
    page_write_header(right, &rhdr);

    /* Update the old right neighbour's prev pointer if it exists */
    if (orig_next != INVALID_PAGE) {
        uint8_t *nbr = bp_fetch_page(bt->bp, bt->dm, bt->table_id, orig_next);
        if (nbr) {
            PageHeader nhdr; page_read_header(nbr, &nhdr);
            nhdr.prev_page = right_pno;
            page_write_header(nbr, &nhdr);
            bp_unpin_page(bt->bp, bt->table_id, orig_next, 1);
        }
    }

    /* Re-insert lower half into left (records already in key order in scratch) */
    for (uint16_t i = 0; i < mid; i++) {
        page_insert_record(left, scratch + rec_off[i], rec_sz[i], i);
    }

    /* Insert upper half into right */
    for (uint16_t i = mid; i < live_count; i++) {
        page_insert_record(right, scratch + rec_off[i], rec_sz[i], (uint16_t)(i - mid));
    }

    /* Separator = first key of the new right page (copy-up for leaf nodes) */
    uint16_t rdoff = page_dir_get(right, 0);
    record_get_key(right + rdoff, sep_key_out, sep_klen_out);

    bp_unpin_page(bt->bp, bt->table_id, left_pno,  1);
    bp_unpin_page(bt->bp, bt->table_id, right_pno, 1);

    *right_pno_out = right_pno;
    return MYDB_OK;
}

/*
 * Split an internal page at `int_pno`. Similar to leaf split but
 * the middle key is pushed up (not copied up) — it is removed from
 * both children.
 */
static int internal_split(BTree *bt, uint32_t int_pno,
                           uint8_t *sep_key_out, uint16_t *sep_klen_out,
                           uint32_t *right_pno_out)
{
    uint8_t *page = bp_fetch_page(bt->bp, bt->dm, bt->table_id, int_pno);
    if (!page) return MYDB_ERR;

    /* Copy all records into scratch before page_init zeroes the page */
    uint8_t  scratch[PAGE_SIZE];
    uint16_t scratch_off = 0;
    uint16_t rec_off[2048];
    uint16_t rec_sz[2048];
    uint16_t cnt = 0;

    uint16_t n = page_dir_count(page);
    for (uint16_t i = 0; i < n && cnt < 2048; i++) {
        uint16_t doff, dsz;
        if (page_get_record(page, i, &doff, &dsz) == MYDB_OK) {
            rec_off[cnt] = scratch_off;
            rec_sz[cnt]  = dsz;
            memcpy(scratch + scratch_off, page + doff, dsz);
            scratch_off = (uint16_t)(scratch_off + dsz);
            cnt++;
        }
    }

    PageHeader hdr;
    page_read_header(page, &hdr);
    uint32_t leftmost_child = hdr.prev_page;

    uint16_t mid = cnt / 2;

    /* Copy the separator key from scratch (push-up: mid key goes to parent) */
    record_get_key(scratch + rec_off[mid], sep_key_out, sep_klen_out);
    uint32_t mid_child = record_get_child(scratch + rec_off[mid]);

    /* Allocate new right internal page */
    uint32_t right_pno;
    uint8_t *right = bp_alloc_page(bt->bp, bt->dm, bt->table_id, &right_pno);
    if (!right) { bp_unpin_page(bt->bp, bt->table_id, int_pno, 0); return MYDB_ERR; }
    page_init(right, right_pno, PAGE_TYPE_INDEX);

    /* Right page's leftmost child = mid_child */
    PageHeader rhdr;
    page_read_header(right, &rhdr);
    rhdr.prev_page = mid_child;
    page_write_header(right, &rhdr);

    /* Insert upper half (after mid) into right */
    for (uint16_t i = mid + 1; i < cnt; i++) {
        page_insert_record(right, scratch + rec_off[i], rec_sz[i], (uint16_t)(i - mid - 1));
    }

    /* Rebuild left with lower half (before mid) */
    page_init(page, int_pno, PAGE_TYPE_INDEX);
    PageHeader lhdr;
    page_read_header(page, &lhdr);
    lhdr.prev_page = leftmost_child;
    page_write_header(page, &lhdr);

    for (uint16_t i = 0; i < mid; i++) {
        page_insert_record(page, scratch + rec_off[i], rec_sz[i], i);
    }

    bp_unpin_page(bt->bp, bt->table_id, int_pno,   1);
    bp_unpin_page(bt->bp, bt->table_id, right_pno, 1);

    *right_pno_out = right_pno;
    return MYDB_OK;
}

/* ================================================================== */
/*  btree_insert                                                        */
/*                                                                      */
/*  Uses a path stack to track parent pages during descent so we can   */
/*  propagate splits back up without a second traversal.               */
/* ================================================================== */

#define MAX_TREE_HEIGHT 16

int btree_insert(BTree *bt, const Value *key,
                 const uint8_t *record, uint16_t record_len,
                 RID *out_rid)
{
    uint8_t  enc_key[MAX_VARCHAR_LEN + 2];
    uint16_t enc_len = btree_key_encode(key, enc_key);

    /* --- Empty tree: create the first root leaf page --- */
    if (bt->root_page_no == INVALID_PAGE) {
        uint32_t root_pno;
        uint8_t *root = bp_alloc_page(bt->bp, bt->dm, bt->table_id, &root_pno);
        if (!root) return MYDB_ERR;
        page_init(root, root_pno, PAGE_TYPE_DATA);

        int rc = page_insert_record(root, record, record_len, 0);
        bp_unpin_page(bt->bp, bt->table_id, root_pno, 1);
        if (rc != MYDB_OK) return rc;

        bt->root_page_no = root_pno;

        /* Update root page in file header */
        FileHeader fh;
        disk_read_header(bt->dm, &fh);
        fh.root_page_no = root_pno;
        disk_write_header(bt->dm, &fh);

        if (out_rid) { out_rid->page_no = root_pno; out_rid->slot_no = 0; }
        return MYDB_OK;
    }

    /* --- Path stack: store page numbers from root to leaf --- */
    uint32_t path[MAX_TREE_HEIGHT];
    int      depth = 0;

    /* Descend to the correct leaf, recording the path */
    uint32_t cur_pno = bt->root_page_no;
    while (1) {
        uint8_t *page = bp_fetch_page(bt->bp, bt->dm, bt->table_id, cur_pno);
        if (!page) return MYDB_ERR;

        PageHeader hdr;
        page_read_header(page, &hdr);

        if (hdr.page_type == PAGE_TYPE_DATA) {
            /* Leaf found — check for duplicate key */
            uint16_t slot; uint8_t found;
            leaf_search(page, enc_key, enc_len, bt->key_type, &slot, &found);
            bp_unpin_page(bt->bp, bt->table_id, cur_pno, 0);

            if (found) return MYDB_ERR_DUPLICATE;
            path[depth++] = cur_pno;
            break;
        }

        /* Internal page */
        uint32_t child = internal_search(page, enc_key, enc_len, bt->key_type);
        bp_unpin_page(bt->bp, bt->table_id, cur_pno, 0);
        path[depth++] = cur_pno;
        cur_pno = child;
    }

    /* `cur_pno` is now the leaf page. Try to insert directly. */
    uint8_t *leaf = bp_fetch_page(bt->bp, bt->dm, bt->table_id, cur_pno);
    if (!leaf) return MYDB_ERR;

    uint16_t at = find_insertion_slot(leaf, enc_key, enc_len, bt->key_type);
    int rc = page_insert_record(leaf, record, record_len, at);

    if (rc == MYDB_OK) {
        /* Simple case: record fit without a split */
        if (out_rid) { out_rid->page_no = cur_pno; out_rid->slot_no = at; }
        bp_unpin_page(bt->bp, bt->table_id, cur_pno, 1);
        return MYDB_OK;
    }

    bp_unpin_page(bt->bp, bt->table_id, cur_pno, 0);

    /* Only a full page should drive us into a split. Any other error
     * (bad args, invalid slot) is a real bug — propagate it. */
    if (rc != MYDB_ERR_FULL) return rc;

    /* --- Leaf is full: split and propagate up the path --- */
    uint8_t  sep_key[MAX_VARCHAR_LEN + 2];
    uint16_t sep_klen;
    uint32_t right_pno;

    if (leaf_split(bt, cur_pno, sep_key, &sep_klen, &right_pno) != MYDB_OK)
        return MYDB_ERR;

    /* Now insert the original record into the correct half */
    int cmp = btree_key_compare(enc_key, enc_len, sep_key, sep_klen, bt->key_type);
    uint32_t insert_pno = (cmp < 0) ? cur_pno : right_pno;

    uint8_t *ins_page = bp_fetch_page(bt->bp, bt->dm, bt->table_id, insert_pno);
    if (!ins_page) return MYDB_ERR;

    uint16_t ins_at = find_insertion_slot(ins_page, enc_key, enc_len, bt->key_type);
    page_insert_record(ins_page, record, record_len, ins_at);
    if (out_rid) { out_rid->page_no = insert_pno; out_rid->slot_no = ins_at; }
    bp_unpin_page(bt->bp, bt->table_id, insert_pno, 1);

    /* --- Propagate split up the path (internal node insertions) ---
     *
     * We work from depth-2 (parent of leaf) up to the root.
     * Each time we insert (sep_key → right_pno) into the parent.
     * If the parent is also full, we split it and continue.
     */
    for (int level = depth - 2; level >= 0; level--) {
        uint32_t parent_pno = path[level];
        uint8_t *parent = bp_fetch_page(bt->bp, bt->dm, bt->table_id, parent_pno);
        if (!parent) return MYDB_ERR;

        /* Build internal record: [sep_key_len:2][sep_key][right_pno:4] */
        uint8_t  int_rec[MAX_VARCHAR_LEN + 2 + 4 + 2];
        uint16_t int_rec_len = build_internal_record(sep_key, sep_klen, right_pno, int_rec);

        uint16_t pat = find_insertion_slot(parent, sep_key, sep_klen, bt->key_type);
        rc = page_insert_record(parent, int_rec, int_rec_len, pat);
        if (rc == MYDB_OK) {
            bp_unpin_page(bt->bp, bt->table_id, parent_pno, 1);
            return MYDB_OK; /* no more splits needed */
        }

        bp_unpin_page(bt->bp, bt->table_id, parent_pno, 0);

        /* Only split on a genuinely full parent — propagate other errors. */
        if (rc != MYDB_ERR_FULL) return rc;

        /* Parent is full — split it too */
        uint8_t  new_sep[MAX_VARCHAR_LEN + 2];
        uint16_t new_sep_len;
        uint32_t new_right_pno;
        if (internal_split(bt, parent_pno, new_sep, &new_sep_len, &new_right_pno) != MYDB_OK)
            return MYDB_ERR;

        /* Insert the int_rec into the correct half of the split parent */
        cmp = btree_key_compare(sep_key, sep_klen, new_sep, new_sep_len, bt->key_type);
        uint32_t ins_pno = (cmp < 0) ? parent_pno : new_right_pno;
        uint8_t *ins_par = bp_fetch_page(bt->bp, bt->dm, bt->table_id, ins_pno);
        if (ins_par) {
            uint16_t at2 = find_insertion_slot(ins_par, sep_key, sep_klen, bt->key_type);
            page_insert_record(ins_par, int_rec, int_rec_len, at2);
            bp_unpin_page(bt->bp, bt->table_id, ins_pno, 1);
        }

        sep_klen  = new_sep_len;
        memcpy(sep_key, new_sep, new_sep_len);
        right_pno = new_right_pno;
    }

    /* --- Root split: create a new root internal page --- */
    uint32_t new_root_pno;
    uint8_t *new_root = bp_alloc_page(bt->bp, bt->dm, bt->table_id, &new_root_pno);
    if (!new_root) return MYDB_ERR;
    page_init(new_root, new_root_pno, PAGE_TYPE_INDEX);

    /* New root has old root as leftmost child, sep_key → right_pno as first record */
    PageHeader nr_hdr;
    page_read_header(new_root, &nr_hdr);
    nr_hdr.prev_page = bt->root_page_no; /* leftmost child */
    page_write_header(new_root, &nr_hdr);

    uint8_t  int_rec[MAX_VARCHAR_LEN + 2 + 4 + 2];
    uint16_t int_rec_len = build_internal_record(sep_key, sep_klen, right_pno, int_rec);
    page_insert_record(new_root, int_rec, int_rec_len, 0);
    bp_unpin_page(bt->bp, bt->table_id, new_root_pno, 1);

    bt->root_page_no = new_root_pno;

    /* Persist new root in the file header */
    FileHeader fh;
    disk_read_header(bt->dm, &fh);
    fh.root_page_no = new_root_pno;
    disk_write_header(bt->dm, &fh);

    return MYDB_OK;
}

/* ================================================================== */
/*  btree_delete                                                      */
/* ================================================================== */

int btree_delete(BTree *bt, const Value *key)
{
    BTreeSearchResult res;
    if (btree_search(bt, key, &res) != MYDB_OK) return MYDB_ERR;
    if (!res.found) return MYDB_ERR_NOT_FOUND;

    uint8_t *page = bp_fetch_page(bt->bp, bt->dm, bt->table_id, res.page_no);
    if (!page) return MYDB_ERR;

    int rc = page_delete_record(page, res.slot_no);
    bp_unpin_page(bt->bp, bt->table_id, res.page_no, (rc == MYDB_OK) ? 1 : 0);
    return rc;
}

/* ================================================================== */
/*  Cursor                                                            */
/* ================================================================== */

/*
 * Walk down the leftmost path to find the leftmost leaf page.
 * The leftmost child at each internal level is in hdr.prev_page.
 */
static uint32_t find_leftmost_leaf(BTree *bt)
{
    if (bt->root_page_no == INVALID_PAGE) return INVALID_PAGE;

    uint32_t pno = bt->root_page_no;
    while (1) {
        uint8_t *page = bp_fetch_page(bt->bp, bt->dm, bt->table_id, pno);
        if (!page) return INVALID_PAGE;

        PageHeader hdr;
        page_read_header(page, &hdr);

        if (hdr.page_type == PAGE_TYPE_DATA) {
            bp_unpin_page(bt->bp, bt->table_id, pno, 0);
            return pno;
        }

        uint32_t next = hdr.prev_page; /* leftmost child */
        bp_unpin_page(bt->bp, bt->table_id, pno, 0);
        pno = next;
    }
}

int btree_cursor_open(BTree *bt, Cursor *cur)
{
    cur->tree      = bt;
    cur->done      = 0;
    cur->page_no   = find_leftmost_leaf(bt);
    cur->next_slot = 0;

    if (cur->page_no == INVALID_PAGE) {
        cur->done = 1;
    }
    return MYDB_OK;
}

int btree_cursor_next(Cursor *cur, uint8_t *data_out, uint16_t *len)
{
    if (cur->done) return MYDB_ERR_NOT_FOUND;

    while (!cur->done) {
        uint8_t *page = bp_fetch_page(cur->tree->bp, cur->tree->dm,
                                       cur->tree->table_id, cur->page_no);
        if (!page) return MYDB_ERR;

        PageHeader hdr;
        page_read_header(page, &hdr);

        if (cur->next_slot < hdr.num_dir_slots) {
            uint16_t doff, dsz;
            if (page_get_record(page, cur->next_slot, &doff, &dsz) == MYDB_OK) {
                cur->last_page_no = cur->page_no;
                cur->last_slot    = cur->next_slot;
                cur->next_slot++;
                memcpy(data_out, page + doff, dsz);
                *len = dsz;
                bp_unpin_page(cur->tree->bp, cur->tree->table_id, cur->page_no, 0);
                return MYDB_OK;
            }
            /* Shouldn't happen: directory only references live records */
            cur->next_slot++;
            bp_unpin_page(cur->tree->bp, cur->tree->table_id, cur->page_no, 0);
            continue;
        }

        /* End of this leaf — follow next_page */
        uint32_t next_pno = hdr.next_page;
        bp_unpin_page(cur->tree->bp, cur->tree->table_id, cur->page_no, 0);

        if (next_pno == INVALID_PAGE || next_pno == 0) {
            cur->done = 1;
        } else {
            cur->page_no   = next_pno;
            cur->next_slot = 0;
        }
    }

    return MYDB_ERR_NOT_FOUND;
}

void btree_cursor_close(Cursor *cur)
{
    cur->done = 1;
}
