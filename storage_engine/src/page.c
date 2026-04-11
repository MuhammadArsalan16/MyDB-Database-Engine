#include "page.h"
#include <string.h>
#include <stdint.h>

/* ------------------------------------------------------------------ */
/*  Record header encode/decode (5 bytes on disk)                     */
/*                                                                    */
/*  Layout:                                                           */
/*    Byte 0   : info_flags                                           */
/*    Bytes 1-2: (heap_no << 3) | rec_type  as big-endian uint16      */
/*    Bytes 3-4: next_offset  as big-endian uint16                    */
/* ------------------------------------------------------------------ */

void rec_hdr_encode(const RecordHeader *rh, uint8_t *dst)
{
    dst[0] = rh->info_flags;
    dst[1] = (uint8_t)(rh->heap_type >> 8);
    dst[2] = (uint8_t)(rh->heap_type & 0xFF);
    dst[3] = (uint8_t)(rh->next_offset >> 8);
    dst[4] = (uint8_t)(rh->next_offset & 0xFF);
}

void rec_hdr_decode(const uint8_t *src, RecordHeader *rh)
{
    rh->info_flags   = src[0];
    rh->heap_type    = ((uint16_t)src[1] << 8) | src[2];
    rh->next_offset  = ((uint16_t)src[3] << 8) | src[4];
}

/* ------------------------------------------------------------------ */
/*  Page header encode/decode                                         */
/*                                                                    */
/*  We store the PageHeader struct at a fixed layout using explicit   */
/*  byte-by-byte writes so the layout is endian-independent.          */
/*                                                                    */
/*  Offsets within the 38-byte header:                                */
/*    0  : checksum      (4B)                                         */
/*    4  : page_no       (4B)                                         */
/*    8  : prev_page     (4B)                                         */
/*    12 : next_page     (4B)                                         */
/*    16 : lsn           (8B)                                         */
/*    24 : page_type     (2B)                                         */
/*    26 : num_records   (2B)                                         */
/*    28 : free_offset   (2B)                                         */
/*    30 : garbage_offset(2B)                                         */
/*    32 : num_dir_slots (2B)                                         */
/*    34 : (4 bytes reserved/padding to reach 38B)                    */
/* ------------------------------------------------------------------ */

/* Store a 4-byte big-endian uint32 at dst */
static void put32(uint8_t *dst, uint32_t v)
{
    dst[0] = (uint8_t)(v >> 24);
    dst[1] = (uint8_t)(v >> 16);
    dst[2] = (uint8_t)(v >>  8);
    dst[3] = (uint8_t)(v      );
}

/* Store an 8-byte big-endian uint64 at dst */
static void put64(uint8_t *dst, uint64_t v)
{
    put32(dst,     (uint32_t)(v >> 32));
    put32(dst + 4, (uint32_t)(v      ));
}

/* Store a 2-byte big-endian uint16 at dst */
static void put16(uint8_t *dst, uint16_t v)
{
    dst[0] = (uint8_t)(v >> 8);
    dst[1] = (uint8_t)(v     );
}

/* Load a 4-byte big-endian uint32 from src */
static uint32_t get32(const uint8_t *src)
{
    return ((uint32_t)src[0] << 24) | ((uint32_t)src[1] << 16)
         | ((uint32_t)src[2] <<  8) |  (uint32_t)src[3];
}

/* Load an 8-byte big-endian uint64 from src */
static uint64_t get64(const uint8_t *src)
{
    return ((uint64_t)get32(src) << 32) | get32(src + 4);
}

/* Load a 2-byte big-endian uint16 from src */
static uint16_t get16(const uint8_t *src)
{
    return ((uint16_t)src[0] << 8) | src[1];
}

void page_read_header(const uint8_t *page, PageHeader *hdr)
{
    hdr->checksum       = get32(page +  0);
    hdr->page_no        = get32(page +  4);
    hdr->prev_page      = get32(page +  8);
    hdr->next_page      = get32(page + 12);
    hdr->lsn            = get64(page + 16);
    hdr->page_type      = get16(page + 24);
    hdr->num_records    = get16(page + 26);
    hdr->free_offset    = get16(page + 28);
    hdr->garbage_offset = get16(page + 30);
    hdr->num_dir_slots  = get16(page + 32);
}

void page_write_header(uint8_t *page, const PageHeader *hdr)
{
    put32(page +  0, hdr->checksum);
    put32(page +  4, hdr->page_no);
    put32(page +  8, hdr->prev_page);
    put32(page + 12, hdr->next_page);
    put64(page + 16, hdr->lsn);
    put16(page + 24, hdr->page_type);
    put16(page + 26, hdr->num_records);
    put16(page + 28, hdr->free_offset);
    put16(page + 30, hdr->garbage_offset);
    put16(page + 32, hdr->num_dir_slots);
}

/* ------------------------------------------------------------------ */
/*  FNV-1a checksum over bytes [4 .. PAGE_SIZE-9]                     */
/*  (skips the checksum field itself and the trailer)                 */
/* ------------------------------------------------------------------ */
static uint32_t fnv1a(const uint8_t *data, size_t len)
{
    uint32_t hash = 0x811C9DC5u;
    for (size_t i = 0; i < len; i++) {
        hash ^= data[i];
        hash *= 0x01000193u;
    }
    return hash;
}

void page_set_checksum(uint8_t *page)
{
    /* Checksum covers everything except the first 4 bytes (checksum field)
       and the last 8 bytes (trailer). */
    uint32_t cs = fnv1a(page + 4, PAGE_SIZE - 4 - PAGE_TRAILER_SIZE);
    put32(page, cs);
    /* Mirror to trailer */
    put32(page + PAGE_SIZE - PAGE_TRAILER_SIZE, cs);
}

int page_verify_checksum(const uint8_t *page)
{
    uint32_t stored  = get32(page);
    uint32_t computed = fnv1a(page + 4, PAGE_SIZE - 4 - PAGE_TRAILER_SIZE);
    return (stored == computed) ? MYDB_OK : MYDB_ERR;
}

/* ------------------------------------------------------------------ */
/*  Page directory helpers                                            */
/* ------------------------------------------------------------------ */

uint16_t page_dir_get(const uint8_t *page, uint16_t i)
{
    return get16(page + DIR_SLOT_OFFSET(i));
}

static void page_dir_set(uint8_t *page, uint16_t i, uint16_t data_offset)
{
    put16(page + DIR_SLOT_OFFSET(i), data_offset);
}

uint16_t page_dir_count(const uint8_t *page)
{
    PageHeader hdr;
    page_read_header(page, &hdr);
    return hdr.num_dir_slots;
}

/* ------------------------------------------------------------------ */
/*  page_init                                                         */
/* ------------------------------------------------------------------ */
int page_init(uint8_t *page, uint32_t page_no, PageType type)
{
    if (!page) return MYDB_ERR;
    memset(page, 0, PAGE_SIZE);

    /* Write the page header */
    PageHeader hdr;
    memset(&hdr, 0, sizeof(hdr));
    hdr.page_no        = page_no;
    hdr.prev_page      = INVALID_PAGE;
    hdr.next_page      = INVALID_PAGE;
    hdr.page_type      = (uint16_t)type;
    hdr.free_offset    = USER_RECORDS_OFFSET;  /* first user record goes at offset 64 */
    hdr.garbage_offset = 0;
    hdr.num_records    = 0;
    hdr.num_dir_slots  = 0;
    page_write_header(page, &hdr);

    /*
     * Infimum record at offset 38 — the lower boundary pseudo-record.
     * Its next_offset initially points to Supremum's DATA offset (56).
     */
    RecordHeader inf;
    inf.info_flags  = 0;
    inf.heap_type   = (0 << 3) | REC_INFIMUM;  /* heap_no=0, type=INFIMUM */
    inf.next_offset = SUPREMUM_DATA;
    rec_hdr_encode(&inf, page + INFIMUM_OFFSET);
    /* Infimum data: 8 bytes "infimum\0" */
    memcpy(page + INFIMUM_DATA, "infimum\0", 8);

    /*
     * Supremum record at offset 51 — the upper boundary pseudo-record.
     * Its next_offset is 0 (end of chain).
     */
    RecordHeader sup;
    sup.info_flags  = 0;
    sup.heap_type   = (1 << 3) | REC_SUPREMUM;  /* heap_no=1, type=SUPREMUM */
    sup.next_offset = 0;
    rec_hdr_encode(&sup, page + SUPREMUM_OFFSET);
    memcpy(page + SUPREMUM_DATA, "supremum", 8);

    return MYDB_OK;
}

/* ------------------------------------------------------------------ */
/*  page_free_space                                                   */
/*                                                                    */
/*  Free space = gap between free_offset (top of records) and the     */
/*  bottom of the page directory.                                     */
/*                                                                    */
/*  Directory bottom = PAGE_SIZE - PAGE_TRAILER_SIZE - 2*(num_slots+1)*/
/*  (+1 because we need room for one more slot after inserting)       */
/* ------------------------------------------------------------------ */
uint16_t page_free_space(const uint8_t *page)
{
    PageHeader hdr;
    page_read_header(page, &hdr);

    /* Byte just below the lowest existing directory slot */
    uint16_t dir_top = (uint16_t)(PAGE_SIZE - PAGE_TRAILER_SIZE
                                  - 2 * hdr.num_dir_slots);
    if (dir_top <= hdr.free_offset) return 0;
    return dir_top - hdr.free_offset;
}

/* ------------------------------------------------------------------ */
/*  page_insert_record                                                */
/*                                                                    */
/*  Inserts a new record after the record whose DATA starts at        */
/*  `predecessor`.  The new record is:                                */
/*    [5B RecordHeader][record_data bytes]                            */
/*                                                                    */
/*  After insertion, the linked list is:                              */
/*    ... → predecessor → NEW RECORD → old-predecessor-next → ...     */
/* ------------------------------------------------------------------ */
int page_insert_record(uint8_t *page,
                       const uint8_t *record_data, uint16_t record_size,
                       uint16_t predecessor,
                       uint16_t *slot_no)
{
    if (!page || !record_data || !slot_no) return MYDB_ERR;

    /* Total bytes needed: record header + data + one directory slot (2B) */
    uint16_t total = RECORD_HEADER_SIZE + record_size + 2;
    if (page_free_space(page) < total) return MYDB_ERR_FULL;

    PageHeader hdr;
    page_read_header(page, &hdr);

    /* Where we will write the new record */
    uint16_t rec_offset  = hdr.free_offset;           /* start of 5B header */
    uint16_t data_offset = rec_offset + RECORD_HEADER_SIZE;  /* start of payload */

    /* Read predecessor's header to get its current next_offset */
    RecordHeader pred_hdr;
    rec_hdr_decode(page + predecessor - RECORD_HEADER_SIZE, &pred_hdr);
    uint16_t old_next = pred_hdr.next_offset;

    /* Build the new record header:
       - next_offset points to what predecessor used to point to
       - heap_no = num_records + 2  (0=infimum, 1=supremum, 2+ = user rows) */
    RecordHeader new_hdr;
    new_hdr.info_flags  = 0;
    new_hdr.heap_type   = (uint16_t)(((hdr.num_records + 2) << 3) | REC_ORDINARY);
    new_hdr.next_offset = old_next;
    rec_hdr_encode(&new_hdr, page + rec_offset);

    /* Write the record payload */
    memcpy(page + data_offset, record_data, record_size);

    /* Splice into the linked list: predecessor now points to us */
    pred_hdr.next_offset = data_offset;
    rec_hdr_encode(&pred_hdr, page + predecessor - RECORD_HEADER_SIZE);

    /* Add a page directory slot for this record */
    uint16_t new_slot = hdr.num_dir_slots;
    page_dir_set(page, new_slot, data_offset);

    /* Update the page header */
    hdr.free_offset   = data_offset + record_size;
    hdr.num_records++;
    hdr.num_dir_slots++;
    page_write_header(page, &hdr);

    *slot_no = new_slot;
    return MYDB_OK;
}

/* ------------------------------------------------------------------ */
/*  page_delete_record — lazy deletion                                */
/*                                                                    */
/*  Sets the deleted flag in the record header.                       */
/*  The record stays in the linked list and directory until compact() */
/* ------------------------------------------------------------------ */
int page_delete_record(uint8_t *page, uint16_t slot_no)
{
    if (!page) return MYDB_ERR;

    PageHeader hdr;
    page_read_header(page, &hdr);
    if (slot_no >= hdr.num_dir_slots) return MYDB_ERR_NOT_FOUND;

    uint16_t data_offset = page_dir_get(page, slot_no);
    uint16_t rec_offset  = data_offset - RECORD_HEADER_SIZE;

    RecordHeader rh;
    rec_hdr_decode(page + rec_offset, &rh);

    if (rh.info_flags & 0x01) return MYDB_ERR;  /* already deleted */

    rh.info_flags |= 0x01;  /* set deleted bit */
    rec_hdr_encode(&rh, page + rec_offset);

    hdr.num_records--;
    page_write_header(page, &hdr);
    return MYDB_OK;
}

/*
 * Compute the physical size of the record at data_offset `doff` by
 * scanning the directory for the nearest record that physically follows
 * doff (i.e., has the smallest data_offset > doff).
 *
 * This approach is correct regardless of whether records were inserted
 * in key order or not, because records are always appended physically
 * at free_offset even when they are spliced earlier in the linked list.
 */
static uint16_t record_phys_size(const uint8_t *page, uint16_t doff)
{
    PageHeader hdr;
    page_read_header(page, &hdr);

    /* Default end = free_offset (no physically later record exists) */
    uint16_t next_phys_hdr = hdr.free_offset;

    uint16_t n = hdr.num_dir_slots;
    for (uint16_t i = 0; i < n; i++) {
        uint16_t d = page_dir_get(page, i);
        if (d > doff) {
            /* d - RECORD_HEADER_SIZE is where this neighbour's header starts,
               which is also where the current record's data ends. */
            uint16_t hdr_start = d - RECORD_HEADER_SIZE;
            if (hdr_start < next_phys_hdr)
                next_phys_hdr = hdr_start;
        }
    }

    return (next_phys_hdr > doff) ? (uint16_t)(next_phys_hdr - doff) : 0;
}

/* ------------------------------------------------------------------ */
/*  page_get_record                                                   */
/* ------------------------------------------------------------------ */
int page_get_record(const uint8_t *page, uint16_t slot_no,
                    uint16_t *data_offset, uint16_t *data_size)
{
    if (!page || !data_offset || !data_size) return MYDB_ERR;

    PageHeader hdr;
    page_read_header(page, &hdr);
    if (slot_no >= hdr.num_dir_slots) return MYDB_ERR_NOT_FOUND;

    uint16_t doff = page_dir_get(page, slot_no);

    RecordHeader rh;
    rec_hdr_decode(page + doff - RECORD_HEADER_SIZE, &rh);
    if (rh.info_flags & 0x01) return MYDB_ERR_NOT_FOUND;  /* deleted */

    *data_offset = doff;
    *data_size   = record_phys_size(page, doff);
    return MYDB_OK;
}

/* ------------------------------------------------------------------ */
/*  page_compact                                                      */
/*                                                                    */
/*  Rebuilds the page by walking the linked list (Infimum → Supremum) */
/*  and copying only non-deleted records into a temp buffer.          */
/*  Then overwrites the original page.                                */
/* ------------------------------------------------------------------ */
int page_compact(uint8_t *page)
{
    if (!page) return MYDB_ERR;

    PageHeader hdr;
    page_read_header(page, &hdr);

    /* Work in a temporary buffer so we can rebuild cleanly */
    uint8_t tmp[PAGE_SIZE];
    page_init(tmp, hdr.page_no, (PageType)hdr.page_type);

    /*
     * Walk the original linked list from Infimum to Supremum,
     * re-inserting every non-deleted record into `tmp`.
     */
    uint16_t cur = INFIMUM_DATA;   /* start at infimum's data */
    RecordHeader cur_hdr;
    rec_hdr_decode(page + cur - RECORD_HEADER_SIZE, &cur_hdr);
    cur = cur_hdr.next_offset;     /* first real record (or supremum) */

    uint16_t slot;
    while (cur != SUPREMUM_DATA && cur != 0) {
        rec_hdr_decode(page + cur - RECORD_HEADER_SIZE, &cur_hdr);

        if (!(cur_hdr.info_flags & 0x01)) {
            /* Live record — figure out its size the same way page_get_record does */
            uint16_t next = cur_hdr.next_offset;
            PageHeader orig_hdr;
            page_read_header(page, &orig_hdr);

            uint16_t end;
            if (next == 0 || next == SUPREMUM_DATA)
                end = orig_hdr.free_offset;
            else
                end = next - RECORD_HEADER_SIZE;

            uint16_t sz = (end > cur) ? (end - cur) : 0;

            /* Insert at the end of the chain in the new page (predecessor = last record or infimum) */
            PageHeader tmp_hdr;
            page_read_header(tmp, &tmp_hdr);

            /* Find the current last record in tmp by walking its chain */
            uint16_t pred = INFIMUM_DATA;
            RecordHeader ph;
            rec_hdr_decode(tmp + INFIMUM_OFFSET, &ph);
            while (ph.next_offset != SUPREMUM_DATA && ph.next_offset != 0) {
                pred = ph.next_offset;
                rec_hdr_decode(tmp + pred - RECORD_HEADER_SIZE, &ph);
            }

            page_insert_record(tmp, page + cur, sz, pred, &slot);
        }

        cur = cur_hdr.next_offset;
    }

    /* Copy the compacted page back */
    memcpy(page, tmp, PAGE_SIZE);
    return MYDB_OK;
}
