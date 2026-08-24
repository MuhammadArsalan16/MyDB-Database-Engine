#ifndef WAL_PAGE_H
#define WAL_PAGE_H

#include <stdint.h>
#include "common.h"
#include "file_header.h"

/*
 * wal_page.h — WAL page header format (MYDB_WAL_DESIGN.md §8.2,
 * MYDB_WAL_IMPLEMENTATION.md §8.3). wal/'s own struct — see wal_types.h
 * for why (system-wide constants/enums live in common.h; a subsystem's
 * own record/container layout does not).
 *
 * WalPageHeader is a plain struct (not __attribute__((packed))) — see
 * wal_types.h's header comment for why: the on-disk byte layout is
 * produced/consumed field-by-field in wal_page.c via explicit offsets,
 * not via this struct's in-memory layout, matching the rest of the
 * codebase's convention.
 *
 * This phase (WAL Phase 1) only covers serialize/deserialize + checksum
 * — no ring buffer / Flusher wiring yet.
 */

/* ------------------------------------------------------------------
 * WalPageHeader — on-disk shape is 36 bytes. Leads every 4KB WAL segment
 * page (and is the ring buffer's frame header too — ring buffer frames
 * are byte-identical to segment pages).
 *
 * end_lsn is an implementation-time addition beyond the design docs'
 * original field list (which only had page_lsn — the FIRST record's
 * LSN). It exists so a segment's own end_lsn (WalSegmentHeader.end_lsn)
 * can be determined reliably when a segment auto-finalizes: wal_segment_
 * pool_write() never parses record content, so page_lsn alone (the
 * first record, not the last) isn't enough — reading this field back
 * from the last page written is the only way to get the true highest
 * LSN in a segment without wal understanding record boundaries. The
 * Flusher maintains it exactly like page_lsn, updating it each time it
 * appends more (higher-LSN) content into a page across possibly several
 * flush cycles. Added by shrinking reserved from 12 to 4 bytes — total
 * struct size, and WAL_PAGE_HEADER_CHECKSUM_OFFSET (checksum.c), are
 * both unchanged.
 * ------------------------------------------------------------------ */
typedef struct {
    FileHeaderId id;            /* magic, version, file_type — 8 bytes    */
    uint64_t     page_lsn;      /* LSN of first record in this page       */
    uint16_t     data_len;      /* exact bytes of valid record data       */
    uint16_t     flags;
    uint64_t     end_lsn;       /* LSN of last record in this page        */
    uint8_t      reserved[4];
    uint32_t     checksum;      /* CRC32 over the preceding 32 bytes      */
} WalPageHeader;

/* ------------------------------------------------------------------
 * Serialize/deserialize + checksum.
 *
 * wal_page_header_serialize fills buf[0..WAL_PAGE_HEADER_SIZE) from *hdr
 * and stamps buf's checksum field (CRC32 over the bytes preceding it).
 * Caller sets every field of *hdr except checksum first.
 *
 * wal_page_header_deserialize validates the FileHeaderId (magic/version/
 * file_type) via file_header_check_id, then the checksum, before filling
 * *out. Returns MYDB_OK, or MYDB_ERR_BAD_MAGIC/BAD_VERSION/BAD_FILE_TYPE/
 * BAD_CHECKSUM.
 * ------------------------------------------------------------------ */
void wal_page_header_serialize(const WalPageHeader *hdr, uint8_t *buf);
int  wal_page_header_deserialize(const uint8_t *buf, WalPageHeader *out);

#endif /* WAL_PAGE_H */
