#ifndef WAL_PAGE_H
#define WAL_PAGE_H

#include <stdint.h>
#include "common.h"
#include "file_header.h"

/*
 * wal_page.h — WAL page header format (MYDB_WAL_DESIGN.md §8.2,
 * MYDB_WAL_IMPLEMENTATION.md §8.3), shared by BOTH subsystems: it frames
 * normal_wal's 4KB segment pages / ring-buffer frames and large_wal's
 * 16KB content pages alike, which is why it lives at wal/ root rather
 * than under normal_wal/. The only thing that differs between the two is
 * the file_type stamped into id (FILETYPE_WAL_PAGE vs
 * FILETYPE_LARGE_WAL_PAGE) — see large_wal/large_wal_page.h for that
 * side's thin serialize/deserialize pair.
 *
 * WalPageHeader is a plain struct (not __attribute__((packed))) — the
 * on-disk byte layout is produced/consumed field-by-field in wal_page.c
 * via explicit offsets, not via this struct's in-memory layout, matching
 * the rest of the codebase's convention.
 */

/* ------------------------------------------------------------------
 * Page flags. Both describe how this page's content relates to record
 * boundaries, which only large_wal can currently violate: normal_wal's
 * records never span pages (wal_ring_buffer_append's no-spanning rule
 * closes a frame rather than splitting a record), so it never sets
 * either bit.
 * ------------------------------------------------------------------ */
#define WAL_PAGE_FLAG_INCOMING_CONTINUATION 0x01   /* this page's content starts
     mid-record: its first bytes continue a record whose header appeared on an
     earlier page */
#define WAL_PAGE_FLAG_OUTGOING_CONTINUATION 0x02   /* the last record starting on
     this page does not finish here; it continues onto the next page */

/* ------------------------------------------------------------------
 * WalPageHeader — on-disk shape is 32 bytes (was 36 — see the field-by-
 * field notes below). Leads every 4KB WAL segment page (and is the ring
 * buffer's frame header too — ring buffer frames are byte-identical to
 * segment pages).
 *
 * end_lsn is an implementation-time addition beyond the design docs'
 * original field list (which only had page_lsn — the FIRST record's
 * LSN). It exists so a segment's own end_lsn (WalSegmentHeader.end_lsn)
 * can be determined reliably when a segment auto-finalizes: wal_segment_
 * pool_write() never parses record content, so start_lsn alone (the
 * first record, not the last) isn't enough — reading this field back
 * from the last page written is the only way to get the true highest
 * LSN in a segment without wal understanding record boundaries. The
 * Flusher maintains it exactly like start_lsn, updating it each time it
 * appends more (higher-LSN) content into a page across possibly several
 * flush cycles.
 *
 * Renamed page_lsn -> start_lsn, and flags/reserved shrunk (flags from
 * uint16_t to uint8_t, reserved from uint8_t[4] to uint8_t), to unify
 * this struct's shape with large_wal's own page header — the two are
 * being brought into byte-for-byte field-layout agreement across a
 * multi-phase redesign (large_wal's own header is untouched so far;
 * only this struct's shape and normal_wal's own call sites change in
 * this pass). flags has no bits defined/used yet by normal_wal; reserved
 * is kept for large_wal's future continuation-tracking use once it
 * adopts this shared layout.
 * ------------------------------------------------------------------ */
typedef struct {
    FileHeaderId id;            /* magic, version, file_type — 8 bytes    */
    uint64_t     start_lsn;     /* LSN of first record in this page       */
    uint64_t     end_lsn;       /* LSN of last record in this page        */
    uint16_t     data_len;      /* exact bytes of valid record data       */
    uint8_t      flags;
    uint8_t      reserved;
    uint32_t     checksum;      /* CRC32 over the preceding 28 bytes      */
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
