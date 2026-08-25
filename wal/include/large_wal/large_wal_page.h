#ifndef LARGE_WAL_PAGE_H
#define LARGE_WAL_PAGE_H

#include <stdint.h>
#include "common.h"
#include "file_header.h"

/*
 * large_wal_page.h — LARGE_WAL page header format (MYDB_WAL_DESIGN.md
 * §10.2, corrected here — see below). Mirrors normal_wal/wal_page.h in
 * shape and discipline exactly: plain (not packed) struct, wire format
 * produced/consumed field-by-field in large_wal_page.c via explicit
 * offsets, not via this struct's in-memory layout.
 *
 * Correction from the design doc: §10.2 lists magic(4)+content_lsn(8)+
 * record_type(1)+page_index(1)+data_len(2)+flags(2)+reserved(2)+
 * checksum(4) = 24 bytes for a struct it claims is 32 bytes — the same
 * unexplained-gap bug MYDB_WAL_IMPLEMENTATION.md §8.3/§8.4 already found
 * and fixed for WalPageHeader/WalSegmentHeader, just never applied here.
 * Fixed the same way: bare magic -> FileHeaderId (+4 bytes, matching
 * every other on-disk struct in this codebase), reserved sized to
 * honestly close the gap at the doc's own originally-claimed 32-byte
 * total (8+8+1+1+2+2+6+4 = 32).
 *
 * What lives inside the data area these pages carry: no new record
 * type. A large record is still a WalRecordHeader (44 bytes,
 * wal_types.c) — that's exactly why the design doc's LargeWalFooter was
 * found redundant and dropped (impl doc §10.4): total_len is self-
 * terminating, checksum is self-validating. This page format is only
 * the *container* — reassembling a multi-page record's content back
 * into one contiguous buffer for wal_record_header_deserialize is a
 * later phase's job (the segment pool / writer), not this layer's.
 */

/* ------------------------------------------------------------------
 * LargeWalPageHeader — 32 bytes. Leads every 16KB LARGE_WAL content
 * page. page_index/CONTINUATION let a reader detect multi-page records
 * without needing anything from the (now-removed) footer.
 * ------------------------------------------------------------------ */
#define LARGE_WAL_PAGE_FLAG_CONTINUATION 0x0001   /* set on any page_index > 0 */

typedef struct {
    FileHeaderId id;
    uint64_t     content_lsn;   /* LSN of the large record this page belongs to */
    uint8_t      record_type;   /* WalRecType — the original record's own type */
    uint8_t      page_index;    /* position within this large record, 0-based */
    uint16_t     data_len;      /* exact bytes of valid content on this page */
    uint16_t     flags;         /* LARGE_WAL_PAGE_FLAG_* */
    uint8_t      reserved[6];
    uint32_t     checksum;      /* CRC32 over the preceding 28 bytes */
} LargeWalPageHeader;             /* 32 bytes */

#define LARGE_WAL_PAGE_HEADER_ON_DISK_SIZE  32   /* matches LargeWalSegmentHeader's own
     LARGE_WAL_SEGMENT_HEADER_ON_DISK_SIZE naming (large_wal_segment.h) — the on-disk
     footprint a caller needs to skip past to reach a page's content bytes. */

/* ------------------------------------------------------------------
 * Serialize/deserialize + checksum — same contract as
 * wal_page_header_serialize/deserialize (normal_wal/wal_page.h).
 *
 * large_wal_page_header_serialize fills buf[0..32) from *hdr and stamps
 * buf's checksum field. Caller sets every field of *hdr except checksum
 * first.
 *
 * large_wal_page_header_deserialize validates the FileHeaderId (magic/
 * version/file_type) via file_header_check_id, then the checksum,
 * before filling *out. Returns MYDB_OK, or MYDB_ERR_BAD_MAGIC/
 * BAD_VERSION/BAD_FILE_TYPE/BAD_CHECKSUM.
 * ------------------------------------------------------------------ */
void large_wal_page_header_serialize(const LargeWalPageHeader *hdr, uint8_t *buf);
int  large_wal_page_header_deserialize(const uint8_t *buf, LargeWalPageHeader *out);

#endif /* LARGE_WAL_PAGE_H */
