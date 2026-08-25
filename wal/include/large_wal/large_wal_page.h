#ifndef LARGE_WAL_PAGE_H
#define LARGE_WAL_PAGE_H

#include <stdint.h>
#include "common.h"
#include "file_header.h"
#include "wal_page.h"

/*
 * large_wal_page.h — LARGE_WAL's side of the page header format.
 *
 * There is no LargeWalPageHeader struct any more: LARGE_WAL's 16KB
 * content pages and normal_wal's 4KB segment pages now share one
 * on-disk shape, WalPageHeader (wal_page.h), byte for byte. The only
 * difference is the file_type stamped into id — FILETYPE_LARGE_WAL_PAGE
 * here vs FILETYPE_WAL_PAGE there — which is exactly the split
 * wal_segment.h / large_wal_segment.h already established for the
 * segment header: same shape, own file_type, own module.
 *
 * What the old struct dropped, and why:
 *   - page_index: a page's position within one multi-page record. Gone
 *     because a page can hold parts of more than one record, so "which
 *     slice of which record is this" is no longer a single number the
 *     page itself can carry. LargeWalIndexEntry (start_page_no/offset/
 *     page_count) locates a record's bytes instead.
 *   - record_type: already carried twice over — by the WalRecordHeader
 *     embedded in the page's own content, and by
 *     LargeWalIndexEntry.rec_type.
 *   - content_lsn (one LSN per page): replaced by the shared header's
 *     start_lsn/end_lsn pair, which can describe a page holding several
 *     records with different LSNs.
 *
 * Multi-record/continuation state lives in WalPageHeader.flags —
 * WAL_PAGE_FLAG_INCOMING_CONTINUATION / _OUTGOING_CONTINUATION
 * (wal_page.h), replacing the old single LARGE_WAL_PAGE_FLAG_CONTINUATION.
 *
 * What lives inside the data area these pages carry: no new record
 * type. A large record is still a WalRecordHeader (44 bytes,
 * wal_types.c) — that's exactly why the design doc's LargeWalFooter was
 * found redundant and dropped (impl doc §10.4): total_len is self-
 * terminating, checksum is self-validating. This page format is only
 * the *container* — reassembling a multi-page record's content back
 * into one contiguous buffer is large_wal_get()'s job (large_wal_api.c),
 * not this layer's.
 */

#define LARGE_WAL_PAGE_HEADER_ON_DISK_SIZE  32   /* same 32 bytes WalPageHeader
     occupies for normal_wal; named separately here (matching
     LARGE_WAL_SEGMENT_HEADER_ON_DISK_SIZE, large_wal_segment.h) as the footprint
     a caller skips past to reach a LARGE_WAL page's content bytes. */

/* ------------------------------------------------------------------
 * Serialize/deserialize + checksum — identical contract to
 * wal_page_header_serialize/deserialize (wal_page.h), just stamping and
 * checking FILETYPE_LARGE_WAL_PAGE.
 *
 * serialize is a straight delegation: wal_page_header_serialize already
 * takes the file_type from hdr->id.file_type, so it is format-agnostic
 * as written. deserialize needs its own body only because it must check
 * against FILETYPE_LARGE_WAL_PAGE rather than FILETYPE_WAL_PAGE.
 * ------------------------------------------------------------------ */
void large_wal_page_header_serialize(const WalPageHeader *hdr, uint8_t *buf);
int  large_wal_page_header_deserialize(const uint8_t *buf, WalPageHeader *out);

#endif /* LARGE_WAL_PAGE_H */
