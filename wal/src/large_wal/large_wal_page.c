#include "large_wal/large_wal_page.h"
#include "checksum.h"

#include <string.h>

/*
 * LARGE_WAL pages carry the shared WalPageHeader (wal_page.h) — same 32
 * bytes, same field offsets as normal_wal's, differing only in the
 * file_type stamped into id. Checksum covers the 28 bytes preceding it
 * (id[8] + start_lsn[8] + end_lsn[8] + data_len[2] + flags[1] +
 * reserved[1]).
 */
#define LARGE_WAL_PAGE_HEADER_CHECKSUM_OFFSET 28

void large_wal_page_header_serialize(const WalPageHeader *hdr, uint8_t *buf)
{
    /* wal_page_header_serialize takes the file_type from hdr->id, so it
     * already writes a LARGE_WAL page header when the caller stamped
     * FILETYPE_LARGE_WAL_PAGE — nothing LARGE_WAL-specific to add. */
    wal_page_header_serialize(hdr, buf);
}

int large_wal_page_header_deserialize(const uint8_t *buf, WalPageHeader *out)
{
    int rc = file_header_check_id(buf, FILETYPE_LARGE_WAL_PAGE);
    if (rc != MYDB_OK) return rc;

    uint32_t stored;
    memcpy(&stored, buf + LARGE_WAL_PAGE_HEADER_CHECKSUM_OFFSET, 4);
    if (stored != crc32(buf, LARGE_WAL_PAGE_HEADER_CHECKSUM_OFFSET))
        return MYDB_ERR_BAD_CHECKSUM;

    file_header_read_id(buf, &out->id);
    memcpy(&out->start_lsn, buf + 8,  8);
    memcpy(&out->end_lsn,   buf + 16, 8);
    memcpy(&out->data_len,  buf + 24, 2);
    out->flags    = buf[26];
    out->reserved = buf[27];
    out->checksum = stored;
    return MYDB_OK;
}
