#include "normal_wal/wal_page.h"
#include "checksum.h"

#include <string.h>

/*
 * Field-by-field memcpy throughout, not a whole-struct memcpy through a
 * cast buf pointer — same reasoning file_header.c already documents:
 * avoids alignment assumptions about caller-provided buffers.
 *
 * Caller sets every field of *hdr except checksum before calling
 * wal_page_header_serialize(); id.magic/id.version are overwritten
 * internally via file_header_write_id() regardless of what the caller
 * put there (only id.file_type is actually read from *hdr) — the same
 * contract every other on-disk struct in this codebase follows.
 */

/* WalPageHeader — 36 bytes. Checksum covers the 32 bytes preceding it
 * (id[8] + page_lsn[8] + data_len[2] + flags[2] + end_lsn[8] +
 * reserved[4]). */
#define WAL_PAGE_HEADER_CHECKSUM_OFFSET 32

void wal_page_header_serialize(const WalPageHeader *hdr, uint8_t *buf)
{
    file_header_write_id(buf, hdr->id.file_type);
    memcpy(buf + 8,  &hdr->page_lsn, 8);
    memcpy(buf + 16, &hdr->data_len, 2);
    memcpy(buf + 18, &hdr->flags,    2);
    memcpy(buf + 20, &hdr->end_lsn,  8);
    memcpy(buf + 28, hdr->reserved,  4);

    uint32_t cs = crc32(buf, WAL_PAGE_HEADER_CHECKSUM_OFFSET);
    memcpy(buf + WAL_PAGE_HEADER_CHECKSUM_OFFSET, &cs, 4);
}

int wal_page_header_deserialize(const uint8_t *buf, WalPageHeader *out)
{
    int rc = file_header_check_id(buf, FILETYPE_WAL_PAGE);
    if (rc != MYDB_OK) return rc;

    uint32_t stored;
    memcpy(&stored, buf + WAL_PAGE_HEADER_CHECKSUM_OFFSET, 4);
    if (stored != crc32(buf, WAL_PAGE_HEADER_CHECKSUM_OFFSET))
        return MYDB_ERR_BAD_CHECKSUM;

    file_header_read_id(buf, &out->id);
    memcpy(&out->page_lsn, buf + 8,  8);
    memcpy(&out->data_len, buf + 16, 2);
    memcpy(&out->flags,    buf + 18, 2);
    memcpy(&out->end_lsn,  buf + 20, 8);
    memcpy(out->reserved,  buf + 28, 4);
    out->checksum = stored;
    return MYDB_OK;
}
