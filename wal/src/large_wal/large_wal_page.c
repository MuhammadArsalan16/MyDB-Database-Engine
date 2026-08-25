#include "large_wal/large_wal_page.h"
#include "checksum.h"

#include <string.h>

/*
 * Field-by-field memcpy throughout — same reasoning normal_wal's
 * wal_page.c already documents: avoids alignment assumptions about
 * caller-provided buffers.
 *
 * Caller sets every field of *hdr except checksum before calling
 * large_wal_page_header_serialize(); id.magic/id.version are
 * overwritten internally via file_header_write_id() regardless of what
 * the caller put there (only id.file_type is actually read from *hdr).
 */

/* LargeWalPageHeader — 32 bytes. Checksum covers the 28 bytes preceding
 * it (id[8] + content_lsn[8] + record_type[1] + page_index[1] +
 * data_len[2] + flags[2] + reserved[6]). */
#define LARGE_WAL_PAGE_HEADER_CHECKSUM_OFFSET 28

void large_wal_page_header_serialize(const LargeWalPageHeader *hdr, uint8_t *buf)
{
    file_header_write_id(buf, hdr->id.file_type);
    memcpy(buf + 8,  &hdr->content_lsn, 8);
    buf[16] = hdr->record_type;
    buf[17] = hdr->page_index;
    memcpy(buf + 18, &hdr->data_len, 2);
    memcpy(buf + 20, &hdr->flags,    2);
    memcpy(buf + 22, hdr->reserved,  6);

    uint32_t cs = crc32(buf, LARGE_WAL_PAGE_HEADER_CHECKSUM_OFFSET);
    memcpy(buf + LARGE_WAL_PAGE_HEADER_CHECKSUM_OFFSET, &cs, 4);
}

int large_wal_page_header_deserialize(const uint8_t *buf, LargeWalPageHeader *out)
{
    int rc = file_header_check_id(buf, FILETYPE_LARGE_WAL_PAGE);
    if (rc != MYDB_OK) return rc;

    uint32_t stored;
    memcpy(&stored, buf + LARGE_WAL_PAGE_HEADER_CHECKSUM_OFFSET, 4);
    if (stored != crc32(buf, LARGE_WAL_PAGE_HEADER_CHECKSUM_OFFSET))
        return MYDB_ERR_BAD_CHECKSUM;

    file_header_read_id(buf, &out->id);
    memcpy(&out->content_lsn, buf + 8,  8);
    out->record_type = buf[16];
    out->page_index  = buf[17];
    memcpy(&out->data_len, buf + 18, 2);
    memcpy(&out->flags,    buf + 20, 2);
    memcpy(out->reserved,  buf + 22, 6);
    out->checksum = stored;
    return MYDB_OK;
}
