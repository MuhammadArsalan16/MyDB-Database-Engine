#include "normal_wal/wal_types.h"
#include "checksum.h"

#include <string.h>

/*
 * WalRecordHeader layout: lsn[8] + prev_lsn[8] + total_len[4] + txn_id[8]
 * + table_id[4] + page_no[4] + rec_type[1] + reserved[3] + checksum[4]
 * = 44 bytes, checksum at offset 40. Body starts immediately after, at
 * offset 44 (WAL_RECORD_HEADER_SIZE).
 */
#define WAL_RECORD_HEADER_CHECKSUM_OFFSET 40

void wal_record_header_serialize(WalRecordHeader *hdr, const void *body,
                                  size_t body_len, uint8_t *buf)
{
    memcpy(buf + 0,  &hdr->lsn,       8);
    memcpy(buf + 8,  &hdr->prev_lsn,  8);
    memcpy(buf + 16, &hdr->total_len, 4);
    memcpy(buf + 20, &hdr->txn_id,    8);
    memcpy(buf + 28, &hdr->table_id,  4);
    memcpy(buf + 32, &hdr->page_no,   4);
    buf[36] = hdr->rec_type;
    memcpy(buf + 37, hdr->reserved,   3);
    memcpy(buf + WAL_RECORD_HEADER_SIZE, body, body_len);

    /* checksum covers header-minus-checksum (bytes 0..40) + body — not
     * contiguous in the final layout (checksum sits at 40..44, body at
     * 44 onward), so hash a scratch buffer combining the two spans. */
    uint8_t scratch[WAL_RECORD_HEADER_CHECKSUM_OFFSET + WAL_MAX_ROW_BODY];
    memcpy(scratch, buf, WAL_RECORD_HEADER_CHECKSUM_OFFSET);
    memcpy(scratch + WAL_RECORD_HEADER_CHECKSUM_OFFSET, body, body_len);
    uint32_t cs = crc32(scratch, WAL_RECORD_HEADER_CHECKSUM_OFFSET + body_len);

    memcpy(buf + WAL_RECORD_HEADER_CHECKSUM_OFFSET, &cs, 4);
    hdr->checksum = cs;
}

int wal_record_header_deserialize(const uint8_t *buf, size_t body_len,
                                   WalRecordHeader *out)
{
    uint32_t stored;
    memcpy(&stored, buf + WAL_RECORD_HEADER_CHECKSUM_OFFSET, 4);

    uint8_t scratch[WAL_RECORD_HEADER_CHECKSUM_OFFSET + WAL_MAX_ROW_BODY];
    memcpy(scratch, buf, WAL_RECORD_HEADER_CHECKSUM_OFFSET);
    memcpy(scratch + WAL_RECORD_HEADER_CHECKSUM_OFFSET, buf + WAL_RECORD_HEADER_SIZE, body_len);
    if (stored != crc32(scratch, WAL_RECORD_HEADER_CHECKSUM_OFFSET + body_len))
        return MYDB_ERR_BAD_CHECKSUM;

    memcpy(&out->lsn,       buf + 0,  8);
    memcpy(&out->prev_lsn,  buf + 8,  8);
    memcpy(&out->total_len, buf + 16, 4);
    memcpy(&out->txn_id,    buf + 20, 8);
    memcpy(&out->table_id,  buf + 28, 4);
    memcpy(&out->page_no,   buf + 32, 4);
    out->rec_type = buf[36];
    memcpy(out->reserved,   buf + 37, 3);
    out->checksum = stored;
    return MYDB_OK;
}
