#include "large_wal/large_wal_api.h"
#include "large_wal/large_wal_page.h"

#include <unistd.h>
#include <string.h>

static int pread_all(int fd, void *buf, size_t n, off_t offset)
{
    ssize_t got = pread(fd, buf, n, offset);
    return (got == (ssize_t)n) ? MYDB_OK : MYDB_ERR;
}

int large_wal_get(LargeWalManager *mgr, uint64_t content_lsn,
                   uint8_t *out_buf, uint32_t *out_len)
{
    if (!mgr || !out_buf || !out_len) return MYDB_ERR;

    LargeWalIndexEntry e;
    if (large_wal_index_lookup(&mgr->lw_idx, content_lsn, &e) != MYDB_OK)
        return MYDB_ERR_NOT_FOUND;

    int fd;
    if (large_wal_registry_lookup(&mgr->lw_registry, e.segment_no, &fd) != MYDB_OK)
        return MYDB_ERR_NOT_FOUND;

    uint32_t written = 0;
    for (uint8_t p = 0; p < e.page_count; p++) {
        uint32_t page_no = e.start_page_no + p;
        uint8_t  page_buf[PAGE_SIZE];
        off_t    off = (off_t)page_no * PAGE_SIZE;

        if (pread_all(fd, page_buf, PAGE_SIZE, off) != MYDB_OK) return MYDB_ERR;

        LargeWalPageHeader hdr;
        if (large_wal_page_header_deserialize(page_buf, &hdr) != MYDB_OK) return MYDB_ERR;

        memcpy(out_buf + written, page_buf + LARGE_WAL_PAGE_HEADER_ON_DISK_SIZE, hdr.data_len);
        written += hdr.data_len;
    }

    *out_len = written;
    return MYDB_OK;
}

int large_wal_write(LargeWalManager *mgr, const uint8_t *content, uint32_t total_size,
                     uint64_t content_lsn, uint8_t rec_type, LargeWalIndexEntry *out_entry)
{
    if (!mgr) return MYDB_ERR;
    return large_wal_writer_submit(&mgr->lw_writer, content, total_size, content_lsn, rec_type, out_entry);
}
