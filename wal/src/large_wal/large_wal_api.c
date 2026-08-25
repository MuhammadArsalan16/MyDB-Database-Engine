#include "large_wal/large_wal_api.h"
#include "large_wal/large_wal_page.h"
#include "large_wal/large_wal_buffer.h"   /* LARGE_WAL_PAGE_USABLE */

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

    /* Walk exactly this record's bytes, starting at its own offset
     * within start_page_no. A page's data_len counts every valid byte on
     * it — which, now that records pack tightly, can include bytes
     * belonging to neighbouring records — so it validates the page but
     * never bounds this record's slice. total_size does that. */
    uint32_t written   = 0;
    uint32_t remaining = e.total_size;
    uint32_t page_no   = e.start_page_no;
    uint32_t pos       = e.offset;

    while (remaining > 0) {
        uint8_t page_buf[PAGE_SIZE];
        if (pread_all(fd, page_buf, PAGE_SIZE, (off_t)page_no * PAGE_SIZE) != MYDB_OK)
            return MYDB_ERR;

        WalPageHeader hdr;
        if (large_wal_page_header_deserialize(page_buf, &hdr) != MYDB_OK) return MYDB_ERR;

        uint32_t room  = LARGE_WAL_PAGE_USABLE - pos;
        uint32_t chunk = (remaining < room) ? remaining : room;

        memcpy(out_buf + written,
               page_buf + LARGE_WAL_PAGE_HEADER_ON_DISK_SIZE + pos, chunk);

        written   += chunk;
        remaining -= chunk;
        page_no++;
        pos = 0;
    }

    *out_len = written;
    return MYDB_OK;
}

int large_wal_write(LargeWalManager *mgr, const uint8_t *content, uint32_t total_size,
                     LargeWalIndexEntry *out_entries, uint32_t out_cap, uint32_t *out_count)
{
    if (!mgr) return MYDB_ERR;
    return large_wal_writer_submit(&mgr->lw_writer, content, total_size,
                                    out_entries, out_cap, out_count);
}
