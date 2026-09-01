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

    /* The index lookup copies the entry out and drops idx->lock before
     * the registry is touched, because e.segment_no is what names the
     * node to lock — the two locks are held in sequence, never together.
     * That is also what keeps this from closing a cycle against
     * try_free, which holds registry then index in that same order.
     *
     * If try_free slips into the gap, the acquire below simply misses
     * and this returns MYDB_ERR_NOT_FOUND — the honest answer, since the
     * record really has been freed. */
    LargeWalIndexEntry e;
    if (large_wal_index_lookup(&mgr->lw_idx, content_lsn, &e) != MYDB_OK)
        return MYDB_ERR_NOT_FOUND;

    /* The node lock is held across the WHOLE pread loop below, not just
     * the resolve: that is what stops copy_out relocating this segment,
     * free_slot zeroing the rotation slot, or the writer rewriting a
     * partially filled tail page, midway through reassembling a record
     * that spans several pages. */
    LargeWalRegistryNode *node = NULL;
    int                   fd   = -1;
    if (large_wal_registry_acquire(&mgr->lw_registry, e.segment_no, &node, &fd) != MYDB_OK)
        return MYDB_ERR_NOT_FOUND;

    /* Single exit through `done:` from here on — every path owes the
     * registry a release(), and skipping one would leave the list read
     * lock held forever, wedging every future register()/remove(). */
    int rc = MYDB_OK;

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
        if (pread_all(fd, page_buf, PAGE_SIZE, (off_t)page_no * PAGE_SIZE) != MYDB_OK) {
            rc = MYDB_ERR;
            goto done;
        }

        WalPageHeader hdr;
        if (large_wal_page_header_deserialize(page_buf, &hdr) != MYDB_OK) {
            rc = MYDB_ERR;
            goto done;
        }

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

done:
    large_wal_registry_release(&mgr->lw_registry, node);
    return rc;
}

int large_wal_write(LargeWalManager *mgr, const uint8_t *content, uint32_t total_size,
                     LargeWalIndexEntry *out_entries, uint32_t out_cap, uint32_t *out_count)
{
    if (!mgr) return MYDB_ERR;
    return large_wal_writer_submit(&mgr->lw_writer, content, total_size,
                                    out_entries, out_cap, out_count);
}
