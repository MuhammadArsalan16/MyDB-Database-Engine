#include "large_wal/large_wal_archiver.h"
#include "large_wal/large_wal_page.h"
#include "common.h"

#include <fcntl.h>
#include <unistd.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>

static int pwrite_all(int fd, const void *buf, size_t n, off_t offset)
{
    ssize_t written = pwrite(fd, buf, n, offset);
    return (written == (ssize_t)n) ? MYDB_OK : MYDB_ERR;
}

static int pread_all(int fd, void *buf, size_t n, off_t offset)
{
    ssize_t got = pread(fd, buf, n, offset);
    return (got == (ssize_t)n) ? MYDB_OK : MYDB_ERR;
}

static void archival_path(const LargeWalArchiver *arc, uint64_t segment_no, char *out, size_t out_len)
{
    snprintf(out, out_len, "%s/large_wal_archival_%llu.mydb",
             arc->wal_dir, (unsigned long long)segment_no);
}

/* ------------------------------------------------------------------
 * Lifecycle
 * ------------------------------------------------------------------ */

int large_wal_archiver_init(LargeWalArchiver *arc, const char *wal_dir)
{
    if (!arc || !wal_dir) return MYDB_ERR;
    memset(arc, 0, sizeof(*arc));
    snprintf(arc->wal_dir, sizeof(arc->wal_dir), "%s", wal_dir);
    return MYDB_OK;
}

int large_wal_archiver_shutdown(LargeWalArchiver *arc)
{
    if (!arc) return MYDB_ERR;
    for (uint32_t i = 0; i < arc->count; i++) {
        if (arc->entries[i].fd >= 0) close(arc->entries[i].fd);
    }
    free(arc->entries);
    arc->entries  = NULL;
    arc->count    = 0;
    arc->capacity = 0;
    return MYDB_OK;
}

/* ------------------------------------------------------------------
 * Copy-out: rotation pool -> holding area
 * ------------------------------------------------------------------ */

int large_wal_archiver_copy_out(LargeWalArchiver *arc, LargeWalSegmentPool *pool, uint32_t slot_index)
{
    if (!arc || !pool || slot_index >= LARGE_WAL_SEGMENT_POOL_SLOTS) return MYDB_ERR;

    LargeWalSlot *slot = &pool->slots[slot_index];
    if (slot->header.state != LSEG_DONE) return MYDB_ERR;

    uint64_t segment_no = slot->header.segment_no;

    uint8_t *buf = malloc(LARGE_WAL_SEGMENT_FILE_SIZE);
    if (!buf) return MYDB_ERR;

    if (large_wal_segment_pool_read_segment(pool, slot_index, buf) != MYDB_OK) {
        free(buf);
        return MYDB_ERR;
    }

    /* Byte-identical holding-area copy except state -> LSEG_ARCHIVING
     * (impl doc §10.1). Patched via the real serialize/deserialize pair
     * rather than a raw byte poke, so the trailing checksum stays valid. */
    LargeWalSegmentHeader hdr;
    if (large_wal_segment_header_deserialize(buf, &hdr) != MYDB_OK) {
        free(buf);
        return MYDB_ERR;
    }
    hdr.state = LSEG_ARCHIVING;
    large_wal_segment_header_serialize(&hdr, buf);

    char path[300];
    archival_path(arc, segment_no, path, sizeof(path));

    int fd = open(path, O_RDWR | O_CREAT | O_EXCL, 0644);
    if (fd < 0) {
        free(buf);
        return MYDB_ERR;
    }

    if (pwrite_all(fd, buf, LARGE_WAL_SEGMENT_FILE_SIZE, 0) != MYDB_OK ||
        fsync(fd) < 0) {
        free(buf);
        close(fd);
        unlink(path);
        return MYDB_ERR;
    }
    free(buf);

    /* Ordering rule (impl doc §10.1): the rotation slot is freed only
     * after the holding-area copy's fsync above has confirmed — never
     * before, or segment_no could transiently exist validly in two
     * places. */
    if (large_wal_segment_pool_free_slot(pool, slot_index) != MYDB_OK) {
        close(fd);
        return MYDB_ERR;
    }

    if (arc->count == arc->capacity) {
        uint32_t new_cap = arc->capacity ? arc->capacity * 2 : 8;
        LargeWalFdEntry *ne = realloc(arc->entries, (size_t)new_cap * sizeof(LargeWalFdEntry));
        if (!ne) {
            close(fd);
            return MYDB_ERR;
        }
        arc->entries  = ne;
        arc->capacity = new_cap;
    }
    arc->entries[arc->count].segment_no = segment_no;
    arc->entries[arc->count].fd         = fd;
    arc->count++;

    return MYDB_OK;
}

/* ------------------------------------------------------------------
 * Lookup + reassembly
 * ------------------------------------------------------------------ */

int large_wal_get(LargeWalArchiver *arc, const LargeWalIndex *idx,
                   uint64_t content_lsn, uint8_t *out_buf, uint32_t *out_len)
{
    if (!arc || !idx || !out_buf || !out_len) return MYDB_ERR;

    LargeWalIndexEntry e;
    if (large_wal_index_lookup(idx, content_lsn, &e) != MYDB_OK)
        return MYDB_ERR_NOT_FOUND;

    int fd = -1;
    for (uint32_t i = 0; i < arc->count; i++) {
        if (arc->entries[i].segment_no == e.segment_no) {
            fd = arc->entries[i].fd;
            break;
        }
    }
    /* Not (yet) copied out — see the header's scope note. */
    if (fd < 0) return MYDB_ERR_NOT_FOUND;

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

/* ------------------------------------------------------------------
 * Freeing — Gate A + Gate B, both caller-supplied
 * ------------------------------------------------------------------ */

int large_wal_archiver_try_free(LargeWalArchiver *arc, LargeWalIndex *idx,
                                 uint64_t segment_no, uint64_t segment_end_lsn,
                                 uint64_t checkpoint_lsn, int gate_b_cleared,
                                 int *out_freed)
{
    if (!arc || !idx || !out_freed) return MYDB_ERR;
    *out_freed = 0;

    if (!(checkpoint_lsn > segment_end_lsn) || !gate_b_cleared)
        return MYDB_OK;

    int found = -1;
    for (uint32_t i = 0; i < arc->count; i++) {
        if (arc->entries[i].segment_no == segment_no) {
            found = (int)i;
            break;
        }
    }
    if (found < 0) return MYDB_OK;   /* nothing to free */

    close(arc->entries[found].fd);

    char path[300];
    archival_path(arc, segment_no, path, sizeof(path));
    unlink(path);

    /* Remove from the table — swap-with-last; order doesn't matter for a
     * linear-scan table. */
    arc->entries[found] = arc->entries[arc->count - 1];
    arc->count--;

    if (large_wal_index_delete_by_segment(idx, segment_no) != MYDB_OK) return MYDB_ERR;

    *out_freed = 1;
    return MYDB_OK;
}
