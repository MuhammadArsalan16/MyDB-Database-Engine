#include "large_wal/large_wal_segment_pool.h"
#include "large_wal/large_wal_page.h"
#include "common.h"

#include <fcntl.h>
#include <unistd.h>
#include <string.h>
#include <stdio.h>
#include <errno.h>
#include <sys/stat.h>
#include <sys/types.h>

/* ------------------------------------------------------------------
 * Internal helpers — same shape as normal_wal/wal_segment_pool.c's own
 * copies (each on-disk-I/O file in this codebase keeps its own static
 * copy rather than sharing one).
 * ------------------------------------------------------------------ */

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

/* mkdir, ignoring EEXIST. Returns 0 if the directory now exists. */
static int ensure_dir(const char *path)
{
    if (mkdir(path, 0755) == 0) return 0;
    if (errno == EEXIST) {
        struct stat st;
        if (stat(path, &st) == 0 && S_ISDIR(st.st_mode)) return 0;
    }
    return -1;
}

static int page_no_valid(uint32_t page_no)
{
    return page_no >= 1 && page_no < LARGE_WAL_SEGMENT_PAGES_PER_FILE;
}

/* ------------------------------------------------------------------
 * Per-slot open-or-create.
 *
 * A segment file's page-0 slot is a full PAGE_SIZE (16KB) page (not just
 * LARGE_WAL_SEGMENT_HEADER_ON_DISK_SIZE bytes) — 64 meaningful bytes
 * followed by zero padding to fill the page, same "page N starts at
 * N * PAGE_SIZE" convention normal_wal already follows (proportionally
 * more wasteful here than normal_wal's 4KB pages, kept consistent
 * anyway). Real content pages therefore start at page_no = 1.
 * ------------------------------------------------------------------ */
static int open_existing_or_create_slot(LargeWalSegmentPool *pool, uint32_t slot_no)
{
    char path[300];
    snprintf(path, sizeof(path), "%s/large_wal_%u.mydb", pool->wal_dir, slot_no);

    LargeWalSlot *slot = &pool->slots[slot_no];
    slot->slot_no = (uint8_t)slot_no;
    slot->fd = -1;

    int fd = open(path, O_RDWR);
    if (fd >= 0) {
        /* Existing segment file from a prior run — reload its header. */
        uint8_t page_buf[PAGE_SIZE];
        if (pread_all(fd, page_buf, PAGE_SIZE, 0) != MYDB_OK) {
            close(fd);
            return MYDB_ERR;
        }
        int rc = large_wal_segment_header_deserialize(page_buf, &slot->header);
        if (rc != MYDB_OK) {
            close(fd);
            return rc;
        }
        slot->fd = fd;
        return MYDB_OK;
    }
    if (errno != ENOENT) return MYDB_ERR;

    /* Doesn't exist yet — create, pre-allocate, write an initial
     * LSEG_FREE header. posix_fallocate() — same portable-POSIX
     * substitution for the design doc's "via fallocate()" normal_wal's
     * pool already uses. */
    fd = open(path, O_RDWR | O_CREAT | O_EXCL, 0644);
    if (fd < 0) return MYDB_ERR;

    if (posix_fallocate(fd, 0, LARGE_WAL_SEGMENT_FILE_SIZE) != 0) {
        close(fd);
        return MYDB_ERR;
    }

    LargeWalSegmentHeader hdr;
    memset(&hdr, 0, sizeof(hdr));
    hdr.id.file_type = FILETYPE_LARGE_WAL_SEGMENT;
    hdr.segment_no   = 0;   /* placeholder — the real value is stamped at claim time */
    hdr.partition_id = pool->partition_id;
    hdr.state        = LSEG_FREE;

    uint8_t page_buf[PAGE_SIZE];
    memset(page_buf, 0, PAGE_SIZE);
    large_wal_segment_header_serialize(&hdr, page_buf);

    if (pwrite_all(fd, page_buf, PAGE_SIZE, 0) != MYDB_OK) {
        close(fd);
        return MYDB_ERR;
    }
    if (fsync(fd) < 0) {
        close(fd);
        return MYDB_ERR;
    }

    /* Read back rather than trust the local hdr — picks up the real
     * checksum large_wal_segment_header_serialize stamped into page_buf. */
    large_wal_segment_header_deserialize(page_buf, &slot->header);
    slot->fd = fd;
    return MYDB_OK;
}

/* ------------------------------------------------------------------
 * Lifecycle
 * ------------------------------------------------------------------ */

int large_wal_segment_pool_init(LargeWalSegmentPool *pool, const char *wal_dir, uint32_t partition_id)
{
    if (!pool || !wal_dir) return MYDB_ERR;
    if (ensure_dir(wal_dir) != 0) return MYDB_ERR;

    memset(pool, 0, sizeof(*pool));
    snprintf(pool->wal_dir, sizeof(pool->wal_dir), "%s", wal_dir);
    pool->partition_id = partition_id;

    for (uint32_t i = 0; i < LARGE_WAL_SEGMENT_POOL_SLOTS; i++) {
        int rc = open_existing_or_create_slot(pool, i);
        if (rc != MYDB_OK) {
            /* Close whatever we already opened before bailing out. */
            for (uint32_t j = 0; j < i; j++) {
                if (pool->slots[j].fd >= 0) close(pool->slots[j].fd);
            }
            return rc;
        }
    }

    /* Any slot reloaded LSEG_ACTIVE has a stale on-disk data_pages by
     * definition — recover the true count now. */
    for (uint32_t i = 0; i < LARGE_WAL_SEGMENT_POOL_SLOTS; i++) {
        if (pool->slots[i].header.state == LSEG_ACTIVE) {
            uint32_t data_pages = 0;
            if (large_wal_segment_pool_tail_scan(pool, i, &data_pages) == MYDB_OK)
                pool->slots[i].header.data_pages = data_pages;
        }
    }

    /* Reseed the shared claim counter from whatever's on disk — same
     * scheme as WalSegmentPool (one pool-level counter, assigned at
     * claim time). */
    uint64_t max_used = 0;
    int      any_used = 0;
    for (uint32_t i = 0; i < LARGE_WAL_SEGMENT_POOL_SLOTS; i++) {
        if (pool->slots[i].header.state != LSEG_FREE) {
            if (!any_used || pool->slots[i].header.segment_no > max_used)
                max_used = pool->slots[i].header.segment_no;
            any_used = 1;
        }
    }
    pool->next_segment_no = any_used ? (max_used + 1) : 0;

    return MYDB_OK;
}

int large_wal_segment_pool_shutdown(LargeWalSegmentPool *pool)
{
    if (!pool) return MYDB_ERR;
    for (uint32_t i = 0; i < LARGE_WAL_SEGMENT_POOL_SLOTS; i++) {
        if (pool->slots[i].fd >= 0) {
            close(pool->slots[i].fd);
            pool->slots[i].fd = -1;
        }
    }
    return MYDB_OK;
}

/* ------------------------------------------------------------------
 * Claiming
 * ------------------------------------------------------------------ */

int large_wal_segment_pool_claim_next(LargeWalSegmentPool *pool, uint32_t *out_slot_index)
{
    if (!pool || !out_slot_index) return MYDB_ERR;

    uint32_t target = (uint32_t)(pool->next_segment_no % LARGE_WAL_SEGMENT_POOL_SLOTS);
    LargeWalSlot *slot = &pool->slots[target];

    if (slot->header.state != LSEG_FREE) return MYDB_ERR;

    slot->header.segment_no   = pool->next_segment_no;
    slot->header.start_lsn    = 0;
    slot->header.end_lsn      = 0;
    slot->header.data_pages   = 0;
    slot->header.state        = LSEG_ACTIVE;
    slot->header.partition_id = pool->partition_id;

    uint8_t page_buf[PAGE_SIZE];
    memset(page_buf, 0, PAGE_SIZE);
    large_wal_segment_header_serialize(&slot->header, page_buf);

    if (pwrite_all(slot->fd, page_buf, PAGE_SIZE, 0) != MYDB_OK) return MYDB_ERR;
    if (fsync(slot->fd) < 0) return MYDB_ERR;

    large_wal_segment_header_deserialize(page_buf, &slot->header);

    pool->next_segment_no++;
    *out_slot_index = target;
    return MYDB_OK;
}

int large_wal_segment_pool_mark_done(LargeWalSegmentPool *pool, uint32_t slot_index,
                                     uint64_t end_lsn, uint32_t data_pages)
{
    if (!pool || slot_index >= LARGE_WAL_SEGMENT_POOL_SLOTS) return MYDB_ERR;

    LargeWalSlot *slot = &pool->slots[slot_index];
    if (slot->header.state != LSEG_ACTIVE) return MYDB_ERR;

    slot->header.end_lsn    = end_lsn;
    slot->header.data_pages = data_pages;
    slot->header.state      = LSEG_DONE;

    uint8_t page_buf[PAGE_SIZE];
    memset(page_buf, 0, PAGE_SIZE);
    large_wal_segment_header_serialize(&slot->header, page_buf);

    if (pwrite_all(slot->fd, page_buf, PAGE_SIZE, 0) != MYDB_OK) return MYDB_ERR;
    if (fsync(slot->fd) < 0) return MYDB_ERR;

    large_wal_segment_header_deserialize(page_buf, &slot->header);
    return MYDB_OK;
}

int large_wal_segment_pool_free_slot(LargeWalSegmentPool *pool, uint32_t slot_index)
{
    if (!pool || slot_index >= LARGE_WAL_SEGMENT_POOL_SLOTS) return MYDB_ERR;

    LargeWalSlot *slot = &pool->slots[slot_index];
    if (slot->header.state != LSEG_DONE) return MYDB_ERR;

    slot->header.segment_no = 0;
    slot->header.start_lsn  = 0;
    slot->header.end_lsn    = 0;
    slot->header.data_pages = 0;
    slot->header.state      = LSEG_FREE;

    uint8_t page_buf[PAGE_SIZE];
    memset(page_buf, 0, PAGE_SIZE);
    large_wal_segment_header_serialize(&slot->header, page_buf);

    if (pwrite_all(slot->fd, page_buf, PAGE_SIZE, 0) != MYDB_OK) return MYDB_ERR;
    if (fsync(slot->fd) < 0) return MYDB_ERR;

    large_wal_segment_header_deserialize(page_buf, &slot->header);
    return MYDB_OK;
}

/* ------------------------------------------------------------------
 * Raw page I/O
 * ------------------------------------------------------------------ */

int large_wal_segment_pool_write_page(LargeWalSegmentPool *pool, uint32_t slot_index,
                                       uint32_t page_no, const uint8_t *buf)
{
    if (!pool || !buf || slot_index >= LARGE_WAL_SEGMENT_POOL_SLOTS) return MYDB_ERR;
    if (!page_no_valid(page_no)) return MYDB_ERR;

    off_t offset = (off_t)page_no * PAGE_SIZE;
    return pwrite_all(pool->slots[slot_index].fd, buf, PAGE_SIZE, offset);
}

int large_wal_segment_pool_read_page(LargeWalSegmentPool *pool, uint32_t slot_index,
                                      uint32_t page_no, uint8_t *out_buf)
{
    if (!pool || !out_buf || slot_index >= LARGE_WAL_SEGMENT_POOL_SLOTS) return MYDB_ERR;
    if (!page_no_valid(page_no)) return MYDB_ERR;

    off_t offset = (off_t)page_no * PAGE_SIZE;
    return pread_all(pool->slots[slot_index].fd, out_buf, PAGE_SIZE, offset);
}

int large_wal_segment_pool_read_segment(LargeWalSegmentPool *pool, uint32_t slot_index,
                                         uint8_t *out_buf)
{
    if (!pool || !out_buf || slot_index >= LARGE_WAL_SEGMENT_POOL_SLOTS) return MYDB_ERR;
    return pread_all(pool->slots[slot_index].fd, out_buf, LARGE_WAL_SEGMENT_FILE_SIZE, 0);
}

/* ------------------------------------------------------------------
 * large_wal_segment_pool_write — the Writer-thread-facing abstraction.
 * A blind byte mover: never parses or constructs LargeWalPageHeader
 * content (the caller's buf already carries whatever headers/LSNs it
 * needs). Only knows page/segment geometry. slot_index/page_no/offset
 * are the caller's own cursor, purely positional — never derived from
 * anything inside buf.
 * ------------------------------------------------------------------ */

int large_wal_segment_pool_write(LargeWalSegmentPool *pool,
                                  uint32_t *slot_index, uint32_t *page_no, uint32_t *offset,
                                  const uint8_t *buf, size_t buf_len)
{
    if (!pool || !slot_index || !page_no || !offset || !buf) return MYDB_ERR;
    if (*slot_index >= LARGE_WAL_SEGMENT_POOL_SLOTS) return MYDB_ERR;
    if (!page_no_valid(*page_no)) return MYDB_ERR;
    if (*offset >= PAGE_SIZE) return MYDB_ERR;

    size_t written = 0;
    while (written < buf_len) {
        uint32_t room_left = PAGE_SIZE - *offset;
        size_t   remaining = buf_len - written;
        uint32_t chunk = (remaining < (size_t)room_left) ? (uint32_t)remaining : room_left;

        off_t abs_offset = (off_t)(*page_no) * PAGE_SIZE + *offset;
        if (pwrite_all(pool->slots[*slot_index].fd, buf + written, chunk, abs_offset) != MYDB_OK)
            return MYDB_ERR;

        written += chunk;
        *offset += chunk;

        if (*offset < PAGE_SIZE) {
            /* This page-slot isn't full — every remaining byte must have
             * fit, so the loop is about to exit with the cursor left
             * mid-page for a future call to resume appending into. */
            break;
        }

        /* This page-slot just filled exactly. */
        *offset = 0;

        if (*page_no + 1 >= LARGE_WAL_SEGMENT_PAGES_PER_FILE) {
            /* The segment is full too. The one non-"blind" step: read
             * the page we just filled back and copy its own content_lsn
             * field — a LargeWalPageHeader carries exactly one LSN per
             * page (the single large record it's a slice of), so unlike
             * normal WAL's page_lsn/end_lsn split there's no first-vs-
             * last ambiguity to resolve. A field copy, not content
             * interpretation. */
            uint8_t  last_page[PAGE_SIZE];
            uint64_t seg_end_lsn = 0;
            if (large_wal_segment_pool_read_page(pool, *slot_index, *page_no, last_page) == MYDB_OK) {
                LargeWalPageHeader hdr;
                if (large_wal_page_header_deserialize(last_page, &hdr) == MYDB_OK)
                    seg_end_lsn = hdr.content_lsn;
            }

            uint32_t finished_slot       = *slot_index;
            uint32_t finished_data_pages = *page_no;
            if (large_wal_segment_pool_mark_done(pool, finished_slot, seg_end_lsn, finished_data_pages) != MYDB_OK)
                return MYDB_ERR;
            if (large_wal_segment_pool_claim_next(pool, slot_index) != MYDB_OK)
                return MYDB_ERR;
            *page_no = 1;
        } else {
            (*page_no)++;
        }
    }

    /* TEMPORARY: fsync every call, since there's no LARGE_WAL Writer
     * thread / group-commit yet to batch many writes behind one fsync
     * (design doc §11). Remove this one line once that thread lands. */
    if (fsync(pool->slots[*slot_index].fd) < 0) return MYDB_ERR;

    return MYDB_OK;
}

/* ------------------------------------------------------------------
 * Crash-reload tail-scan
 * ------------------------------------------------------------------ */

int large_wal_segment_pool_tail_scan(LargeWalSegmentPool *pool, uint32_t slot_index,
                                      uint32_t *out_data_pages)
{
    if (!pool || !out_data_pages || slot_index >= LARGE_WAL_SEGMENT_POOL_SLOTS) return MYDB_ERR;

    int      fd    = pool->slots[slot_index].fd;
    uint32_t count = 0;
    uint8_t  buf[PAGE_SIZE];

    for (uint32_t page_no = 1; page_no < LARGE_WAL_SEGMENT_PAGES_PER_FILE; page_no++) {
        off_t offset = (off_t)page_no * PAGE_SIZE;
        if (pread_all(fd, buf, PAGE_SIZE, offset) != MYDB_OK) break;

        LargeWalPageHeader hdr;
        /* An unwritten (still-zero, sparse) page fails file_header_
         * check_id's magic check inside deserialize — that failure is
         * exactly the stop condition, not an error to report upward. */
        if (large_wal_page_header_deserialize(buf, &hdr) != MYDB_OK) break;

        count++;
    }

    *out_data_pages = count;
    return MYDB_OK;
}
