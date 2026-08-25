#include "large_wal/large_wal_archiver.h"
#include "common.h"

#include <fcntl.h>
#include <unistd.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/stat.h>

static int pwrite_all(int fd, const void *buf, size_t n, off_t offset)
{
    ssize_t written = pwrite(fd, buf, n, offset);
    return (written == (ssize_t)n) ? MYDB_OK : MYDB_ERR;
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
    return MYDB_OK;   /* nothing owned here anymore -- see large_wal_registry.h */
}

/* ------------------------------------------------------------------
 * Copy-out: rotation pool -> holding area
 * ------------------------------------------------------------------ */

int large_wal_archiver_copy_out(LargeWalArchiver *arc, LargeWalSegmentPool *pool,
                                 LargeWalRegistry *reg, uint32_t slot_index)
{
    if (!arc || !pool || !reg || slot_index >= LARGE_WAL_SEGMENT_POOL_SLOTS) return MYDB_ERR;

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

    /* Repoints the entry large_wal_writer already registered at claim
     * time (owns_fd=0, the pool's fd) to this new holding-area fd
     * (owns_fd=1) -- not a second, stale entry. */
    if (large_wal_registry_register(reg, segment_no, fd, /*owns_fd=*/1) != MYDB_OK) {
        close(fd);
        return MYDB_ERR;
    }

    return MYDB_OK;
}

/* ------------------------------------------------------------------
 * Freeing — Gate A + Gate B, both caller-supplied
 * ------------------------------------------------------------------ */

int large_wal_archiver_try_free(LargeWalArchiver *arc, LargeWalRegistry *reg, LargeWalIndex *idx,
                                 uint64_t segment_no, uint64_t segment_end_lsn,
                                 uint64_t checkpoint_lsn, int gate_b_cleared,
                                 int *out_freed)
{
    if (!arc || !reg || !idx || !out_freed) return MYDB_ERR;
    *out_freed = 0;

    if (!(checkpoint_lsn > segment_end_lsn) || !gate_b_cleared)
        return MYDB_OK;

    int fd;
    if (large_wal_registry_lookup(reg, segment_no, &fd) != MYDB_OK)
        return MYDB_OK;   /* nothing to free */

    char path[300];
    archival_path(arc, segment_no, path, sizeof(path));

    /* reg's entry could still be a rotation slot's own live fd if this
     * segment was never actually archived. Confirm the holding-area
     * file genuinely exists before touching anything. */
    struct stat st;
    if (stat(path, &st) != 0) return MYDB_OK;   /* not archived yet — nothing to free */

    close(fd);
    unlink(path);
    large_wal_registry_remove(reg, segment_no);

    if (large_wal_index_delete_by_segment(idx, segment_no) != MYDB_OK) return MYDB_ERR;

    *out_freed = 1;
    return MYDB_OK;
}
