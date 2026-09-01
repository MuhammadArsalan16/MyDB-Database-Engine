#include "large_wal/large_wal_index.h"
#include "file_header.h"
#include "checksum.h"

#include <fcntl.h>
#include <unistd.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <sys/stat.h>

/* Defined below next to the public lookup() it backs — declared here so
 * insert()'s duplicate check can reach it without re-locking. */
static int lookup_locked(const LargeWalIndex *idx, uint64_t content_lsn, LargeWalIndexEntry *out);

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

/* ------------------------------------------------------------------
 * Wire format: [FileHeaderId(8)][count(4)][entries...][checksum(4)].
 * Each entry is 30 bytes, field-by-field (no struct-layout assumptions,
 * same reasoning as every other on-disk struct in this codebase).
 * ------------------------------------------------------------------ */
#define LARGE_WAL_INDEX_HEADER_SIZE      12
#define LARGE_WAL_INDEX_ENTRY_WIRE_SIZE  30

static void serialize_entry(uint8_t *buf, const LargeWalIndexEntry *e)
{
    memcpy(buf + 0,  &e->content_lsn,    8);
    buf[8] = e->rec_type;
    memcpy(buf + 9,  &e->segment_no,     8);
    memcpy(buf + 17, &e->start_page_no,  4);
    memcpy(buf + 21, &e->offset,         4);
    buf[25] = e->page_count;
    memcpy(buf + 26, &e->total_size,     4);
}

static void deserialize_entry(const uint8_t *buf, LargeWalIndexEntry *e)
{
    memcpy(&e->content_lsn,   buf + 0,  8);
    e->rec_type = buf[8];
    memcpy(&e->segment_no,    buf + 9,  8);
    memcpy(&e->start_page_no, buf + 17, 4);
    memcpy(&e->offset,        buf + 21, 4);
    e->page_count = buf[25];
    memcpy(&e->total_size,    buf + 26, 4);
}

/* ------------------------------------------------------------------
 * Hash map — growable open addressing, linear probing, rebuilt wholesale
 * on every mutation (see the header's doc comment for why).
 * ------------------------------------------------------------------ */

static uint32_t hash_lsn(uint64_t lsn, uint32_t bucket_capacity)
{
    uint64_t h = lsn;
    h ^= h >> 33;
    h *= 0xff51afd7ed558ccdULL;
    h ^= h >> 33;
    h *= 0xc4ceb9fe1a85ec53ULL;
    h ^= h >> 33;
    return (uint32_t)(h & (bucket_capacity - 1));
}

static uint32_t next_pow2_at_least_16(uint32_t n)
{
    uint32_t p = 16;
    while (p < n) p <<= 1;
    return p;
}

static int rebuild_hash_map(LargeWalIndex *idx)
{
    uint32_t needed = next_pow2_at_least_16(idx->count * 2);
    if (idx->bucket_capacity != needed) {
        int32_t *nb = realloc(idx->buckets, (size_t)needed * sizeof(int32_t));
        if (!nb) return MYDB_ERR;
        idx->buckets = nb;
        idx->bucket_capacity = needed;
    }
    for (uint32_t i = 0; i < idx->bucket_capacity; i++) idx->buckets[i] = -1;

    for (uint32_t i = 0; i < idx->count; i++) {
        uint32_t b = hash_lsn(idx->entries[i].content_lsn, idx->bucket_capacity);
        while (idx->buckets[b] != -1) b = (b + 1) & (idx->bucket_capacity - 1);
        idx->buckets[b] = (int32_t)i;
    }
    return MYDB_OK;
}

/* ------------------------------------------------------------------
 * Whole-file save
 * ------------------------------------------------------------------ */

static int idx_save(LargeWalIndex *idx)
{
    size_t entries_bytes = (size_t)idx->count * LARGE_WAL_INDEX_ENTRY_WIRE_SIZE;
    size_t total = LARGE_WAL_INDEX_HEADER_SIZE + entries_bytes + 4;

    uint8_t *buf = malloc(total);
    if (!buf) return MYDB_ERR;

    file_header_write_id(buf, FILETYPE_LARGE_WAL_INDEX);
    memcpy(buf + 8, &idx->count, 4);
    for (uint32_t i = 0; i < idx->count; i++)
        serialize_entry(buf + LARGE_WAL_INDEX_HEADER_SIZE + (size_t)i * LARGE_WAL_INDEX_ENTRY_WIRE_SIZE,
                        &idx->entries[i]);

    uint32_t cs = crc32(buf, LARGE_WAL_INDEX_HEADER_SIZE + entries_bytes);
    memcpy(buf + LARGE_WAL_INDEX_HEADER_SIZE + entries_bytes, &cs, 4);

    int rc = MYDB_OK;
    if (ftruncate(idx->fd, (off_t)total) < 0) rc = MYDB_ERR;
    if (rc == MYDB_OK) rc = pwrite_all(idx->fd, buf, total, 0);
    if (rc == MYDB_OK && fsync(idx->fd) < 0) rc = MYDB_ERR;

    free(buf);
    return rc;
}

/* ------------------------------------------------------------------
 * Public API
 * ------------------------------------------------------------------ */

int large_wal_index_open(LargeWalIndex *idx, const char *wal_dir)
{
    if (!idx || !wal_dir) return MYDB_ERR;
    memset(idx, 0, sizeof(*idx));
    idx->fd = -1;
    if (pthread_mutex_init(&idx->lock, NULL) != 0) return MYDB_ERR;
    snprintf(idx->path, sizeof(idx->path), "%s/large_wal_index.mydb", wal_dir);

    int fd = open(idx->path, O_RDWR);
    if (fd >= 0) {
        struct stat st;
        if (fstat(fd, &st) != 0 || st.st_size < (off_t)(LARGE_WAL_INDEX_HEADER_SIZE + 4)) {
            close(fd);
            return MYDB_ERR;
        }

        uint8_t *buf = malloc((size_t)st.st_size);
        if (!buf) { close(fd); return MYDB_ERR; }
        if (pread_all(fd, buf, (size_t)st.st_size, 0) != MYDB_OK) {
            free(buf); close(fd); return MYDB_ERR;
        }

        int rc = file_header_check_id(buf, FILETYPE_LARGE_WAL_INDEX);
        if (rc != MYDB_OK) { free(buf); close(fd); return rc; }

        uint32_t count;
        memcpy(&count, buf + 8, 4);
        size_t entries_bytes = (size_t)count * LARGE_WAL_INDEX_ENTRY_WIRE_SIZE;
        size_t expected = LARGE_WAL_INDEX_HEADER_SIZE + entries_bytes + 4;
        if (expected != (size_t)st.st_size) { free(buf); close(fd); return MYDB_ERR; }

        uint32_t stored_cs;
        memcpy(&stored_cs, buf + LARGE_WAL_INDEX_HEADER_SIZE + entries_bytes, 4);
        if (stored_cs != crc32(buf, LARGE_WAL_INDEX_HEADER_SIZE + entries_bytes)) {
            free(buf); close(fd); return MYDB_ERR_BAD_CHECKSUM;
        }

        if (count > 0) {
            idx->entries = malloc((size_t)count * sizeof(LargeWalIndexEntry));
            if (!idx->entries) { free(buf); close(fd); return MYDB_ERR; }
            for (uint32_t i = 0; i < count; i++)
                deserialize_entry(buf + LARGE_WAL_INDEX_HEADER_SIZE + (size_t)i * LARGE_WAL_INDEX_ENTRY_WIRE_SIZE,
                                  &idx->entries[i]);
        }
        idx->count    = count;
        idx->capacity = count;
        free(buf);
        idx->fd = fd;

        if (rebuild_hash_map(idx) != MYDB_OK) { large_wal_index_close(idx); return MYDB_ERR; }
        return MYDB_OK;
    }
    if (errno != ENOENT) return MYDB_ERR;

    /* Doesn't exist yet — create fresh and empty. */
    fd = open(idx->path, O_RDWR | O_CREAT | O_EXCL, 0644);
    if (fd < 0) return MYDB_ERR;
    idx->fd = fd;

    if (rebuild_hash_map(idx) != MYDB_OK) {
        close(fd); idx->fd = -1;
        return MYDB_ERR;
    }
    int rc = idx_save(idx);
    if (rc != MYDB_OK) {
        close(fd); idx->fd = -1;
        return rc;
    }
    return MYDB_OK;
}

int large_wal_index_close(LargeWalIndex *idx)
{
    if (!idx) return MYDB_ERR;
    if (idx->fd >= 0) close(idx->fd);
    free(idx->entries);
    free(idx->buckets);
    idx->fd             = -1;
    idx->entries         = NULL;
    idx->buckets          = NULL;
    idx->count            = 0;
    idx->capacity         = 0;
    idx->bucket_capacity  = 0;
    pthread_mutex_destroy(&idx->lock);
    return MYDB_OK;
}

int large_wal_index_insert(LargeWalIndex *idx, const LargeWalIndexEntry *e)
{
    if (!idx || !e) return MYDB_ERR;

    pthread_mutex_lock(&idx->lock);

    LargeWalIndexEntry existing;
    if (lookup_locked(idx, e->content_lsn, &existing) == MYDB_OK) {
        pthread_mutex_unlock(&idx->lock);
        return MYDB_ERR_DUPLICATE;
    }

    if (idx->count == idx->capacity) {
        uint32_t new_cap = idx->capacity ? idx->capacity * 2 : 8;
        LargeWalIndexEntry *ne = realloc(idx->entries, (size_t)new_cap * sizeof(LargeWalIndexEntry));
        if (!ne) {
            pthread_mutex_unlock(&idx->lock);
            return MYDB_ERR;
        }
        idx->entries  = ne;
        idx->capacity = new_cap;
    }
    idx->entries[idx->count++] = *e;

    int rc = (rebuild_hash_map(idx) != MYDB_OK) ? MYDB_ERR : idx_save(idx);

    pthread_mutex_unlock(&idx->lock);
    return rc;
}

int large_wal_index_lookup(LargeWalIndex *idx, uint64_t content_lsn, LargeWalIndexEntry *out)
{
    if (!idx || !out) return MYDB_ERR_NOT_FOUND;

    pthread_mutex_lock(&idx->lock);
    int rc = lookup_locked(idx, content_lsn, out);
    pthread_mutex_unlock(&idx->lock);
    return rc;
}

/* The lookup body, minus the locking — insert() needs its duplicate
 * check without re-entering a non-recursive mutex it already holds. */
static int lookup_locked(const LargeWalIndex *idx, uint64_t content_lsn, LargeWalIndexEntry *out)
{
    if (idx->bucket_capacity == 0) return MYDB_ERR_NOT_FOUND;

    uint32_t b     = hash_lsn(content_lsn, idx->bucket_capacity);
    uint32_t start = b;
    do {
        int32_t slot = idx->buckets[b];
        if (slot == -1) return MYDB_ERR_NOT_FOUND;
        if (idx->entries[slot].content_lsn == content_lsn) {
            *out = idx->entries[slot];
            return MYDB_OK;
        }
        b = (b + 1) & (idx->bucket_capacity - 1);
    } while (b != start);

    return MYDB_ERR_NOT_FOUND;
}

int large_wal_index_delete_by_segment(LargeWalIndex *idx, uint64_t segment_no)
{
    if (!idx) return MYDB_ERR;

    pthread_mutex_lock(&idx->lock);

    uint32_t w = 0;
    for (uint32_t r = 0; r < idx->count; r++) {
        if (idx->entries[r].segment_no != segment_no)
            idx->entries[w++] = idx->entries[r];
    }
    idx->count = w;

    int rc = (rebuild_hash_map(idx) != MYDB_OK) ? MYDB_ERR : idx_save(idx);

    pthread_mutex_unlock(&idx->lock);
    return rc;
}
