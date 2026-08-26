#ifndef LARGE_WAL_INDEX_H
#define LARGE_WAL_INDEX_H

#include <stdint.h>
#include <pthread.h>
#include "common.h"

/*
 * large_wal_index.h — on-disk large_wal_index.mydb, one per partition's
 * wal/ directory (MYDB_WAL_IMPLEMENTATION.md §10.6). Maps a large
 * record's content_lsn to where its pages live *within* a segment
 * (start_page_no/offset/page_count) plus which segment_no owns it.
 * segment_no is identity only, not location — resolving segment_no to
 * "rotation pool slot" vs. "holding-area file" is large_wal_archiver.h's
 * job, not this module's (§10.1's in-memory (segment_no -> fd) table).
 *
 * I/O style follows system_schema.h's established precedent for small
 * engine-managed files: loaded fully into RAM at open, direct
 * pread/pwrite bypassing the buffer pool, open-addressing hash map for
 * key lookup. Deliberately NOT system_schema's fixed-capacity-array
 * shape though — entries/buckets are both growable (realloc'd), since
 * the holding area this index tracks is explicitly "dynamically sized,
 * not fixed" (§10.1); a hard cap here would recreate the exact
 * writer-stalls-on-backlog coupling that design choice exists to avoid.
 *
 * Persistence: whole-file rewrite on every mutating call (insert /
 * delete_by_segment) — same "small file, rewrite wholesale on write"
 * convention Catalog/__catalog.mydb already uses, appropriate here
 * since mutations are paced by large-record writes and archiver
 * gate-clears, not a hot loop.
 *
 * Concurrency: one plain mutex covering the whole structure, following
 * the pattern normal_wal's WalRingBuffer already uses (a named lock
 * embedded in the struct it protects). A mutex rather than an rwlock
 * because the two mutating calls each rewrite the entries array,
 * rebuild the bucket table wholesale and fsync the file — there is no
 * cheap-write case for a reader/writer split to exploit, and
 * large_wal_get (the only reader) is a cold path.
 *
 * This is the fourth lock in large_wal's global order
 *     reg->lock -> node->lock -> pool->lock -> idx->lock -> state->lock
 * and it is a leaf: nothing here acquires anything else while held.
 * large_wal_get deliberately releases this lock before touching the
 * registry, so a reader never holds the index and the registry at the
 * same time — which is what keeps it from closing a cycle against
 * try_free, which holds them in sequence.
 */

typedef struct {
    uint64_t content_lsn;    /* primary key */
    uint8_t  rec_type;       /* WalRecType of the original record */
    uint64_t segment_no;     /* identity only, not location */
    uint32_t start_page_no;
    uint32_t offset;
    uint8_t  page_count;
    uint32_t total_size;
} LargeWalIndexEntry;

typedef struct {
    LargeWalIndexEntry *entries;         /* growable, realloc'd */
    uint32_t             count;
    uint32_t             capacity;

    /* Open-addressing hash map, content_lsn -> index into entries[].
     * Rebuilt wholesale on every insert/delete rather than incrementally
     * patched — this structure isn't a hot path, and full rebuild avoids
     * the probe-chain-fixup hazard open-addressing deletion normally
     * needs tombstones for. */
    int32_t              *buckets;
    uint32_t              bucket_capacity;

    int    fd;
    char   path[300];

    pthread_mutex_t lock;   /* protects entries/count/capacity, buckets/
                                bucket_capacity, and the file behind fd */
} LargeWalIndex;

/* Opens wal_dir/large_wal_index.mydb, creating an empty one if it
 * doesn't exist yet. Validates FileHeaderId + trailing checksum and
 * rebuilds the in-memory hash map on load. */
int large_wal_index_open(LargeWalIndex *idx, const char *wal_dir);
int large_wal_index_close(LargeWalIndex *idx);

/* Rejects a duplicate content_lsn with MYDB_ERR_DUPLICATE. Persists
 * (whole-file rewrite + fsync) on success. */
int large_wal_index_insert(LargeWalIndex *idx, const LargeWalIndexEntry *e);

/* Copies the matching entry into *out. Returns MYDB_ERR_NOT_FOUND on
 * miss. Non-const because it takes idx->lock — the copy-out is exactly
 * so the caller can use the entry after the lock is dropped. */
int large_wal_index_lookup(LargeWalIndex *idx, uint64_t content_lsn, LargeWalIndexEntry *out);

/* Removes every entry whose segment_no matches — called by the archiver
 * once a segment_no's holding-area copy has actually been freed
 * (try_free, large_wal_archiver.h). Persists. A no-op (still MYDB_OK) if
 * nothing matched. */
int large_wal_index_delete_by_segment(LargeWalIndex *idx, uint64_t segment_no);

#endif /* LARGE_WAL_INDEX_H */
