#ifndef LARGE_WAL_STATE_H
#define LARGE_WAL_STATE_H

#include <stdint.h>
#include <pthread.h>
#include "common.h"

/*
 * large_wal_state.h — wal/large_wal_state.mydb (MYDB_WAL_IMPLEMENTATION.md
 * §10.9). The single persisted watermark a future Recovery Gate compares
 * against checkpoint_lsn to decide whether a LARGE_WAL pre-scan is
 * needed at all: "read large_wal_state.mydb -> large_wal_flush_lsn...
 * single cheap comparison of two small persisted values."
 *
 * Fsynced on every durable write here — per §10.9, this ordering (state
 * file fsync completes strictly before a caller is told a large-record
 * write is durable) is "the single load-bearing fact the entire
 * single-fsync-suffices guarantee depends on."
 *
 * Concurrency: one plain mutex, the last link in large_wal's global
 * lock order
 *     reg->lock -> node->lock -> pool->lock -> idx->lock -> state->lock
 * and a leaf — advance() acquires nothing else while holding it. Only
 * the writer thread advances flush_lsn today, but §10.7's commit-wait
 * will read it from every committing transaction's thread, so the lock
 * goes in now rather than being retrofitted around live readers.
 */

typedef struct {
    uint64_t flush_lsn;   /* highest content_lsn known durable */
    int      fd;
    char     path[300];

    pthread_mutex_t lock; /* protects flush_lsn and the file behind fd */
} LargeWalState;

/* Opens wal_dir/large_wal_state.mydb, creating one with flush_lsn == 0
 * if it doesn't exist yet. Validates FileHeaderId + checksum on load. */
int large_wal_state_open(LargeWalState *st, const char *wal_dir);
int large_wal_state_close(LargeWalState *st);

/* Persists + fdatasyncs new_flush_lsn. Rejects new_flush_lsn < the current
 * value (flush_lsn is monotonic) with MYDB_ERR. */
int large_wal_state_advance(LargeWalState *st, uint64_t new_flush_lsn);

#endif /* LARGE_WAL_STATE_H */
