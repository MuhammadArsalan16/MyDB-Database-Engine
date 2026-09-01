#ifndef WAL_WORKER_H
#define WAL_WORKER_H

#include <pthread.h>
#include <stdint.h>
#include "common.h"

/*
 * wal_worker.h — one persistent background thread, shared by both
 * normal_wal and large_wal (owned by the top-level WalManager,
 * wal_manager.h), used to overlap a segment rollover's two genuinely
 * distinct-file fsyncs instead of paying for them sequentially.
 *
 * A rollover does mark_done() (old segment's header) then eventually a
 * trailing fdatasync (new segment's header+data) — two different files,
 * so the two flushes can't be collapsed into one, but they don't need
 * to be sequential either. mark_done() hands its fdatasync to this
 * worker and returns immediately; the caller (write()) continues
 * writing the new segment and fdatasyncs it on its own thread, then
 * calls wal_worker_wait() — total latency becomes max(old, new)
 * instead of old + new.
 *
 * Single-slot mailbox, same mutex+condvar shape LargeWalWriter's
 * submit() already uses — but usable safely from *two* different
 * caller threads (WalFlusher's and LargeWalWriter's, both sharing one
 * WalManager per partition), so async_fdatasync() blocks (briefly,
 * only on the rare collision) until any previous request has been
 * fully consumed via wait(), rather than assuming a single caller.
 *
 * Deliberately narrow: this is not a generic task-executor. The only
 * operation this subsystem currently needs to offload is fdatasync,
 * so that's the only thing exposed.
 */

typedef struct {
    pthread_t        thread;
    pthread_mutex_t  lock;
    pthread_cond_t   cond;
    uint8_t          started;
    uint8_t          stop_requested;

    uint8_t  pending;   /* a request is submitted, not yet picked up/finished */
    uint8_t  done;      /* the worker finished; result is ready to collect */
    int      req_fd;
    int      last_result;
} WalWorker;

int wal_worker_start(WalWorker *w);

/* Signals stop, joins the thread. Safe to call even if start() was
 * never called, or after an earlier stop() — a no-op in both cases. */
int wal_worker_stop(WalWorker *w);

/* Hands fd off to the background thread for fdatasync() and returns
 * immediately. Blocks only if a previous request hasn't been wait()'d
 * on yet (the rare collision between normal_wal's and large_wal's
 * threads sharing this one worker) — otherwise returns without
 * waiting for the fdatasync itself to complete. */
int wal_worker_async_fdatasync(WalWorker *w, int fd);

/* Blocks until the in-flight request completes and returns its
 * fdatasync() result. A cheap no-op returning MYDB_OK if nothing was
 * ever submitted since the last wait() — this is what makes it safe
 * for write() to call unconditionally rather than tracking "did I
 * roll over this call" locally. */
int wal_worker_wait(WalWorker *w);

#endif /* WAL_WORKER_H */
