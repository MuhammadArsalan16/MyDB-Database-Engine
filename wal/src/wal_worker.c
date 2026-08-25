#include "wal_worker.h"

#include <unistd.h>
#include <string.h>

static void *worker_main(void *arg)
{
    WalWorker *w = (WalWorker *)arg;

    for (;;) {
        pthread_mutex_lock(&w->lock);
        while (!w->pending && !w->stop_requested)
            pthread_cond_wait(&w->cond, &w->lock);

        if (w->stop_requested && !w->pending) {
            pthread_mutex_unlock(&w->lock);
            break;
        }

        int fd = w->req_fd;
        pthread_mutex_unlock(&w->lock);

        int result = (fdatasync(fd) < 0) ? MYDB_ERR : MYDB_OK;

        pthread_mutex_lock(&w->lock);
        w->pending     = 0;
        w->done        = 1;
        w->last_result = result;
        pthread_cond_broadcast(&w->cond);
        pthread_mutex_unlock(&w->lock);
    }

    return NULL;
}

int wal_worker_start(WalWorker *w)
{
    if (!w) return MYDB_ERR;

    memset(w, 0, sizeof(*w));

    if (pthread_mutex_init(&w->lock, NULL) != 0) return MYDB_ERR;
    if (pthread_cond_init(&w->cond, NULL) != 0) {
        pthread_mutex_destroy(&w->lock);
        return MYDB_ERR;
    }

    if (pthread_create(&w->thread, NULL, worker_main, w) != 0) {
        pthread_cond_destroy(&w->cond);
        pthread_mutex_destroy(&w->lock);
        return MYDB_ERR;
    }

    w->started = 1;
    return MYDB_OK;
}

int wal_worker_stop(WalWorker *w)
{
    if (!w || !w->started) return MYDB_OK;

    pthread_mutex_lock(&w->lock);
    w->stop_requested = 1;
    pthread_cond_broadcast(&w->cond);
    pthread_mutex_unlock(&w->lock);

    pthread_join(w->thread, NULL);

    pthread_cond_destroy(&w->cond);
    pthread_mutex_destroy(&w->lock);
    w->started = 0;

    return MYDB_OK;
}

int wal_worker_async_fdatasync(WalWorker *w, int fd)
{
    if (!w || !w->started) return MYDB_ERR;

    pthread_mutex_lock(&w->lock);

    /* Wait until any previous request has been fully consumed via
     * wait() before accepting a new one -- one in-flight request at a
     * time, regardless of which caller thread submits it. */
    while (w->pending || w->done)
        pthread_cond_wait(&w->cond, &w->lock);

    w->req_fd  = fd;
    w->pending = 1;
    w->done    = 0;
    pthread_cond_broadcast(&w->cond);

    pthread_mutex_unlock(&w->lock);
    return MYDB_OK;
}

int wal_worker_wait(WalWorker *w)
{
    if (!w || !w->started) return MYDB_ERR;

    pthread_mutex_lock(&w->lock);

    if (!w->pending && !w->done) {
        pthread_mutex_unlock(&w->lock);
        return MYDB_OK;   /* nothing was ever submitted -- cheap no-op */
    }

    while (!w->done)
        pthread_cond_wait(&w->cond, &w->lock);

    int result = w->last_result;
    w->done = 0;
    pthread_cond_broadcast(&w->cond);   /* wake any async_fdatasync waiting for IDLE */

    pthread_mutex_unlock(&w->lock);
    return result;
}
