#include "large_wal/large_wal_archiver.h"
#include "common.h"

#include <fcntl.h>
#include <unistd.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <dirent.h>
#include <errno.h>
#include <time.h>

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

/* Parses "large_wal_archival_<N>.mydb" and hands back N. Returns 0 for
 * anything else in the directory -- normal_wal's own wal_<N>.mydb files
 * and large_wal's rotation slots live in this same folder. */
static int parse_archival_name(const char *name, uint64_t *out_segment_no)
{
    static const char PREFIX[] = "large_wal_archival_";
    static const char SUFFIX[] = ".mydb";

    size_t plen = sizeof(PREFIX) - 1, slen = sizeof(SUFFIX) - 1;
    size_t len  = strlen(name);
    if (len <= plen + slen)              return 0;
    if (strncmp(name, PREFIX, plen) != 0) return 0;
    if (strcmp(name + len - slen, SUFFIX) != 0) return 0;

    uint64_t n = 0;
    for (size_t i = plen; i < len - slen; i++) {
        if (name[i] < '0' || name[i] > '9') return 0;
        n = n * 10u + (uint64_t)(name[i] - '0');
    }
    *out_segment_no = n;
    return 1;
}

/* Reads a holding-area file's page-0 header. MYDB_OK only if the file is
 * long enough AND the header deserializes -- which is what tells a
 * complete copy apart from one a crash cut short mid-pwrite. */
static int read_archival_header(const char *path, int *out_fd,
                                 LargeWalSegmentHeader *out_hdr)
{
    int fd = open(path, O_RDWR);
    if (fd < 0) return MYDB_ERR;

    struct stat st;
    if (fstat(fd, &st) != 0 || st.st_size < (off_t)LARGE_WAL_SEGMENT_FILE_SIZE) {
        close(fd);
        return MYDB_ERR;
    }

    uint8_t page_buf[PAGE_SIZE];
    if (pread(fd, page_buf, PAGE_SIZE, 0) != (ssize_t)PAGE_SIZE ||
        large_wal_segment_header_deserialize(page_buf, out_hdr) != MYDB_OK) {
        close(fd);
        return MYDB_ERR;
    }

    *out_fd = fd;
    return MYDB_OK;
}

/* What state is this segment's holding-area copy in?
 *
 * The distinction is the whole basis of copy_out's crash recovery,
 * because a crash can land in either of two places and they need
 * OPPOSITE responses:
 *
 *   HOLD_PARTIAL  crash during copy_out's own 2MB pwrite. The file is
 *                 short or its header won't deserialize. The rotation
 *                 slot is still intact and still LSEG_DONE, so the fix
 *                 is to throw this away and copy again from scratch.
 *
 *   HOLD_COMPLETE crash during free_slot, AFTER this file was written
 *                 and fsynced. The file is good; the rotation slot's
 *                 content pages are partly zeroed and its header still
 *                 says LSEG_DONE because free_slot never got to
 *                 rewriting it. Copying again is not just wasteful, it
 *                 is WRONG -- it would overwrite the only surviving
 *                 copy with the half-zeroed slot. The fix is to skip
 *                 the copy and finish the free.
 *
 * Getting these two the wrong way round loses data in one direction and
 * wedges a slot forever in the other. */
enum { HOLD_ABSENT = 0, HOLD_COMPLETE = 1, HOLD_PARTIAL = 2 };

static int holding_copy_status(const char *path)
{
    struct stat st;
    if (stat(path, &st) != 0) return HOLD_ABSENT;

    int                   fd = -1;
    LargeWalSegmentHeader hdr;
    if (read_archival_header(path, &fd, &hdr) != MYDB_OK) return HOLD_PARTIAL;

    close(fd);
    return HOLD_COMPLETE;
}

/* Startup scan of the holding area -- see the header's doc comment on
 * init for why both halves of this matter (re-registering good files,
 * deleting bad ones). Best-effort by design: one unreadable file must
 * not stop the engine from coming up, so failures here are skipped
 * rather than propagated. */
static void scan_holding_area(LargeWalArchiver *arc, LargeWalRegistry *reg)
{
    DIR *d = opendir(arc->wal_dir);
    if (!d) return;

    struct dirent *ent;
    while ((ent = readdir(d)) != NULL) {
        uint64_t segment_no = 0;
        if (!parse_archival_name(ent->d_name, &segment_no)) continue;

        char path[300];
        archival_path(arc, segment_no, path, sizeof(path));

        int                   fd = -1;
        LargeWalSegmentHeader hdr;
        if (read_archival_header(path, &fd, &hdr) != MYDB_OK) {
            /* Truncated or corrupt: delete it so copy_out's O_EXCL can
             * create a fresh one. The rotation slot still holds the
             * original (it is only freed after this copy's fsync
             * confirmed), so nothing is lost. */
            unlink(path);
            continue;
        }

        /* owns_fd=1: this fd was opened here, so this registry closes
         * it -- the same ownership copy_out hands over. */
        if (large_wal_registry_register(reg, segment_no, fd, /*owns_fd=*/1) != MYDB_OK)
            close(fd);
    }

    closedir(d);
}

int large_wal_archiver_init(LargeWalArchiver *arc, const char *wal_dir,
                             LargeWalRegistry *reg)
{
    if (!arc || !wal_dir) return MYDB_ERR;
    memset(arc, 0, sizeof(*arc));
    snprintf(arc->wal_dir, sizeof(arc->wal_dir), "%s", wal_dir);
    arc->registry         = reg;
    arc->poll_interval_ms = LARGE_WAL_ARCHIVER_POLL_MS;

    if (reg) scan_holding_area(arc, reg);
    return MYDB_OK;
}

int large_wal_archiver_shutdown(LargeWalArchiver *arc)
{
    if (!arc) return MYDB_ERR;
    large_wal_archiver_stop(arc);   /* no-op if never started */
    return MYDB_OK;   /* nothing else owned here -- see large_wal_registry.h */
}

int large_wal_archiver_set_gates(LargeWalArchiver *arc, LargeWalGateFn fn, void *ctx)
{
    if (!arc) return MYDB_ERR;

    /* Under pool->lock once the thread is running, because the thread
     * reads these two fields every tick. They are set together and must
     * be seen together -- a torn update could pair a new function with
     * an old context. Before start() there is no second thread, and
     * arc->pool isn't set yet, so the plain store is the only option
     * and is also correct. */
    if (arc->started && arc->pool) {
        pthread_mutex_lock(&arc->pool->lock);
        arc->gate_fn  = fn;
        arc->gate_ctx = ctx;
        pthread_mutex_unlock(&arc->pool->lock);
    } else {
        arc->gate_fn  = fn;
        arc->gate_ctx = ctx;
    }
    return MYDB_OK;
}

/* ------------------------------------------------------------------
 * Copy-out: rotation pool -> holding area
 * ------------------------------------------------------------------ */

int large_wal_archiver_copy_out(LargeWalArchiver *arc, LargeWalSegmentPool *pool,
                                 LargeWalRegistry *reg, uint32_t slot_index)
{
    if (!arc || !pool || !reg || slot_index >= LARGE_WAL_SEGMENT_POOL_SLOTS) return MYDB_ERR;

    /* Snapshot under pool->lock rather than reading slots[].header
     * directly: the segment_no is needed *before* the registry node can
     * be acquired, and the lock order (reg -> node -> pool) forbids
     * reaching for pool->lock once that node is held. */
    uint64_t segment_no = 0;
    uint8_t  state      = 0;
    if (large_wal_segment_pool_slot_info(pool, slot_index, &segment_no, &state) != MYDB_OK)
        return MYDB_ERR;
    if (state != LSEG_DONE) return MYDB_ERR;

    char path[300];
    archival_path(arc, segment_no, path, sizeof(path));

    /* Is a copy of this segment already sitting in the holding area?
     * Only a previous run that crashed can have left one -- within a
     * single run, copy_out either completes (and the slot stops being
     * LSEG_DONE, so we never come back to it) or backs its own file out
     * on the way to returning an error. */
    int status = holding_copy_status(path);
    if (status == HOLD_PARTIAL) {
        /* Throw the short file away and copy again below. Safe because
         * this crash window is BEFORE free_slot ran, so the rotation
         * slot still holds the original -- exactly the recovery impl
         * doc 10.1 prescribes ("not durable, re-copy from the
         * still-valid rotation-slot original"). */
        unlink(path);
        status = HOLD_ABSENT;
    }

    /* fd is the holding-area file this call will point the registry at,
     * or -1 to mean "leave the registry alone" -- see the resume branch
     * further down. */
    int fd        = -1;
    int made_copy = 0;

    if (status == HOLD_ABSENT) {
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

        /* O_EXCL stays. It is what guarantees two live copy_outs can
         * never write the same path at once. The resume case above is
         * why that no longer wedges anything: a pre-existing file is
         * now classified before we get here, never merely collided
         * with. */
        fd = open(path, O_RDWR | O_CREAT | O_EXCL, 0644);
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
        made_copy = 1;
    }

    /* Everything above ran unlocked on purpose: a LSEG_DONE segment is
     * immutable, so the 2MB read and the holding-area write have nothing
     * to exclude. Only the transition below has to look atomic to a
     * reader, and it is the cheap part.
     *
     * Hold this segment's registry node across BOTH steps: free_slot
     * zeroes the rotation slot's content pages, and until set_fd has
     * repointed the registry, a large_wal_get resolving this segment_no
     * would still be sent to that very slot. One node lock spanning the
     * pair is what makes "the segment moved" indivisible. */
    LargeWalRegistryNode *node   = NULL;
    int                   old_fd = -1;
    int                   held   = (large_wal_registry_acquire(reg, segment_no, &node, &old_fd) == MYDB_OK);

    if (!made_copy) {
        /* Resume path: the copy already exists and is durable, so all
         * that is left is the free. Whether the registry needs
         * repointing depends on where it currently points.
         *
         * archiver_init's startup scan registers every valid
         * holding-area file, so after a restart it usually points there
         * already -- and then there is nothing to repoint, because the
         * "the segment moved" step effectively happened at init.
         *
         * The rotation slot's own fd is the test. It is assigned once
         * when the pool opens its files and never changes, so comparing
         * against it is a stable way to ask "is this entry still aimed
         * at the slot we are about to zero?" without a second lookup. */
        if (held && old_fd != pool->slots[slot_index].fd) {
            fd = -1;   /* already aimed at the holding area — leave it */
        } else {
            /* Either unregistered, or still aimed at the rotation slot.
             * Open the existing file WITHOUT O_EXCL: here it is meant
             * to be there. */
            fd = open(path, O_RDWR);
            if (fd < 0) {
                if (held) large_wal_registry_release(reg, node);
                return MYDB_ERR;
            }
        }
    }

    /* Ordering rule (impl doc §10.1): the rotation slot is freed only
     * after the holding-area copy's fsync above has confirmed — never
     * before, or segment_no could transiently exist validly in two
     * places. */
    if (large_wal_segment_pool_free_slot(pool, slot_index) != MYDB_OK) {
        if (held) large_wal_registry_release(reg, node);
        if (fd >= 0) close(fd);
        /* Back the copy out completely, but ONLY a copy this call made.
         * Nothing has been published yet -- the registry still points at
         * the rotation slot and the slot is still LSEG_DONE -- so
         * removing the file restores exactly the state this call
         * started in.
         *
         * Leaving it behind used to be harmless because nothing retried
         * copy_out. With the archiver thread it is not: the next tick
         * would hit O_EXCL on a path that already exists and fail the
         * same way forever, so this slot would never drain and writers
         * would eventually block on a permanently full pool.
         *
         * On the resume path the file is NOT ours to delete -- it is the
         * only surviving copy of that segment, since the crash that put
         * us here already zeroed part of the rotation slot. */
        if (made_copy) unlink(path);
        return MYDB_ERR;
    }

    /* Repoints the entry large_wal_writer already registered at claim
     * time (owns_fd=0, the pool's fd) to this new holding-area fd
     * (owns_fd=1) -- not a second, stale entry. set_fd rather than
     * register() because we are inside the node lock already, and
     * register() takes the registry's WRITE lock, which cannot be had
     * while acquire()'s read lock is still held.
     *
     * The unregistered case falls back to register(): nothing can be
     * reading a segment no reader can resolve, so there was nothing to
     * hold a node lock against in the first place. */
    int rc;
    if (fd < 0) {
        /* Resume, registry already correct: nothing to repoint. */
        rc = MYDB_OK;
        if (held) large_wal_registry_release(reg, node);
    } else if (held) {
        rc = large_wal_registry_set_fd(node, fd, /*owns_fd=*/1);
        large_wal_registry_release(reg, node);
    } else {
        rc = large_wal_registry_register(reg, segment_no, fd, /*owns_fd=*/1);
    }
    if (rc != MYDB_OK) {
        if (fd >= 0) close(fd);
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

    /* Unlink the node FIRST, and take the fd back from remove() rather
     * than reusing the one lookup() returned above — lookup's fd is an
     * identity answer, not a usable handle. remove() runs under the
     * registry's WRITE lock, which cannot be held while any reader holds
     * the read lock, so by the time it returns every in-flight
     * large_wal_get on this segment has finished. Closing after that is
     * safe: the node is gone, so no new reader can ever resolve to this
     * fd either. Closing it before would yank the descriptor out from
     * under a reader mid-pread. */
    int removed_fd = -1;
    if (large_wal_registry_remove(reg, segment_no, &removed_fd, NULL) != MYDB_OK)
        return MYDB_ERR;

    if (removed_fd >= 0) close(removed_fd);
    unlink(path);

    /* Outside the registry lock deliberately: this rewrites the whole
     * index file and fsyncs it, and there is no reason for every reader
     * to be locked out for that. A reader that finds an index entry
     * whose segment is already deregistered simply misses in the
     * registry and gets MYDB_ERR_NOT_FOUND — the correct answer. */
    if (large_wal_index_delete_by_segment(idx, segment_no) != MYDB_OK) return MYDB_ERR;

    *out_freed = 1;
    return MYDB_OK;
}

/* ------------------------------------------------------------------
 * The thread — Appendix A's "LARGE_WAL Archiver"
 * ------------------------------------------------------------------ */

/* Copy out every slot currently sitting at LSEG_DONE.
 *
 * The slot states are read through slot_info(), which takes and
 * releases pool->lock on its own, so no pool->lock is held when
 * copy_out runs. That is required, not stylistic: copy_out's path is
 * reg -> node -> free_slot -> pool, and holding pool->lock first would
 * be the global lock order backwards.
 *
 * A slot can stop being DONE between the snapshot and the copy (nothing
 * else frees slots, but a future second archiver would), so copy_out
 * re-checks the state itself under the lock and returns MYDB_ERR if it
 * changed. A failure here is skipped rather than propagated: one bad
 * segment must not stop the other three draining. */
static void drain_done_slots(LargeWalArchiver *arc, LargeWalSegmentPool *pool,
                              LargeWalRegistry *reg)
{
    for (uint32_t i = 0; i < LARGE_WAL_SEGMENT_POOL_SLOTS; i++) {
        uint8_t  state      = 0;
        uint64_t segment_no = 0;
        if (large_wal_segment_pool_slot_info(pool, i, &segment_no, &state) != MYDB_OK)
            continue;
        if (state != LSEG_DONE) continue;

        (void)large_wal_archiver_copy_out(arc, pool, reg, i);
    }
}

/* Walk the holding area and offer each segment to the gates.
 *
 * Re-reads the directory every pass rather than keeping a list: the
 * files ARE the state (impl doc 10.1 -- "self-describing regardless of
 * which directory a file is found in"), so a fresh readdir needs no
 * reconciliation after a crash, and it is the same walk init already
 * uses. A few dozen entries scanned every 50ms costs nothing next to
 * the 2MB copies this thread also does.
 *
 * segment_end_lsn comes out of each file's own page-0 header, which is
 * the only place it is recorded once the rotation slot has been freed
 * and zeroed. */
static void reap_holding_area(LargeWalArchiver *arc, LargeWalRegistry *reg,
                               LargeWalIndex *idx)
{
    /* Snapshot the pair under pool->lock (see set_gates), then use the
     * locals for the whole pass -- the walk below is long and there is
     * no reason to hold pool->lock across a directory scan. */
    LargeWalGateFn gate_fn  = NULL;
    void          *gate_ctx = NULL;
    if (arc->pool) {
        pthread_mutex_lock(&arc->pool->lock);
        gate_fn  = arc->gate_fn;
        gate_ctx = arc->gate_ctx;
        pthread_mutex_unlock(&arc->pool->lock);
    } else {
        gate_fn  = arc->gate_fn;
        gate_ctx = arc->gate_ctx;
    }

    if (!gate_fn) return;   /* no provider => nothing is ever freed */

    DIR *d = opendir(arc->wal_dir);
    if (!d) return;

    struct dirent *ent;
    while ((ent = readdir(d)) != NULL) {
        uint64_t segment_no = 0;
        if (!parse_archival_name(ent->d_name, &segment_no)) continue;

        char path[300];
        archival_path(arc, segment_no, path, sizeof(path));

        int                   fd = -1;
        LargeWalSegmentHeader hdr;
        if (read_archival_header(path, &fd, &hdr) != MYDB_OK) continue;
        close(fd);   /* only wanted end_lsn; try_free uses the registry's own fd */

        uint64_t checkpoint_lsn = 0;
        int      gate_b         = 0;
        if (gate_fn(gate_ctx, segment_no, hdr.end_lsn,
                    &checkpoint_lsn, &gate_b) != MYDB_OK)
            continue;

        int freed = 0;
        (void)large_wal_archiver_try_free(arc, reg, idx, segment_no, hdr.end_lsn,
                                           checkpoint_lsn, gate_b, &freed);
    }

    closedir(d);
}

int large_wal_archiver_tick(LargeWalArchiver *arc, LargeWalSegmentPool *pool,
                             LargeWalRegistry *reg, LargeWalIndex *idx)
{
    if (!arc || !pool || !reg) return MYDB_ERR;

    drain_done_slots(arc, pool, reg);
    if (idx) reap_holding_area(arc, reg, idx);

    return MYDB_OK;
}

/* now + ms as an absolute CLOCK_REALTIME deadline — same helper shape
 * large_wal_segment_pool.c uses for claim_next_wait, kept local rather
 * than shared since neither file otherwise depends on the other. */
static void deadline_from_now(struct timespec *ts, uint32_t ms)
{
    clock_gettime(CLOCK_REALTIME, ts);
    ts->tv_sec  += (time_t)(ms / 1000u);
    ts->tv_nsec += (long)(ms % 1000u) * 1000000L;
    if (ts->tv_nsec >= 1000000000L) { ts->tv_sec++; ts->tv_nsec -= 1000000000L; }
}

static void *archiver_main(void *argp)
{
    LargeWalArchiver *arc = (LargeWalArchiver *)argp;

    for (;;) {
        /* Wait on the POOL's condvar, not one of our own. mark_done
         * broadcasts slot_done_cv the moment a slot fills, so the
         * common case reacts immediately instead of waiting out a poll
         * interval — and the pool never has to know an archiver is what
         * is listening.
         *
         * Timed, because the holding-area half of the job is driven by
         * the gates, which change on their own with nothing to signal
         * on. The timeout is also what bounds how long stop() waits. */
        pthread_mutex_lock(&arc->pool->lock);
        if (!arc->stop_requested) {
            struct timespec deadline;
            deadline_from_now(&deadline, arc->poll_interval_ms);
            pthread_cond_timedwait(&arc->pool->slot_done_cv, &arc->pool->lock, &deadline);
        }
        int stopping = arc->stop_requested;
        pthread_mutex_unlock(&arc->pool->lock);

        /* One last pass before exiting: a slot that filled while we were
         * being told to stop still deserves copying out, and doing it
         * now saves the next startup the work. */
        (void)large_wal_archiver_tick(arc, arc->pool, arc->registry, arc->idx);

        if (stopping) break;
    }

    return NULL;
}

int large_wal_archiver_start(LargeWalArchiver *arc, LargeWalSegmentPool *pool,
                              LargeWalRegistry *reg, LargeWalIndex *idx)
{
    if (!arc || !pool || !reg) return MYDB_ERR;
    if (arc->started) return MYDB_OK;

    arc->pool     = pool;
    arc->registry = reg;
    arc->idx      = idx;
    if (arc->poll_interval_ms == 0) arc->poll_interval_ms = LARGE_WAL_ARCHIVER_POLL_MS;

    /* No lock/condvar of its own to create — it waits on the pool's,
     * which pool_init already brought up. */
    arc->stop_requested = 0;
    if (pthread_create(&arc->thread, NULL, archiver_main, arc) != 0) return MYDB_ERR;

    arc->started = 1;
    return MYDB_OK;
}

int large_wal_archiver_stop(LargeWalArchiver *arc)
{
    if (!arc || !arc->started) return MYDB_OK;

    /* stop_requested is written under pool->lock because that is the
     * lock the thread's cond_timedwait uses — setting it outside would
     * race the check-then-wait and could park the thread for a full
     * poll interval after it had already been told to stop. */
    pthread_mutex_lock(&arc->pool->lock);
    arc->stop_requested = 1;
    pthread_cond_broadcast(&arc->pool->slot_done_cv);
    pthread_mutex_unlock(&arc->pool->lock);

    pthread_join(arc->thread, NULL);
    arc->started = 0;

    return MYDB_OK;
}
