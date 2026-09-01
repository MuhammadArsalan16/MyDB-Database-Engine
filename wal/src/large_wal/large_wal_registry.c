#include "large_wal/large_wal_registry.h"

#include <unistd.h>
#include <string.h>
#include <stdlib.h>

/* Walks the list for segment_no. Caller must already hold reg->lock in
 * either mode — this does no locking of its own. */
static LargeWalRegistryNode *find_node(LargeWalRegistry *reg, uint64_t segment_no)
{
    for (LargeWalRegistryNode *n = reg->head; n; n = n->next) {
        if (n->segment_no == segment_no) return n;
    }
    return NULL;
}

int large_wal_registry_init(LargeWalRegistry *reg)
{
    if (!reg) return MYDB_ERR;
    memset(reg, 0, sizeof(*reg));

    /* Writer-preferring: see the header's note on why glibc's default
     * would let a read storm starve register()/remove() forever. */
    pthread_rwlockattr_t attr;
    if (pthread_rwlockattr_init(&attr) != 0) return MYDB_ERR;
#ifdef PTHREAD_RWLOCK_PREFER_WRITER_NONRECURSIVE_NP
    pthread_rwlockattr_setkind_np(&attr, PTHREAD_RWLOCK_PREFER_WRITER_NONRECURSIVE_NP);
#endif
    int rc = pthread_rwlock_init(&reg->lock, &attr);
    pthread_rwlockattr_destroy(&attr);

    return (rc == 0) ? MYDB_OK : MYDB_ERR;
}

int large_wal_registry_shutdown(LargeWalRegistry *reg)
{
    if (!reg) return MYDB_ERR;

    LargeWalRegistryNode *n = reg->head;
    while (n) {
        LargeWalRegistryNode *next = n->next;
        if (n->owns_fd && n->fd >= 0) close(n->fd);
        pthread_mutex_destroy(&n->lock);
        free(n);
        n = next;
    }

    reg->head  = NULL;
    reg->count = 0;
    pthread_rwlock_destroy(&reg->lock);
    return MYDB_OK;
}

int large_wal_registry_register(LargeWalRegistry *reg, uint64_t segment_no, int fd, int owns_fd)
{
    if (!reg) return MYDB_ERR;

    pthread_rwlock_wrlock(&reg->lock);

    LargeWalRegistryNode *n = find_node(reg, segment_no);
    if (n) {
        n->fd      = fd;
        n->owns_fd = owns_fd;
        pthread_rwlock_unlock(&reg->lock);
        return MYDB_OK;
    }

    n = malloc(sizeof(*n));
    if (!n) {
        pthread_rwlock_unlock(&reg->lock);
        return MYDB_ERR;
    }
    if (pthread_mutex_init(&n->lock, NULL) != 0) {
        free(n);
        pthread_rwlock_unlock(&reg->lock);
        return MYDB_ERR;
    }

    n->segment_no = segment_no;
    n->fd         = fd;
    n->owns_fd    = owns_fd;

    /* Push at the head: one pointer store, and it leaves every existing
     * node exactly where it is — the property the array version could
     * not offer, since its realloc copied all of them. */
    n->next   = reg->head;
    reg->head = n;
    reg->count++;

    pthread_rwlock_unlock(&reg->lock);
    return MYDB_OK;
}

int large_wal_registry_acquire(LargeWalRegistry *reg, uint64_t segment_no,
                                LargeWalRegistryNode **out_node, int *out_fd)
{
    if (!reg || !out_node || !out_fd) return MYDB_ERR;

    pthread_rwlock_rdlock(&reg->lock);

    LargeWalRegistryNode *n = find_node(reg, segment_no);
    if (!n) {
        /* Nothing acquired, so nothing to hold — drop the read lock and
         * report the miss. */
        pthread_rwlock_unlock(&reg->lock);
        *out_node = NULL;
        return MYDB_ERR_NOT_FOUND;
    }

    /* The read lock stays held past this return, deliberately: it is
     * what guarantees the node cannot be unlinked and freed while the
     * caller is inside its mutex. release() drops both. */
    pthread_mutex_lock(&n->lock);
    *out_node = n;
    *out_fd   = n->fd;
    return MYDB_OK;
}

void large_wal_registry_release(LargeWalRegistry *reg, LargeWalRegistryNode *node)
{
    if (!reg || !node) return;
    pthread_mutex_unlock(&node->lock);
    pthread_rwlock_unlock(&reg->lock);
}

int large_wal_registry_set_fd(LargeWalRegistryNode *node, int fd, int owns_fd)
{
    if (!node) return MYDB_ERR;
    /* No locking here — the caller holds this node via acquire(). */
    node->fd      = fd;
    node->owns_fd = owns_fd;
    return MYDB_OK;
}

int large_wal_registry_lookup(LargeWalRegistry *reg, uint64_t segment_no, int *out_fd)
{
    if (!reg || !out_fd) return MYDB_ERR;

    pthread_rwlock_rdlock(&reg->lock);
    LargeWalRegistryNode *n = find_node(reg, segment_no);
    if (n) {
        /* The node mutex is needed for the fd read itself, not just to
         * keep the node alive: set_fd writes this exact field while
         * holding it, so reading it under the list read lock alone is a
         * genuine data race (ThreadSanitizer flags it as one). The read
         * lock protects the LIST -- head/next/count -- and nothing
         * inside a node.
         *
         * Taking it here cannot deadlock: it is the same reg -> node
         * order acquire() uses, and no caller of lookup() holds a node
         * mutex already (try_free, its only non-test caller, calls this
         * before it acquires anything). */
        pthread_mutex_lock(&n->lock);
        *out_fd = n->fd;
        pthread_mutex_unlock(&n->lock);
    }
    pthread_rwlock_unlock(&reg->lock);

    return n ? MYDB_OK : MYDB_ERR_NOT_FOUND;
}

int large_wal_registry_remove(LargeWalRegistry *reg, uint64_t segment_no,
                               int *out_fd, int *out_owns_fd)
{
    if (!reg) return MYDB_ERR;

    pthread_rwlock_wrlock(&reg->lock);

    LargeWalRegistryNode **link = &reg->head;
    while (*link && (*link)->segment_no != segment_no)
        link = &(*link)->next;

    LargeWalRegistryNode *n = *link;
    if (!n) {
        pthread_rwlock_unlock(&reg->lock);
        return MYDB_OK;   /* not present -- harmless no-op */
    }

    if (out_fd)      *out_fd      = n->fd;
    if (out_owns_fd) *out_owns_fd = n->owns_fd;

    *link = n->next;
    reg->count--;

    /* Safe to destroy and free here, not merely likely to be: we hold
     * the WRITE lock, and a node mutex is only ever taken under the READ
     * lock (see the header's invariant), so no thread can be inside this
     * one. That is the entire reason this design needs no refcount. */
    pthread_mutex_destroy(&n->lock);
    free(n);

    pthread_rwlock_unlock(&reg->lock);
    return MYDB_OK;
}
