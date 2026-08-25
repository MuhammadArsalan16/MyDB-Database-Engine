#ifndef LARGE_WAL_REGISTRY_H
#define LARGE_WAL_REGISTRY_H

#include <stdint.h>
#include "common.h"

/*
 * large_wal_registry.h — the (segment_no -> fd) table spanning a
 * segment's whole life, extracted out of large_wal_archiver (it stopped
 * being archiver-specific the moment large_wal_writer started
 * registering into it too — this is shared infrastructure both the
 * writer and the archiver reach into, not archiver-owned).
 *
 * large_wal_writer (large_wal_writer.h) calls register() immediately
 * after every claim_next(), passing the rotation slot's own
 * process-lifetime fd (owns_fd = 0 — the pool's own shutdown closes it,
 * not this registry). large_wal_archiver's copy_out() (large_wal_
 * archiver.h) calls register() again to repoint the same entry at the
 * holding-area fd once the segment migrates (owns_fd = 1 — this
 * registry opened that fd itself and is responsible for closing it).
 * A single fd-keyed table works for both locations with no separate
 * location tag: a rotation slot's fd and a holding-area file's fd are
 * just two different fds the same segment_no points at over its
 * lifetime.
 *
 * owns_fd exists to fix a latent bug the writer's widening introduced:
 * blindly closing every entry's fd on shutdown was correct back when
 * every entry was archiver-opened (Phase 3), but wrong once pool-owned
 * rotation fds joined the same table — closing those here would race
 * the pool's own shutdown closing them a second time.
 */

typedef struct {
    uint64_t segment_no;
    int      fd;
    int      owns_fd;   /* 1 = this registry closes fd on shutdown/remove
                            (archiver-opened holding-area file); 0 = fd is
                            borrowed from the pool's own lifecycle */
} LargeWalRegistryEntry;

typedef struct {
    LargeWalRegistryEntry *entries;   /* growable, linear scan — small N in
                                          practice, not a hot path */
    uint32_t                 count;
    uint32_t                 capacity;
} LargeWalRegistry;

int large_wal_registry_init(LargeWalRegistry *reg);

/* Closes every entry whose owns_fd == 1, leaves borrowed (owns_fd == 0)
 * entries untouched — their owner (the segment pool) closes those
 * itself. Frees the entries array either way. */
int large_wal_registry_shutdown(LargeWalRegistry *reg);

/* Registers segment_no -> fd if not already present, else updates the
 * existing entry's fd (and owns_fd). */
int large_wal_registry_register(LargeWalRegistry *reg, uint64_t segment_no, int fd, int owns_fd);

/* Copies the matching fd into *out_fd. Returns MYDB_ERR_NOT_FOUND if
 * segment_no was never registered. */
int large_wal_registry_lookup(const LargeWalRegistry *reg, uint64_t segment_no, int *out_fd);

/* Removes the entry for segment_no (swap-with-last; order doesn't
 * matter for a linear-scan table). Does NOT close its fd — the caller
 * (large_wal_archiver_try_free, which already knows whether it opened
 * this fd) is responsible for that. A no-op, still MYDB_OK, if
 * segment_no isn't registered. */
int large_wal_registry_remove(LargeWalRegistry *reg, uint64_t segment_no);

#endif /* LARGE_WAL_REGISTRY_H */
