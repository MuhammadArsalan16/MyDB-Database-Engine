#ifndef LARGE_WAL_ARCHIVER_H
#define LARGE_WAL_ARCHIVER_H

#include <stdint.h>
#include "common.h"
#include "large_wal/large_wal_segment_pool.h"
#include "large_wal/large_wal_index.h"
#include "large_wal/large_wal_registry.h"

/*
 * large_wal_archiver.h — moves a filled rotation-pool segment into the
 * holding area, and frees a holding-area copy once both freeing gates
 * clear (MYDB_WAL_IMPLEMENTATION.md §10.1). Genuinely archiver-only
 * work — the (segment_no -> fd) table this used to own moved out to
 * large_wal_registry.h once large_wal_writer started registering into
 * it too (it was never archiver-specific, just archiver-opened by
 * coincidence of build order). copy_out()/try_free() now take an
 * explicit LargeWalRegistry* instead of an implicit arc-owned one.
 *
 * LargeWalArchiver itself shrinks to just wal_dir — kept as a real
 * struct rather than eliminated, since it's the natural home for the
 * not-yet-built Archiver thread (Appendix A) to grow into later, the
 * same shape large_wal_writer.h already established for its own thread.
 *
 * Gate A (checkpoint_lsn > segment_end_lsn) and Gate B ("every
 * content_lsn in this segment resolved by the Normal WAL Archiver") are
 * both caller-supplied parameters to try_free() — neither a Checkpointer
 * nor a Normal WAL Archiver exists yet to compute them for real.
 *
 * large_wal_get() does NOT live here — it's large_wal's external
 * read-path contract, not archiver-internal plumbing. See
 * large_wal_api.h.
 *
 * Concurrency: none built here — see this session's Concurrency notes
 * (Phase 3's plan).
 */

typedef struct {
    char wal_dir[256];
} LargeWalArchiver;

int large_wal_archiver_init(LargeWalArchiver *arc, const char *wal_dir);
int large_wal_archiver_shutdown(LargeWalArchiver *arc);

/* Copy-out: reads the DONE segment's full bytes (via
 * large_wal_segment_pool_read_segment), writes them to
 * wal/large_wal_archival_<segment_no>.mydb with state overwritten to
 * LSEG_ARCHIVING (byte-identical otherwise, per §10.1), fsyncs, THEN
 * (only after that fsync confirms) calls
 * large_wal_segment_pool_free_slot() and registers the new fd in reg
 * (large_wal_registry_register(reg, segment_no, fd, owns_fd=1) —
 * repointing the entry large_wal_writer already registered at claim
 * time, not leaving a stale second one). Returns MYDB_ERR if the slot
 * isn't LSEG_DONE. */
int large_wal_archiver_copy_out(LargeWalArchiver *arc, LargeWalSegmentPool *pool,
                                 LargeWalRegistry *reg, uint32_t slot_index);

/* Gate A + Gate B, both caller-supplied. If checkpoint_lsn >
 * segment_end_lsn (Gate A) AND gate_b_cleared is true (Gate B): unlinks
 * the holding-area file, closes + removes its fd from reg, and prunes
 * every index entry for that segment_no via
 * large_wal_index_delete_by_segment(). Confirms the holding-area file
 * actually exists on disk before touching anything — reg's entry for
 * segment_no could still be a rotation slot's own live fd if this
 * segment was never actually archived, and closing that would break
 * the live pool. Returns MYDB_OK whether or not the segment actually
 * cleared (out_freed reports which). */
int large_wal_archiver_try_free(LargeWalArchiver *arc, LargeWalRegistry *reg, LargeWalIndex *idx,
                                 uint64_t segment_no, uint64_t segment_end_lsn,
                                 uint64_t checkpoint_lsn, int gate_b_cleared,
                                 int *out_freed);

#endif /* LARGE_WAL_ARCHIVER_H */
