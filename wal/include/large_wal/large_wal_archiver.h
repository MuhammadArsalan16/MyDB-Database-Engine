#ifndef LARGE_WAL_ARCHIVER_H
#define LARGE_WAL_ARCHIVER_H

#include <stdint.h>
#include "common.h"
#include "large_wal/large_wal_segment_pool.h"
#include "large_wal/large_wal_index.h"

/*
 * large_wal_archiver.h — moves a filled rotation-pool segment into the
 * holding area, resolves large_wal_get() lookups transparently across
 * both locations, and frees a holding-area copy once both freeing gates
 * clear (MYDB_WAL_IMPLEMENTATION.md §10.1).
 *
 * Owns the in-memory (segment_no -> fd) table for holding-area files.
 * Scope note: this table only gains an entry at copy_out() time — a
 * segment_no still resident in the rotation pool (ACTIVE or DONE, not
 * yet copied out) is not resolvable through large_wal_get() in this
 * phase. Widening the registry to track a segment from claim_next()
 * onward is deferred to the writer-thread phase (see this phase's plan,
 * "Concurrency note" -> "Structural consequence") — claim_next()'s only
 * real caller is that not-yet-built thread.
 *
 * Gate A (checkpoint_lsn > segment_end_lsn) and Gate B ("every
 * content_lsn in this segment resolved by the Normal WAL Archiver") are
 * both caller-supplied parameters to try_free() — neither a Checkpointer
 * nor a Normal WAL Archiver exists yet to compute them for real.
 *
 * Concurrency: none built here — see the plan's "Concurrency note".
 */

typedef struct {
    uint64_t segment_no;
    int      fd;
} LargeWalFdEntry;

typedef struct {
    LargeWalFdEntry *entries;   /* growable, linear scan — small N in practice
                                    (in-flight holding-area backlog), not a hot
                                    path, no hash map needed */
    uint32_t          count;
    uint32_t          capacity;
    char              wal_dir[256];
} LargeWalArchiver;

int large_wal_archiver_init(LargeWalArchiver *arc, const char *wal_dir);
int large_wal_archiver_shutdown(LargeWalArchiver *arc);

/* Copy-out: reads the DONE segment's full bytes (via
 * large_wal_segment_pool_read_segment), writes them to
 * wal/large_wal_archival_<segment_no>.mydb with state overwritten to
 * LSEG_ARCHIVING (byte-identical otherwise, per §10.1), fsyncs, THEN
 * (only after that fsync confirms) calls
 * large_wal_segment_pool_free_slot() and registers the new fd in the
 * table. Returns MYDB_ERR if the slot isn't LSEG_DONE. */
int large_wal_archiver_copy_out(LargeWalArchiver *arc, LargeWalSegmentPool *pool,
                                 uint32_t slot_index);

/* Resolves a previously-inserted index entry to its actual content
 * bytes, wherever the segment currently lives in the archiver's table
 * (holding area — see the scope note above for the rotation-pool-
 * resident gap). Strips each page's LargeWalPageHeader while
 * reassembling multi-page content. out_buf must have room for the
 * entry's total_size bytes. Returns MYDB_ERR_NOT_FOUND if content_lsn
 * isn't indexed, or if its segment isn't (yet) in the table. */
int large_wal_get(LargeWalArchiver *arc, const LargeWalIndex *idx,
                   uint64_t content_lsn, uint8_t *out_buf, uint32_t *out_len);

/* Gate A + Gate B, both caller-supplied (see the header doc comment —
 * neither Checkpointer nor Normal WAL Archiver exist yet). If
 * checkpoint_lsn > segment_end_lsn (Gate A) AND gate_b_cleared is true
 * (Gate B, computed however the caller wants — real logic is a later
 * phase's job): unlinks the holding-area file, closes + removes its fd
 * from the table, and prunes every index entry for that segment_no via
 * large_wal_index_delete_by_segment(). Returns MYDB_OK whether or not
 * the segment actually cleared (out_freed reports which) — also
 * MYDB_OK, *out_freed = 0, if segment_no isn't currently in the table
 * (nothing to free). */
int large_wal_archiver_try_free(LargeWalArchiver *arc, LargeWalIndex *idx,
                                 uint64_t segment_no, uint64_t segment_end_lsn,
                                 uint64_t checkpoint_lsn, int gate_b_cleared,
                                 int *out_freed);

#endif /* LARGE_WAL_ARCHIVER_H */
