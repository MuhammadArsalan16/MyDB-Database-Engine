#ifndef LARGE_WAL_SEGMENT_H
#define LARGE_WAL_SEGMENT_H

#include <stdint.h>
#include "common.h"
#include "file_header.h"

/*
 * large_wal_segment.h — LARGE_WAL segment header format only
 * (MYDB_WAL_IMPLEMENTATION.md §10.2). large_wal's own struct — mirrors
 * normal_wal/wal_segment.h exactly: WalSegmentHeader and
 * LargeWalSegmentHeader are documented as "the same shape" (§10.2), just
 * a different state enum (LSEG_* — three rotation-lifecycle states plus
 * one holding-area-only state, §10.1: "This is why rotation slots have
 * no ARCHIVING state at all").
 *
 * Page-level format (LargeWalPageHeader) lives in large_wal_page.h, not
 * here; the pool of rotation slots + holding area (fallocate, copy-out,
 * the in-memory (segment_no -> fd) table) live in a later
 * large_wal_segment_pool.h — each file owns only its own concern, same
 * split normal_wal already established.
 *
 * LargeWalSegmentHeader is a plain struct (not __attribute__((packed)))
 * — same reasoning as every other on-disk struct in this codebase: the
 * on-disk byte layout is produced/consumed field-by-field in
 * large_wal_segment.c via explicit offsets, not via this struct's
 * in-memory layout.
 */

/* ------------------------------------------------------------------
 * Rotation lifecycle: LSEG_FREE -> LSEG_ACTIVE -> LSEG_DONE -> (copy
 * out to the large_wal/ holding area, fsync) -> LSEG_FREE. Unlike
 * normal WAL's SEG_DONE -> SEG_ARCHIVING transition, this hand-off is
 * NOT gated on checkpoint_lsn — the moment a rotation slot's segment
 * fills, it's copied out immediately and the slot resets right away
 * (impl doc §10.1). LSEG_ARCHIVING is stamped only on the holding-area
 * copy (byte-identical file, only state changed) — never observed on a
 * rotation-pool segment itself.
 * ------------------------------------------------------------------ */
typedef enum {
    LSEG_FREE      = 0,
    LSEG_ACTIVE    = 1,
    LSEG_DONE      = 2,
    LSEG_ARCHIVING = 3
} LargeWalSegState;

/* ------------------------------------------------------------------
 * LargeWalSegmentHeader — on-disk shape is 48 bytes, same field layout
 * as WalSegmentHeader (normal_wal/wal_segment.h). Occupies page 0 of
 * each LARGE_WAL segment file, padded to a 64-byte on-disk footprint —
 * see LARGE_WAL_SEGMENT_HEADER_ON_DISK_SIZE below.
 * ------------------------------------------------------------------ */
typedef struct {
    FileHeaderId id;
    uint64_t segment_no;       /* monotonic, never reused                */
    uint64_t start_lsn;
    uint64_t end_lsn;
    uint32_t partition_id;
    uint32_t data_pages;       /* valid 16KB content pages written       */
    uint8_t  state;            /* LargeWalSegState                       */
    uint8_t  reserved[3];
    uint32_t checksum;         /* CRC32 over the preceding 44 bytes      */
} LargeWalSegmentHeader;

#define LARGE_WAL_SEGMENT_HEADER_ON_DISK_SIZE  64   /* on-disk footprint: 48 documented bytes + 16 padding */

/* ------------------------------------------------------------------
 * Serialize/deserialize + checksum — same contract as
 * wal_segment_header_serialize/deserialize (normal_wal/wal_segment.h).
 * ------------------------------------------------------------------ */
void large_wal_segment_header_serialize(const LargeWalSegmentHeader *hdr, uint8_t *buf);
int  large_wal_segment_header_deserialize(const uint8_t *buf, LargeWalSegmentHeader *out);

#endif /* LARGE_WAL_SEGMENT_H */
