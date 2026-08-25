#ifndef LARGE_WAL_API_H
#define LARGE_WAL_API_H

#include <stdint.h>
#include "common.h"
#include "large_wal/large_wal_manager.h"
#include "large_wal/large_wal_index.h"

/*
 * large_wal_api.h — large_wal's external contract, mirroring pm_api.h's
 * role for partition_manager: the only thing outside large_wal is
 * meant to include. Takes a LargeWalManager* the same way pm_* takes a
 * PartitionCtx*.
 *
 * large_wal_get()'s real orchestration logic (index lookup + registry
 * resolve + page reassembly) lives directly here, against mgr's own
 * idx/registry fields — genuine cross-module work that doesn't belong
 * inside large_wal_index.c or large_wal_registry.c either, the same
 * reasoning pm_insert's FK-check logic lives in pm_api.c rather than
 * inside schema_file.c or partition.c.
 *
 * large_wal_write() is a direct, one-hop call into
 * large_wal_writer_submit(&mgr->writer, ...) — legitimately thin,
 * because all of that record's real work (fit-check, packing, index
 * insert, state advance) already correctly lives inside the writer's
 * own thread.
 *
 * copy_out()/try_free() are deliberately NOT exposed here — they're
 * large_wal-internal operations for the not-yet-built Archiver thread
 * to call directly against mgr->pool/mgr->registry/mgr->idx/mgr->arc,
 * not something anything outside large_wal should ever call.
 */

int large_wal_get(LargeWalManager *mgr, uint64_t content_lsn,
                   uint8_t *out_buf, uint32_t *out_len);

int large_wal_write(LargeWalManager *mgr, const uint8_t *content, uint32_t total_size,
                     uint64_t content_lsn, uint8_t rec_type, LargeWalIndexEntry *out_entry);

#endif /* LARGE_WAL_API_H */
