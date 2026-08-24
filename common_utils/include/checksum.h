#ifndef CHECKSUM_H
#define CHECKSUM_H

#include <stddef.h>
#include <stdint.h>

/* ------------------------------------------------------------------ */
/*  FNV-1a 32-bit hash. No longer the metadata-file trailer checksum  */
/*  (see crc32() below) — kept for the one remaining consumer that    */
/*  genuinely wants a hash, not a corruption-detection checksum:      */
/*  system_schema.c's name_hash() (username -> hash-table bucket).    */
/*                                                                    */
/*    seed  = 0x811c9dc5                                              */
/*    prime = 0x01000193                                              */
/*                                                                    */
/*  Originally lived in database_file.c and partition.c as static     */
/*  helpers; lifted here once schema_file.c became the third          */
/*  consumer.                                                         */
/* ------------------------------------------------------------------ */
uint32_t fnv1a(const void *data, size_t len);

/* ------------------------------------------------------------------ */
/*  CRC32 (reflected, polynomial 0xEDB88320 / IEEE 802.3 — the same   */
/*  algorithm InnoDB and zlib use). This is the trailer checksum for  */
/*  every fixed-size MyDB metadata file (__database.mydb,             */
/*  __catalog.mydb, __schema.mydb, __stats.mydb, users.mydb,          */
/*  privileges.mydb) and for WAL page/record/segment headers          */
/*  (MYDB_WAL_DESIGN.md / MYDB_WAL_IMPLEMENTATION.md both specify     */
/*  CRC32 explicitly, distinct from the FNV-1a used elsewhere).       */
/*  Table-driven, 256-entry lookup table computed lazily on first     */
/*  call.                                                             */
/* ------------------------------------------------------------------ */
uint32_t crc32(const void *data, size_t len);

#endif /* CHECKSUM_H */
