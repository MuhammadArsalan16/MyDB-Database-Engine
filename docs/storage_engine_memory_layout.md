# StorageEngine In-Memory Size

Computed with a `sizeof()` probe compiled directly against
`storage_engine/include/*.h` (not hand-derived), to get exact
compiler-applied padding/alignment. One `StorageEngine` instance is
embedded per `PartitionCtx` (see `V3_DESIGN.md`), so this is the
per-active-partition memory cost of the storage layer alone (buffer
pool + open-table cache), not counting `Catalog`, `PartitionBuffer`,
or `TransactionManager`.

Relevant constants (`storage_engine/include/common.h`):

| constant | value | note |
|---|---|---|
| `PAGE_SIZE` | 16384 (16 KB) | matches InnoDB |
| `BUFFER_POOL_SIZE` | 4096 | frame count; `4096 × 16 KB = 64 MB` of raw page data |
| `MAX_TABLES` | 64 | open-table cache slots |
| `MAX_SECONDARY_IDX` | 8 | secondary `BTree` handles per `OpenTable` |

> **Doc/comment drift:** `buffer_pool.h` (lines 11, 36) and CLAUDE.md's
> architecture section both say "64 frames." The actual constant is
> 4096. `4096 × 16 KB = 64 MB`, matching the "64MB buffer pool size per
> partition" comment next to `BUFFER_POOL_SIZE` in `common.h` — so the
> constant is correct and the "64 frames" comments are stale. Flagged
> for a doc fix pass.

## `BPFrame` (`storage_engine/include/buffer_pool.h`)

One cache frame holding a single page.

| field | type | size |
|---|---|---|
| `data` | `uint8_t[PAGE_SIZE]` | 16384 |
| `page_no` | `uint32_t` | 4 |
| `table_id` | `int` | 4 |
| `dm` | `DiskManager *` | 8 |
| `pin_count` | `uint16_t` | 2 |
| `is_dirty` | `uint8_t` | 1 |
| `is_valid` | `uint8_t` | 1 |
| **subtotal** | | 16404 |
| struct padding (align to 8, for the `dm` pointer) | | 4 |
| **`sizeof(BPFrame)`** | | **16408** |

## `BufferPool` (`storage_engine/include/buffer_pool.h`)

| field | type | size |
|---|---|---|
| `frames` | `BPFrame[BUFFER_POOL_SIZE]` = `BPFrame[4096]` | 16408 × 4096 = 67,223,552 |
| `lru_order` | `int[BUFFER_POOL_SIZE]` = `int[4096]` | 4 × 4096 = 16,384 |
| `num_valid` | `int` | 4 |
| **`sizeof(BufferPool)`** | | **67,223,560** |

(`num_valid` adds 4 bytes with no extra padding needed — the preceding
`lru_order` array already ends on a 4-byte boundary and the struct's
overall alignment requirement, 8 bytes from `BPFrame.dm`, is already
satisfied by the frames array being a multiple of 8.)

## `OpenTable` (`storage_engine/include/storage.h`)

One entry in the open-table cache — one clustered `BTree` + up to
`MAX_SECONDARY_IDX` secondary `BTree` handles, sharing one
`DiskManager` (one `.mydb` file per table).

`BTree` itself (`storage_engine/include/btree.h`):

| field | type | size |
|---|---|---|
| `bp` | `BufferPool *` | 8 |
| `dm` | `DiskManager *` | 8 |
| `table_id` | `int` | 4 |
| `root_page_no` | `uint32_t` | 4 |
| `key_type` | `DataType` (enum, 4 bytes) | 4 |
| `is_secondary` | `uint8_t` | 1 |
| padding (align to 8) | | 3 |
| **`sizeof(BTree)`** | | **32** |

`OpenTable`:

| field | type | size |
|---|---|---|
| `name` | `char[MAX_TABLE_NAME]` = `char[64]` | 64 |
| `schema_name` | `char[32]` | 32 |
| `id` | `int` | 4 |
| `dm` | `DiskManager` (embedded, not a pointer) | 264 |
| `clustered` | `BTree` | 32 |
| `secondary` | `BTree[MAX_SECONDARY_IDX]` = `BTree[8]` | 32 × 8 = 256 |
| `is_open` | `int` | 4 |
| **subtotal** | | 656 |
| struct padding (align to 8) | | 8 |
| **`sizeof(OpenTable)`** | | **664** |

Note the dominant contributor is the embedded `DiskManager` (264
bytes), not the nine `BTree` handles combined (288 bytes) — both are
comparable in weight, contrary to what you'd guess from field count
alone.

## `StorageEngine` (`storage_engine/include/storage.h`)

| field | type | size |
|---|---|---|
| `partition_path` | `char[256]` | 256 |
| `bp` | `BufferPool` | 67,223,560 |
| `open_tables` | `OpenTable[MAX_TABLES]` = `OpenTable[64]` | 664 × 64 = 42,496 |
| `num_open` | `int` | 4 |
| `next_table_id` | `int` | 4 |
| `initialized` | `int` | 4 |
| `last_written_dm` | `DiskManager *` | 8 |
| **subtotal** | | 67,266,332 |
| tail padding (struct ends on 8-byte boundary, from the trailing pointer) | | 4 |
| **`sizeof(StorageEngine)`** | | **67,266,336 bytes ≈ 64.15 MB** |

## Summary

```
64 MB   (67,108,864 B)  raw page data:  4096 frames x 16 KB
+ ~157 KB (157,472 B)   bookkeeping overhead:
                          - 98,304 B  per-frame metadata (page_no/table_id/dm/pin_count/is_dirty/is_valid + padding), 24 B x 4096 frames
                          - 16,384 B  BufferPool.lru_order[4096]
                          - 42,496 B  open_tables[64] (OpenTable cache)
                          -    288 B  StorageEngine's own scalar fields + padding
= 67,266,336 B  (~64.15 MB) per StorageEngine instance
```

One `StorageEngine` is embedded per `PartitionCtx`, so a partition's
storage-layer memory footprint scales linearly with the number of
simultaneously active partitions (`EngineState.partitions[MAX_PARTITIONS]`)
— each active partition pays this ~64.15 MB independently, since there
is no shared buffer pool across partitions (v3 design; see "Storage
v3" in CLAUDE.md).

---

*Generated during storage_engine code review, 2026-08-12. Sizes
verified by compiling a `sizeof()` probe against the real headers
(`gcc -Istorage_engine/include`), not hand-computed, to avoid
alignment/padding mistakes on a 64-bit target.*
