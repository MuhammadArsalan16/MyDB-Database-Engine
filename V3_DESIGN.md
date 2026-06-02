# MYDB v3 — Proposed Redesign (Pre-WAL)

This document captures the proposed architectural changes for v3. No code has been
written yet. The goal is to introduce a **partition manager layer** between the engine
and the storage engine, which makes WAL implementation clean, enables true multi-user
isolation, and removes the static global `StorageState` from storage.c.

---

## Motivation

In v2 the engine holds a single `EngineState` with one active partition, one active
schema, and one storage engine instance (a `static StorageState g` singleton). This
works for one user at a time but has three structural problems:

1. **No multi-user support.** A second login would stomp on the same `StorageState`.
2. **Transaction manager is in the wrong place.** After WAL it will be entirely
   concerned with log records, LSNs, and WAL file I/O — none of which belongs
   inside the storage engine.
3. **Cache 2 is a single slot.** `USE` forces a flush-and-close of the old schema
   before opening the new one. Multiple schemas cannot be held open simultaneously
   per partition.

---

## Four-Level Runtime Hierarchy

```
Engine
└── PartitionCtx  (one per active partition)
    └── StorageEngine  (one per PartitionCtx)
        └── BufferPool  (stays inside StorageEngine)
```

---

## Full Architecture Diagram

```
                        bin/mydb
                           |
                    engine_execute_sql(sql)
                           |
               +-----------+------------+
               |          Engine        |
               |                        |
               |  DatabaseFile          |  __database.mydb (global)
               |  SystemSchema          |  users.mydb + privileges.mydb (global)
               |                        |
               |  ConnectionPool        |  master pool — all sessions
               |    conn_0 (root)       |
               |    conn_1 (alice)      |  each conn carries partition_id
               |    conn_2 (root)       |
               |    ...                 |
               +-----------+------------+
                           |
                  routes conn → PartitionCtx
                           |
          +----------------+----------------+
          |                                 |
  +-------+--------+               +--------+-------+
  | PartitionCtx   |               | PartitionCtx   |
  | (root)         |               | (alice)        |
  |                |               |                |
  | SubConnPool    |               | SubConnPool    |  slice of engine pool
  | [conn_0,conn_2]|               | [conn_1]       |
  |                |               |                |
  | Catalog        |               | Catalog        |  Cache 1: __catalog.mydb
  |                |               |                |
  | PartitionBuf   |               | PartitionBuf   |  Cache 2: open SchemaFiles
  |  schema_A.sf   |               |  schema_X.sf   |  LRU cache of SchemaFile
  |  schema_B.sf   |               |                |  handles, capacity ~8 slots
  |                |               |                |
  | TxnManager     |               | TxnManager     |  promoted from StorageEngine
  |  wal_buffer[]  |               |  wal_buffer[]  |  owns WAL entirely post-WAL
  |  flushed_lsn   |               |  flushed_lsn   |
  |  next_lsn      |               |  next_lsn      |
  |  active_txns[] |               |  active_txns[] |  one TxnContext per conn
  |                |               |                |
  | StorageEngine  |               | StorageEngine  |  one instance per partition
  |  BufferPool    |               |  BufferPool    |  stays inside StorageEngine
  |  OpenTableCache|               |  OpenTableCache|
  |  DiskManager[] |               |  DiskManager[] |
  +----------------+               +----------------+
```

---

## Layer Responsibilities

### Engine

- Owns the master `ConnectionPool` (all connections across all partitions).
- Holds global-only state: `DatabaseFile` and `SystemSchema`.
- Routes each query: given a connection, finds its `partition_id`, looks up or
  lazily loads the corresponding `PartitionCtx`, then calls:
  ```
  1. parser_parse(sql)
  2. exec_engine_execute(PartitionCtx*, ast, out, cap)
  3. parser_free_ast(ast)
  ```
- Loads a `PartitionCtx` on the first login to a partition. Keeps it alive while
  its `SubConnPool` is non-empty. May evict it (after flushing) when idle.
- Does not own any schema or table data.

### PartitionCtx

Replaces the per-partition fields currently scattered across `EngineState`.
One instance per active partition, shared by all connections that belong to it.

| Member | Description |
|--------|-------------|
| `partition_id` | Numeric partition ID |
| `partition_path` | Absolute path to partition directory |
| `SubConnPool` | Slice of engine's master pool for this partition |
| `Catalog` | Cache 1: `__catalog.mydb` — schema list, quota, used_bytes |
| `PartitionBuffer` | Cache 2: LRU cache of open `SchemaFile` handles |
| `TxnManager` | Promoted from storage; WAL owner post-WAL |
| `StorageEngine` | One storage engine instance for this partition |

### PartitionBuffer (Cache 2)

Currently `EngineState.active_schema` is a single `SchemaFile` slot. `USE` forces
a flush-and-close of the old schema before opening a new one.

In v3, `PartitionBuffer` is a proper LRU cache of `SchemaFile` handles:

```
PartitionBuffer
  capacity: ~8 slots
  slot_0: SchemaFile* for "shop"    (most recently used)
  slot_1: SchemaFile* for "archive"
  ...

USE shop:
  if "shop" already in cache  →  bump LRU, return pointer (no I/O)
  else  →  open __schema.mydb, insert at MRU position
           if cache full: evict LRU slot
             - flush dirty pages of that schema's tables in BufferPool
             - close SchemaFile fd
```

Connections on the same partition switching between schemas pay no file-open cost
after the first access.

### TransactionManager (promoted from StorageEngine)

In v2 the `TransactionManager` lives inside `StorageState` and tracks which
`DiskManager` handles were written in the current transaction (for rollback). This
is the minimal pre-WAL model.

In v3 it is promoted to `PartitionCtx`. Its responsibilities expand when WAL is added:

```
TxnManager responsibilities:

Pre-WAL (current scope):
  - Assign transaction IDs
  - Track per-connection TxnContext (state, dirty RIDs for rollback)
  - Manage begin / commit / rollback across multiple connections

Post-WAL (future scope):
  - Own the WAL ring buffer (in-memory log)
  - Assign LSNs monotonically
  - fsync WAL file on commit (durability before acking client)
  - Enforce WAL-before-page: buffer pool eviction must confirm
    page.lsn <= flushed_lsn before writing a dirty frame to disk
  - Apply undo records on rollback
  - Replay WAL from last checkpoint on recovery
```

The storage engine calls back into `TxnManager` for two things only:
- `txn_alloc_lsn()` — get the next log sequence number when a page is dirtied
- `txn_lsn_is_flushed(lsn)` — check before evicting a dirty page

### StorageEngine (inside PartitionCtx)

One instance per `PartitionCtx`. Structurally identical to the current `StorageState`
except:
- The `TransactionManager` field is removed (promoted to `PartitionCtx`).
- The `EngineState*` pointer is replaced by a `PartitionCtx*` pointer.
- Initialized via `storage_init(PartitionCtx*)` instead of `storage_init(EngineState*)`.
- All storage API function signatures are **unchanged**.

```
StorageEngine
  BufferPool         64-frame LRU cache (stays here, not promoted)
  OpenTableCache     open DiskManager handles for recently accessed tables
  PartitionCtx*      back-pointer for filesystem paths and schema lookups
```

---

## Connection Pool Relationship

The sub-pool is not a copy of connection objects. Each connection lives once in the
engine's master pool. The partition's sub-pool holds pointers into that master pool.

```
Engine.ConnectionPool  (master)
  [conn_0]  partition_id = root   ──┐
  [conn_1]  partition_id = alice  ──┼──┐
  [conn_2]  partition_id = root   ──┘  │
                                   │   │
PartitionCtx(root).SubConnPool     │   │
  [&conn_0, &conn_2]  ←────────────┘   │
                                        │
PartitionCtx(alice).SubConnPool         │
  [&conn_1]           ←─────────────────┘
```

On connection close: remove from sub-pool and master pool.
When sub-pool empties: `PartitionCtx` may be evicted from memory (after flush).

---

## Stats Files: Moved to system_schema/

Stats files (`__stats.mydb`) are relocated from the schema directory into
`system_schema/stats/`. Rationale:

- **Read-mostly:** written only by `ANALYZE TABLE`, read on every SELECT by the
  planner. Same usage pattern as `privileges.mydb`.
- **System metadata:** stats describe *properties of data*, not the data itself.
  Analogous to `pg_statistic` in PostgreSQL's system catalog.
- **Bounded at engine level:** 16 partitions max keeps the file count manageable
  from the engine. Engine can hold all open handles in a flat `StatsBuffer` array.
- **Planner access:** planner is logically part of the execution engine and must
  not reach into `PartitionBuffer` (Cache 2), which is private to the partition
  manager. Engine manages stats handles and passes them through `ExecContext`.

### File Location

```
system_schema/
  users.mydb
  privileges.mydb
  stats/
    stats_<partition_id>_<schema_name>.mydb    one per schema
```

`partition_id` is the engine-assigned uint32, globally unique and never reused.
Together with `schema_name` the filename is unique across the entire installation.
`FILETYPE_STATS = 7` is unchanged.

### Engine StatsBuffer

Engine adds a `StatsBuffer` alongside `SystemSchema`:

```
Engine
  DatabaseFile
  SystemSchema          users.mydb + privileges.mydb
  StatsBuffer           system_schema/stats/*.mydb, lazy-opened, engine-managed
    entry[]
      partition_id      uint32
      schema_name[32]
      StatsFile*        NULL until first access
```

Lifecycle:
- **Open:** lazily on first SELECT (or ANALYZE) that targets a schema with stats.
- **Create:** ANALYZE TABLE asks the engine to get-or-create the handle; engine
  creates `stats_<pid>_<name>.mydb` in `system_schema/stats/` if absent.
- **Delete:** DROP DATABASE (schema) removes the matching entry and deletes the
  file. DROP USER removes all entries for that `partition_id`.
- **Close:** engine shutdown, or when the owning partition is fully evicted.

### ExecContext

```c
typedef struct {
    PartitionCtx *partition;   /* storage, catalog, schema (Cache 1 + Cache 2) */
    TxnContext   *txn;         /* this connection's transaction state */
    StatsFile    *stats;       /* engine StatsBuffer entry; NULL = no ANALYZE yet */
} ExecContext;
```

Engine resolves all three fields before every `exec_engine_execute` call.
The planner reads from `ectx->stats`. ANALYZE writes to `ectx->stats`.
Cache 2 (`PartitionBuffer`) is never exposed outside the partition manager.

### ANALYZE Write Path

ANALYZE reads table data through the partition (storage engine, buffer pool) and
writes stats through the engine-managed handle. No new coupling: ExecContext
already carries both pointers.

```
ANALYZE TABLE orders:
  read:   ectx->partition  →  storage_scan(rel)   (partition-local)
  write:  ectx->stats      →  stats_save_relation  (system_schema/stats/)
```

### Memory (StatsBuffer)

Stats pages are lazy-loaded per relation inside each open StatsFile:

```
Per loaded relation stats page:              ~16 KB
Realistic (16 partitions, 5 schemas each,
           10 hot tables per schema):   16 × 5 × 10 × 16 KB = 12.8 MB
Minimum (1 hot table per partition):    16 × 1 × 16 KB       =  256 KB
```

---

## What Changes vs v2

| Component | v2 | v3 |
|-----------|----|----|
| `StorageState` | `static g` singleton in storage.c | Member of `PartitionCtx`; one per active partition |
| `BufferPool` | Inside `StorageState` | Stays inside `StorageEngine` (unchanged location) |
| `TransactionManager` | Inside `StorageState` | Promoted to `PartitionCtx`; becomes WAL owner |
| `EngineState.active_schema` | Single `SchemaFile` slot (Cache 2) | `PartitionBuffer`: LRU cache of multiple `SchemaFile` handles, private to partition manager |
| `EngineState.active_catalog` | Single `Catalog` slot (Cache 1) | Moves to `PartitionCtx.Catalog` |
| `__stats.mydb` location | `<partition>/<schema>/__stats.mydb` | `system_schema/stats/stats_<pid>_<schema>.mydb` |
| Stats handle ownership | Opened per-query inside planner | Engine-managed `StatsBuffer`; handle passed via `ExecContext` |
| Connection model | One session, one `EngineState` | Master pool + per-partition sub-pools |
| `exec_engine_execute` signature | `(EngineState*, ast)` | `(ExecContext*, ast)` |
| `storage_init` | Takes `EngineState*`, sets `static g` | Takes `PartitionCtx*`, fills `ctx->storage` |
| Storage API function signatures | e.g. `storage_insert(rel, row)` | **Unchanged** |
| Parser | Stateless, unchanged | Unchanged |
| `bin/mydb` | Calls `engine_execute_sql` | Unchanged |

---

## WAL File Placement

Each partition gets its own WAL file, placed inside the partition directory:

```
~/.mydb/root/__wal.mydb       WAL for root's partition
~/.mydb/alice/__wal.mydb      WAL for alice's partition
```

Recovery is fully partitioned: a crash requires replaying only the WAL of the
affected partition. There is no global WAL to coordinate across users. This is the
natural consequence of baking partition isolation into the physical layout in v2.

---

## Commit Path with WAL (Future)

```
1.  DML writes page in BufferPool  →  page is dirty
2.  TxnManager.alloc_lsn()         →  assigns LSN to this page modification
3.  Log record written to WAL buffer (in memory)
4.  On COMMIT:
    a. Flush WAL buffer to __wal.mydb  (fsync — durable)
    b. Ack COMMIT to client
    c. Buffer pool pages remain dirty — flushed lazily
5.  On page eviction from BufferPool:
    a. Assert page.lsn <= TxnManager.flushed_lsn
    b. If not: flush WAL first, then write page
    c. Write dirty page to table file
```

---

## Memory Estimate (16 Active Partitions)

| Component | Per partition | x 16 |
|-----------|:---:|:---:|
| StorageEngine: BufferPool (64 x 16 KB) | 1 MB | 16 MB |
| StorageEngine: OpenTableCache (~10 tables) | ~100 KB | ~1.6 MB |
| PartitionBuffer / Cache 2 (8 SchemaFile handles) | ~50 KB | ~0.8 MB |
| Catalog / Cache 1 | ~4 KB | ~64 KB |
| TxnManager (pre-WAL) | ~2 KB | ~32 KB |
| TxnManager WAL buffer (post-WAL, 4 MB ring) | 4 MB | 64 MB |
| **Per-partition subtotal (no WAL)** | **~1.15 MB** | **~18.5 MB** |
| **Per-partition subtotal (with WAL)** | **~5.15 MB** | **~83 MB** |

Engine-level (shared): `DatabaseFile` + `SystemSchema` maps ~50 KB.
Per connection (TxnContext only): ~1 KB each.
Partitions not in use: zero memory (lazy load, evict on idle).

---

## Out of Scope for v3

- WAL file format and log record structure (v4)
- Crash recovery and checkpoint protocol (v4)
- Multi-partition queries and cross-partition joins (deferred)
- Concurrency control beyond single-writer-per-partition (deferred)
- Session-per-thread concurrency model (deferred)
