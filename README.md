# MyDB — Relational Database Engine

MyDB is a relational database engine built from scratch, modeled after InnoDB's
storage architecture. It implements a full SQL pipeline — lexer, parser,
cost-based query planner, and execution engine — backed by a B+ Tree clustered
index with a 64-frame LRU buffer pool. Multi-user sessions are supported through
isolated per-user partition contexts, and the engine runs as a background daemon
(`mydbd`) that clients reach over a Unix domain socket.

**Storage engine in C11. Parser and execution engine in C++17. Built for Linux.**

## Team

| Member | ID | Role |
|--------|----|------|
| **Hasnat Akram** | NUM-BSCS-2023-01 | Storage Engine |
| **Muhammad Arsalan** | NUM-BSCS-2023-34 | Query Parser |
| **Rehan Ali Abbasi** | NUM-BSCS-2023-15 | Query Processor / Execution Engine |

---

## Table of Contents

- [SQL Support](#sql-support)
- [Architecture](#architecture)
  - [Query Parser](#query-parser)
  - [Execution Engine](#execution-engine)
  - [Query Planner](#query-planner)
  - [Partition Manager](#partition-manager)
  - [Storage Engine](#storage-engine)
  - [Buffer Pool & Disk Manager](#buffer-pool--disk-manager)
  - [Engine — Orchestrator](#engine--orchestrator)
  - [Server & Client](#server--client)
- [Protocol](#protocol)
- [Storage Internals](#storage-internals)
- [Data Types & Constraints](#data-types--constraints)
- [SQL Reference](#sql-reference)
- [Getting Started](#getting-started)
- [Testing](#testing)
- [Project Structure](#project-structure)
- [Roadmap & Known Limitations](#roadmap--known-limitations)
- [Quick Example](#quick-example)

---

## SQL Support

**DDL** — `CREATE`/`DROP DATABASE`, `CREATE`/`DROP TABLE`, `CREATE INDEX`,
`CREATE`/`DROP`/`ALTER USER` (with quotas), `SHOW DATABASES`, `SHOW TABLES`,
`ANALYZE TABLE`

**DML** — `INSERT` (single + multi-row), `UPDATE`, `DELETE`

**DQL** — `SELECT` with `WHERE`, `GROUP BY`, `HAVING`, `ORDER BY`, `LIMIT`/`OFFSET`,
aggregates (`COUNT`, `SUM`, `AVG`, `MIN`, `MAX`), all comparison and null operators
(`=`, `!=`, `<>`, `<`, `>`, `<=`, `>=`, `AND`, `OR`, `NOT`, `IS [NOT] NULL`,
`BETWEEN`, `LIKE`, `IN`), `INNER`/`LEFT`/`RIGHT`/`FULL OUTER JOIN` (explicit and
implicit comma joins, chained multi-table joins, table aliases)

**TCL** — `BEGIN`, `COMMIT`, `ROLLBACK`

**Constraints** — `PRIMARY KEY`, `NOT NULL`, `UNIQUE`, `FOREIGN KEY` (with
`ON DELETE SET NULL`/`RESTRICT`/`CASCADE`), `AUTO_INCREMENT`, `DEFAULT`

**Data types** — `INT`, `DECIMAL(p,s)`, `VARCHAR(n)`, `ENUM(...)`, `BOOL`,
`DATE`, `DATETIME`

**Query optimization** — cost-based access path selection (PK lookup, index
lookup, range scan, full scan) driven by per-table statistics from `ANALYZE TABLE`;
join algorithm selection per step: sort-merge, index nested-loop, or hash join

**Multi-user** — quota-limited per-user partition directories; salted SHA-256
passwords; challenge-response auth over the wire; read-only privilege grants

**Client/server** — daemon (`mydbd`) + thin client (`mydb`) over a Unix domain
socket; no engine code in the client binary

---

## Architecture

SQL enters the system as a string and passes through a layered pipeline before
any bytes hit the disk. The diagram below shows the full flow; the sections that
follow describe each module in isolation.

```
  mydb  (client REPL — no engine code)
    │  Unix domain socket
    ▼
  mydbd (daemon: listener · session · auth · dispatch · poll loop)
    │
    │  engine_execute_sql(conn_id, sql)
    │    1. parser_parse()          → AST
    │    2. build ExecContext        (engine / partition / connection / stats)
    │    3. exec_engine_execute()   → result string
    │    4. parser_free_ast()
    ▼
┌─────────────────────────────────────────────────────┐
│  Engine  (global state)                              │
│  DatabaseFile · SystemSchema · StatsBuffer           │
│  ConnectionPool (up to 32 simultaneous connections)  │
└────────────────────────┬────────────────────────────┘
                         │  routes each connection to its partition
             ┌───────────┴──────────────┐
             ▼                          ▼
  ┌────────────────────┐    ┌────────────────────┐
  │  PartitionCtx      │    │  PartitionCtx      │  one per active
  │  Catalog (Cache 1) │    │  Catalog (Cache 1) │  user partition;
  │  PartitionBuffer   │    │  PartitionBuffer   │  shared by all
  │   (Cache 2, 8-LRU) │    │   (Cache 2, 8-LRU) │  connections of
  │  TxnManager        │    │  TxnManager        │  that user
  │  StorageEngine     │    │  StorageEngine     │
  │   └ BufferPool     │    │   └ BufferPool     │
  └────────────────────┘    └────────────────────┘
```

Layer stack, bottom to top:

```
10. Query Parser       Lexer → Parser → AST                              (C++17)
 9. Execution Engine   walks AST; dispatches DDL/DML/DQL/TCL via pm_*   (C++17)
 8. Query Planner      cost-based access path + join algorithm selection  (C11)
 7. Partition Manager  Catalog · SchemaFile LRU · TxnMgr · stats ·      (C11)
                        FK/NOT NULL/UNIQUE checks · quota — pm_* API
 6. Storage Engine     stateless B+ tree storage API                      (C11)
 5. Buffer Pool        64-frame LRU cache, dirty-page tracking            (C11)
 4. Page Format        InnoDB-style slotted pages, 16 KB                  (C11)
 3. Disk Manager       pread/pwrite, one .mydb file per table             (C11)
```

---

### Query Parser

The parser is the entry point for every SQL statement. It is written in C++17
and lives in `query_parser/`. A `Lexer` tokenizes the raw SQL string — keywords
are uppercased and classified; identifiers are case-preserved. The `Parser` then
builds an `ASTNode` tree according to the grammar (see [SQL Reference](#sql-reference)).
Sixty-four keywords are reserved and rejected as identifiers; twenty-two
non-reserved keywords (such as `TABLE`, `INDEX`, `PRIMARY`) are accepted as
table or column names.

The parser exposes two interfaces. `parser_api.h` is a C-compatible header used
by the engine, which treats `ParserAST*` as an opaque token and never reads
the tree itself. `AST.hpp` is a C++ header included directly by the execution
engine, which must walk the tree node by node. These two interfaces are
deliberately separate: the engine owns the AST lifetime; the execution engine
owns the AST semantics.

---

### Execution Engine

The execution engine (`execution_engine/`) walks the AST produced by the parser
and dispatches to per-statement handlers: `ddl.cpp`, `dml.cpp`, `dql.cpp`,
`tcl.cpp`. It never calls `storage_*` functions directly. Instead, it calls
`pm_*` wrappers from the Partition Manager, passing `ectx->partition` as the
session context. This indirection is intentional: the partition manager is the
single place that enforces constraints, checks quotas, updates schema statistics,
and translates between the logical schema view and physical storage.

For `SELECT`, the execution engine first calls `extract_sargs` to decode the
`WHERE` clause into a `Sarg[]` array, passes that to `planner_choose_path` to
get a `PlanNode`, then calls `plan_to_ap` to translate the plan into the right
`pm_*` cursor or lookup call. JOIN execution is handled separately by
`exec_join_select`, which builds a left-deep join tree and selects an algorithm
per step based on which columns are indexed.

---

### Query Planner

The planner (`planner/`) sits between the execution engine and the partition
manager. It takes a `Sarg[]` array decoded from the `WHERE` clause, an open
`SchemaFile*` for physical-size metadata, and an optional `StatsFile*` populated
by `ANALYZE TABLE`, then returns a `PlanNode` naming the cheapest access path.

The planner first tries four short-circuit rules before any cost math:

| Rule | Condition | Access path chosen |
|------|-----------|-------------------|
| No predicates | `WHERE` is empty | Full scan |
| PK equality | `pk_col = value` | PK lookup |
| Unique-index equality | unique-indexed `col = value` | Index lookup |
| No indexed column | no indexed col in predicates | Full scan |

If no rule fires, it computes cost for each candidate path using these formulas
(unit = one sequential page read; random I/O counts as 4×):

| Access path | Cost formula |
|-------------|-------------|
| Full scan | `pages` |
| PK lookup | `height × 4` |
| PK range | `height × 4 + selectivity × pages` |
| Index lookup | `(height + 1) × 4` |
| Index range | `height × 4 + selectivity × rows × 4` |

Per-column statistics stored by `ANALYZE TABLE`: NDV, null count, row count,
min/max, and either an MCV list (for low-cardinality columns and ENUM/BOOL) or
an equi-height histogram (16 buckets) for everything else. When no statistics
exist (`ANALYZE TABLE` has never been run), the planner falls back to fixed
selectivity defaults (equality → 5 %, range → 33 %) rather than failing.

For joins, the planner selects the algorithm per step: sort-merge when both join
columns are indexed, index nested-loop when only the inner column is indexed,
and hash join when neither is indexed.

---

### Partition Manager

The partition manager (`partition_manager/`) owns all per-user session state and
is the only layer that calls into the stateless storage engine. It exposes the
`pm_*` API, which the execution engine calls for every DML, DQL, and DDL
operation.

Each active user gets a `PartitionCtx` struct that bundles:

- **Catalog (Cache 1)** — the `__catalog.mydb` file, which records schemas and
  enforces the partition's disk-space quota.
- **PartitionBuffer (Cache 2)** — an 8-slot LRU of open `SchemaFile` handles,
  one per schema. Schema switches within a partition are free after the first
  open; a flush-and-swap is only needed when all 8 slots are occupied.
- **TransactionManager** — owns `BEGIN`/`COMMIT`/`ROLLBACK` logic and will
  become the WAL owner when crash recovery is added.
- **StorageEngine** — embedded as a plain struct (not a pointer, not a global),
  one per `PartitionCtx`.

Before forwarding a write to `storage_*`, `pm_*` wrappers check NOT NULL, UNIQUE
(via secondary index lookup), and FOREIGN KEY (referenced row must exist on
INSERT/UPDATE; ON DELETE action applied on DELETE of the referenced row). After
the write, they update the `SchemaFile` row-count statistics and the `Catalog`
quota bookkeeping.

A `PartitionCtx` is lazy-loaded on first login and evicted from memory when its
last connection logs out. Sixteen simultaneously active partitions consume
approximately 18.5 MB without WAL.

---

### Storage Engine

The storage engine (`storage_engine/`) is a stateless B+ Tree storage library.
It has no global variables and holds no session state. Every function takes an
explicit `StorageEngine*` that is embedded inside — and owned by — a
`PartitionCtx`.

Every table is stored as a B+ Tree clustered on its primary key. Leaf nodes
store full row data, so there is no separate heap file. Secondary indexes are
created for `UNIQUE` columns and explicit `CREATE INDEX` statements, stored as
separate `.mydb` files that map the indexed column value to the primary key.
All access — reads and writes — goes through the buffer pool; nothing is ever
loaded as a complete file into memory.

The storage API (`storage.h`) is intentionally narrow: DDL (`create_table`,
`drop_table`, `add_index`), DML (`insert`, `update`, `delete`), and DQL
(`get_by_pk`, `get_by_index`, `scan`, `scan_from`, `scan_by_index`). Constraint
enforcement and quota tracking are deliberately excluded — those belong to the
partition manager.

---

### Buffer Pool & Disk Manager

The buffer pool (`buffer_pool/`) sits between the B+ Tree and the disk. It
manages 64 fixed-size frames, each holding one 16 KB page, using an LRU
eviction policy. Every page access by the B+ Tree goes through the buffer pool;
dirty pages are flushed to disk on eviction or on `storage_flush_all_dirty`.

The disk manager (`disk_manager/`) is the only module that performs actual I/O.
It reads and writes pages using `pread`/`pwrite` at the byte offset
`page_number × 16384`. Each table is stored in its own `.mydb` binary file;
page 0 of every file is a reserved header holding the magic number
(`0x4D594442`), format version, page count, and root page number of the B+ Tree.

---

### Engine — Orchestrator

The engine (`engine/`) is the top-level coordinator of the SQL pipeline. It owns
global state — the `DatabaseFile` (`__database.mydb`), the `SystemSchema`
(`users.mydb` + `privileges.mydb`), the master `ConnectionPool` (up to 32
slots), and the `StatsBuffer` (lazy-opened handles to per-schema stats files).

Its main entry point, `engine_execute_sql(eng, conn_id, sql, out, cap)`:
1. Resolves the `Connection` from `conn_id` and verifies it is logged in.
2. Calls `parser_parse` to produce an AST.
3. Builds an `ExecContext` — `{engine, partition, conn, stats}` — lazily loading
   the `PartitionCtx` on first use.
4. Calls `exec_engine_execute(&ectx, ast)`.
5. Frees the AST and writes the result string into the caller's buffer.

The engine also exposes challenge-response login (`engine_login_response`),
logout with automatic rollback (`engine_logout`), and the bootstrap path
(`mydbd init -u <user>`) that creates the root partition on first run.

`users.mydb` and `privileges.mydb` bypass the buffer pool — they use direct
`pread`/`pwrite` with a RAM hash-map mirror, because metadata lookups need to
be fast and these files are small and fixed in structure.

---

### Server & Client

`mydbd` is the daemon. It runs a `poll()`-based event loop that accepts incoming
Unix socket connections, runs the challenge-response handshake, and then handles
SQL queries from all connected clients in a single thread. While a query is
executing, the session's file descriptor is removed from the `poll` watchlist
(`SESSION_BUSY`) so no other events interfere. Disconnection triggers
`engine_logout`, which rolls back any open transaction automatically.

`mydb` is the client. It contains no engine or storage code — just a socket
connection and a `readline`-based REPL. It sends `PKT_QUERY` packets and prints
the `PKT_RESPONSE` payload verbatim. The client binary is deliberately thin:
all query processing, formatting, and error handling happen inside the daemon.

---

### On-Disk Layout

```
$MYDB_HOME/                          (default ~/.mydb/)
├── __database.mydb                  engine registry: partitions, users
├── system_schema/                   engine-level, outside any partition
│   ├── users.mydb                   flat-file user table (direct pread/pwrite)
│   ├── privileges.mydb              flat-file grant table (direct pread/pwrite)
│   └── stats/
│       └── stats_<id>_<schema>.mydb per-schema stats file for the planner
├── root/                            default partition for the root user
│   ├── __catalog.mydb               partition catalog: schemas + quota
│   └── <schema>/
│       ├── __schema.mydb            RelationDef pages (one per table definition)
│       └── <table>.mydb             B+ tree data pages for that table
└── <other_user_partition>/...
```

All `.mydb` files share the same file-header format and magic number
`0x4D594442`. The path to the engine root is set by `$MYDB_HOME`; `mydbd init`
creates the directory structure on first run.

---

## Protocol

`mydbd` and `mydb` communicate over a Unix domain socket. The protocol
implementation lives in `server/src/protocol.c` and is the only source file
compiled into both binaries.

**Wire format:** a fixed 9-byte header followed by a variable-length payload.

```
┌──────────────────┬────────┬─────────────┐
│  length (4 B)    │type(1B)│ seq_no (4B) │  ← all fields in network byte order
└──────────────────┴────────┴─────────────┘
│  payload (length bytes)                  │
└──────────────────────────────────────────┘
```

Per-direction sequence numbers are enforced — a mismatch closes the connection
immediately. All output (SELECT rows, DML status, error messages) is formatted
by the engine inside the daemon and sent as a single `PKT_RESPONSE`; the client
prints it verbatim.

**Packet types:**

| Packet | Direction | Purpose |
|--------|-----------|---------|
| `PKT_HANDSHAKE` | server → client | protocol version |
| `PKT_AUTH_CHALLENGE` | server → client | 16-byte salt + 32-byte nonce |
| `PKT_AUTH_RESPONSE` | client → server | `SHA-256(nonce ‖ SHA-256(salt ‖ password))` |
| `PKT_AUTH_OK` | server → client | login accepted |
| `PKT_AUTH_FAIL` | server → client | login rejected |
| `PKT_QUERY` | client → server | SQL statement text |
| `PKT_RESPONSE` | server → client | formatted query output |
| `PKT_QUIT` | client → server | clean disconnect |

**Authentication detail:** the server sends a fresh 32-byte nonce with every
handshake. The client hashes `SHA-256(salt ‖ password)` locally, then wraps it:
`SHA-256(nonce ‖ inner_hash)`. Unknown usernames receive a randomly generated
fake salt so an attacker cannot determine whether an account exists. The nonce
is zeroed immediately after the comparison regardless of the outcome. All hash
logic lives inside the engine — `auth.c` performs no cryptography itself.

**Socket path resolution (priority order):**
1. `$MYDB_SOCKET` — explicit override
2. `$MYDB_HOME/mydb.sock` — development / foreground daemon
3. `/run/mydb/mydb.sock` — production systemd service

---

## Storage Internals

### Page Layout

Every table is stored as a B+ Tree clustered on its primary key. Leaf nodes hold
full row data — there is no separate heap file. Page size is 16 KB, matching
InnoDB.

```
offset 0
┌──────────────────────────────────┐
│  Page Header  (38 B)             │  checksum · page_no · prev/next ptr
│                                  │  LSN · page type · space ID
├──────────────────────────────────┤ offset 38
│  Infimum + Supremum  (26 B)      │  2 fixed boundary records (13 B each)
├──────────────────────────────────┤ offset 64
│  User Records  (variable)        │  row data, grows downward
│  each record: 5 B header         │  + hidden system cols + user cols
│                                  │
│  (free space)                    │
│                                  │
│  Page Directory  (variable)      │  2-byte slots, grows upward
│  1 slot per 4–8 records          │
├──────────────────────────────────┤ offset 16376
│  Page Trailer  (8 B)             │  old checksum + LSN low 32 bits
└──────────────────────────────────┘ offset 16384
```

### Record Header (5 bytes)

```
│ Info Flags (1B) │ Owned Count (1B) │ Heap No (13 bits) │ Type (3 bits) │ Next Offset (2B) │
```

| Field | Meaning |
|-------|---------|
| Info Flags | bit 0 = deleted mark; bit 1 = min-record flag |
| Owned Count | records owned by this slot's directory entry |
| Heap No | insertion order — Infimum=0, Supremum=1, rows from 2 |
| Type | 0=regular row, 1=B+ tree internal, 2=infimum, 3=supremum |
| Next Offset | relative offset to the next record (singly linked list in the page) |

### Hidden System Columns

Every row carries hidden system columns, following InnoDB convention:

| Column | Size | Present |
|--------|------|---------|
| `DB_TRX_ID` | 6 bytes | always |
| `DB_ROLL_PTR` | 7 bytes | always |
| `DB_ROW_ID` | 6 bytes | only when the table has no declared primary key |

A RID (Record ID) is `(page_number, slot_number)`.

---

## Data Types & Constraints

| Type | Storage | Notes |
|------|---------|-------|
| `INT` | 4 bytes | 32-bit signed |
| `DECIMAL(p,s)` | 8 bytes | fixed-point; defaults to `DECIMAL(10,2)` |
| `VARCHAR(n)` | 2-byte prefix + n bytes | defaults to `VARCHAR(150)` |
| `ENUM(a,b,…)` | 1 byte | stored as an index into the value list |
| `BOOL` | 1 byte | accepts `TRUE`/`FALSE` or `1`/`0` |
| `DATE` | 4 bytes | stored as `YYYYMMDD`; default format `DD-MM-YYYY`, configurable |
| `DATETIME` | 8 bytes | stored as `YYYYMMDDHHmmSS`; always displayed as `YYYY-MM-DD HH:MM:SS` |

**Column modifiers:** `DEFAULT <value>` (all types; `DEFAULT NOW` for `DATETIME`),
`AUTO_INCREMENT` (INT primary keys only), inline and named `CONSTRAINT`s.

**Constraint enforcement:**

| Constraint | Checked on | Notes |
|------------|-----------|-------|
| `PRIMARY KEY` | INSERT, UPDATE | NOT NULL + UNIQUE + clustered index |
| `NOT NULL` | INSERT, UPDATE | |
| `UNIQUE` | INSERT, UPDATE | enforced via secondary index |
| `FOREIGN KEY` | INSERT, UPDATE | referenced row must exist |
| `FOREIGN KEY ON DELETE` | DELETE of referenced row | SET NULL / RESTRICT / CASCADE |

---

## SQL Reference

### SELECT

```
SELECT [DISTINCT] col [AS alias] {, col [AS alias]}*  |  *
  FROM table [alias] {, table [alias]}*
  [INNER|LEFT|RIGHT|FULL OUTER JOIN table [alias] ON col = col]*
  [WHERE expr]
  [GROUP BY col {, col}*]
  [HAVING expr]
  [ORDER BY col [ASC|DESC] {, col [ASC|DESC]}*]
  [LIMIT n [OFFSET m]]
```

Aggregates: `COUNT(*)`, `COUNT(col)`, `SUM(col)`, `AVG(col)`, `MIN(col)`,
`MAX(col)` — each optionally with `DISTINCT`.

Expression precedence (high → low): `NOT` > comparisons (`=` `!=` `<>` `<`
`>` `<=` `>=`) > `IS [NOT] NULL` > `[NOT] BETWEEN` > `[NOT] IN (…)` >
`[NOT] LIKE` > `AND` > `OR`.

Qualified identifiers in `table.column` form are supported wherever a column
name is accepted.

### CREATE TABLE

```sql
CREATE TABLE name (
    col  type_spec  [col_constraint]*
    [, col  type_spec  [col_constraint]*]
    [, table_constraint]
);
```

`type_spec` choices: `INT` | `INTEGER` | `DECIMAL[(p,s)]` | `VARCHAR[(n)]` |
`ENUM(v,…)` | `BOOL` | `BOOLEAN` | `DATE[(format)]` | `DATETIME`

Column constraints: `PRIMARY KEY`, `NOT NULL`, `UNIQUE`, `AUTO_INCREMENT` /
`AUTOINCR`, `DEFAULT value`

Table constraints: `[CONSTRAINT name] PRIMARY KEY (col)`,
`FOREIGN KEY (col) REFERENCES table(col) [ON DELETE SET NULL|RESTRICT|CASCADE]`

### User Management

```sql
CREATE USER name IDENTIFIED BY 'password' [PARTITION p] [QUOTA nM|nG]
ALTER USER name IDENTIFIED BY 'new_password'
ALTER USER name SET QUOTA nM|nG
DROP USER name          -- root only; deletes the partition directory
```

Minimum quota: 100 MB. Maximum quota: 5 GB.

---

## Getting Started

### Prerequisites

- Linux (tested on Fedora)
- CMake 3.20+
- GCC or Clang with C11 + C++17 support
- `readline` development headers (`readline-devel` on Fedora, `libreadline-dev`
  on Debian/Ubuntu)

### Build

```bash
./build.sh
```

Configures and builds with CMake, symlinks `mydbd` and `mydb` into
`~/.local/bin`, and on the very first run bootstraps the engine interactively
(`mydbd init`), prompting for a root password.

```bash
./build.sh --clean   # wipe build/ and $MYDB_HOME for a from-scratch rebuild
```

### Run (foreground)

Start the daemon in one terminal:

```bash
mydbd
```

Open a session in another:

```bash
mydb connect -u root
```

### Run as a systemd service

```bash
sudo packaging/install-service.sh
sudo systemctl start mydbd
journalctl -u mydbd -f        # follow logs
```

The service runs under a dedicated `mydb` system user with data in
`/var/lib/mydb` (mode `0750`) and the socket at `/run/mydb/mydb.sock`
(mode `0666` — database authentication still gates all access).

### Environment Variables

| Variable | Default | Purpose |
|----------|---------|---------|
| `MYDB_HOME` | `~/.mydb/` | Engine data root (partitions, system_schema) |
| `MYDB_SOCKET` | `$MYDB_HOME/mydb.sock` | Unix socket path, overrides default resolution |
| `MYDB_INIT_USER` | `root` | Username `build.sh` bootstraps on first run |

### CLI Reference

```
mydbd init -u <user>                     bootstrap a new engine; prompts for password
mydbd                                    run the daemon

mydb connect -u <user> [-S <path>]       connect and open a SQL REPL
                                         -S / --socket overrides $MYDB_SOCKET / $MYDB_HOME
```

---

## Testing

```bash
cd build
ctest --output-on-failure
```

**13 / 14 tests pass.** `test_disk_manager` has a pre-existing
`MYDB_FORMAT_VERSION` constant mismatch unrelated to any current work; all other
13 binaries are green, including `test_joins` which drives end-to-end JOIN
queries through the full v3 pipeline.

| Library under test | Test binaries |
|--------------------|--------------|
| `storage_engine` | `test_disk_manager`, `test_page`, `test_buffer_pool`, `test_btree`, `test_relation_def`, `test_file_header` |
| `partition_manager` | `test_transaction`, `test_partition`, `test_schema_file` |
| `engine` | `test_database_file`, `test_crypto` |
| standalone | `test_system_schema`, `test_parser`, `test_joins` |

---

## Project Structure

```
MyDB-Database-Engine/
├── storage_engine/      stateless B+ tree I/O: disk manager · page · buffer
│                         pool · btree · relation_def
├── partition_manager/   per-partition session state: catalog · schema-file
│                         cache · transactions · stats · pm_* API
├── system_schema/       engine-level users.mydb + privileges.mydb
├── crypto/              SHA-256 + salt; shared by engine and client
├── query_parser/        Lexer → Parser → AST  (C++17)
├── planner/             cost-based access path + join algorithm selection
├── execution_engine/    AST walker: DDL / DML / DQL / TCL handlers  (C++17)
├── engine/              orchestrator: EngineState · connections · auth ·
│                         engine_execute_sql pipeline
├── server/              mydbd: listener · protocol · session · auth · dispatch
├── client/              mydb: socket connection + REPL (no engine)
├── bin/                 mydbd.c, mydb.c entry points
├── packaging/           systemd unit + install script
└── tests/               ctest binaries per module
```

Detailed design documents — architecture diagrams, CBO formulas, full parser
grammar, server protocol spec, join-ordering design — live in the sibling
`Design Documents/` folder, one level above this repo.

---

## Roadmap & Known Limitations

| Area | Status | Notes |
|------|--------|-------|
| `ALTER TABLE` | not implemented | schema changes require `DROP` + `CREATE` |
| WAL / crash recovery | not implemented | `BEGIN`/`COMMIT`/`ROLLBACK` work per-connection, but a crash mid-write can leave a table inconsistent |
| True concurrency | not implemented | single-threaded `poll()` loop; many clients connect simultaneously but only one statement executes at a time across the whole daemon |
| Views, triggers, stored procedures | not implemented | — |
| Cross-partition queries | not implemented | a connection has one active schema at a time |
| Cost-based join *ordering* | designed, not wired | join *algorithm* per step is cost-driven; join *order* follows FROM/JOIN clause order. Full System-R-style DP design exists (`JOIN_PLANNER.md`) but is not yet integrated |
| `ACCESS_INDEX_RANGE` | partial | `storage_scan_by_index` exists but is not wired into the planner's range-scan path; falls back to a full scan with a filter |
| `GROUP BY` / aggregates inside a `JOIN` query | not implemented | each feature works fine independently; combining them in one query is not yet supported |
| `HAVING` with aggregate expressions | not implemented | `HAVING COUNT(*) > 1` is not yet parsed; only plain column/literal predicates work |

---

## Quick Example

```sql
mydb> CREATE DATABASE shop;
mydb> USE shop;

mydb> CREATE TABLE departments (
          id   INT AUTO_INCREMENT PRIMARY KEY,
          name VARCHAR(50) NOT NULL UNIQUE
      );

mydb> CREATE TABLE employees (
          id        INT AUTO_INCREMENT PRIMARY KEY,
          name      VARCHAR(100) NOT NULL,
          role      ENUM(manager, staff, intern) DEFAULT staff,
          salary    DECIMAL(10,2) DEFAULT 0.00,
          hired_on  DATE,
          dept_id   INT,
          FOREIGN KEY (dept_id) REFERENCES departments(id) ON DELETE SET NULL
      );

mydb> INSERT INTO departments (name) VALUES ('Engineering'), ('Sales');

mydb> INSERT INTO employees (name, role, salary, hired_on, dept_id)
          VALUES ('Aisha Khan', 'manager', 95000.00, '01-03-2022', 1);

-- Collect per-column statistics so the planner can choose optimal access paths
mydb> ANALYZE TABLE employees;

-- Planner picks sort-merge or hash join based on which columns are indexed
mydb> SELECT e.name, e.role, d.name AS department
      FROM employees e
      LEFT JOIN departments d ON e.dept_id = d.id
      WHERE e.salary > 50000
      ORDER BY e.salary DESC;

-- Aggregates work independently
mydb> SELECT role, COUNT(*) AS headcount
      FROM employees
      GROUP BY role
      HAVING role = 'manager';
```
