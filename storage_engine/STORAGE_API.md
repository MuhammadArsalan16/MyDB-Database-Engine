# MyDB Storage Engine — API Reference

**Author:** Hasnat Akram (Storage Engine)  
**Audience:** Execution Engine (Rehan Ali Abbasi) and Query Planner  
**Headers:** `#include "engine.h"` and `#include "storage.h"`

---

## Table of Contents

1. [What Changed from v1](#1-what-changed-from-v1)
2. [Session Lifecycle](#2-session-lifecycle)
3. [Looking Up Tables](#3-looking-up-tables)
4. [Key Data Types](#4-key-data-types)
   - [RelationDef and ColumnDef](#41-relationdef-and-columndef)
   - [ForeignKey](#42-foreignkey)
   - [Value](#43-value)
   - [Row](#44-row)
   - [RID](#45-rid)
   - [Cursor](#46-cursor)
5. [Error Codes](#5-error-codes)
6. [DDL — Create, Drop, Index](#6-ddl--create-drop-index)
7. [DML — Insert, Update, Delete](#7-dml--insert-update-delete)
8. [DQL — Querying Data](#8-dql--querying-data)
9. [TCL — Transactions](#9-tcl--transactions)
10. [Statistics — ANALYZE TABLE](#10-statistics--analyze-table)
11. [Complete Examples](#11-complete-examples)
12. [Constraints and Limits](#12-constraints-and-limits)
13. [What is NOT Implemented](#13-what-is-not-implemented)

---

## 1. What Changed from v1

The storage engine went through a complete redesign (v2). If you wrote code
against the old API, **everything below has changed**:

| v1 (old) | v2 (current) |
|---|---|
| `storage_init("./data")` | `storage_init(&eng)` — takes an `EngineState` |
| `storage_insert("users", &row)` | `storage_insert(rel, &row)` — takes a `RelationDef *` |
| `storage_get_by_pk("users", &pk)` | `storage_get_by_pk(rel, &pk)` |
| `storage_scan("users")` | `storage_scan(rel)` |
| `storage_create_table("users", &schema)` | `storage_create_table(&rel)` |
| `storage_drop_table("users")` | `storage_drop_table(rel)` |
| `Schema` struct, `table_name` field | `RelationDef` struct, `relation_name` field |
| FK constraints stored but not checked | FK constraints enforced (RESTRICT) |
| No login / auth | Full login + privilege system |
| No secondary index cursor | `storage_scan_by_index` — secondary cursor from `lo` |
| No statistics / CBO | `storage_analyze_table` + `planner_choose_path` in `planner/` |
| No schema removal | `storage_drop_schema` — deletes all schema files, credits quota |

---

## 2. Session Lifecycle

The execution engine must open a session through the engine module before
calling any storage function. The full startup sequence is:

```c
#include "engine.h"
#include "storage.h"

EngineState eng;

// 1. Open the engine (reads __database.mydb + system_schema/)
int rc = engine_init(root_dir, &eng);
if (rc != MYDB_OK) { /* engine root missing or corrupt */ }

// 2. Authenticate
rc = engine_login(&eng, username, password);
// MYDB_ERR_NOT_FOUND  → unknown username
// MYDB_ERR_PERM       → wrong password or account disabled

// 3. Select a schema (equivalent to SQL: USE schema_name)
rc = engine_use_schema(&eng, schema_name);
// MYDB_ERR_NOT_FOUND  → schema not registered in user's catalog
// MYDB_ERR_PERM       → analyst user has no grant for this schema

// 4. Initialise storage against the open session
rc = storage_init(&eng);

// --- now use storage DDL / DML / DQL ---

// 5. Shut down (flushes, closes files)
storage_shutdown();
engine_close(&eng);
```

In practice, `bin/mydb` calls `engine_start` (a single combined call that
sequences init → login → storage_init) and `engine_close` (which calls
`storage_shutdown` internally).

### `engine_init`

```c
int engine_init(const char *root_dir, EngineState *out);
```

Opens an already-bootstrapped engine at `root_dir`. No user is logged in yet.
`root_dir` is typically `~/.mydb/` (the `$MYDB_HOME` environment variable,
resolved by the CLI layer).

---

### `engine_login`

```c
int engine_login(EngineState *eng, const char *username, const char *password);
```

Verifies the password against the system user table (SHA-256 + per-user salt).
On success, opens the user's partition catalog and stamps `last_login`.

| Return | Meaning |
|---|---|
| `MYDB_OK` | Authenticated |
| `MYDB_ERR_NOT_FOUND` | Unknown username |
| `MYDB_ERR_PERM` | Wrong password or account disabled |

---

### `engine_use_schema`

```c
int engine_use_schema(EngineState *eng, const char *schema_name);
```

Selects the active schema. Called every time the execution engine processes
`USE schema_name`. Flushes dirty pages from the previously active schema
before switching.

**Owner path** (user owns the partition): schema must exist in the user's
catalog.  
**Analyst path** (no partition): user must have a SELECT privilege grant for
the schema.

| Return | Meaning |
|---|---|
| `MYDB_OK` | Schema is now active |
| `MYDB_ERR_NOT_FOUND` | Schema not registered |
| `MYDB_ERR_PERM` | No privilege grant (analyst) |

---

### `storage_init`

```c
int storage_init(EngineState *eng);
```

Binds the storage runtime to the open engine session. Sets up the buffer pool
and transaction manager. Call once after `engine_login`. Does **not** need to
be called again after `engine_use_schema` — storage re-reads the active schema
on every call lazily.

---

### `storage_shutdown`

```c
int storage_shutdown(void);
```

Commits any open transaction, flushes all dirty pages, closes all table files.
Call once before `engine_close`.

---

## 3. Looking Up Tables

Before calling any DDL/DML/DQL function, the execution engine must resolve the
table name into a `RelationDef *`. The execution engine does this at execution
time using:

```c
const RelationDef *engine_find_relation(EngineState *eng,
                                        const char *relation_name);
```

Returns a pointer into the active schema's in-memory table, or `NULL` if no
such relation exists. The pointer is valid until the next `engine_use_schema`
or `engine_close`.

**Example:**

```c
const RelationDef *rel = engine_find_relation(&eng, "users");
if (rel == NULL) {
    // table does not exist — report error
}
// cast away const for storage functions that mutate auto_incr_counter
storage_insert((RelationDef *)rel, &row);
```

> The pointer is `const` because the execution layer must not modify the
> schema directly. Storage is the only writer. Cast to `RelationDef *` only
> when calling storage functions (they update `auto_incr_counter` after an
> AUTO_INCREMENT insert).

---

## 4. Key Data Types

### 4.1 RelationDef and ColumnDef

`RelationDef` replaces the old `Schema` struct. Field name changed:
`table_name` → `relation_name`.

```c
struct RelationDef {
    char       relation_name[64];     // table name, e.g. "users"

    uint8_t    num_columns;
    ColumnDef  columns[32];           // in declaration order
    uint8_t    pk_col_idx;            // index into columns[] for the PK

    uint8_t    num_foreign_keys;
    ForeignKey foreign_keys[8];

    uint32_t   auto_incr_counter;     // next AUTO_INCREMENT value (storage-managed)
    uint32_t   root_page_no;          // clustered B+ tree root page (storage-managed)

    uint8_t    num_secondary_indexes;
    uint8_t    secondary_col_idx[8];       // which columns have secondary indexes
    uint32_t   secondary_root_page_no[8];  // their root pages (storage-managed)
};
```

```c
struct ColumnDef {
    char     name[64];
    DataType type;
    uint16_t max_len;           // VARCHAR: max chars; DECIMAL: total digits
    uint8_t  scale;             // DECIMAL: digits after the decimal point

    uint8_t  is_not_null;
    uint8_t  is_primary_key;
    uint8_t  is_unique;         // triggers a secondary B+ tree on CREATE TABLE
    uint8_t  is_auto_increment; // only valid on INT PRIMARY KEY

    uint8_t  has_default;
    Value    default_value;     // valid when has_default == 1

    uint8_t  num_enum_values;
    char     enum_values[16][32]; // only for TYPE_ENUM
};
```

**Fields managed by storage — do NOT set these yourself:**
- `root_page_no`
- `secondary_root_page_no[]`
- `auto_incr_counter` (set to `1` at creation; storage increments it on each
  AUTO_INCREMENT insert)

**Secondary indexes** are created automatically for every column where
`is_unique = 1` and the column is not the primary key. You must tell storage
which columns those are:

```c
rel.num_secondary_indexes = 1;
rel.secondary_col_idx[0]  = 2;  // column 2 has UNIQUE → secondary B+ tree
```

Storage also supports non-unique secondary indexes for `INDEXED` columns via
`storage_add_index` (§6).

---

### 4.2 ForeignKey

```c
typedef struct {
    char constraint_name[64];   // optional name, e.g. "fk_dept"
    char column_name[64];       // FK column in this relation, e.g. "dept_id"
    char ref_relation_name[64]; // referenced relation, e.g. "departments"
    char ref_column_name[64];   // referenced column, e.g. "id"
} ForeignKey;
```

FK constraints are **automatically enforced** by storage (RESTRICT — see §7).

---

### 4.3 Value

A `Value` holds one column's data. Always set `type` and `is_null`.

```c
typedef struct {
    DataType type;
    uint8_t  is_null;   // 1 = NULL, 0 = has a value
    union {
        int32_t  int_val;           // TYPE_INT
        int64_t  decimal_val;       // TYPE_DECIMAL (value × 10^scale)
        struct {
            uint16_t len;
            char     data[150];
        } varchar_val;              // TYPE_VARCHAR
        uint8_t  enum_val;          // TYPE_ENUM (0-based index)
        uint8_t  bool_val;          // TYPE_BOOL (0 or 1)
        int32_t  date_val;          // TYPE_DATE (YYYYMMDD)
        int64_t  datetime_val;      // TYPE_DATETIME (YYYYMMDDHHmmSS)
    } v;
} Value;
```

**DataType constants:**

| Constant | Union member | Storage | Notes |
|---|---|---|---|
| `TYPE_INT` | `int_val` | 4 bytes | 32-bit signed integer |
| `TYPE_DECIMAL` | `decimal_val` | 8 bytes | `value × 10^scale`. E.g. `3.14` at `scale=2` → store `314` |
| `TYPE_VARCHAR` | `varchar_val` | 2 + n bytes | `len` = byte count; `data` is **not** NUL-terminated |
| `TYPE_ENUM` | `enum_val` | 1 byte | 0-based index into the column's `enum_values[]` list |
| `TYPE_BOOL` | `bool_val` | 1 byte | 0 = false, 1 = true |
| `TYPE_DATE` | `date_val` | 4 bytes | Integer YYYYMMDD. 25 Dec 2024 → `20241225` |
| `TYPE_DATETIME` | `datetime_val` | 8 bytes | Integer YYYYMMDDHHmmSS. `20241225143000` |

**Setting values:**

```c
// INT
Value v;
v.type = TYPE_INT;  v.is_null = 0;  v.v.int_val = 100;

// VARCHAR
v.type = TYPE_VARCHAR;  v.is_null = 0;
v.v.varchar_val.len = 5;
memcpy(v.v.varchar_val.data, "Alice", 5);

// DECIMAL — 3.14 with scale=2
v.type = TYPE_DECIMAL;  v.is_null = 0;  v.v.decimal_val = 314;

// BOOL
v.type = TYPE_BOOL;  v.is_null = 0;  v.v.bool_val = 1;

// DATE — 2024-12-25
v.type = TYPE_DATE;  v.is_null = 0;  v.v.date_val = 20241225;

// DATETIME — 2024-12-25 14:30:00
v.type = TYPE_DATETIME;  v.is_null = 0;
v.v.datetime_val = 20241225143000LL;

// ENUM — index 0 = first value in enum list
v.type = TYPE_ENUM;  v.is_null = 0;  v.v.enum_val = 0;

// NULL
v.type = TYPE_INT;  v.is_null = 1;
```

---

### 4.4 Row

```c
typedef struct Row {
    uint8_t  num_cols;   // must match rel->num_columns
    Value    cols[32];   // cols[i] corresponds to rel->columns[i]
    RID      rid;        // set by storage on GET/SCAN; ignored on INSERT
} Row;
```

- `cols[i]` must match `rel->columns[i]` in type and order.
- `rid` is populated by `storage_get_by_pk`, `storage_get_by_index`, and
  `cursor_next`. Pass it to `storage_update` / `storage_delete`.
- On INSERT, `rid` is ignored.

---

### 4.5 RID

```c
typedef struct {
    uint32_t page_no;
    uint16_t slot_no;
} RID;
```

A Record ID uniquely identifies a row on disk. Storage fills it in; you never
construct one. **Save it before the next `cursor_next` call overwrites it.**

---

### 4.6 Cursor

An opaque scan handle. Always close it when done, even on early exit.

```c
Cursor *cur = storage_scan(rel);
Row *row;
while ((row = cursor_next(cur)) != NULL) {
    // use row->cols[]
}
cursor_close(cur);
```

---

## 5. Error Codes

All functions that return `int` return one of these:

| Constant | Value | Meaning |
|---|---|---|
| `MYDB_OK` | `0` | Success |
| `MYDB_ERR` | `-1` | Generic error |
| `MYDB_ERR_NOT_FOUND` | `-2` | Row or schema does not exist |
| `MYDB_ERR_DUPLICATE` | `-3` | PK or UNIQUE constraint violation |
| `MYDB_ERR_FULL` | `-4` | Buffer pool full, catalog full, or partition quota exhausted |
| `MYDB_ERR_FK_VIOLATION` | `-5` | Foreign key RESTRICT violation |
| `MYDB_ERR_NULL_VIOLATION` | `-6` | NULL in a NOT NULL column |
| `MYDB_ERR_NO_TXN` | `-7` | COMMIT / ROLLBACK with no active transaction |
| `MYDB_ERR_PERM` | `-8` | Not logged in, no active schema, or insufficient privileges |
| `MYDB_ERR_CROSS_SCHEMA` | `-9` | Relation belongs to a different schema |

---

## 6. DDL — Create, Drop, Index

### `storage_create_schema`

```c
int storage_create_schema(const char *name);
```

Creates a new schema inside the current partition (`CREATE DATABASE`).

- **Owner-only.** Analyst sessions get `MYDB_ERR_PERM`.
- Creates `<partition>/<name>/`, writes its `__schema.mydb`, and registers in
  the partition's `__catalog.mydb`.
- After this returns `MYDB_OK`, call `engine_use_schema(eng, name)` to make it
  active and start creating tables.

| Return | Meaning |
|---|---|
| `MYDB_OK` | Schema created |
| `MYDB_ERR_PERM` | Caller does not own a partition |
| `MYDB_ERR_DUPLICATE` | Schema name already exists |
| `MYDB_ERR_FULL` | All 64 catalog schema slots are used |

---

### `storage_drop_schema`

```c
int storage_drop_schema(const char *name);
```

Destroys a schema and all its contents (`DROP DATABASE`). **Permanent and
unrecoverable.** The execution engine must:
1. Confirm the schema is **not** the currently active schema before calling
   (returns `MYDB_ERR` with `errno` set to `EBUSY` if it is active).
2. After success, any cached `RelationDef *` pointers that pointed into this
   schema are dangling — do not use them.

**What it does:**
- Opens the schema's `__schema.mydb`, enumerates all valid relation slots.
- For each relation: closes any open B+ tree file handles, unlinks `<rel>.mydb`.
- Unlinks `__stats.mydb` if present.
- Unlinks `__schema.mydb`, removes the schema directory.
- Calls `cat_track_alloc` to credit the freed bytes back to the partition quota.
- Calls `cat_remove_schema` to remove the catalog slot from `__catalog.mydb`.

| Return | Meaning |
|---|---|
| `MYDB_OK` | Schema, all its tables, and stats file removed; quota credited |
| `MYDB_ERR` | Schema is currently active (cannot drop the active schema) |
| `MYDB_ERR_NOT_FOUND` | No schema with that name exists in the current partition |

```c
/* DROP DATABASE shop; */
int rc = storage_drop_schema("shop");
// rc == MYDB_ERR → "shop" is the currently active schema; USE another first
// rc == MYDB_OK  → all files deleted, quota updated
```

---

### `storage_create_table`

```c
int storage_create_table(RelationDef *rel);
```

Creates a new relation in the active schema. Allocates the clustered B+ tree
root page (and one secondary B+ tree root page per UNIQUE column) and writes
the definition to `__schema.mydb`.

**You must fill in:**
- `rel->relation_name`
- `rel->num_columns` and `rel->columns[i]` for each column
- `rel->pk_col_idx`
- `rel->num_secondary_indexes` and `rel->secondary_col_idx[]` for columns
  where `is_unique = 1`
- `rel->num_foreign_keys` and `rel->foreign_keys[]`
- `rel->auto_incr_counter = 1` if any column uses AUTO_INCREMENT

**Do NOT fill in:** `root_page_no`, `secondary_root_page_no[]` — storage sets these.

```c
RelationDef rel;
memset(&rel, 0, sizeof(rel));
strncpy(rel.relation_name, "users", 64);
rel.num_columns = 3;
rel.pk_col_idx  = 0;
rel.auto_incr_counter = 1;

// col 0: id INT AUTO_INCREMENT PRIMARY KEY
strncpy(rel.columns[0].name, "id", 64);
rel.columns[0].type              = TYPE_INT;
rel.columns[0].max_len           = 4;
rel.columns[0].is_not_null       = 1;
rel.columns[0].is_primary_key    = 1;
rel.columns[0].is_auto_increment = 1;

// col 1: email VARCHAR(100) NOT NULL UNIQUE
strncpy(rel.columns[1].name, "email", 64);
rel.columns[1].type        = TYPE_VARCHAR;
rel.columns[1].max_len     = 100;
rel.columns[1].is_not_null = 1;
rel.columns[1].is_unique   = 1;

// col 2: score DECIMAL(10,2)
strncpy(rel.columns[2].name, "score", 64);
rel.columns[2].type    = TYPE_DECIMAL;
rel.columns[2].max_len = 10;
rel.columns[2].scale   = 2;

// register the UNIQUE column so storage creates its secondary tree
rel.num_secondary_indexes = 1;
rel.secondary_col_idx[0]  = 1;   // column 1 (email)

int rc = storage_create_table(&rel);
// rel.root_page_no now populated by storage
```

---

### `storage_drop_table`

```c
int storage_drop_table(RelationDef *rel);
```

Deletes the relation's `.mydb` data file, removes it from `__schema.mydb`,
and rebates its quota. **Permanent.**

---

### `storage_add_index`

```c
int storage_add_index(RelationDef *rel, int col_idx);
```

Adds a secondary index on an existing column **after** the table has been
created. Use this for `CREATE INDEX` statements and for `INDEXED` (non-unique)
columns that the query planner can exploit for range scans.

- Works for both UNIQUE (`rel->columns[col_idx].is_unique = 1`) and non-unique
  (`is_unique = 0`) columns. The B+ tree type is derived from the column definition.
- Allocates a new root page, backfills all existing rows in one pass, and
  persists the updated `RelationDef` back into `__schema.mydb`.
- Updates `rel->num_secondary_indexes` and `rel->secondary_col_idx[]` in memory
  so further calls to `storage_get_by_index` / `storage_scan_by_index` see the
  new index immediately.

| Return | Meaning |
|---|---|
| `MYDB_OK` | Index created and backfilled |
| `MYDB_ERR_DUPLICATE` | `col_idx` is already indexed |
| `MYDB_ERR_FULL` | `MAX_SECONDARY_IDX` (8) reached, or quota exceeded |

```c
// CREATE INDEX ON orders(customer_id)
RelationDef *rel = (RelationDef *)engine_find_relation(&eng, "orders");
int ci = -1;
for (int i = 0; i < rel->num_columns; i++)
    if (strcmp(rel->columns[i].name, "customer_id") == 0) { ci = i; break; }

int rc = storage_add_index((RelationDef *)rel, ci);
// rc == MYDB_OK → secondary B+ tree exists; scan_by_index and planner can use it
```

---

## 7. DML — Insert, Update, Delete

All DML functions enforce these rules automatically:

- **NOT NULL** — `MYDB_ERR_NULL_VIOLATION` if a NOT NULL column gets NULL.
- **Duplicate key** — `MYDB_ERR_DUPLICATE` on PK or UNIQUE collision.
- **Authorization** — `MYDB_ERR_PERM` if the user lacks write access (e.g.
  analyst with SELECT-only grant).
- **FK RESTRICT** — see below.

### Foreign Key Enforcement (RESTRICT)

| Operation | Check | Error |
|---|---|---|
| `storage_insert` | FK column value must exist as a PK in the referenced table | `MYDB_ERR_FK_VIOLATION` |
| `storage_update` | If FK column changes, new value must exist; if PK changes, old PK must not be referenced | `MYDB_ERR_FK_VIOLATION` |
| `storage_delete` | This row's PK must not be referenced by any FK in the schema | `MYDB_ERR_FK_VIOLATION` |

NULL FK values are **not checked** — a NULL FK column bypasses the reference
check (standard SQL behaviour).

---

### `storage_insert`

```c
int storage_insert(RelationDef *rel, Row *row);
```

Inserts one row.

- AUTO_INCREMENT PK: set `row->cols[pk_idx].is_null = 1`. Storage assigns the
  next counter value and writes it back into `row->cols[pk_idx].v.int_val`.

```c
RelationDef *rel = (RelationDef *)engine_find_relation(&eng, "users");
Row row;
memset(&row, 0, sizeof(Row));
row.num_cols = 3;

row.cols[0].type    = TYPE_INT;     row.cols[0].is_null = 1;  // AUTO

row.cols[1].type    = TYPE_VARCHAR; row.cols[1].is_null = 0;
row.cols[1].v.varchar_val.len = 15;
memcpy(row.cols[1].v.varchar_val.data, "alice@email.com", 15);

row.cols[2].type    = TYPE_DECIMAL; row.cols[2].is_null = 0;
row.cols[2].v.decimal_val = 9550;   // 95.50 with scale=2

int rc = storage_insert(rel, &row);
// row.cols[0].v.int_val == 1  (assigned id)
```

---

### `storage_update`

```c
int storage_update(RelationDef *rel, RID rid, Row *new_row);
```

Replaces the row at `rid` with `new_row`. Internally a delete + insert, so all
constraints apply. The RID comes from `row->rid` on a prior GET or SCAN.

```c
Value pk = { .type = TYPE_INT, .v.int_val = 1 };
Row *existing = storage_get_by_pk(rel, &pk);
if (existing) {
    RID rid = existing->rid;       // save before next call overwrites
    Row updated = *existing;
    updated.cols[2].v.decimal_val = 10000;   // change score to 100.00
    storage_update(rel, rid, &updated);
}
```

---

### `storage_delete`

```c
int storage_delete(RelationDef *rel, RID rid);
```

Deletes the row at `rid`.

```c
Value pk = { .type = TYPE_INT, .v.int_val = 1 };
Row *row = storage_get_by_pk(rel, &pk);
if (row) {
    int rc = storage_delete(rel, row->rid);
    // MYDB_ERR_FK_VIOLATION → another table still references this row
}
```

---

## 8. DQL — Querying Data

All DQL functions return `NULL` (point lookups) or `NULL` cursor if the user
does not have at least SELECT access. Analyst users with a SELECT grant can read.

### Access Path — How the Execution Engine Chooses a Storage Call

The execution engine does **not** select storage calls directly from raw AST
patterns. Instead, `exec_select` in `dql.cpp` runs through the planner pipeline:

```
extract_sargs(where, rel, sargs, 32)     → decode AND-tree into Sarg[]
planner_choose_path(eng, rel, sargs, n)  → short-circuit rules + CBO
plan_to_ap(plan, where, rel)             → typed storage API call + key
```

The planner's output maps to storage calls as follows:

| PlanNode path | Storage call | Notes |
|---|---|---|
| `ACCESS_PK_LOOKUP` | `storage_get_by_pk(rel, &key)` | Exact PK equality |
| `ACCESS_INDEX_LOOKUP` | `storage_get_by_index(rel, ci, &key)` | UNIQUE col equality |
| `ACCESS_PK_RANGE` | `storage_scan_from(rel, &lo_key)` | PK range; WHERE filter applies upper bound |
| `ACCESS_INDEX_RANGE` | `storage_scan(rel)` + filter | Secondary range; falls back to full scan in Phase 1 |
| `ACCESS_FULL_SCAN` | `storage_scan(rel)` + filter | No usable index |

See `PLANNER.md` at the repo root for the full cost-based optimizer design.

---

### `storage_get_by_pk`

```c
Row *storage_get_by_pk(RelationDef *rel, Value *pk);
```

Point lookup by primary key via the clustered B+ tree. O(log n).

- Returns a pointer to an **internal static buffer** — valid until the next
  `storage_get_by_pk` call on any relation.
- Returns `NULL` if the row does not exist or permission is denied.
- `row->rid` is populated — use it for `storage_update` / `storage_delete`.

```c
Value pk;
pk.type = TYPE_INT;  pk.is_null = 0;  pk.v.int_val = 42;

Row *row = storage_get_by_pk(rel, &pk);
if (row) {
    int32_t id   = row->cols[0].v.int_val;
    int     nlen = row->cols[1].v.varchar_val.len;
    char   *name = row->cols[1].v.varchar_val.data;  // not NUL-terminated
}
```

> **Warning:** The pointer is overwritten by the next call. Copy the row or
> save the RID if you need it across calls.

---

### `storage_get_by_index`

```c
Row *storage_get_by_index(RelationDef *rel, int col_idx, Value *key);
```

Point lookup by a secondary (UNIQUE) index. O(log n) on the secondary tree +
one clustered page read.

- `col_idx` must be in `rel->secondary_col_idx[]`; returns `NULL` otherwise.
- Descends the secondary B+ tree to find the matching leaf → reads the stored
  RID → fetches the full row from the clustered tree in one page read.
- Returns a pointer to an **internal static buffer** (same caveat as
  `storage_get_by_pk` — save RID before next call).
- `row->rid` is populated.

```c
// SELECT * FROM users WHERE email = 'ali@example.com'
int col_idx = -1;
for (int i = 0; i < rel->num_columns; i++)
    if (strcmp(rel->columns[i].name, "email") == 0) { col_idx = i; break; }

Value key;
key.type = TYPE_VARCHAR;  key.is_null = 0;
key.v.varchar_val.len = strlen("ali@example.com");
memcpy(key.v.varchar_val.data, "ali@example.com", key.v.varchar_val.len);

Row *row = storage_get_by_index(rel, col_idx, &key);
if (row) { /* row->cols[], row->rid both valid */ }
```

---

### `storage_scan`

```c
Cursor *storage_scan(RelationDef *rel);
```

Opens a full-table scan cursor. Rows are returned in **primary key order**
(ascending). Returns `NULL` on error or permission denied.

---

### `storage_scan_from`

```c
Cursor *storage_scan_from(RelationDef *rel, Value *lo);
```

Opens a scan cursor positioned at the first row whose primary key is `>= lo`.
Use for `WHERE pk >= x`, `WHERE pk > x`, or `BETWEEN lo AND hi`.

- `lo` must be the PK column's type.
- The caller applies any **upper bound** by comparing each row's PK and
  breaking when it passes the bound. Storage does not know about upper bounds.
- Returns `NULL` on error or permission denied.

```c
// SELECT * FROM orders WHERE id BETWEEN 1000 AND 2000
Value lo = { .type = TYPE_INT, .v.int_val = 1000 };
Cursor *cur = storage_scan_from(rel, &lo);
Row *row;
while ((row = cursor_next(cur)) != NULL) {
    if (row->cols[0].v.int_val > 2000) break;   // upper bound: caller's job
    // ... emit row ...
}
cursor_close(cur);
```

---

### `storage_scan_by_index`

```c
Cursor *storage_scan_by_index(RelationDef *rel, int col_idx, Value *lo);
```

Opens a scan cursor on a secondary index, positioned at the first entry
where `indexed_column >= lo`. Pass `lo = NULL` to start from the leftmost key.

- `col_idx` must be in `rel->secondary_col_idx[]`; returns `NULL` otherwise.
- `cursor_next` returns **full rows** fetched from the clustered index (RID
  resolution included) — the same `Row *` format as a clustered scan.
- The caller applies any upper bound by checking the indexed column value in
  each returned row and breaking when it passes the desired range.
- Secondary index entries are in **indexed-column order**, not PK order.
  Nearby index keys may point to distant clustered pages — each `cursor_next`
  call may do a random page fetch.

```c
// SELECT * FROM orders WHERE customer_id >= 100
// assuming col 2 (customer_id) has a secondary index

Value lo;
lo.type = TYPE_INT;  lo.is_null = 0;  lo.v.int_val = 100;

Cursor *cur = storage_scan_by_index(rel, 2, &lo);
Row *row;
while ((row = cursor_next(cur)) != NULL) {
    // WHERE filter applies here (e.g. upper bound check)
    // row->cols[], row->rid both valid
}
cursor_close(cur);
```

> **Note:** `storage_scan_by_index` exists in the storage API but is not yet
> wired in `exec_select` for `ACCESS_INDEX_RANGE` queries — Phase 1 falls back
> to `storage_scan` + WHERE filter for secondary range scans. The storage call
> itself is correct and usable directly.

---

### `cursor_next` / `cursor_close`

```c
Row    *cursor_next(Cursor *cursor);
void    cursor_close(Cursor *cursor);
```

- `cursor_next` returns the next row in order, or `NULL` at end-of-scan.
- Each call **overwrites the previous row pointer** — save values or RID before
  the next call.
- **Always call `cursor_close`**, even on early exit or error. Memory leak
  otherwise.
- Deleting or updating the **current row** during a scan is safe. Do not insert
  during a scan.

---

## 9. TCL — Transactions

```c
int storage_begin(void);
int storage_commit(void);
int storage_rollback(void);
```

| Function | Behaviour |
|---|---|
| `storage_begin` | Starts a transaction. Returns `MYDB_ERR` if one is already active. |
| `storage_commit` | Writes all dirty pages to disk. Returns `MYDB_ERR_NO_TXN` if no active transaction. |
| `storage_rollback` | Discards all changes since BEGIN (dirty pages evicted without flushing). Returns `MYDB_ERR_NO_TXN` if no active transaction. |

**Auto-commit:** DML functions called without a prior `storage_begin`
automatically wrap the operation in a single-statement transaction.

```c
storage_begin();
int rc = storage_insert(rel_a, &r1);
if (rc == MYDB_OK) rc = storage_insert(rel_b, &r2);
if (rc == MYDB_OK) storage_commit();
else               storage_rollback();
```

---

## 10. Statistics — ANALYZE TABLE

### `storage_analyze_table`

```c
int storage_analyze_table(RelationDef *rel);
```

Scans the entire clustered B+ tree and computes per-column statistics for the
cost-based optimizer. Writes results to `__stats.mydb` in the active schema
directory (creates the file if it does not exist yet).

Called by the execution engine when the user issues `ANALYZE TABLE <table>;`.

**What is collected per column:**

| Stat | Description |
|---|---|
| `total_rows` | Total row count (same value for every column in the table) |
| `num_nulls` | Number of NULL values |
| `num_distinct` | Number of distinct non-null values seen (NDV) |
| `min_numeric` / `max_numeric` | Min and max values (int64-encoded) |
| MCV entries | Most Common Values + frequency, for low-cardinality columns |
| Histogram buckets | Equi-height histogram, for high-cardinality columns |

**MCV vs Histogram decision:**
- BOOL and ENUM columns → always MCV (natural low cardinality)
- `num_distinct ≤ 16` → MCV, sorted by frequency descending
- `num_distinct > 16` → equi-height histogram (16 buckets), sorted by value

**VARCHAR columns:** Only scalar stats collected (total_rows, num_nulls). No
MCV or histogram (values are not int64-comparable). The planner uses default
selectivity for VARCHAR predicates.

| Return | Meaning |
|---|---|
| `MYDB_OK` | Statistics written to `__stats.mydb` |
| `MYDB_ERR_PERM` | Caller does not have read access to the active schema |

```c
// ANALYZE TABLE orders;
RelationDef *rel = (RelationDef *)engine_find_relation(&eng, "orders");
int rc = storage_analyze_table((RelationDef *)rel);
// rc == MYDB_OK → __stats.mydb updated; next query benefits from CBO
```

The planner reads `__stats.mydb` in `planner_choose_path`. If the file does
not exist (ANALYZE has not been run), the planner falls back to hard-coded
selectivity defaults and typically chooses FULL_SCAN as the safe option.
See `PLANNER.md` for the full design.

---

## 11. Complete Examples

### Example A — CREATE TABLE + INSERT + SELECT

```c
#include "engine.h"
#include "storage.h"
#include <string.h>
#include <stdio.h>

int main(void) {
    EngineState eng;
    engine_init(root_dir, &eng);
    engine_login(&eng, "root", "secret");
    engine_use_schema(&eng, "shop");
    storage_init(&eng);

    /* CREATE TABLE products (
           id    INT AUTO_INCREMENT PRIMARY KEY,
           name  VARCHAR(50) NOT NULL,
           price DECIMAL(8,2)
       ); */
    RelationDef rel;
    memset(&rel, 0, sizeof(rel));
    strncpy(rel.relation_name, "products", 64);
    rel.num_columns = 3;
    rel.pk_col_idx  = 0;
    rel.auto_incr_counter = 1;

    strncpy(rel.columns[0].name, "id", 64);
    rel.columns[0].type = TYPE_INT; rel.columns[0].max_len = 4;
    rel.columns[0].is_not_null = 1; rel.columns[0].is_primary_key = 1;
    rel.columns[0].is_auto_increment = 1;

    strncpy(rel.columns[1].name, "name", 64);
    rel.columns[1].type = TYPE_VARCHAR; rel.columns[1].max_len = 50;
    rel.columns[1].is_not_null = 1;

    strncpy(rel.columns[2].name, "price", 64);
    rel.columns[2].type = TYPE_DECIMAL;
    rel.columns[2].max_len = 8; rel.columns[2].scale = 2;

    storage_create_table(&rel);

    /* INSERT INTO products VALUES (AUTO, 'Keyboard', 49.99) */
    RelationDef *r = (RelationDef *)engine_find_relation(&eng, "products");
    Row row;
    memset(&row, 0, sizeof(Row));
    row.num_cols = 3;
    row.cols[0].type = TYPE_INT;     row.cols[0].is_null = 1;    // AUTO
    row.cols[1].type = TYPE_VARCHAR; row.cols[1].is_null = 0;
    row.cols[1].v.varchar_val.len = 8;
    memcpy(row.cols[1].v.varchar_val.data, "Keyboard", 8);
    row.cols[2].type = TYPE_DECIMAL; row.cols[2].is_null = 0;
    row.cols[2].v.decimal_val = 4999;   // 49.99 × 100

    storage_insert(r, &row);
    // row.cols[0].v.int_val == 1  (assigned by storage)

    /* SELECT * FROM products WHERE id = 1 */
    Value pk;
    pk.type = TYPE_INT; pk.is_null = 0; pk.v.int_val = 1;
    Row *found = storage_get_by_pk(r, &pk);
    if (found) {
        printf("id=%d  name=%.*s  price=%.2f\n",
               found->cols[0].v.int_val,
               found->cols[1].v.varchar_val.len,
               found->cols[1].v.varchar_val.data,
               found->cols[2].v.decimal_val / 100.0);
    }

    storage_shutdown();
    engine_close(&eng);
    return 0;
}
```

---

### Example B — Full table scan with UPDATE

```c
/* UPDATE products SET price = price * 1.10  (10% price increase) */
RelationDef *r = (RelationDef *)engine_find_relation(&eng, "products");
Cursor *cur = storage_scan(r);
Row *row;
while ((row = cursor_next(cur)) != NULL) {
    RID rid = row->rid;
    Row updated = *row;
    updated.cols[2].v.decimal_val =
        (int64_t)(row->cols[2].v.decimal_val * 1.10);
    storage_update(r, rid, &updated);
}
cursor_close(cur);
```

---

### Example C — Transaction with rollback

```c
RelationDef *r = (RelationDef *)engine_find_relation(&eng, "accounts");

storage_begin();

Row r1 = { /* ... */ };
int rc = storage_insert(r, &r1);
if (rc != MYDB_OK) {
    storage_rollback();
} else {
    storage_commit();
}
```

---

### Example D — FK tables (departments → employees)

```c
/* INSERT a department first (parent row). */
RelationDef *dept = (RelationDef *)engine_find_relation(&eng, "departments");
Row dr;
memset(&dr, 0, sizeof(Row));
dr.num_cols = 1;
dr.cols[0].type = TYPE_INT; dr.cols[0].is_null = 0; dr.cols[0].v.int_val = 10;
storage_insert(dept, &dr);   // MYDB_OK

/* INSERT an employee referencing dept=10. */
RelationDef *emp = (RelationDef *)engine_find_relation(&eng, "employees");
Row er;
memset(&er, 0, sizeof(Row));
er.num_cols = 2;
er.cols[0].type = TYPE_INT;  er.cols[0].is_null = 1;         // AUTO id
er.cols[1].type = TYPE_INT;  er.cols[1].is_null = 0;
er.cols[1].v.int_val = 10;                                    // dept_id = 10
storage_insert(emp, &er);    // MYDB_OK

/* Deleting the department now fails (RESTRICT). */
Value dept_pk;
dept_pk.type = TYPE_INT; dept_pk.is_null = 0; dept_pk.v.int_val = 10;
Row *dept_row = storage_get_by_pk(dept, &dept_pk);
int rc = storage_delete(dept, dept_row->rid);
// rc == MYDB_ERR_FK_VIOLATION  (employee still references dept 10)
```

---

### Example E — ENUM column

```c
/* Schema: status ENUM(active, inactive, pending) */
strncpy(rel.columns[3].name, "status", 64);
rel.columns[3].type            = TYPE_ENUM;
rel.columns[3].num_enum_values = 3;
strncpy(rel.columns[3].enum_values[0], "active",   32);
strncpy(rel.columns[3].enum_values[1], "inactive", 32);
strncpy(rel.columns[3].enum_values[2], "pending",  32);

/* INSERT: status = "active" (index 0) */
row.cols[3].type       = TYPE_ENUM;
row.cols[3].is_null    = 0;
row.cols[3].v.enum_val = 0;   // "active"

/* Reading back */
Row *r = storage_get_by_pk(rel_ptr, &pk);
uint8_t idx = r->cols[3].v.enum_val;
const char *label = rel->columns[3].enum_values[idx];  // "active"
```

---

### Example F — Secondary index lookup (UNIQUE column)

```c
/* SELECT * FROM users WHERE email = 'ali@example.com'
   (col 1 = email, UNIQUE, has secondary index)          */

RelationDef *rel = (RelationDef *)engine_find_relation(&eng, "users");

int col_idx = -1;
for (int i = 0; i < rel->num_columns; i++)
    if (strcmp(rel->columns[i].name, "email") == 0) { col_idx = i; break; }

Value key;
key.type = TYPE_VARCHAR; key.is_null = 0;
key.v.varchar_val.len = strlen("ali@example.com");
memcpy(key.v.varchar_val.data, "ali@example.com", key.v.varchar_val.len);

Row *row = storage_get_by_index(rel, col_idx, &key);
if (row) {
    printf("id=%d  email=%.*s\n",
           row->cols[0].v.int_val,
           row->cols[1].v.varchar_val.len,
           row->cols[1].v.varchar_val.data);
}
```

---

### Example G — Secondary index range scan

```c
/* SELECT * FROM orders WHERE customer_id >= 100 AND customer_id <= 200
   (col 2 = customer_id, has secondary index)                           */

Value lo;
lo.type = TYPE_INT;  lo.is_null = 0;  lo.v.int_val = 100;

Cursor *cur = storage_scan_by_index(rel, 2, &lo);
Row *row;
while ((row = cursor_next(cur)) != NULL) {
    if (row->cols[2].v.int_val > 200) break;   // upper bound
    // ... emit row ...
}
cursor_close(cur);
```

---

### Example H — ANALYZE TABLE + planner benefit

```c
/* Run once after bulk loading data. */
RelationDef *rel = (RelationDef *)engine_find_relation(&eng, "orders");
storage_analyze_table((RelationDef *)rel);

/* Subsequent queries against orders now benefit from CBO:
   the planner reads __stats.mydb to decide whether a full scan
   or an index range scan is cheaper, based on actual row counts
   and value distributions.                                       */
```

---

## 12. Constraints and Limits

| Item | Limit |
|---|---|
| Max relations per schema | 64 |
| Max columns per relation | 32 |
| Max VARCHAR length | 150 characters |
| Max ENUM values per column | 16 |
| Max ENUM string length | 32 characters |
| Max secondary indexes (UNIQUE + INDEXED) per relation | 8 (`MAX_SECONDARY_IDX`) |
| Max foreign keys per relation | 8 |
| Max partitions (users with data) | 16 |
| Max schemas per partition | 64 |
| Max MCV / histogram entries per column | 16 (`STATS_MAX_ENTRIES`) |
| Buffer pool size | 64 pages (1 MB) |
| Page size | 16 KB |
| Default partition quota | 1 GB (`ENGINE_DEFAULT_QUOTA_BYTES`) |
| Minimum partition quota | 100 MB (`ENGINE_MIN_QUOTA_BYTES`) |
| Maximum partition quota | 5 GB (`ENGINE_MAX_QUOTA_BYTES`) |

---

## 13. What is NOT Implemented

- **ALTER TABLE** — table definitions cannot be changed after creation.
- **CASCADE FK** — only RESTRICT is enforced (no ON DELETE CASCADE / UPDATE CASCADE).
- **Crash recovery / WAL** — data since the last COMMIT may be lost on unclean exit.
- **Concurrent access** — single-threaded; do not call from multiple threads.
- **Cross-schema queries** — all tables in one query must belong to the same active
  schema; cross-schema references return `MYDB_ERR_CROSS_SCHEMA`.
- **FULL OUTER JOIN** — out of scope for Phase 1.
- **Views, triggers, stored procedures** — not supported.
- **`ACCESS_INDEX_RANGE` wired in exec_select** — the planner correctly identifies
  secondary range scans as the cheapest path when selectivity is very low, but
  `plan_to_ap` currently maps `ACCESS_INDEX_RANGE` to `AP_SCAN` (full scan + WHERE
  filter). `storage_scan_by_index` is available; it is not yet called by exec_select.

---

*For questions about storage engine internals, contact Hasnat Akram.*  
*For planner design details, see `PLANNER.md` at the repo root.*
