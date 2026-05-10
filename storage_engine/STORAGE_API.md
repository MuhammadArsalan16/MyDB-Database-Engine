# MyDB Storage Engine — API Reference for the Execution Engine

**Author:** Hasnat Akram (Storage Engine)  
**Audience:** Rehan Ali Abbasi (Execution Engine)  
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
6. [DDL — Create and Drop Tables](#6-ddl--create-and-drop-tables)
7. [DML — Insert, Update, Delete](#7-dml--insert-update-delete)
8. [DQL — Querying Data](#8-dql--querying-data)
9. [TCL — Transactions](#9-tcl--transactions)
10. [Complete Examples](#10-complete-examples)
11. [Constraints and Limits](#11-constraints-and-limits)
12. [What is NOT Implemented](#12-what-is-not-implemented)

---

## 1. What Changed from v1

The storage engine went through a complete redesign (v2). If you wrote code against the old API, **everything below has changed**:

| v1 (old) | v2 (current) |
|---|---|
| `storage_init("./data")` | `storage_init(&eng)` — takes an EngineState |
| `storage_insert("users", &row)` | `storage_insert(rel, &row)` — takes a `RelationDef *` |
| `storage_get_by_pk("users", &pk)` | `storage_get_by_pk(rel, &pk)` |
| `storage_scan("users")` | `storage_scan(rel)` |
| `storage_create_table("users", &schema)` | `storage_create_table(&rel)` |
| `storage_drop_table("users")` | `storage_drop_table(rel)` |
| `Schema` struct, `table_name` field | `RelationDef` struct, `relation_name` field |
| FK constraints stored but not checked | FK constraints enforced (RESTRICT) |
| No login / auth | Full login + privilege system |

---

## 2. Session Lifecycle

The execution engine must open a session through the engine module before calling any storage function. The full startup sequence is:

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

### `engine_init`

```c
int engine_init(const char *root_dir, EngineState *out);
```

Opens an already-bootstrapped engine at `root_dir`. No user is logged in yet. `root_dir` is typically `~/.mydb/` (the `$MYDB_HOME` environment variable, resolved by the CLI layer).

---

### `engine_login`

```c
int engine_login(EngineState *eng, const char *username, const char *password);
```

Verifies the password against the system user table (SHA-256 + per-user salt). On success, opens the user's partition catalog and stamps `last_login`.

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

Selects the active schema. Called every time the parser processes `USE schema_name`. Flushes dirty pages from the previously active schema before switching.

**Owner path** (user owns the partition): schema must exist in the user's catalog.  
**Analyst path** (no partition): user must have a SELECT privilege grant for the schema.

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

Binds the storage runtime to the open engine session. Sets up the buffer pool and transaction manager. Call once after `engine_login` (and after each `engine_use_schema` is NOT needed — storage re-reads the active schema on every call).

---

### `storage_shutdown`

```c
int storage_shutdown(void);
```

Commits any open transaction, flushes all dirty pages, closes all table files. Call once before `engine_close`.

---

## 3. Looking Up Tables

Before calling any DDL/DML/DQL function, the execution engine must resolve the table name into a `RelationDef *`. The parser should do this at parse time using:

```c
const RelationDef *engine_find_relation(EngineState *eng, const char *relation_name);
```

Returns a pointer into the active schema's in-memory table, or `NULL` if no such relation exists. The pointer is valid until the next `engine_use_schema` or `engine_close`.

**Example — parse-time lookup:**

```c
const RelationDef *rel = engine_find_relation(&eng, "users");
if (rel == NULL) {
    // table does not exist — report a parse error to the user
}
// cast away const for storage functions that mutate the auto_incr counter
storage_insert((RelationDef *)rel, &row);
```

> The pointer returned is `const` because the parser/exec layer must not modify the schema directly. Storage is the only writer. Cast to `RelationDef *` only when calling storage functions — they need to update `auto_incr_counter` after an AUTO_INCREMENT insert.

---

## 4. Key Data Types

### 4.1 RelationDef and ColumnDef

`RelationDef` replaces the old `Schema` struct. Field names changed: `table_name` → `relation_name`.

```c
struct RelationDef {
    char       relation_name[64];     // table name, e.g. "users"

    uint8_t    num_columns;
    ColumnDef  columns[32];           // column definitions, in declaration order
    uint8_t    pk_col_idx;            // index into columns[] for the primary key

    uint8_t    num_foreign_keys;
    ForeignKey foreign_keys[8];

    uint32_t   auto_incr_counter;     // next AUTO_INCREMENT value (managed by storage)
    uint32_t   root_page_no;          // root of the clustered B+ tree (set by storage)

    uint8_t    num_secondary_indexes;
    uint8_t    secondary_col_idx[8];        // which columns have secondary indexes
    uint32_t   secondary_root_page_no[8];   // their root pages (set by storage)
};
```

```c
struct ColumnDef {
    char     name[64];
    DataType type;
    uint16_t max_len;           // VARCHAR: max characters; DECIMAL: total digits
    uint8_t  scale;             // DECIMAL only: digits after the decimal point

    uint8_t  is_not_null;
    uint8_t  is_primary_key;
    uint8_t  is_unique;
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
- `auto_incr_counter` (set to `1` at creation; storage increments it)

**Secondary indexes** are created automatically for every column with `is_unique = 1` that is not the primary key. You must tell storage which columns those are:

```c
rel.num_secondary_indexes = 1;
rel.secondary_col_idx[0]  = 2;  // column index 2 has UNIQUE constraint
```

---

### 4.2 ForeignKey

```c
typedef struct {
    char constraint_name[64];     // optional name, e.g. "fk_dept"
    char column_name[64];         // FK column in this relation, e.g. "dept_id"
    char ref_relation_name[64];   // referenced relation, e.g. "departments"
    char ref_column_name[64];     // referenced column, e.g. "id"
} ForeignKey;
```

FK constraints are **automatically enforced** by storage (RESTRICT behavior — see [Section 7](#7-dml--insert-update-delete)).

---

### 4.3 Value

A `Value` holds one column's data. Always set `type` and `is_null` first.

```c
typedef struct {
    DataType type;
    uint8_t  is_null;   // 1 = NULL, 0 = has a value
    union {
        int32_t  int_val;           // TYPE_INT
        int64_t  decimal_val;       // TYPE_DECIMAL  (value × 10^scale)
        struct {
            uint16_t len;
            char     data[150];
        } varchar_val;              // TYPE_VARCHAR
        uint8_t  enum_val;          // TYPE_ENUM  (0-based index into enum list)
        uint8_t  bool_val;          // TYPE_BOOL  (0 or 1)
        int32_t  date_val;          // TYPE_DATE  (YYYYMMDD)
        int64_t  datetime_val;      // TYPE_DATETIME  (YYYYMMDDHHmmSS)
    } v;
} Value;
```

**DataType constants:**

| Constant | Union member | Storage | Notes |
|---|---|---|---|
| `TYPE_INT` | `int_val` | 4 bytes | 32-bit signed integer |
| `TYPE_DECIMAL` | `decimal_val` | 8 bytes | Stored as `value × 10^scale`. E.g. `3.14` with `scale=2` → store `314` |
| `TYPE_VARCHAR` | `varchar_val` | 2 + n bytes | `len` = actual byte count, `data` = string (not NUL-terminated) |
| `TYPE_ENUM` | `enum_val` | 1 byte | 0-based index into the column's `enum_values[]` list |
| `TYPE_BOOL` | `bool_val` | 1 byte | 0 = false, 1 = true |
| `TYPE_DATE` | `date_val` | 4 bytes | Integer YYYYMMDD. E.g. 25 Dec 2024 → `20241225` |
| `TYPE_DATETIME` | `datetime_val` | 8 bytes | Integer YYYYMMDDHHmmSS. E.g. `20241225143000` |

**Setting values:**

```c
// INT
Value v;
v.type = TYPE_INT;  v.is_null = 0;  v.v.int_val = 100;

// VARCHAR
v.type = TYPE_VARCHAR;  v.is_null = 0;
v.v.varchar_val.len = 5;
memcpy(v.v.varchar_val.data, "Alice", 5);

// DECIMAL — e.g. 3.14 with scale=2
v.type = TYPE_DECIMAL;  v.is_null = 0;  v.v.decimal_val = 314;

// BOOL
v.type = TYPE_BOOL;  v.is_null = 0;  v.v.bool_val = 1;

// DATE — 2024-12-25
v.type = TYPE_DATE;  v.is_null = 0;  v.v.date_val = 20241225;

// DATETIME — 2024-12-25 14:30:00
v.type = TYPE_DATETIME;  v.is_null = 0;  v.v.datetime_val = 20241225143000LL;

// ENUM — index 0 means the first value in the column's enum list
v.type = TYPE_ENUM;  v.is_null = 0;  v.v.enum_val = 0;

// NULL (type still needed for the engine to know what kind of NULL it is)
v.type = TYPE_INT;  v.is_null = 1;
```

---

### 4.4 Row

```c
typedef struct Row {
    uint8_t  num_cols;       // must match rel->num_columns
    Value    cols[32];       // cols[i] corresponds to rel->columns[i]
    RID      rid;            // set by the engine on GET/SCAN; ignored on INSERT
} Row;
```

- `cols[i]` must match `rel->columns[i]` in type and order.
- `rid` is filled in by `storage_get_by_pk` and `cursor_next`. Pass it back to `storage_update` / `storage_delete`.
- On INSERT, `rid` is ignored.

---

### 4.5 RID

```c
typedef struct {
    uint32_t page_no;
    uint16_t slot_no;
} RID;
```

A Record ID uniquely identifies a row on disk. You never construct one — the engine fills it in. Save it before the next `cursor_next` call overwrites it.

---

### 4.6 Cursor

An opaque scan handle. Always close it when done.

```c
Cursor *cur = storage_scan(rel);
Row *row;
while ((row = cursor_next(cur)) != NULL) {
    // use row->cols[]...
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
| `MYDB_ERR_DUPLICATE` | `-3` | Primary key or UNIQUE constraint violation |
| `MYDB_ERR_FULL` | `-4` | Buffer pool full, catalog full, or partition quota exhausted |
| `MYDB_ERR_FK_VIOLATION` | `-5` | Foreign key RESTRICT violation |
| `MYDB_ERR_NULL_VIOLATION` | `-6` | NULL inserted into a NOT NULL column |
| `MYDB_ERR_NO_TXN` | `-7` | `commit` or `rollback` called with no active transaction |
| `MYDB_ERR_PERM` | `-8` | User not logged in, no active schema, or insufficient privileges |
| `MYDB_ERR_CROSS_SCHEMA` | `-9` | Operation references a relation outside the active schema |

**`MYDB_ERR_FULL` covers two distinct situations:**
- Buffer pool has no free frames (all 64 frames pinned).
- The user's partition quota would be exceeded by the operation.

---

## 6. DDL — Create and Drop Tables

### `storage_create_table`

```c
int storage_create_table(RelationDef *rel);
```

Creates a new relation in the active schema. Allocates B+ tree root pages and writes the definition to `__schema.mydb`.

**You must fill in:**
- `rel->relation_name`
- `rel->num_columns` and `rel->columns[i]` for each column
- `rel->pk_col_idx`
- `rel->num_secondary_indexes` and `rel->secondary_col_idx[]` for UNIQUE columns
- `rel->num_foreign_keys` and `rel->foreign_keys[]` for FK constraints
- `rel->auto_incr_counter = 1` if any column uses AUTO_INCREMENT

**Do NOT fill in:** `root_page_no`, `secondary_root_page_no[]` — storage fills these in.

Returns `MYDB_ERR_DUPLICATE` if a relation with that name already exists in the schema.  
Returns `MYDB_ERR_FULL` if the partition quota would be exceeded.

**Example:**

```c
RelationDef rel;
memset(&rel, 0, sizeof(rel));
strncpy(rel.relation_name, "users", 64);

rel.num_columns = 3;
rel.pk_col_idx  = 0;
rel.auto_incr_counter = 1;

// col 0: id INT AUTO_INCREMENT PRIMARY KEY
strncpy(rel.columns[0].name, "id", 64);
rel.columns[0].type             = TYPE_INT;
rel.columns[0].max_len          = 4;
rel.columns[0].is_not_null      = 1;
rel.columns[0].is_primary_key   = 1;
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

// register the UNIQUE column
rel.num_secondary_indexes = 1;
rel.secondary_col_idx[0]  = 1;   // column 1 (email) is UNIQUE

int rc = storage_create_table(&rel);
// rc == MYDB_OK on success
// rel.root_page_no is now populated by storage
```

---

### `storage_drop_table`

```c
int storage_drop_table(RelationDef *rel);
```

Deletes the relation's `.mydb` file from disk, removes it from `__schema.mydb`, and rebates its quota usage. **This is permanent.**

```c
RelationDef *rel = (RelationDef *)engine_find_relation(&eng, "users");
storage_drop_table(rel);
```

---

## 7. DML — Insert, Update, Delete

All DML functions enforce the following automatically — the execution engine does not check these manually:

- **NOT NULL** — returns `MYDB_ERR_NULL_VIOLATION` if a NOT NULL column is given a NULL value.
- **Duplicate key** — returns `MYDB_ERR_DUPLICATE` on PK or UNIQUE collision.
- **Authorization** — returns `MYDB_ERR_PERM` if the current user does not have write access (e.g. analyst with SELECT-only grant).
- **FK RESTRICT** — see below.

### Foreign Key Enforcement (RESTRICT)

FKs are enforced automatically on INSERT, UPDATE, and DELETE:

| Operation | Check | Error on violation |
|---|---|---|
| `storage_insert` | The FK column's value must exist as a PK in the referenced table. | `MYDB_ERR_FK_VIOLATION` |
| `storage_update` | If the FK column changes, the new value must exist in the referenced table. If the PK changes, the old PK must not be referenced by any other table. | `MYDB_ERR_FK_VIOLATION` |
| `storage_delete` | The row's PK must not be referenced by any FK in any other table in the schema. | `MYDB_ERR_FK_VIOLATION` |

**NULL FK values are allowed** — a NULL FK column is not checked against the referenced table (standard SQL behavior).

---

### `storage_insert`

```c
int storage_insert(RelationDef *rel, Row *row);
```

Inserts one row.

- For AUTO_INCREMENT PK: set `row->cols[pk_idx].is_null = 1` — storage assigns the next counter value and writes it back into `row->cols[pk_idx].v.int_val`.
- All other rules above apply.

**Example — auto-increment id:**

```c
RelationDef *rel = (RelationDef *)engine_find_relation(&eng, "users");

Row row;
memset(&row, 0, sizeof(Row));
row.num_cols = 3;

row.cols[0].type    = TYPE_INT;
row.cols[0].is_null = 1;                  // AUTO_INCREMENT — leave null

row.cols[1].type = TYPE_VARCHAR;  row.cols[1].is_null = 0;
row.cols[1].v.varchar_val.len = 15;
memcpy(row.cols[1].v.varchar_val.data, "alice@email.com", 15);

row.cols[2].type = TYPE_DECIMAL;  row.cols[2].is_null = 0;
row.cols[2].v.decimal_val = 9550;         // 95.50 with scale=2

int rc = storage_insert(rel, &row);
// rc == MYDB_OK  →  row.cols[0].v.int_val now holds the assigned id (e.g. 1)
```

---

### `storage_update`

```c
int storage_update(RelationDef *rel, RID rid, Row *new_row);
```

Replaces the row at `rid` with `new_row`. The RID comes from `row->rid` returned by `storage_get_by_pk` or `cursor_next`. Internally this is a delete + insert, so the same constraints apply.

**Example:**

```c
RelationDef *rel = (RelationDef *)engine_find_relation(&eng, "users");

// Fetch to get the RID
Value pk;
pk.type = TYPE_INT;  pk.is_null = 0;  pk.v.int_val = 1;
Row *existing = storage_get_by_pk(rel, &pk);

// Build updated row
Row updated = *existing;          // copy all column values
updated.cols[2].v.decimal_val = 10000;  // change score to 100.00

// Update using the saved RID
RID rid = existing->rid;          // save before next storage call overwrites it
storage_update(rel, rid, &updated);
```

---

### `storage_delete`

```c
int storage_delete(RelationDef *rel, RID rid);
```

Deletes the row at `rid`. Returns `MYDB_ERR_FK_VIOLATION` if another table's FK column references this row's primary key.

**Example:**

```c
RelationDef *rel = (RelationDef *)engine_find_relation(&eng, "users");

Value pk;
pk.type = TYPE_INT;  pk.is_null = 0;  pk.v.int_val = 1;
Row *row = storage_get_by_pk(rel, &pk);
if (row != NULL) {
    int rc = storage_delete(rel, row->rid);
    // rc == MYDB_ERR_FK_VIOLATION → another table still references this row
}
```

---

## 8. DQL — Querying Data

Both `storage_get_by_pk` and `cursor_next` return `MYDB_ERR_PERM` (as NULL) if the user does not have at least SELECT access on the schema. Analyst users with a SELECT grant can read; they cannot write.

### `storage_get_by_pk`

```c
Row *storage_get_by_pk(RelationDef *rel, Value *pk);
```

Point lookup by primary key via the B+ tree. O(log n).

- Returns a pointer to an **internal static buffer** — valid until the next call to `storage_get_by_pk` on any relation.
- Returns `NULL` if the row does not exist or permission is denied.
- `row->rid` is populated — use it for `storage_update` / `storage_delete`.

```c
RelationDef *rel = (RelationDef *)engine_find_relation(&eng, "products");

Value pk;
pk.type = TYPE_INT;  pk.is_null = 0;  pk.v.int_val = 42;

Row *row = storage_get_by_pk(rel, &pk);
if (row == NULL) {
    // not found
} else {
    int32_t id    = row->cols[0].v.int_val;
    int     nlen  = row->cols[1].v.varchar_val.len;
    char   *name  = row->cols[1].v.varchar_val.data;  // NOT NUL-terminated
    // use nlen to bound string operations
}
```

> **Warning:** Two simultaneous pointers from `storage_get_by_pk` are not valid — the second call overwrites the buffer from the first. Copy the row if you need both.

---

### `storage_scan` / `cursor_next` / `cursor_close`

```c
Cursor *storage_scan(RelationDef *rel);
Row    *cursor_next(Cursor *cursor);
void    cursor_close(Cursor *cursor);
```

Full sequential scan in **primary key order** (ascending).

- `storage_scan` returns a `Cursor *`, or `NULL` on error / permission denied.
- `cursor_next` returns the next row, or `NULL` at end-of-scan.
- Each `cursor_next` call **overwrites the previous row pointer** — save the RID or copy values before calling again.
- **Always call `cursor_close`**, even on early exit. Memory leak otherwise.

**Example — full scan:**

```c
RelationDef *rel = (RelationDef *)engine_find_relation(&eng, "orders");
Cursor *cur = storage_scan(rel);
if (cur == NULL) { /* error or no permission */ }

Row *row;
while ((row = cursor_next(cur)) != NULL) {
    int32_t id     = row->cols[0].v.int_val;
    uint8_t status = row->cols[3].v.enum_val;
    printf("order %d  status=%d\n", id, status);
}
cursor_close(cur);
```

**Example — scan with WHERE filter and DELETE:**

```c
// DELETE FROM orders WHERE status = 2
Cursor *cur = storage_scan(rel);
Row *row;
while ((row = cursor_next(cur)) != NULL) {
    if (row->cols[3].v.enum_val == 2) {
        RID rid = row->rid;          // save before cursor_next overwrites
        storage_delete(rel, rid);
    }
}
cursor_close(cur);
```

> Deleting or updating the **current row** during a scan is safe. Do not insert rows during a scan.

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
| `storage_commit` | Writes all dirty pages to disk. Returns `MYDB_ERR_NO_TXN` if no transaction is active. |
| `storage_rollback` | Discards all changes since `BEGIN` (pages are evicted without flushing). Returns `MYDB_ERR_NO_TXN` if no transaction is active. |

**Auto-commit:** If you call any DML function without calling `storage_begin` first, the engine automatically wraps the operation in a single-statement transaction. Use explicit transactions only when you need to group multiple operations atomically.

**Example:**

```c
storage_begin();

Row r1 = { ... };  storage_insert(rel_a, &r1);
Row r2 = { ... };  storage_insert(rel_b, &r2);

if (everything_ok) {
    storage_commit();     // both rows written to disk together
} else {
    storage_rollback();   // neither row persisted
}
```

---

## 10. Complete Examples

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
    row.cols[0].type = TYPE_INT;     row.cols[0].is_null = 1;   // AUTO
    row.cols[1].type = TYPE_VARCHAR; row.cols[1].is_null = 0;
    row.cols[1].v.varchar_val.len = 8;
    memcpy(row.cols[1].v.varchar_val.data, "Keyboard", 8);
    row.cols[2].type = TYPE_DECIMAL; row.cols[2].is_null = 0;
    row.cols[2].v.decimal_val = 4999;    // 49.99 × 100

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

Row r1 = { ... };
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
/* Schema setup: departments.id is PK, employees.dept_id FK → departments.id */

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
er.cols[0].type = TYPE_INT;  er.cols[0].is_null = 1;          // AUTO id
er.cols[1].type = TYPE_INT;  er.cols[1].is_null = 0;
er.cols[1].v.int_val = 10;                                     // dept_id = 10
storage_insert(emp, &er);    // MYDB_OK

/* Trying to delete the department now returns MYDB_ERR_FK_VIOLATION. */
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
rel.columns[3].type           = TYPE_ENUM;
rel.columns[3].num_enum_values = 3;
strncpy(rel.columns[3].enum_values[0], "active",   32);
strncpy(rel.columns[3].enum_values[1], "inactive", 32);
strncpy(rel.columns[3].enum_values[2], "pending",  32);

/* INSERT — status = "active" (index 0) */
row.cols[3].type       = TYPE_ENUM;
row.cols[3].is_null    = 0;
row.cols[3].v.enum_val = 0;   // "active"

/* Reading back */
Row *r = storage_get_by_pk(rel_ptr, &pk);
uint8_t idx = r->cols[3].v.enum_val;
// idx 0 = "active", 1 = "inactive", 2 = "pending"
// To convert: rel->columns[3].enum_values[idx]
```

---

## 11. Constraints and Limits

| Item | Limit |
|---|---|
| Max relations per schema | 64 |
| Max columns per relation | 32 |
| Max VARCHAR length | 150 characters |
| Max ENUM values per column | 16 |
| Max ENUM string length | 32 characters |
| Max UNIQUE columns (secondary indexes) per relation | 8 |
| Max foreign keys per relation | 8 |
| Max partitions (users with data) | 16 |
| Max schemas per partition | 64 |
| Buffer pool size | 64 pages (1 MB) |
| Page size | 16 KB |
| Default partition quota | 1 GB |

---

## 12. What is NOT Implemented

- **ALTER TABLE** — table definitions cannot be changed after creation.
- **CASCADE FK** — only RESTRICT is enforced. There is no ON DELETE CASCADE or ON UPDATE CASCADE.
- **Crash recovery / WAL** — if the process exits without calling `storage_shutdown`, data written since the last `COMMIT` may be lost.
- **Concurrent access** — single-threaded. Do not call from multiple threads simultaneously.
- **Cross-schema queries** — all tables in a single query must belong to the same active schema. Referencing a table in a different schema returns `MYDB_ERR_CROSS_SCHEMA`.
- **FULL OUTER JOIN** — handled by the execution engine, not the storage layer.
- **Views, triggers, stored procedures** — not supported.

---

*For questions about the storage engine internals, contact Hasnat Akram.*
