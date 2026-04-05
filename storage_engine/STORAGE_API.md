# MyDB Storage Engine — API Reference for the Execution Engine

**Author:** Hasnat Akram (Storage Engine)  
**Audience:** Rehan Ali Abbasi (Execution Engine)  
**Include:** `#include "storage.h"` — this is the only header you need.

---

## Table of Contents

1. [Quick Start](#1-quick-start)
2. [Key Data Types](#2-key-data-types)
   - [Value](#21-value)
   - [Row](#22-row)
   - [Schema and ColumnDef](#23-schema-and-columndef)
   - [RID](#24-rid)
   - [Cursor](#25-cursor)
3. [Error Codes](#3-error-codes)
4. [Engine Lifecycle](#4-engine-lifecycle)
5. [DDL — Create and Drop Tables](#5-ddl--create-and-drop-tables)
6. [DML — Insert, Update, Delete](#6-dml--insert-update-delete)
7. [DQL — Querying Data](#7-dql--querying-data)
8. [TCL — Transactions](#8-tcl--transactions)
9. [Complete Examples](#9-complete-examples)
10. [Constraints and Limits](#10-constraints-and-limits)
11. [What is NOT Implemented (Phase 1)](#11-what-is-not-implemented-phase-1)

---

## 1. Quick Start

```c
#include "storage.h"

// 1. Initialise once at program startup
storage_init("/path/to/data/directory");

// 2. Use the API
storage_begin();

Row row = {0};
row.num_cols = 1;
row.cols[0].type      = TYPE_INT;
row.cols[0].is_null   = 0;
row.cols[0].v.int_val = 42;

storage_insert("my_table", &row);
storage_commit();

// 3. Shut down once at program exit
storage_shutdown();
```

---

## 2. Key Data Types

### 2.1 Value

A `Value` holds a single column's data. It is a tagged union — always set `type` and `is_null` first.

```c
typedef struct {
    DataType type;      // which union member to use
    uint8_t  is_null;   // 1 = NULL value, 0 = has a value
    union {
        int32_t  int_val;           // TYPE_INT
        int64_t  decimal_val;       // TYPE_DECIMAL  (value * 10^scale)
        struct {
            uint16_t len;           // TYPE_VARCHAR
            char     data[150];
        } varchar_val;
        uint8_t  enum_val;          // TYPE_ENUM  (index into enum list)
        uint8_t  bool_val;          // TYPE_BOOL  (0 or 1)
        int32_t  date_val;          // TYPE_DATE  (YYYYMMDD as integer)
        int64_t  datetime_val;      // TYPE_DATETIME  (YYYYMMDDHHmmSS)
    } v;
} Value;
```

**DataType enum values:**

| Constant | C type used | Notes |
|---|---|---|
| `TYPE_INT` | `int32_t int_val` | 4-byte signed integer |
| `TYPE_DECIMAL` | `int64_t decimal_val` | Stored as `value * 10^scale`. E.g. `3.14` with scale=2 → `314` |
| `TYPE_VARCHAR` | `varchar_val.len` + `varchar_val.data` | Max 150 chars |
| `TYPE_ENUM` | `uint8_t enum_val` | 0-based index into the column's enum list |
| `TYPE_BOOL` | `uint8_t bool_val` | 0 = false, 1 = true |
| `TYPE_DATE` | `int32_t date_val` | Integer YYYYMMDD. E.g. 25 Dec 2024 → `20241225` |
| `TYPE_DATETIME` | `int64_t datetime_val` | Integer YYYYMMDDHHmmSS. E.g. `20241225143000` |

**How to set a Value:**

```c
// INT
Value v;
v.type = TYPE_INT;  v.is_null = 0;  v.v.int_val = 100;

// VARCHAR
v.type = TYPE_VARCHAR;  v.is_null = 0;
v.v.varchar_val.len = 5;
memcpy(v.v.varchar_val.data, "Alice", 5);

// DECIMAL — e.g. 3.14 with scale=2 → store as 314
v.type = TYPE_DECIMAL;  v.is_null = 0;  v.v.decimal_val = 314;

// BOOL
v.type = TYPE_BOOL;  v.is_null = 0;  v.v.bool_val = 1;

// DATE — 2024-12-25
v.type = TYPE_DATE;  v.is_null = 0;  v.v.date_val = 20241225;

// DATETIME — 2024-12-25 14:30:00
v.type = TYPE_DATETIME;  v.is_null = 0;  v.v.datetime_val = 20241225143000LL;

// ENUM — "active" is index 0 in the column's enum list
v.type = TYPE_ENUM;  v.is_null = 0;  v.v.enum_val = 0;

// NULL
v.type = TYPE_INT;  v.is_null = 1;  // v.v is ignored
```

---

### 2.2 Row

A `Row` is an array of `Value`s — one per column, in the same order as defined in the schema.

```c
typedef struct Row {
    uint8_t  num_cols;          // number of columns (must match schema)
    Value    cols[32];          // column values, cols[0] = first column
    RID      rid;               // set by the engine — use for UPDATE/DELETE
} Row;
```

**Rules:**
- `num_cols` must equal the table's `schema->num_columns`.
- `cols[i]` corresponds to `schema->columns[i]` — **order matters**.
- `rid` is **set by the engine** when rows are returned from `storage_get_by_pk` or `cursor_next`. You pass it back to `storage_update` / `storage_delete`.
- On INSERT, `rid` is ignored — don't fill it in.

---

### 2.3 Schema and ColumnDef

You build a `Schema` when creating a table, then pass it to `storage_create_table`.

```c
typedef struct Schema {
    char       table_name[64];
    uint8_t    num_columns;
    ColumnDef  columns[32];
    uint8_t    pk_col_idx;           // which column is the primary key

    uint8_t    num_foreign_keys;
    ForeignKey foreign_keys[8];

    uint32_t   auto_incr_counter;    // set to 1 for AUTO_INCREMENT tables
    // root_page_no and secondary_root_page_no[] are set BY THE ENGINE — leave as 0
    ...
} Schema;
```

```c
typedef struct ColumnDef {
    char     name[64];
    DataType type;
    uint16_t max_len;           // VARCHAR: max characters; DECIMAL: total digits
    uint8_t  scale;             // DECIMAL only: digits after decimal point
    uint8_t  is_not_null;
    uint8_t  is_primary_key;
    uint8_t  is_unique;
    uint8_t  is_auto_increment; // only valid on INT PRIMARY KEY
    uint8_t  has_default;
    Value    default_value;     // used when has_default == 1
    uint8_t  num_enum_values;
    char     enum_values[16][32]; // only for TYPE_ENUM
} ColumnDef;
```

**Important:** `root_page_no` and `secondary_root_page_no[]` are managed by the storage engine. Leave them as `0` when building a schema — `storage_create_table` fills them in.

**Secondary indexes** are created automatically for every column that has `is_unique = 1` (and is not the primary key). You must populate `schema->num_secondary_indexes` and `schema->secondary_col_idx[]` to tell the engine which columns those are:

```c
schema.num_secondary_indexes = 1;
schema.secondary_col_idx[0]  = 1;  // column index 1 is UNIQUE
```

---

### 2.4 RID

A Record ID — uniquely identifies a row on disk.

```c
typedef struct {
    uint32_t page_no;   // which page the row lives on
    uint16_t slot_no;   // which slot in that page's directory
} RID;
```

You never construct a RID yourself. It is **returned by the engine** inside every `Row` from `storage_get_by_pk` and `cursor_next`. You pass it back for `storage_update` and `storage_delete`.

---

### 2.5 Cursor

An opaque handle for sequential table scans. You never access its internals directly.

```c
Cursor *cur = storage_scan("users");
Row *row;
while ((row = cursor_next(cur)) != NULL) {
    // use row...
}
cursor_close(cur);
```

---

## 3. Error Codes

All functions that return `int` return one of these:

| Constant | Value | Meaning |
|---|---|---|
| `MYDB_OK` | `0` | Success |
| `MYDB_ERR` | `-1` | Generic error |
| `MYDB_ERR_NOT_FOUND` | `-2` | Row or table does not exist |
| `MYDB_ERR_DUPLICATE` | `-3` | Primary key or UNIQUE constraint violation |
| `MYDB_ERR_FULL` | `-4` | Buffer pool full or catalog full |
| `MYDB_ERR_FK_VIOLATION` | `-5` | Foreign key violation (reserved, Phase 1 not enforced) |
| `MYDB_ERR_NULL_VIOLATION` | `-6` | NULL inserted into a NOT NULL column |
| `MYDB_ERR_NO_TXN` | `-7` | `commit` or `rollback` called with no active transaction |

---

## 4. Engine Lifecycle

### `storage_init`

```c
int storage_init(const char *data_dir);
```

Opens (or creates) the schema catalog in `data_dir`. Sets up the buffer pool and transaction manager. **Must be called once before anything else.**

- `data_dir` is the folder where all `.mydb` table files are stored.
- Returns `MYDB_OK` on success.
- Safe to call again if already initialized (no-op).

```c
storage_init("./data");
```

---

### `storage_shutdown`

```c
int storage_shutdown(void);
```

Commits any open transaction, flushes all dirty pages to disk, closes all table files. **Call once at program exit.**

---

## 5. DDL — Create and Drop Tables

### `storage_create_table`

```c
int storage_create_table(const char *name, Schema *schema);
```

Creates a new table with the given schema. Allocates the B+ Tree root pages.

**You must fill in:**
- `schema->num_columns`
- `schema->columns[i]` for each column
- `schema->pk_col_idx`
- `schema->num_secondary_indexes` and `schema->secondary_col_idx[]` for UNIQUE columns
- `schema->auto_incr_counter = 1` if using AUTO_INCREMENT

**Do NOT fill in:** `root_page_no`, `secondary_root_page_no[]` — the engine sets these.

Returns `MYDB_ERR_DUPLICATE` if a table with that name already exists.

**Example:**

```c
Schema s;
memset(&s, 0, sizeof(Schema));

s.num_columns = 3;
s.pk_col_idx  = 0;
s.auto_incr_counter = 1;

// col 0: id INT AUTO_INCREMENT PRIMARY KEY
strncpy(s.columns[0].name, "id", 64);
s.columns[0].type             = TYPE_INT;
s.columns[0].max_len          = 4;
s.columns[0].is_not_null      = 1;
s.columns[0].is_primary_key   = 1;
s.columns[0].is_auto_increment = 1;

// col 1: email VARCHAR(100) NOT NULL UNIQUE
strncpy(s.columns[1].name, "email", 64);
s.columns[1].type        = TYPE_VARCHAR;
s.columns[1].max_len     = 100;
s.columns[1].is_not_null = 1;
s.columns[1].is_unique   = 1;

// col 2: score DECIMAL(10,2)
strncpy(s.columns[2].name, "score", 64);
s.columns[2].type    = TYPE_DECIMAL;
s.columns[2].max_len = 10;
s.columns[2].scale   = 2;

// register the UNIQUE column as a secondary index
s.num_secondary_indexes  = 1;
s.secondary_col_idx[0]   = 1;   // column 1 (email) is UNIQUE

int rc = storage_create_table("users", &s);
// rc == MYDB_OK
```

---

### `storage_drop_table`

```c
int storage_drop_table(const char *name);
```

Deletes the table's `.mydb` file from disk and removes it from the catalog. **This is permanent and cannot be undone.**

```c
storage_drop_table("users");
```

---

## 6. DML — Insert, Update, Delete

### `storage_insert`

```c
int storage_insert(const char *table, Row *row);
```

Inserts a row into the table.

**Rules:**
- `row->num_cols` must equal the table's column count.
- `row->cols[i]` must match `schema->columns[i]` in type and order.
- NOT NULL columns must have `is_null = 0`.
- For AUTO_INCREMENT PK: set `cols[pk_idx].is_null = 1` (or `int_val = 0`) — the engine assigns the next ID.
- Returns `MYDB_ERR_DUPLICATE` if the PK or any UNIQUE column already exists.
- Returns `MYDB_ERR_NULL_VIOLATION` if a NOT NULL column is given a NULL value.

**Example — insert with explicit PK:**

```c
Row row;
memset(&row, 0, sizeof(Row));
row.num_cols = 3;

row.cols[0].type = TYPE_INT;  row.cols[0].is_null = 0;  row.cols[0].v.int_val = 1;

row.cols[1].type = TYPE_VARCHAR;  row.cols[1].is_null = 0;
row.cols[1].v.varchar_val.len = 15;
memcpy(row.cols[1].v.varchar_val.data, "alice@email.com", 15);

row.cols[2].type = TYPE_DECIMAL;  row.cols[2].is_null = 0;
row.cols[2].v.decimal_val = 9550;  // 95.50 with scale=2

int rc = storage_insert("users", &row);
```

**Example — insert with AUTO_INCREMENT (let engine assign the id):**

```c
Row row;
memset(&row, 0, sizeof(Row));
row.num_cols = 3;

row.cols[0].type = TYPE_INT;  row.cols[0].is_null = 1;  // AUTO_INCREMENT

row.cols[1].type = TYPE_VARCHAR;  row.cols[1].is_null = 0;
row.cols[1].v.varchar_val.len = 13;
memcpy(row.cols[1].v.varchar_val.data, "bob@email.com", 13);

row.cols[2].type = TYPE_DECIMAL;  row.cols[2].is_null = 0;
row.cols[2].v.decimal_val = 7200;  // 72.00

storage_insert("users", &row);
// row.cols[0].v.int_val is now set to the assigned id (e.g. 1)
```

---

### `storage_update`

```c
int storage_update(const char *table, RID rid, Row *new_row);
```

Replaces the row at `rid` with `new_row`. The RID comes from `row->rid` returned by `storage_get_by_pk` or `cursor_next`.

Internally this is a delete + insert, so the same constraints apply (NOT NULL, UNIQUE, etc.).

**Example:**

```c
// 1. fetch the row to get its RID
Value pk;
pk.type = TYPE_INT;  pk.is_null = 0;  pk.v.int_val = 1;
Row *existing = storage_get_by_pk("users", &pk);

// 2. build the updated row
Row updated;
memset(&updated, 0, sizeof(Row));
updated.num_cols = 3;
updated.cols[0] = existing->cols[0];  // keep id the same
updated.cols[1] = existing->cols[1];  // keep email the same
updated.cols[2].type = TYPE_DECIMAL;  updated.cols[2].is_null = 0;
updated.cols[2].v.decimal_val = 10000;  // update score to 100.00

// 3. update using the RID from the fetched row
storage_update("users", existing->rid, &updated);
```

---

### `storage_delete`

```c
int storage_delete(const char *table, RID rid);
```

Deletes the row at `rid`. The RID comes from `row->rid`.

**Example:**

```c
Value pk;
pk.type = TYPE_INT;  pk.is_null = 0;  pk.v.int_val = 1;
Row *row = storage_get_by_pk("users", &pk);
if (row != NULL) {
    storage_delete("users", row->rid);
}
```

---

## 7. DQL — Querying Data

### `storage_get_by_pk`

```c
Row *storage_get_by_pk(const char *table, Value *pk);
```

Looks up a single row by its primary key using the B+ Tree index. O(log n).

- Returns a pointer to an **internal static buffer** — the data is valid until the next call to `storage_get_by_pk`.
- Returns `NULL` if the row does not exist.
- The returned `row->rid` is populated — use it for `storage_update` / `storage_delete`.

```c
Value pk;
pk.type = TYPE_INT;  pk.is_null = 0;  pk.v.int_val = 42;

Row *row = storage_get_by_pk("users", &pk);
if (row == NULL) {
    // row with id=42 does not exist
} else {
    // access columns:
    int32_t id    = row->cols[0].v.int_val;
    char   *email = row->cols[1].v.varchar_val.data;
    // ...
}
```

> **Warning:** Do not call `storage_get_by_pk` twice and expect both pointers to be valid simultaneously — the second call overwrites the buffer from the first.

---

### `storage_scan` / `cursor_next` / `cursor_close`

```c
Cursor *storage_scan(const char *table);
Row    *cursor_next(Cursor *cursor);
void    cursor_close(Cursor *cursor);
```

Full sequential scan. Rows are returned in **primary key order** (ascending).

- `storage_scan` returns a `Cursor *`, or `NULL` on error.
- `cursor_next` returns the next row, or `NULL` when the scan is finished.
- Each call to `cursor_next` **overwrites the previous row** — copy values before calling again.
- **Always call `cursor_close`** when done, even if you exit the loop early.
- The returned `row->rid` is populated on each call — safe to use for update/delete within the loop.

**Example — scan and collect all rows:**

```c
Cursor *cur = storage_scan("users");
if (cur == NULL) { /* error */ }

Row *row;
while ((row = cursor_next(cur)) != NULL) {
    int32_t id    = row->cols[0].v.int_val;
    int     nlen  = row->cols[1].v.varchar_val.len;
    char   *email = row->cols[1].v.varchar_val.data;

    printf("id=%d  email=%.*s\n", id, nlen, email);
}

cursor_close(cur);
```

**Example — scan, apply WHERE filter, and delete matching rows:**

```c
Cursor *cur = storage_scan("orders");
Row *row;
while ((row = cursor_next(cur)) != NULL) {
    // DELETE FROM orders WHERE status = 2
    if (row->cols[3].v.enum_val == 2) {
        RID rid = row->rid;          // save before cursor_next overwrites
        storage_delete("orders", rid);
    }
}
cursor_close(cur);
```

> **Note on modifying while scanning:** Deleting or updating the **current row** during a scan is safe. Inserting new rows during a scan may or may not return those rows — do not rely on this.

---

## 8. TCL — Transactions

```c
int storage_begin(void);
int storage_commit(void);
int storage_rollback(void);
```

The storage engine supports **single-user transactions** with begin / commit / rollback.

| Function | Behaviour |
|---|---|
| `storage_begin` | Starts a transaction. Returns `MYDB_ERR` if one is already active. |
| `storage_commit` | Writes all dirty pages to disk. Returns `MYDB_ERR_NO_TXN` if no transaction is active. |
| `storage_rollback` | Discards all changes since `BEGIN` (they were never written to disk). Returns `MYDB_ERR_NO_TXN` if no transaction is active. |

**Auto-commit:** If you call `storage_insert`, `storage_update`, or `storage_delete` without calling `storage_begin` first, the engine automatically wraps the operation in a single-statement transaction (begin + commit). This means you only need explicit transactions when you want to group multiple operations together.

**Explicit transaction example:**

```c
storage_begin();

Row r1 = ...; storage_insert("accounts", &r1);
Row r2 = ...; storage_insert("accounts", &r2);

if (everything_ok) {
    storage_commit();
} else {
    storage_rollback();  // neither row was written to disk
}
```

**Return code check:**

```c
int rc = storage_begin();
if (rc != MYDB_OK) {
    // a transaction is already active — commit or rollback first
}
```

---

## 9. Complete Examples

### Example A — CREATE TABLE + INSERT + SELECT

```c
#include "storage.h"
#include <string.h>
#include <stdio.h>

int main(void) {
    storage_init("./data");

    /* CREATE TABLE products (
         id    INT AUTO_INCREMENT PRIMARY KEY,
         name  VARCHAR(50) NOT NULL,
         price DECIMAL(8,2)
       ); */
    Schema s;
    memset(&s, 0, sizeof(Schema));
    s.num_columns = 3;
    s.pk_col_idx  = 0;
    s.auto_incr_counter = 1;

    strncpy(s.columns[0].name, "id",    64);
    s.columns[0].type = TYPE_INT; s.columns[0].max_len = 4;
    s.columns[0].is_not_null = 1; s.columns[0].is_primary_key = 1;
    s.columns[0].is_auto_increment = 1;

    strncpy(s.columns[1].name, "name",  64);
    s.columns[1].type = TYPE_VARCHAR; s.columns[1].max_len = 50;
    s.columns[1].is_not_null = 1;

    strncpy(s.columns[2].name, "price", 64);
    s.columns[2].type = TYPE_DECIMAL; s.columns[2].max_len = 8;
    s.columns[2].scale = 2;

    storage_create_table("products", &s);

    /* INSERT INTO products VALUES (AUTO, 'Keyboard', 49.99) */
    Row row;
    memset(&row, 0, sizeof(Row));
    row.num_cols = 3;
    row.cols[0].type = TYPE_INT;     row.cols[0].is_null = 1;  // AUTO
    row.cols[1].type = TYPE_VARCHAR; row.cols[1].is_null = 0;
    row.cols[1].v.varchar_val.len = 8;
    memcpy(row.cols[1].v.varchar_val.data, "Keyboard", 8);
    row.cols[2].type = TYPE_DECIMAL; row.cols[2].is_null = 0;
    row.cols[2].v.decimal_val = 4999;  // 49.99 * 100

    storage_insert("products", &row);

    /* SELECT * FROM products WHERE id = 1 */
    Value pk;
    pk.type = TYPE_INT; pk.is_null = 0; pk.v.int_val = 1;
    Row *found = storage_get_by_pk("products", &pk);
    if (found) {
        printf("id=%d  name=%.*s  price=%.2f\n",
               found->cols[0].v.int_val,
               found->cols[1].v.varchar_val.len,
               found->cols[1].v.varchar_val.data,
               found->cols[2].v.decimal_val / 100.0);
    }

    storage_shutdown();
    return 0;
}
```

---

### Example B — Full table scan with UPDATE

```c
/* UPDATE products SET price = price * 1.10  (apply 10% price increase) */
Cursor *cur = storage_scan("products");
Row *row;
while ((row = cursor_next(cur)) != NULL) {
    RID rid = row->rid;   // save now — cursor_next will overwrite row next iteration

    Row updated = *row;   // copy all column values
    updated.cols[2].v.decimal_val =
        (int64_t)(row->cols[2].v.decimal_val * 1.10);

    storage_update("products", rid, &updated);
}
cursor_close(cur);
```

---

### Example C — Transaction with rollback

```c
storage_begin();

Row r;
memset(&r, 0, sizeof(Row));
r.num_cols = 3;
r.cols[0].type = TYPE_INT; r.cols[0].is_null = 1;
r.cols[1].type = TYPE_VARCHAR; r.cols[1].is_null = 0;
r.cols[1].v.varchar_val.len = 5;
memcpy(r.cols[1].v.varchar_val.data, "Mouse", 5);
r.cols[2].type = TYPE_DECIMAL; r.cols[2].is_null = 0;
r.cols[2].v.decimal_val = 1999;

int rc = storage_insert("products", &r);
if (rc != MYDB_OK) {
    storage_rollback();   // insert failed, undo everything
} else {
    storage_commit();
}
```

---

### Example D — ENUM column

```c
/* Schema: status ENUM(active, inactive, pending) */
strncpy(s.columns[3].name, "status", 64);
s.columns[3].type = TYPE_ENUM;
s.columns[3].num_enum_values = 3;
strncpy(s.columns[3].enum_values[0], "active",   32);
strncpy(s.columns[3].enum_values[1], "inactive", 32);
strncpy(s.columns[3].enum_values[2], "pending",  32);

/* INSERT — set status = "active" (index 0) */
row.cols[3].type        = TYPE_ENUM;
row.cols[3].is_null     = 0;
row.cols[3].v.enum_val  = 0;  // "active"

/* Reading back — convert index to string */
Row *r = storage_get_by_pk("tickets", &pk);
uint8_t idx = r->cols[3].v.enum_val;
// idx == 0 → "active", 1 → "inactive", 2 → "pending"
```

---

## 10. Constraints and Limits

| Item | Limit |
|---|---|
| Max tables | 64 |
| Max columns per table | 32 |
| Max VARCHAR length | 150 characters |
| Max ENUM values per column | 16 |
| Max ENUM string length | 32 characters |
| Max UNIQUE columns per table | 8 |
| Max foreign keys per table | 8 |
| Max open tables simultaneously | 64 |
| Buffer pool size | 64 pages (1 MB) |
| Page size | 16 KB |

---

## 11. What is NOT Implemented (Phase 1)

The following features are **out of scope** and will not work:

- **Foreign key enforcement** — FK constraints are stored in the schema but not checked on INSERT/UPDATE/DELETE. No cascade behavior.
- **ALTER TABLE** — table definitions cannot be changed after creation.
- **Crash recovery / WAL** — if the process crashes mid-transaction, data may be lost. Always call `storage_shutdown()` cleanly.
- **Concurrent access** — the engine is single-user. Do not call from multiple threads.
- **FULL OUTER JOIN** — handled by the execution engine, not the storage layer.
- **Views, triggers, stored procedures** — not supported.

---

*For questions about the storage engine internals, contact Hasnat Akram.*
