# MyDB Engine — Public API Reference

**Audience:** `bin/mydb.c` (CLI), `execution_engine` (AST walker),
future admin tools. The parser does NOT call into engine — it just
produces an AST that engine routes.

**Header:** `#include "engine.h"` — the only engine header any caller
needs. Internal helpers (`crypto.h`, `database_file.h`) are not
exposed beyond the engine module.

---

## Table of Contents

1. [Role of the engine module](#1-role-of-the-engine-module)
2. [`EngineState` — the session struct](#2-enginestate--the-session-struct)
3. [Lifecycle](#3-lifecycle)
4. [Session](#4-session)
5. [Authorization](#5-authorization)
6. [Read-only metadata](#6-read-only-metadata)
7. [SQL entry point](#7-sql-entry-point)
8. [Error codes](#8-error-codes)
9. [Calling patterns](#9-calling-patterns)
10. [What the engine does NOT own](#10-what-the-engine-does-not-own)

---

## 1. Role of the engine module

The engine sits between bin and the rest of the database:

```
bin (REPL)        ─┐
                   ├─▶ engine ─┬─▶ query_parser     (raw SQL → AST)
execution_engine ──┘            ├─▶ execution_engine (AST → storage calls)
                                ├─▶ system_schema    (users, privileges)
                                └─▶ storage_engine   (transitive, via exec)
```

What it owns:

- **Engine-level metadata files**: `__database.mydb` (partition registry)
  and `system_schema/{users,privileges}.mydb`. Loaded once at
  `engine_init`, kept in RAM, mutated through `users_*` / `privileges_*`
  primitives.
- **Per-session state**: which user is logged in, which partition catalog
  (Cache 1) is open, which schema (Cache 2) is active.
- **The single front door for SQL**: `engine_execute_sql` — bin only
  sees this; everything else is internal.

What it does NOT own: see [Section 10](#10-what-the-engine-does-not-own).

---

## 2. `EngineState` — the session struct

```c
typedef struct EngineState {
    /* Always-resident metadata, opened by engine_init. */
    DatabaseFile   database;          /* __database.mydb */
    SystemSchema   system_schema;     /* users + privileges */

    /* Per-session state — set by engine_login / engine_use_schema. */
    Catalog        active_catalog;        /* current partition's catalog */
    SchemaFile     active_schema;         /* current schema */
    uint32_t       current_user_id;
    uint32_t       current_partition_id;
    char           current_partition_path[256];
    char           current_schema_name[32];

    uint8_t        logged_in;
    uint8_t        partition_open;        /* 0 if user is an analyst */
    uint8_t        schema_active;         /* 1 once USE has succeeded */

    char           root_dir[256];
} EngineState;
```

Lifecycle of the flags:

| Flag | Set by | Cleared by |
|---|---|---|
| `logged_in` | `engine_login` (success) | `engine_close` |
| `partition_open` | `engine_login` if user owns a partition | `engine_close` |
| `schema_active` | `engine_use_schema` (success) | `engine_close` or next `engine_use_schema` |

Read-only access by execution engine: yes. Write access: never; only
the engine module mutates `EngineState`.

---

## 3. Lifecycle

### `engine_bootstrap`

```c
int engine_bootstrap(const char *root_dir,
                     const char *root_username,
                     const char *root_password);
```

First-run only. Creates the entire engine directory tree:
- mkdir `root_dir/` and `root_dir/system_schema/`
- write `__database.mydb` (partition registry)
- write `users.mydb` + `privileges.mydb`
- insert the root user (SHA-256 + 32-byte random salt from `/dev/urandom`)
- register the root partition at `root_dir/<root_username>/`
- create that partition's `__catalog.mydb` with default 1 GB quota

**Returns** `MYDB_OK`, or `MYDB_ERR` if `root_dir/__database.mydb`
already exists (re-bootstrap is refused).

Called by `mydb init -u <username>` only.

---

### `engine_init`

```c
int engine_init(const char *root_dir, EngineState *out);
```

Opens an already-bootstrapped engine. Memory-maps `__database.mydb`,
loads `users.mydb` and `privileges.mydb` into RAM. No user is logged
in yet — `out->logged_in == 0`.

Failure (`MYDB_ERR_*`) means the engine root is missing, the wrong
file type, version mismatch, or checksum failure.

---

### `engine_close`

```c
int engine_close(EngineState *eng);
```

Closes everything in the right order: active schema → active catalog
→ system_schema files → database file. Safe on a partially-initialised
EngineState (each fd is checked individually) and on an already-closed
state.

---

## 4. Session

### `engine_login`

```c
int engine_login(EngineState *eng, const char *username, const char *password);
```

Verifies the password against `system_schema.users` (SHA-256 over
`salt || password`). On success:

- `eng->current_user_id` populated
- if the user owns a partition (per `__database.mydb`), `eng->active_catalog`
  is opened and `eng->partition_open = 1`. Analyst accounts (no partition)
  log in successfully but `partition_open` stays 0.
- `last_login` field on the user record is bumped and persisted.

| Return | Meaning |
|---|---|
| `MYDB_OK` | Authenticated |
| `MYDB_ERR_NOT_FOUND` | Unknown username |
| `MYDB_ERR_PERM` | Wrong password OR account `is_active == 0` |

Double-login on the same EngineState is rejected (`MYDB_ERR`).

---

### `engine_use_schema`

```c
int engine_use_schema(EngineState *eng, const char *schema_name);
```

Selects the active schema (the SQL `USE <schema>` statement). Two paths:

- **Owner path** (`partition_open == 1`): the schema must be registered
  in the user's own catalog (`cat_find_schema`).
- **Analyst path** (`partition_open == 0`): scans `system_schema.privileges`
  for a grant `(grantee_id == current_user_id, schema_name)`. Resolves
  the owning partition's path via `db_find_by_id`, then opens that
  partition's copy of the schema.

Side effects on success:
1. Flushes dirty pages of the previously active schema (calls
   `storage_flush_all_dirty`).
2. Closes the previous `active_schema`.
3. Opens `<partition>/<schema>/__schema.mydb` into `eng->active_schema`.
4. Updates `eng->current_schema_name` and (analyst path)
   `eng->current_partition_path` / `current_partition_id`.

| Return | Meaning |
|---|---|
| `MYDB_OK` | Schema is active |
| `MYDB_ERR_NOT_FOUND` | Schema not registered |
| `MYDB_ERR_PERM` | Analyst with no grant for this schema |

---

## 5. Authorization

### `engine_check_access`

```c
int engine_check_access(EngineState *eng, int write_required);
```

Single auth gate. Storage DML/DQL functions call this internally before
every operation; the execution engine can call it explicitly when
needed.

| Caller state | `write_required` | Result |
|---|---|---|
| not logged in OR no schema active | any | `MYDB_ERR_PERM` |
| owner of active partition | 0 (read) | `MYDB_OK` |
| owner of active partition | 1 (write) | `MYDB_OK` |
| analyst with grant on schema | 0 (read) | `MYDB_OK` |
| analyst with grant on schema | 1 (write) | `MYDB_ERR_PERM` (grants are SELECT-only) |
| analyst with no grant | any | `MYDB_ERR_PERM` |

The "analyst write" rejection is the load-bearing rule — the rest of
the matrix follows from session flags.

---

## 6. Read-only metadata

### `engine_find_relation`

```c
const RelationDef *engine_find_relation(EngineState *eng,
                                        const char *relation_name);
```

Look up a table in the active schema. Returns `NULL` if not registered
or no schema is active.

The `const` return signals "read-only — storage is the single writer."
The execution engine **must** cast away const before passing into
storage functions that mutate state (`auto_incr_counter` after an
`INSERT`, `root_page_no` after `CREATE TABLE`).

The pointer is valid until the next `engine_use_schema` or
`engine_close` call.

---

## 7. SQL entry point

### `engine_execute_sql`

```c
int engine_execute_sql(EngineState *eng, const char *sql,
                       char *result_out, size_t result_cap);
```

The single entry point bin uses to run a SQL string. Internally:

1. `parser_parse(sql, &ast, err_buf, sizeof(err_buf))`
2. On parse error → format `"parse error: <msg>"` into `result_out`,
   return `MYDB_ERR`.
3. Otherwise → `exec_engine_execute(eng, ast, result_out, result_cap)`
   and propagate its return code.
4. `parser_free_ast(ast)` on every path before returning.

Invariants:
- Caller must have logged in (`logged_in == 1`). Otherwise returns
  `MYDB_ERR_PERM` immediately.
- `result_out` is always NUL-terminated on return (assuming `result_cap
  >= 1`).
- The parser handle is fully owned by this function; the execution
  engine sees only a borrowed `ParserAST *`.

**Why bin never calls parser or executor directly**: keeps bin's
include surface to `engine.h` + `storage.h`. Parser/executor changes
don't ripple into the CLI.

---

## 8. Error codes

All defined in `common.h`. Engine-relevant subset:

| Code | Engine functions that return it |
|---|---|
| `MYDB_OK` | All success paths |
| `MYDB_ERR` | Generic / argument validation |
| `MYDB_ERR_NOT_FOUND` | `engine_login` (no user), `engine_use_schema` (no schema) |
| `MYDB_ERR_PERM` | `engine_login` (bad pw / inactive), `engine_use_schema` (analyst no grant), `engine_check_access`, `engine_execute_sql` (not logged in) |
| `MYDB_ERR_DUPLICATE` | (none from engine itself; storage owns this) |
| `MYDB_ERR_BAD_*` | `engine_init` (corrupt files) |

Functions that return `int` use these codes; `engine_find_relation`
returns `NULL` instead.

---

## 9. Calling patterns

### Bin (`bin/mydb.c`)

Sees the lifecycle + SQL door, nothing else:

```c
EngineState eng;
engine_init(root_dir, &eng);
engine_login(&eng, username, password);

while (read_sql_into(buf)) {
    engine_execute_sql(&eng, buf, result, sizeof(result));
    print(result);
}

engine_close(&eng);
```

### Execution engine (when integrated)

Walks ASTs, calls into engine for session-aware lookups + auth, calls
storage for the actual table operations:

```cpp
case StatementType::USE: {
    int rc = engine_use_schema(eng, name);
    /* format result */
}
case StatementType::INSERT: {
    /* engine_check_access is called inside storage_insert internally,
       so no manual call needed here */
    const RelationDef *rel = engine_find_relation(eng, table_name);
    if (!rel) return /* not found */;
    storage_insert((RelationDef *)rel, &row);
}
```

Future admin DDL (`CREATE USER`, `GRANT`) lands as new engine functions
called by the execution engine the same way.

### What never calls engine

- `bin/` — only via `engine_execute_sql` (and lifecycle).
- The parser — the parser is a pure SQL→AST function. It does not
  consult engine state.
- `storage_engine/` — storage calls engine only via `engine_check_access`
  and reads `EngineState` fields through `g.eng`. Never mutates engine
  state.

---

## 10. What the engine does NOT own

Storage / partition concerns belong to `storage_engine`:

| Operation | Lives in |
|---|---|
| Create / drop a schema | `storage_create_schema` (storage) |
| Create / drop a table | `storage_create_table` / `storage_drop_table` (storage) |
| INSERT / UPDATE / DELETE | `storage_insert` / `_update` / `_delete` (storage) |
| BEGIN / COMMIT / ROLLBACK | `storage_begin` / `_commit` / `_rollback` (storage) |
| Partition catalog (`__catalog.mydb`) | `partition.{h,c}` in storage |

User / privilege admin DDL belongs in engine (not yet implemented):

| Operation | Future engine function |
|---|---|
| `CREATE USER` | `engine_create_user(eng, name, pw)` |
| `DROP USER` | `engine_drop_user(eng, name)` |
| `GRANT … ON <schema>` | `engine_grant(eng, user, schema)` |
| `REVOKE … FROM <user>` | `engine_revoke(eng, user, schema)` |

The split rule: anything that touches `users.mydb` or `privileges.mydb`
is engine; anything that touches a partition catalog or schema files
is storage.

---

## File layout

```
engine/
├── include/
│   ├── engine.h          <-- public API (this doc)
│   ├── crypto.h          internal: SHA-256 + salt
│   └── database_file.h   internal: __database.mydb registry
├── src/
│   ├── engine.c          all engine_* function bodies
│   ├── crypto.c
│   └── database_file.c
├── ENGINE_API.md         <-- this file
└── CMakeLists.txt        builds libengine.a
```

Link dependencies (PUBLIC):
- `storage_engine` (for common.h, storage.h, etc.)
- `system_schema` (for users + privileges primitives)
- `query_parser` (for `parser_parse` in `engine_execute_sql`)
- `execution_engine` (for `exec_engine_execute`)

---

*Maintained by Hasnat Akram (Storage Engine + Engine modules).
Cross-team contracts: parser API at `query_parser/PARSER_API.md`,
storage API at `storage_engine/STORAGE_API.md`.*
