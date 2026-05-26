#ifndef ENGINE_H
#define ENGINE_H

#include <stddef.h>

#include "common.h"
#include "database_file.h"
#include "partition.h"
#include "relation_def.h"
#include "schema_file.h"
#include "system_schema.h"

#ifdef __cplusplus
extern "C" {
#endif

/* ------------------------------------------------------------------ */
/*  Engine orchestrator                                               */
/*                                                                    */
/*  Single point of entry above storage_engine and system_schema.     */
/*  Owns the engine-level metadata (`__database.mydb`,                */
/*  `system_schema/users.mydb`, `system_schema/privileges.mydb`)      */
/*  and the per-session active state (current user, active partition */
/*  catalog, active schema).                                          */
/*                                                                    */
/*  Layering:                                                         */
/*    bin/mydb         ─▶ engine_bootstrap                            */
/*    parser+exec      ─▶ engine_init / engine_login / use_schema     */
/*    storage layer    ─▶ reads validated identifiers from EngineState*/
/*                                                                    */
/*  Phase 8 owns ONLY the session lifecycle. DML, transactions, and   */
/*  authorization checks land in phases 9-11.                         */
/* ------------------------------------------------------------------ */


/* Default per-partition quota assigned at bootstrap. 1 GB is plenty
 * for the academic workload; configurable via a future `mydb` admin
 * subcommand. */
#define ENGINE_DEFAULT_QUOTA_BYTES   (1024ULL * 1024ULL * 1024ULL)


typedef struct EngineState {
    /* Always-resident metadata, opened by engine_init. */
    DatabaseFile   database;          /* __database.mydb */
    SystemSchema   system_schema;     /* users.mydb + privileges.mydb */

    /* Per-session state. */
    Catalog        active_catalog;    /* Cache 1 — current partition's __catalog.mydb */
    SchemaFile     active_schema;     /* Cache 2 — current schema */
    uint32_t       current_user_id;
    uint32_t       current_partition_id;
    char           current_partition_path[256];
    char           current_schema_name[32];

    uint8_t        logged_in;
    uint8_t        partition_open;    /* 0 if user owns no partition (analyst) */
    uint8_t        schema_active;     /* 1 once USE has succeeded */

    char           root_dir[256];     /* engine root, e.g. ~/.mydb/ */
} EngineState;


/* ------------------------------------------------------------------ */
/*  First-run setup                                                    */
/* ------------------------------------------------------------------ */

/* Create a fresh engine at `root_dir`:
 *   - mkdir root_dir (if missing) and root_dir/system_schema/
 *   - create __database.mydb (file_type=DATABASE)
 *   - create users.mydb + privileges.mydb
 *   - insert root user (SHA-256 hash with random salt)
 *   - register root partition path = root_dir/<root_username>/
 *   - mkdir that partition dir + write its __catalog.mydb
 *
 * Fails with MYDB_ERR if the root_dir already contains __database.mydb.
 * `root_username` becomes the first user (user_id=1). */
int engine_bootstrap(const char *root_dir,
                     const char *root_username,
                     const char *root_password);

/* Open an already-bootstrapped engine. No user is logged in yet. */
int engine_init (const char *root_dir, EngineState *out);

/* Close all open files, including the storage layer. Safe to call on a
 * partially-initialized EngineState (each fd is checked individually). */
int engine_close(EngineState *eng);


/* ------------------------------------------------------------------ */
/*  Combined start / stop (for bin — the only caller)                 */
/* ------------------------------------------------------------------ */

/* Open an engine, authenticate a user, and initialise the storage
 * layer in one call.  This is the single function bin/mydb uses to
 * bring the engine fully up before entering the REPL.
 *
 * Internally it sequences:
 *   1. engine_init   (open __database.mydb + system_schema files)
 *   2. engine_login  (authenticate, open partition catalog)
 *   3. storage_init  (buffer pool + B+ tree layer, needs partition ctx)
 *
 * On any failure the partially-initialised state is cleaned up and
 * MYDB_ERR / MYDB_ERR_NOT_FOUND / MYDB_ERR_PERM is returned.
 * On success *out is fully ready and engine_close() will tear it down. */
int engine_start(const char *root_dir,
                 const char *username,
                 const char *password,
                 EngineState *out);


/* ------------------------------------------------------------------ */
/*  Session lifecycle                                                  */
/* ------------------------------------------------------------------ */

/* Verify password against system_schema.users. On success, populate
 * eng->current_user_id, open the user's partition catalog into
 * eng->active_catalog (if they own one), and stamp last_login.
 *
 * Returns:
 *   MYDB_ERR_NOT_FOUND  unknown username
 *   MYDB_ERR_PERM       wrong password OR is_active==0 */
int engine_login(EngineState *eng,
                 const char *username, const char *password);

/* Switch the active schema. Caller must be logged in.
 *
 * Owner path  (partition_open == 1):
 *   Look up schema_name in the user's own active_catalog.
 *
 * Analyst path (partition_open == 0):
 *   Scan privileges for a grant (current_user_id, *, schema_name).
 *   On match, resolve the owning partition path from __database.mydb
 *   and open that partition's copy of the schema.
 *
 *   1. Flush dirty pages of old schema's relations (if any active).
 *
 *   2. Verify access and locate the schema file.
 *   3. Open <partition>/<schema>/__schema.mydb into eng->active_schema. */
int engine_use_schema(EngineState *eng, const char *schema_name);


/* ------------------------------------------------------------------ */
/*  Authorization                                                      */
/* ------------------------------------------------------------------ */

/* Check whether the current session may perform the requested operation
 * on the active schema.
 *
 *   write_required == 0  →  SELECT (read)
 *   write_required == 1  →  INSERT / UPDATE / DELETE (write)
 *
 * Owner of the active partition always passes for both reads and writes.
 * A privilege-granted analyst passes for reads only.
 *
 * Returns MYDB_ERR_PERM if no schema is active, the user is not logged
 * in, or the privilege level is insufficient. */
int engine_check_access(EngineState *eng, int write_required);


/* ------------------------------------------------------------------ */
/*  Read-only metadata accessors for parser + execution engine        */
/*                                                                    */
/*  Per design doc §6, all metadata files load once into RAM and are  */
/*  exposed as read-only shared infrastructure. Storage is the single */
/*  writer; parser/exec only read. The const return type signals that.*/
/* ------------------------------------------------------------------ */

/* Look up a relation by name in the active schema. Requires that a
 * schema is currently active (USE has succeeded). Returns NULL if the
 * relation is not registered or no schema is active. The returned
 * pointer is valid until the next USE / engine_close. */
const RelationDef *engine_find_relation(EngineState *eng,
                                        const char *relation_name);


/* ------------------------------------------------------------------ */
/*  SQL execution entry point                                          */
/*                                                                    */
/*  The single door bin/REPL uses to run a SQL string. The engine     */
/*  pipeline:                                                          */
/*    1. parser_parse(sql)              -> opaque AST handle           */
/*    2. exec_engine_execute(eng, ast)  -> walks AST, writes result    */
/*    3. parser_free_ast(ast)           -> always, on every path       */
/*                                                                    */
/*  Bin never sees the parser or execution engine modules — engine    */
/*  is the single front door for raw SQL. Result string is NUL-       */
/*  terminated and truncated to result_cap-1 bytes.                   */
/*                                                                    */
/*  Returns:                                                          */
/*    MYDB_OK         statement executed; result_out has the result    */
/*    MYDB_ERR        parse error (message in result_out)              */
/*    MYDB_ERR_PERM   not logged in                                    */
/*    other           propagated from execution engine                 */
/* ------------------------------------------------------------------ */
int engine_execute_sql(EngineState *eng,
                       const char *sql,
                       char *result_out, size_t result_cap);


#ifdef __cplusplus
}
#endif

#endif /* ENGINE_H */
