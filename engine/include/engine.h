#ifndef ENGINE_H
#define ENGINE_H

#include <stddef.h>

#include "common.h"
#include "crypto.h"           /* SALT_LEN, SHA256_DIGEST_LEN, MYDB_NONCE_LEN */
#include "database_file.h"
#include "relation_def.h"
#include "system_schema.h"
#include "connection.h"
#include "stats_buffer.h"

#ifdef __cplusplus
extern "C" {
#endif

/* PartitionCtx is owned and defined by partition_manager.  The engine
 * only holds a pointer to it, so a forward declaration is enough here.
 * Files that dereference it (engine.c, the execution engine) include
 * partition_ctx.h / pm_api.h directly. */
struct PartitionCtx;

/* ------------------------------------------------------------------ */
/*  Engine orchestrator                                               */
/*                                                                    */
/*  Single point of entry above partition_manager and system_schema.  */
/*  Owns the engine-level metadata (`__database.mydb`,                */
/*  `system_schema/users.mydb`, `system_schema/privileges.mydb`),     */
/*  the master connection pool, and orchestration of the SQL pipeline.*/
/*                                                                    */
/*  v3 separation of concerns:                                        */
/*    - engine          : connections, auth, engine-level metadata,   */
/*                         query-flow orchestration.                  */
/*    - partition_manager: everything from the partition catalog down */
/*                         to schemas, tables, transactions, stats.   */
/*                         The engine holds an opaque PartitionCtx*    */
/*                         and never touches its internals.           */
/*                                                                    */
/*  Layering:                                                         */
/*    bin/mydb         ─▶ engine_bootstrap                            */
/*    parser+exec      ─▶ engine_init / engine_login / use_schema     */
/*    partition_manager ─▶ pctx_* / pm_* (called by engine + executor)*/
/* ------------------------------------------------------------------ */


/* Per-partition quota constants.
 *   DEFAULT — assigned at bootstrap and for new users when no QUOTA clause given.
 *   MIN     — smallest quota accepted by CREATE USER / ALTER USER SET QUOTA.
 *   MAX     — largest quota accepted. */
#define ENGINE_DEFAULT_QUOTA_BYTES  (1024ULL * 1024ULL * 1024ULL)         /* 1 GB */
#define ENGINE_MIN_QUOTA_BYTES      (100ULL  * 1024ULL * 1024ULL)         /* 100 MB */
#define ENGINE_MAX_QUOTA_BYTES      (5ULL    * 1024ULL * 1024ULL * 1024ULL) /* 5 GB */


typedef struct EngineState {
    /* Always-resident engine-level metadata, opened by engine_init. */
    DatabaseFile   database;          /* __database.mydb */
    SystemSchema   system_schema;     /* users.mydb + privileges.mydb */

    /* Master connection pool.  All per-session state (logged_in, user id,
     * active schema name, partition_open, schema_active) lives in the
     * Connection slots — NOT on EngineState directly.  Phase 1 runs a
     * single session at conns[0]. */
    ConnectionPool conn_pool;

    /* Index into conn_pool.conns of the connection whose request is currently
     * being served, or -1 when none.  Set at the top of each public entry
     * point (engine_execute_sql / engine_login* / engine_logout) and cleared
     * on return; cur_conn() resolves it.  Safe as a single field only because
     * execution is single-threaded (the server's poll loop runs one statement
     * at a time).  When concurrency lands this becomes thread-local or moves
     * fully into ExecContext. */
    int            active_conn;

    /* Table of loaded partition runtime contexts — one PartitionCtx per
     * active partition (Catalog, Cache 2, TransactionManager, embedded
     * StorageEngine), heap-allocated by partition_manager (pctx_init).
     * The engine owns connection→partition routing: each Connection carries
     * a partition_id, and the engine resolves it to a slot here, lazily
     * loading on login / USE and evicting (flush + close) when the last
     * referencing connection leaves (PartitionCtx.n_refs hits 0).  A
     * PartitionCtx is shared by all connections on the same partition.
     * Empty slots are NULL.  The engine never dereferences a PartitionCtx's
     * internals — that is partition_manager's job (pctx_* / pm_* calls). */
    struct PartitionCtx *partitions[MAX_PARTITIONS];

    /* Engine-owned optimizer-statistics pool.  Lazily opens the stats file
     * for each (partition_id, schema) under system_schema/stats/ and hands
     * the StatsFile* to the executor via ExecContext.stats.  The planner
     * reads it; ANALYZE writes through it.  Owned entirely by the engine —
     * partition_manager has no visibility into it. */
    StatsBuffer    stats_buf;

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
 *   2. engine_login  (authenticate, then pctx_init opens the partition
 *                     catalog and its embedded StorageEngine)
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

/* Plaintext login (local / bootstrap path, e.g. engine_start + tests).
 * Verifies the password against system_schema.users, claims a connection
 * slot, acquires the user's partition (if owned), and stamps last_login.
 * Also makes the new session the active connection so an embedded
 * single-session caller's engine_execute_sql works without a conn_id.
 * The network server uses engine_login_response instead.
 *
 * Returns:
 *   MYDB_ERR_NOT_FOUND  unknown username
 *   MYDB_ERR_PERM       wrong password OR is_active==0
 *   MYDB_ERR            connection pool full / partition load failure */
int engine_login(EngineState *eng,
                 const char *username, const char *password);

/* ------------------------------------------------------------------ */
/*  Network session lifecycle (mydb-server)                            */
/* ------------------------------------------------------------------ */

/* Look up a user's password salt for the challenge-response handshake.
 * Copies SALT_LEN bytes into salt_out.  MYDB_ERR_NOT_FOUND for an unknown
 * user (the auth handler answers with a fake salt to avoid leaking which
 * usernames exist). */
int engine_get_user_salt(EngineState *eng, const char *username,
                         uint8_t salt_out[SALT_LEN]);

/* Network login via challenge-response.  The client sends
 * response = SHA-256(nonce || SHA-256(salt || password)); the engine
 * recomputes it from the stored hash and the server-issued nonce and
 * compares.  On success opens a session and writes the new conn_id (>= 0)
 * to *conn_id_out.  Returns MYDB_ERR_NOT_FOUND / MYDB_ERR_PERM on auth
 * failure (the caller reports both identically), MYDB_ERR otherwise. */
int engine_login_response(EngineState *eng, const char *username,
                          const uint8_t response[SHA256_DIGEST_LEN],
                          const uint8_t nonce[MYDB_NONCE_LEN],
                          int *conn_id_out);

/* Close a network session: roll back any open transaction, drop the
 * connection's reference on its partition (flush + evict at refcount 0),
 * and free the slot.  Called by the server on QUIT or client disconnect. */
int engine_logout(EngineState *eng, int conn_id);

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

/* Deactivate the currently active schema without switching to another.
 * Flushes dirty pages, closes the schema file, and clears schema_active /
 * current_schema_name.  Partition state (logged_in, partition_open, etc.)
 * is preserved — the user remains logged in.
 * Returns MYDB_OK immediately if no schema is active. */
int engine_deactivate_schema(EngineState *eng);


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
/*  User management                                                    */
/* ------------------------------------------------------------------ */

/* Create a new user account, partition directory, and catalog.
 *
 *   username       — must be unique, non-empty, < MAX_USERNAME chars.
 *   password       — plain-text; hashed + salted internally.
 *   partition_name — directory name under engine root; NULL → username.
 *                    Must not already exist as a partition path.
 *   quota_bytes    — 0 → ENGINE_DEFAULT_QUOTA_BYTES.
 *                    Clamped to [ENGINE_MIN_QUOTA_BYTES, ENGINE_MAX_QUOTA_BYTES].
 *
 * Returns MYDB_ERR_DUPLICATE  if username or partition_name already taken.
 *         MYDB_ERR_FULL       if the partition directory limit is reached.
 *         MYDB_ERR_PERM       if caller is not root (user_id == 1).  */
int engine_create_user(EngineState *eng,
                       const char  *username,
                       const char  *password,
                       const char  *partition_name,
                       uint64_t     quota_bytes);

/* Drop a user account, their partition directory and all its contents.
 * Only root (user_id == 1) may call this.
 * Rejects DROP USER root.
 *
 * Returns MYDB_ERR_PERM      if caller is not root.
 *         MYDB_ERR_NOT_FOUND if the username does not exist. */
int engine_drop_user(EngineState *eng, const char *username);

/* Change a user's password.  Root may change any user's password.
 * A non-root user may only change their own password (future — for
 * Phase 1 only root uses these commands).
 *
 * Returns MYDB_ERR_NOT_FOUND if the username does not exist. */
int engine_alter_user_password(EngineState *eng,
                               const char  *username,
                               const char  *new_password);

/* Change a user's partition quota.
 *   new_quota_bytes must be in [ENGINE_MIN_QUOTA_BYTES, ENGINE_MAX_QUOTA_BYTES].
 *   Rejects if new_quota_bytes < current used_bytes (cannot set quota
 *   below what has already been allocated).
 *
 * Returns MYDB_ERR_NOT_FOUND if the username does not exist.
 *         MYDB_ERR_FULL      if new quota < current used_bytes.
 *         MYDB_ERR           if quota is outside the allowed range. */
int engine_alter_user_quota(EngineState *eng,
                            const char  *username,
                            uint64_t     new_quota_bytes);


/* ------------------------------------------------------------------ */
/*  SQL execution entry point                                          */
/*                                                                    */
/*  The single door the server (and embedded callers) use to run a    */
/*  SQL string for a given connection. `conn_id` is the handle from    */
/*  engine_login_response (or 0 for the embedded single-session path); */
/*  the engine scopes the statement to that connection. The pipeline:  */
/*    1. parser_parse(sql)              -> opaque AST handle           */
/*    2. exec_engine_execute(eng, ast)  -> walks AST, writes result    */
/*    3. parser_free_ast(ast)           -> always, on every path       */
/*                                                                    */
/*  Callers never see the parser or execution engine modules — engine  */
/*  is the single front door for raw SQL. Result string is NUL-       */
/*  terminated and truncated to result_cap-1 bytes.                   */
/*                                                                    */
/*  Returns:                                                          */
/*    MYDB_OK         statement executed; result_out has the result    */
/*    MYDB_ERR        parse error (message in result_out)              */
/*    MYDB_ERR_PERM   not logged in / unknown conn_id                  */
/*    other           propagated from execution engine                 */
/* ------------------------------------------------------------------ */
int engine_execute_sql(EngineState *eng, int conn_id,
                       const char *sql,
                       char *result_out, size_t result_cap);


#ifdef __cplusplus
}
#endif

#endif /* ENGINE_H */
