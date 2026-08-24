#include "engine.h"
#include "crypto.h"
#include "partition.h"        /* Catalog, cat_*, PartitionEntry */
#include "schema_file.h"      /* SchemaFile — partition cleanup helper */
#include "partition_ctx.h"    /* PartitionCtx, pctx_* lifecycle */
#include "pm_api.h"           /* pm_find_relation_const */
#include "parser_api.h"
#include "exec_engine_api.h"
#include "exec_context.h"     /* ExecContext — built for the executor */

#include <fcntl.h>
#include <unistd.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <time.h>
#include <assert.h>


/* ====================================================================
 *  Path helpers
 * ==================================================================== */

/* "<dir>/<name>" — returns 0 on success, -1 on overflow. */
static int join1(char *out, size_t cap, const char *dir, const char *name)
{
    int n = snprintf(out, cap, "%s/%s", dir, name);
    return (n < 0 || (size_t)n >= cap) ? -1 : 0;
}

/* "<a>/<b>/<c>" */
static int join2(char *out, size_t cap, const char *a, const char *b, const char *c)
{
    int n = snprintf(out, cap, "%s/%s/%s", a, b, c);
    return (n < 0 || (size_t)n >= cap) ? -1 : 0;
}

/* mkdir, ignoring EEXIST. Returns 0 if the directory now exists. */
static int ensure_dir(const char *path)
{
    if (mkdir(path, 0755) == 0) return 0;
    if (errno == EEXIST) {
        struct stat st;
        if (stat(path, &st) == 0 && S_ISDIR(st.st_mode)) return 0;
    }
    return -1;
}

static int file_exists(const char *path)
{
    struct stat st;
    return stat(path, &st) == 0 && S_ISREG(st.st_mode);
}

static uint64_t now_yyyymmddhhmmss(void)
{
    time_t t = time(NULL);
    struct tm tm;
    localtime_r(&t, &tm);
    return (uint64_t)(tm.tm_year + 1900) * 10000000000ULL
         + (uint64_t)(tm.tm_mon + 1)     * 100000000ULL
         + (uint64_t)tm.tm_mday          * 1000000ULL
         + (uint64_t)tm.tm_hour          * 10000ULL
         + (uint64_t)tm.tm_min           * 100ULL
         + (uint64_t)tm.tm_sec;
}


/* ====================================================================
 *  Connection slots — the engine owns the master ConnectionPool.
 *
 *  A conn_id IS the slot index (0..MAX_CONNECTIONS-1); -1 means "none".
 *  cur_conn() returns the connection whose request is currently in flight
 *  (eng->active_conn), set by the public entry points.  Single-threaded:
 *  exactly one request runs at a time, so one active_conn field suffices.
 * ==================================================================== */

/* Resolve a conn_id to its Connection*, or NULL if out of range / not live. */
static Connection *conn_at(EngineState *eng, int conn_id)
{
    if (!eng || conn_id < 0 || conn_id >= MAX_CONNECTIONS) return NULL;
    Connection *c = &eng->conn_pool.conns[conn_id];
    return c->logged_in ? c : NULL;
}

/* The connection whose request is currently being served. */
static Connection *cur_conn(EngineState *eng)
{
    int idx = eng->active_conn;
    if (idx < 0 || idx >= MAX_CONNECTIONS) idx = 0;   /* defensive fallback */
    return &eng->conn_pool.conns[idx];
}

/* Claim a free connection slot.  Returns its index (the conn_id) or -1 when
 * the pool is full.  The slot is zeroed and marked with a connection_id. */
static int conn_alloc(EngineState *eng)
{
    for (int i = 0; i < MAX_CONNECTIONS; i++) {
        Connection *c = &eng->conn_pool.conns[i];
        if (!c->logged_in) {
            memset(c, 0, sizeof(*c));
            c->connection_id = (uint32_t)(i + 1);
            return i;
        }
    }
    return -1;
}

/* Release a connection slot back to the pool. */
static void conn_free(EngineState *eng, int conn_id)
{
    if (conn_id < 0 || conn_id >= MAX_CONNECTIONS) return;
    memset(&eng->conn_pool.conns[conn_id], 0, sizeof(Connection));
}


/* ====================================================================
 *  Partition table — the engine owns connection→partition routing.
 *
 *  Each Connection carries a partition_id (>0 when it has a partition);
 *  the engine resolves it to a loaded PartitionCtx here, lazily loading
 *  on login / USE and evicting when the last referencing connection
 *  detaches (n_refs → 0).  One PartitionCtx is shared by all connections
 *  on the same partition.
 * ==================================================================== */

/* Loaded PartitionCtx for partition_id, or NULL.  pid == 0 means "none". */
static PartitionCtx *engine_find_partition(EngineState *eng, uint32_t pid)
{
    if (pid == 0) return NULL;
    for (int i = 0; i < MAX_PARTITIONS; i++)
        if (eng->partitions[i] && eng->partitions[i]->partition_id == pid)
            return eng->partitions[i];
    return NULL;
}

/* The PartitionCtx for the current connection, or NULL if it has none. */
static PartitionCtx *cur_partition(EngineState *eng)
{
    return engine_find_partition(eng, cur_conn(eng)->partition_id);
}

/* Get-or-load the PartitionCtx for (pid, path) and add one reference.
 * On a miss, loads into a free slot (pctx_init + pctx_open_catalog).
 * Returns the context, or NULL on error / table full. */
static PartitionCtx *engine_acquire_partition(EngineState *eng, uint32_t pid,
                                              const char *path)
{
    PartitionCtx *ctx = engine_find_partition(eng, pid);
    if (ctx) { ctx->n_refs++; return ctx; }

    int slot = -1;
    for (int i = 0; i < MAX_PARTITIONS; i++)
        if (eng->partitions[i] == NULL) { slot = i; break; }
    if (slot < 0) return NULL;   /* partition table full */

    ctx = pctx_init(pid, path);
    if (!ctx) return NULL;
    if (pctx_open_catalog(ctx) != MYDB_OK) {
        pctx_close(ctx);
        free(ctx);
        return NULL;
    }
    ctx->n_refs = 1;
    eng->partitions[slot] = ctx;
    return ctx;
}

/* Drop one reference to partition_id; flush + close + free at zero. */
static void engine_release_partition(EngineState *eng, uint32_t pid)
{
    if (pid == 0) return;
    for (int i = 0; i < MAX_PARTITIONS; i++) {
        PartitionCtx *ctx = eng->partitions[i];
        if (ctx && ctx->partition_id == pid) {
            if (--ctx->n_refs <= 0) {
                pctx_close(ctx);
                free(ctx);
                eng->partitions[i] = NULL;
            }
            return;
        }
    }
}


/* ====================================================================
 *  Bootstrap
 * ==================================================================== */

int engine_bootstrap(const char *root_dir,
                     const char *root_username,
                     const char *root_password)
{
    if (!root_dir || !root_username || !root_password) return MYDB_ERR;
    if (root_username[0] == '\0' || root_password[0] == '\0') return MYDB_ERR;
    if (strlen(root_username) >= MAX_USERNAME)               return MYDB_ERR;

    char db_path[256], sys_dir[256], part_dir[256], cat_path[256];
    if (join1(db_path,  sizeof(db_path),  root_dir, "__database.mydb") < 0) return MYDB_ERR;
    if (join1(sys_dir,  sizeof(sys_dir),  root_dir, "system_schema")   < 0) return MYDB_ERR;
    if (join1(part_dir, sizeof(part_dir), root_dir, root_username)     < 0) return MYDB_ERR;
    if (join1(cat_path, sizeof(cat_path), part_dir, "__catalog.mydb")  < 0) return MYDB_ERR;

    /* Refuse to re-bootstrap an existing engine. */
    if (file_exists(db_path)) return MYDB_ERR;

    /* Create the engine root and system_schema dir up front. */
    if (ensure_dir(root_dir) < 0) return MYDB_ERR;
    if (ensure_dir(sys_dir)  < 0) return MYDB_ERR;

    /* system_schema/stats/ holds the engine-owned optimizer stats files
     * (v3).  sb_init also creates it, but doing it here keeps bootstrap
     * self-contained. */
    char stats_dir[256];
    if (join1(stats_dir, sizeof(stats_dir), sys_dir, "stats") < 0) return MYDB_ERR;
    if (ensure_dir(stats_dir) < 0) return MYDB_ERR;

    /* ---- __database.mydb ---- */
    DatabaseFile db;
    int rc = db_create(db_path, "MyDB Engine", &db);
    if (rc != MYDB_OK) return rc;

    /* ---- system_schema (users + privileges) ---- */
    SystemSchema ss;
    rc = system_schema_create(root_dir, &ss);
    if (rc != MYDB_OK) {
        db_close(&db);
        unlink(db_path);
        return rc;
    }

    /* ---- root user ---- */
    UserSlot u;
    memset(&u, 0, sizeof(u));
    strncpy(u.username, root_username, MAX_USERNAME - 1);
    if (crypto_random_salt(u.password_salt) != MYDB_OK) {
        system_schema_close(&ss);
        db_close(&db);
        return MYDB_ERR;
    }
    crypto_hash_password(root_password, u.password_salt, u.password_hash);
    u.hash_algorithm = 1;            /* SHA-256 */
    u.is_active      = 1;
    u.created_at     = now_yyyymmddhhmmss();

    uint32_t root_user_id;
    rc = users_insert(&ss.users, &u, &root_user_id);
    if (rc != MYDB_OK) {
        system_schema_close(&ss);
        db_close(&db);
        return rc;
    }

    /* ---- root partition ---- */
    if (ensure_dir(part_dir) < 0) {
        system_schema_close(&ss);
        db_close(&db);
        return MYDB_ERR;
    }
    uint32_t root_partition_id;
    rc = db_add_partition(&db, root_user_id, part_dir, &root_partition_id);
    if (rc != MYDB_OK) {
        system_schema_close(&ss);
        db_close(&db);
        return rc;
    }

    /* ---- root partition catalog ---- */
    Catalog cat;
    rc = cat_create(cat_path, root_partition_id, root_user_id,
                    ENGINE_DEFAULT_QUOTA_BYTES, &cat);
    if (rc != MYDB_OK) {
        system_schema_close(&ss);
        db_close(&db);
        return rc;
    }
    cat_close(&cat);

    /* All metadata persisted by the individual create calls; close. */
    system_schema_close(&ss);
    db_close(&db);
    return MYDB_OK;
}


/* ====================================================================
 *  Init / Close
 * ==================================================================== */

int engine_init(const char *root_dir, EngineState *out)
{
    if (!root_dir || !out) return MYDB_ERR;
    if (strlen(root_dir) >= sizeof(out->root_dir)) return MYDB_ERR;

    memset(out, 0, sizeof(*out));
    out->database.fd                  = -1;
    out->system_schema.users.fd       = -1;
    out->system_schema.privileges.fd  = -1;
    /* partitions[] already NULL from memset; loaded lazily on login / USE. */
    out->conn_pool.n_active           = 0;
    out->active_conn                  = -1;   /* no request in flight */

    char db_path[256];
    if (join1(db_path, sizeof(db_path), root_dir, "__database.mydb") < 0)
        return MYDB_ERR;

    int rc = db_open(db_path, &out->database);
    if (rc != MYDB_OK) return rc;

    rc = system_schema_open(root_dir, &out->system_schema);
    if (rc != MYDB_OK) {
        db_close(&out->database);
        return rc;
    }

    /* Engine-owned stats pool: ensures system_schema/stats/ exists and
     * records its path.  Handles open lazily on first SELECT / ANALYZE. */
    rc = sb_init(&out->stats_buf, root_dir);
    if (rc != MYDB_OK) {
        system_schema_close(&out->system_schema);
        db_close(&out->database);
        return rc;
    }

    strncpy(out->root_dir, root_dir, sizeof(out->root_dir) - 1);
    return MYDB_OK;
}

int engine_close(EngineState *eng)
{
    if (!eng) return MYDB_ERR;
    int rc = MYDB_OK;

    /* Tear down every loaded partition: pctx_close flushes dirty pages,
     * shuts down the embedded StorageEngine, closes Cache 2 and the
     * catalog.  partition_manager owns all of that — the engine just
     * frees the heap handle afterward. */
    for (int i = 0; i < MAX_PARTITIONS; i++) {
        if (eng->partitions[i]) {
            if (pctx_close(eng->partitions[i]) != MYDB_OK) rc = MYDB_ERR;
            free(eng->partitions[i]);
            eng->partitions[i] = NULL;
        }
    }

    /* Close every open stats handle (does not unlink the files). */
    sb_destroy(&eng->stats_buf);

    if (eng->system_schema.users.fd >= 0 || eng->system_schema.privileges.fd >= 0) {
        if (system_schema_close(&eng->system_schema) != MYDB_OK) rc = MYDB_ERR;
    }
    if (eng->database.fd >= 0) {
        if (db_close(&eng->database) != MYDB_OK) rc = MYDB_ERR;
    }

    /* Clear every connection slot (server may have several open at shutdown). */
    for (int i = 0; i < MAX_CONNECTIONS; i++)
        memset(&eng->conn_pool.conns[i], 0, sizeof(Connection));
    eng->conn_pool.n_active = 0;
    eng->active_conn        = -1;
    return rc;
}


/* ====================================================================
 *  Combined start (the single call bin/mydb makes)
 * ==================================================================== */

int engine_start(const char *root_dir,
                 const char *username,
                 const char *password,
                 EngineState *out)
{
    if (!root_dir || !username || !password || !out) return MYDB_ERR;

    int rc = engine_init(root_dir, out);
    if (rc != MYDB_OK) return rc;

    /* engine_login now creates the PartitionCtx (via pctx_init), which
     * initialises the embedded StorageEngine internally — there is no
     * longer a separate storage_init step at the engine level. */
    rc = engine_login(out, username, password);
    if (rc != MYDB_OK) {
        engine_close(out);
        return rc;
    }

    return MYDB_OK;
}


/* ====================================================================
 *  Login
 * ==================================================================== */

/* Open a fresh session for an already-authenticated user: claim a connection
 * slot, acquire the user's partition (if they own one), stamp last_login, and
 * populate the slot.  On success returns MYDB_OK and writes the new conn_id
 * (slot index) to *conn_id_out.  Shared by engine_login (plaintext, local) and
 * engine_login_response (network challenge-response).  `u` is updated with the
 * new last_login. */
static int session_open(EngineState *eng, UserSlot *u,
                        const char *username, int *conn_id_out)
{
    int slot = conn_alloc(eng);
    if (slot < 0) return MYDB_ERR;      /* connection pool full */

    /* Acquire the partition runtime context — only if the user owns one.
     * Analyst accounts (no owned partition) still get a slot; their
     * partition_open stays 0 and partition_id stays 0 until USE resolves a
     * target partition through their privileges. */
    uint8_t  partition_open = 0;
    uint32_t partition_id   = 0;
    PartitionEntry *p = db_find_by_owner(&eng->database, u->user_id);
    if (p) {
        if (engine_acquire_partition(eng, p->partition_id, p->path) == NULL) {
            conn_free(eng, slot);
            return MYDB_ERR;
        }
        partition_open = 1;
        partition_id   = p->partition_id;
    }

    /* Stamp last_login on the user record (persists to disk). */
    u->last_login = now_yyyymmddhhmmss();
    if (users_update(&eng->system_schema.users, u) != MYDB_OK) {
        engine_release_partition(eng, partition_id);
        conn_free(eng, slot);
        return MYDB_ERR;
    }

    /* Populate the session's Connection slot (connection_id set by conn_alloc). */
    Connection *c = &eng->conn_pool.conns[slot];
    c->user_id        = u->user_id;
    c->partition_id   = partition_id;
    c->partition_open = partition_open;
    c->schema_active  = 0;
    c->logged_in      = 1;
    c->current_schema_name[0] = '\0';
    strncpy(c->username, username, sizeof(c->username) - 1);
    c->username[sizeof(c->username) - 1] = '\0';
    eng->conn_pool.n_active++;

    if (conn_id_out) *conn_id_out = slot;
    return MYDB_OK;
}

/* Plaintext login (local / bootstrap path, e.g. engine_start + tests).  The
 * network server uses engine_login_response instead.  On success the new
 * session is also made the active connection so the embedded single-session
 * caller's subsequent engine_execute_sql works. */
int engine_login(EngineState *eng,
                 const char *username, const char *password)
{
    if (!eng || !username || !password)         return MYDB_ERR;

    UserSlot u;
    int rc = users_find_by_name(&eng->system_schema.users, username, &u);
    if (rc != MYDB_OK) return rc;        /* MYDB_ERR_NOT_FOUND on miss */
    if (!u.is_active) return MYDB_ERR_PERM;

    /* Verify password — SHA-256(salt || password). */
    uint8_t computed[SHA256_DIGEST_LEN];
    crypto_hash_password(password, u.password_salt, computed);
    if (memcmp(computed, u.password_hash, SHA256_DIGEST_LEN) != 0)
        return MYDB_ERR_PERM;

    int conn_id = -1;
    rc = session_open(eng, &u, username, &conn_id);
    if (rc != MYDB_OK) return rc;
    eng->active_conn = conn_id;          /* embedded path: make it current */
    return MYDB_OK;
}

/* Look up a user's password salt for the network challenge-response handshake.
 * Copies SALT_LEN bytes into salt_out.  Returns MYDB_ERR_NOT_FOUND for an
 * unknown user (the auth handler answers with a fake salt to avoid leaking
 * which usernames exist). */
int engine_get_user_salt(EngineState *eng, const char *username,
                         uint8_t salt_out[SALT_LEN])
{
    if (!eng || !username || !salt_out) return MYDB_ERR;
    UserSlot u;
    int rc = users_find_by_name(&eng->system_schema.users, username, &u);
    if (rc != MYDB_OK) return rc;
    memcpy(salt_out, u.password_salt, SALT_LEN);
    return MYDB_OK;
}

/* Network login: verify a challenge-response and open a session.
 *
 *   stored_hash = SHA-256(salt || password)          (already on disk)
 *   client sends response = SHA-256(nonce || stored_hash)
 *   we recompute the same and compare.
 *
 * On success writes the new conn_id (>= 0) to *conn_id_out and returns
 * MYDB_OK.  MYDB_ERR_NOT_FOUND (unknown user) and MYDB_ERR_PERM (bad
 * response / inactive) are both reported as auth failure by the caller. */
int engine_login_response(EngineState *eng, const char *username,
                          const uint8_t response[SHA256_DIGEST_LEN],
                          const uint8_t nonce[MYDB_NONCE_LEN],
                          int *conn_id_out)
{
    if (!eng || !username || !response || !nonce || !conn_id_out)
        return MYDB_ERR;
    *conn_id_out = -1;

    UserSlot u;
    int rc = users_find_by_name(&eng->system_schema.users, username, &u);
    if (rc != MYDB_OK) return rc;        /* MYDB_ERR_NOT_FOUND */
    if (!u.is_active) return MYDB_ERR_PERM;

    /* expected = SHA-256(nonce || stored_hash) */
    uint8_t buf[MYDB_NONCE_LEN + SHA256_DIGEST_LEN];
    memcpy(buf, nonce, MYDB_NONCE_LEN);
    memcpy(buf + MYDB_NONCE_LEN, u.password_hash, SHA256_DIGEST_LEN);
    uint8_t expected[SHA256_DIGEST_LEN];
    sha256(buf, sizeof(buf), expected);
    if (memcmp(expected, response, SHA256_DIGEST_LEN) != 0)
        return MYDB_ERR_PERM;

    return session_open(eng, &u, username, conn_id_out);
}

/* Close a network session: roll back any open transaction, drop the
 * connection's reference on its partition (flush + evict at n_refs 0), and
 * free the slot.  Called by the server on QUIT or client disconnect. */
int engine_logout(EngineState *eng, int conn_id)
{
    Connection *c = conn_at(eng, conn_id);
    if (!c) return MYDB_ERR;

    eng->active_conn = conn_id;          /* scope cleanup to this connection */

    /* Best-effort rollback of any open explicit transaction on this
     * connection's partition (pre-WAL, single-user txn manager). */
    PartitionCtx *part = engine_find_partition(eng, c->partition_id);
    if (part) pm_rollback(part);

    uint32_t pid = c->partition_id;
    conn_free(eng, conn_id);
    if (eng->conn_pool.n_active > 0) eng->conn_pool.n_active--;
    engine_release_partition(eng, pid);  /* --n_refs; flush + evict at 0 */

    eng->active_conn = -1;
    return MYDB_OK;
}


/* ====================================================================
 *  Deactivate the currently active schema without switching to another.
 *
 *  Flushes dirty pages, closes the schema file, and clears the
 *  schema_active flag.  Partition state (logged_in, partition_open,
 *  current_partition_path, etc.) is left intact — the user stays
 *  logged in and can still run USE, CREATE DATABASE, etc.
 *
 *  Called by DROP DATABASE when the user drops their active database.
 * ==================================================================== */

int engine_deactivate_schema(EngineState *eng)
{
    if (!eng) return MYDB_OK;
    Connection *c = cur_conn(eng);
    if (!c->schema_active) return MYDB_OK;             /* nothing to do */

    /* Clear the active-schema marker on the connection's partition.  Cache 2
     * entries stay open; partition state (logged_in, partition_open) is left
     * intact — the user stays logged in. */
    PartitionCtx *part = cur_partition(eng);
    if (part && pctx_deactivate_schema(part) != MYDB_OK)
        return MYDB_ERR;

    c->schema_active = 0;
    c->current_schema_name[0] = '\0';

    return MYDB_OK;
}

/* ====================================================================
 *  USE schema_name
 * ==================================================================== */

int engine_use_schema(EngineState *eng, const char *schema_name)
{
    if (!eng || !schema_name)   return MYDB_ERR;
    Connection *c = cur_conn(eng);
    if (!c->logged_in)          return MYDB_ERR_PERM;
    if (schema_name[0] == '\0') return MYDB_ERR;

    /* Deactivate any currently active schema on the connection's partition
     * (clears the active marker; Cache 2 entries stay open). */
    if (c->schema_active) {
        PartitionCtx *old = cur_partition(eng);
        if (old) pctx_deactivate_schema(old);
        c->schema_active = 0;
        c->current_schema_name[0] = '\0';
    }

    char schema_path[256];
    PartitionCtx *part;

    if (c->partition_open) {
        /* Owner path: the partition was acquired at login. */
        part = cur_partition(eng);
        if (!part) return MYDB_ERR;

        /* Schema must exist in the user's own catalog. */
        Catalog *part_cat = pctx_catalog(part);
        if (!part_cat || cat_find_schema(part_cat, schema_name) == NULL)
            return MYDB_ERR_NOT_FOUND;

        if (join2(schema_path, sizeof(schema_path),
                  part->partition_path, schema_name, "__schema.mydb") < 0)
            return MYDB_ERR;
    } else {
        /* Analyst path: find a privilege grant for this user + schema. */
        PrivilegeSlot priv;
        memset(&priv, 0, sizeof(priv));

        /* Scan privilege slots for (grantee=current_user, schema=schema_name). */
        int found = 0;
        PrivilegesFile *pf = &eng->system_schema.privileges;
        for (int i = 0; i < PRIVILEGES_MAX_SLOTS; i++) {
            PrivilegeSlot *s = &pf->slots[i];
            if (!s->is_valid) continue;
            if (s->grantee_id != c->user_id) continue;
            if (strncmp(s->schema_name, schema_name, sizeof(s->schema_name)) != 0)
                continue;
            priv  = *s;
            found = 1;
            break;
        }
        if (!found) return MYDB_ERR_PERM;

        /* Resolve the owning partition's path from __database.mydb. */
        PartitionEntry *pe = db_find_by_id(&eng->database, priv.partition_id);
        if (!pe) return MYDB_ERR;

        /* Switch this connection to the target partition: release the old one
         * (if different) and acquire the target from the engine table.  The
         * PartitionCtx is shared with any other connection on that partition. */
        if (c->partition_id != priv.partition_id) {
            engine_release_partition(eng, c->partition_id);
            part = engine_acquire_partition(eng, priv.partition_id, pe->path);
            if (!part) return MYDB_ERR;
            c->partition_id = priv.partition_id;
        } else {
            part = engine_find_partition(eng, c->partition_id);
            if (!part) return MYDB_ERR;
        }

        if (join2(schema_path, sizeof(schema_path),
                  pe->path, schema_name, "__schema.mydb") < 0)
            return MYDB_ERR;
    }

    /* partition_manager loads the schema into Cache 2 and makes it active. */
    if (pctx_open_schema(part, schema_name, schema_path) == NULL)
        return MYDB_ERR;

    strncpy(c->current_schema_name, schema_name,
            sizeof(c->current_schema_name) - 1);
    c->current_schema_name[sizeof(c->current_schema_name) - 1] = '\0';
    c->schema_active = 1;
    return MYDB_OK;
}


/* ====================================================================
 *  Authorization
 * ==================================================================== */

int engine_check_access(EngineState *eng, int write_required)
{
    if (!eng) return MYDB_ERR_PERM;
    Connection *c = cur_conn(eng);
    if (!c->logged_in || !c->schema_active)
        return MYDB_ERR_PERM;

    /* Owner of the active partition has full access. */
    if (c->partition_open)
        return MYDB_OK;

    /* Analyst: verify a privilege grant exists for this (user, partition, schema). */
    PrivilegeSlot slot;
    int rc = privileges_find(&eng->system_schema.privileges,
                             c->user_id,
                             c->partition_id,
                             c->current_schema_name,
                             &slot);
    if (rc != MYDB_OK)      return MYDB_ERR_PERM;
    if (write_required)     return MYDB_ERR_PERM; /* grants are SELECT-only */
    return MYDB_OK;
}


/* ====================================================================
 *  Read-only metadata accessors (parser + execution engine)
 * ==================================================================== */

const RelationDef *engine_find_relation(EngineState *eng,
                                        const char *relation_name)
{
    if (!eng || !relation_name)       return NULL;
    Connection *c = cur_conn(eng);
    if (!c->schema_active)            return NULL;
    /* Delegate to partition_manager — it owns the active SchemaFile. */
    return pm_find_relation_const(cur_partition(eng), relation_name);
}


/* ====================================================================
 *  User management helpers
 * ==================================================================== */

/*
 * Delete every file inside a partition directory and the directory itself.
 * Layout assumed:
 *   <part_dir>/
 *       __catalog.mydb
 *       <schema>/
 *           __schema.mydb
 *           <rel>.mydb  (one per table)
 *           __stats.mydb (optional)
 *
 * We enumerate schemas from the already-opened catalog `cat` rather than
 * using readdir so we stay within the storage model.
 */
static void remove_partition_dir(const char *part_dir, Catalog *cat)
{
    char path[512];
    int  n;

    for (int s = 0; s < MAX_SCHEMAS_PER_PARTITION; s++) {
        if (!cat->schemas[s].is_valid) continue;
        const char *sname = cat->schemas[s].schema_name;

        char schema_dir[512];
        int n = snprintf(schema_dir, sizeof(schema_dir), "%s/%s", part_dir, sname);
        if (n < 0 || (size_t)n >= sizeof(schema_dir)) continue;

        char sf_path[512];
        n = snprintf(sf_path, sizeof(sf_path), "%s/__schema.mydb", schema_dir);
        if (n < 0 || (size_t)n >= sizeof(sf_path)) continue;

        /* Open schema file to enumerate relation files. */
        SchemaFile sf;
        if (schema_open(sf_path, &sf) == MYDB_OK) {
            for (int r = 0; r < MAX_RELATIONS_PER_SCHEMA; r++) {
                if (!sf.relations[r].is_valid) continue;
                n = snprintf(path, sizeof(path), "%s/%s.mydb",
                             schema_dir, sf.relations[r].relation_name);
                if (n > 0 && (size_t)n < sizeof(path))
                    unlink(path);
            }
            schema_close(&sf);
        }

        /* Remove stats and schema metadata files. */
        n = snprintf(path, sizeof(path), "%s/__stats.mydb", schema_dir);
        if (n > 0 && (size_t)n < sizeof(path)) unlink(path);
        unlink(sf_path);
        rmdir(schema_dir);
    }

    /* Remove the catalog file then the partition directory itself. */
    n = snprintf(path, sizeof(path), "%s/__catalog.mydb", part_dir);
    if (n > 0 && (size_t)n < sizeof(path)) unlink(path);
    rmdir(part_dir);
}

/* Check that a proposed partition path does not collide with any
 * existing active partition. Returns 0 if unique, -1 if taken. */
static int partition_path_unique(const DatabaseFile *db, const char *path)
{
    for (int i = 0; i < MAX_PARTITIONS; i++) {
        if (!db->partitions[i].is_active) continue;
        if (strcmp(db->partitions[i].path, path) == 0) return -1;
    }
    return 0;
}

int engine_create_user(EngineState *eng,
                       const char  *username,
                       const char  *password,
                       const char  *partition_name,
                       uint64_t     quota_bytes)
{
    if (!eng || !username || !password) return MYDB_ERR;
    Connection *c = cur_conn(eng);
    if (!c->logged_in)                 return MYDB_ERR_PERM;
    /* Only root (user_id == 1) may create users. */
    if (c->user_id != 1)               return MYDB_ERR_PERM;

    if (username[0] == '\0' || strlen(username) >= MAX_USERNAME) return MYDB_ERR;

    /* Resolve quota. */
    if (quota_bytes == 0)                        quota_bytes = ENGINE_DEFAULT_QUOTA_BYTES;
    if (quota_bytes < ENGINE_MIN_QUOTA_BYTES)     return MYDB_ERR;
    if (quota_bytes > ENGINE_MAX_QUOTA_BYTES)     return MYDB_ERR;

    /* Resolve partition directory name — default to username. */
    const char *part_name = (partition_name && partition_name[0] != '\0')
                            ? partition_name : username;

    /* Build the full partition path. */
    char part_dir[256], cat_path[256];
    if (join1(part_dir,  sizeof(part_dir),  eng->root_dir, part_name) < 0) return MYDB_ERR;
    if (join1(cat_path,  sizeof(cat_path),  part_dir, "__catalog.mydb") < 0) return MYDB_ERR;

    /* Reject duplicate username. */
    UserSlot existing;
    if (users_find_by_name(&eng->system_schema.users, username, &existing) == MYDB_OK)
        return MYDB_ERR_DUPLICATE;

    /* Reject duplicate partition path. */
    if (partition_path_unique(&eng->database, part_dir) < 0)
        return MYDB_ERR_DUPLICATE;

    /* Insert user — gets a new user_id from the monotonic counter. */
    UserSlot u;
    memset(&u, 0, sizeof(u));
    strncpy(u.username, username, MAX_USERNAME - 1);
    u.is_active  = 1;
    u.created_at = now_yyyymmddhhmmss();
    u.last_login = 0;
    crypto_hash_password(password, u.password_salt, u.password_hash);

    uint32_t new_user_id;
    int rc = users_insert(&eng->system_schema.users, &u, &new_user_id);
    if (rc != MYDB_OK) return rc;

    /* Create the partition directory. */
    if (ensure_dir(part_dir) < 0) {
        users_delete(&eng->system_schema.users, new_user_id);
        return MYDB_ERR;
    }

    /* Register partition in __database.mydb. */
    uint32_t new_part_id;
    rc = db_add_partition(&eng->database, new_user_id, part_dir, &new_part_id);
    if (rc != MYDB_OK) {
        rmdir(part_dir);
        users_delete(&eng->system_schema.users, new_user_id);
        return rc;
    }

    /* Create the partition catalog. */
    Catalog cat;
    rc = cat_create(cat_path, new_part_id, new_user_id, quota_bytes, &cat);
    if (rc != MYDB_OK) {
        db_remove_partition(&eng->database, new_part_id);
        rmdir(part_dir);
        users_delete(&eng->system_schema.users, new_user_id);
        return rc;
    }
    cat_close(&cat);
    return MYDB_OK;
}

int engine_drop_user(EngineState *eng, const char *username)
{
    if (!eng || !username)     return MYDB_ERR;
    Connection *c = cur_conn(eng);
    if (!c->logged_in)         return MYDB_ERR_PERM;
    /* Only root may drop users. */
    if (c->user_id != 1)       return MYDB_ERR_PERM;
    /* Cannot drop root itself. */
    if (strcmp(username, c->username) == 0) return MYDB_ERR_PERM;

    /* Look up the target user. */
    UserSlot target;
    int rc = users_find_by_name(&eng->system_schema.users, username, &target);
    if (rc != MYDB_OK) return rc;   /* MYDB_ERR_NOT_FOUND on miss */

    /* Extra guard: never allow dropping the root user (user_id == 1). */
    if (target.user_id == 1) return MYDB_ERR_PERM;

    /* Find and remove the user's partition. */
    PartitionEntry *pe = db_find_by_owner(&eng->database, target.user_id);
    if (pe) {
        char part_dir[256];
        strncpy(part_dir, pe->path, sizeof(part_dir) - 1);
        part_dir[sizeof(part_dir) - 1] = '\0';
        uint32_t part_id = pe->partition_id;

        /* Open the catalog to enumerate schemas for cleanup. */
        char cat_path[256];
        if (join1(cat_path, sizeof(cat_path), part_dir, "__catalog.mydb") == 0) {
            Catalog cat;
            if (cat_open(cat_path, &cat) == MYDB_OK) {
                remove_partition_dir(part_dir, &cat);
                cat_close(&cat);
            } else {
                /* Catalog unreadable — best-effort: just rmdir. */
                rmdir(part_dir);
            }
        }

        /* Drop every engine-owned stats file for this partition
         * (system_schema/stats/) — they live outside the partition dir. */
        sb_remove_partition(&eng->stats_buf, part_id);

        /* Remove partition entry from __database.mydb. */
        db_remove_partition(&eng->database, part_id);
    }

    /* Remove user from users.mydb. */
    return users_delete(&eng->system_schema.users, target.user_id);
}

int engine_alter_user_password(EngineState *eng,
                               const char  *username,
                               const char  *new_password)
{
    if (!eng || !username || !new_password) return MYDB_ERR;
    Connection *c = cur_conn(eng);
    if (!c->logged_in)                      return MYDB_ERR_PERM;
    /* Phase 1: only root may alter any user's password. */
    if (c->user_id != 1)                    return MYDB_ERR_PERM;

    UserSlot u;
    int rc = users_find_by_name(&eng->system_schema.users, username, &u);
    if (rc != MYDB_OK) return rc;

    crypto_hash_password(new_password, u.password_salt, u.password_hash);
    return users_update(&eng->system_schema.users, &u);
}

int engine_alter_user_quota(EngineState *eng,
                            const char  *username,
                            uint64_t     new_quota_bytes)
{
    if (!eng || !username) return MYDB_ERR;
    Connection *c = cur_conn(eng);
    if (!c->logged_in)     return MYDB_ERR_PERM;
    /* Phase 1: only root may alter quotas. */
    if (c->user_id != 1)   return MYDB_ERR_PERM;

    if (new_quota_bytes < ENGINE_MIN_QUOTA_BYTES) return MYDB_ERR;
    if (new_quota_bytes > ENGINE_MAX_QUOTA_BYTES) return MYDB_ERR;

    UserSlot u;
    int rc = users_find_by_name(&eng->system_schema.users, username, &u);
    if (rc != MYDB_OK) return rc;

    /* Find the user's partition catalog. */
    PartitionEntry *pe = db_find_by_owner(&eng->database, u.user_id);
    if (!pe) return MYDB_ERR_NOT_FOUND;   /* user has no partition */

    char cat_path[256];
    if (join1(cat_path, sizeof(cat_path), pe->path, "__catalog.mydb") < 0)
        return MYDB_ERR;

    Catalog cat;
    rc = cat_open(cat_path, &cat);
    if (rc != MYDB_OK) return rc;

    /* Reject if the new quota is below what the user has already used. */
    if (new_quota_bytes < cat.header.used_bytes) {
        cat_close(&cat);
        return MYDB_ERR_FULL;
    }

    cat.header.quota_bytes = new_quota_bytes;
    rc = cat_save(&cat);
    cat_close(&cat);

    /* If this is the currently logged-in user's own catalog, keep the
     * in-memory copy (inside its PartitionCtx's PB[0]) consistent. */
    PartitionCtx *self_part = cur_partition(eng);
    if (u.user_id == c->user_id && c->partition_open && self_part) {
        Catalog *self_cat = pctx_catalog(self_part);
        if (self_cat) self_cat->header.quota_bytes = new_quota_bytes;
    }

    return rc;
}


/* ====================================================================
 *  SQL execution
 *
 *  Engine is the single front door for raw SQL coming from bin/REPL.
 *  Two-stage pipeline:
 *    1. parser_parse(sql)                  -> opaque AST handle
 *    2. exec_engine_execute(ectx, ast)     -> walks AST, writes result
 *
 *  The engine builds an ExecContext bundling the session's Connection,
 *  the active PartitionCtx, and itself (for engine-level metadata) and
 *  hands that to the executor.  The AST handle is heap-allocated by the
 *  parser and freed here on every path. Parser errors are formatted into
 *  result_out and the parser's PARSER_ERR is returned to the caller.
 * ==================================================================== */

int engine_execute_sql(EngineState *eng, int conn_id, const char *sql,
                       char *result_out, size_t result_cap)
{
    if (!eng || !sql || !result_out || result_cap == 0) return MYDB_ERR;

    /* Scope the statement to the requesting connection.  cur_conn() and every
     * engine helper it reaches resolve through eng->active_conn for the
     * duration of this call (single-threaded: one statement at a time). */
    Connection *c = conn_at(eng, conn_id);
    if (!c) { eng->active_conn = -1; return MYDB_ERR_PERM; }
    eng->active_conn = conn_id;

    ParserAST *ast = NULL;
    char err_buf[256];
    err_buf[0] = '\0';

    int rc = parser_parse(sql, &ast, err_buf, sizeof(err_buf));
    if (rc != PARSER_OK) {
        snprintf(result_out, result_cap, "  Error: %s",
                 err_buf[0] ? err_buf : "parse error");
        eng->active_conn = -1;
        return MYDB_ERR;
    }

    /* Bundle the per-query context for the execution engine.  When a schema
     * is active, resolve its stats handle from the engine pool (lazily
     * opened/created under system_schema/stats/).  The planner reads it and
     * ANALYZE writes through it; NULL means no schema active. */
    ExecContext ectx;
    PartitionCtx *part = cur_partition(eng);
    ectx.engine    = eng;
    ectx.partition = part;
    ectx.conn      = c;
    ectx.stats     = c->schema_active
                     ? sb_get(&eng->stats_buf, c->partition_id,
                              c->current_schema_name)
                     : NULL;

    /* Project this connection's active schema onto the shared partition for
     * the duration of the statement.  The connection owns its current schema
     * (set by USE); the partition is shared by all of a partition's
     * connections, so the active schema must be (re)established per statement
     * from the executing connection.  pctx_open_schema is a cache-hit bump in
     * the common case (and reloads if the schema was evicted), and keeps the
     * active schema most-recently-used so it cannot be evicted mid-use. */
    if (part) {
        if (c->schema_active && c->current_schema_name[0] != '\0') {
            char schema_path[512];
            if (join2(schema_path, sizeof(schema_path),
                      part->partition_path,
                      c->current_schema_name, "__schema.mydb") == 0)
                pctx_open_schema(part, c->current_schema_name, schema_path);
        } else {
            pctx_deactivate_schema(part);
        }
    }

    /* --- Time the execution pipeline --- */
    struct timespec t0, t1;
    clock_gettime(CLOCK_MONOTONIC, &t0);

    rc = exec_engine_execute(&ectx, ast, result_out, result_cap);
    parser_free_ast(ast);

    /* Phase 1 pin/release discipline leak check (PARTITION_BUFFER_DESIGN.md) —
     * every pm_find_relation_const() taken during this statement must have
     * been released (via a RelationGuard or a direct pm_release_relation())
     * by the time the statement is done, on both the success and error path.
     * A no-op in NDEBUG builds. */
    assert(pctx_debug_no_pinned_relations(part));

    clock_gettime(CLOCK_MONOTONIC, &t1);

    /* Append "  (Xs)" to the last character of a successful result. */
    if (rc == MYDB_OK) {
        double elapsed = (double)(t1.tv_sec  - t0.tv_sec)
                       + (double)(t1.tv_nsec - t0.tv_nsec) * 1e-9;
        size_t len = strlen(result_out);
        snprintf(result_out + len, result_cap - len,
                 "  (%.2fs)", elapsed);
    }

    eng->active_conn = -1;
    return rc;
}
