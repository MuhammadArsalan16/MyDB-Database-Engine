#include "engine.h"
#include "crypto.h"
#include "storage.h"
#include "parser_api.h"
#include "exec_engine_api.h"

#include <fcntl.h>
#include <unistd.h>
#include <string.h>
#include <stdio.h>
#include <errno.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <time.h>


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
    out->database.fd          = -1;
    out->system_schema.users.fd       = -1;
    out->system_schema.privileges.fd  = -1;
    out->active_catalog.fd    = -1;
    out->active_schema.fd     = -1;

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

    strncpy(out->root_dir, root_dir, sizeof(out->root_dir) - 1);
    return MYDB_OK;
}

int engine_close(EngineState *eng)
{
    if (!eng) return MYDB_ERR;
    storage_shutdown();
    int rc = MYDB_OK;

    if (eng->schema_active && eng->active_schema.fd >= 0) {
        if (schema_close(&eng->active_schema) != MYDB_OK) rc = MYDB_ERR;
        eng->schema_active = 0;
    }
    if (eng->partition_open && eng->active_catalog.fd >= 0) {
        if (cat_close(&eng->active_catalog) != MYDB_OK) rc = MYDB_ERR;
        eng->partition_open = 0;
    }
    if (eng->system_schema.users.fd >= 0 || eng->system_schema.privileges.fd >= 0) {
        if (system_schema_close(&eng->system_schema) != MYDB_OK) rc = MYDB_ERR;
    }
    if (eng->database.fd >= 0) {
        if (db_close(&eng->database) != MYDB_OK) rc = MYDB_ERR;
    }
    eng->logged_in = 0;
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

    rc = engine_login(out, username, password);
    if (rc != MYDB_OK) {
        engine_close(out);
        return rc;
    }

    rc = storage_init(out);
    if (rc != MYDB_OK) {
        engine_close(out);
        return rc;
    }

    return MYDB_OK;
}


/* ====================================================================
 *  Login
 * ==================================================================== */

int engine_login(EngineState *eng,
                 const char *username, const char *password)
{
    if (!eng || !username || !password)         return MYDB_ERR;
    if (eng->logged_in)                          return MYDB_ERR;

    UserSlot u;
    int rc = users_find_by_name(&eng->system_schema.users, username, &u);
    if (rc != MYDB_OK) return rc;        /* MYDB_ERR_NOT_FOUND on miss */

    if (!u.is_active) return MYDB_ERR_PERM;

    /* Verify password — SHA-256(salt || password). */
    uint8_t computed[SHA256_DIGEST_LEN];
    crypto_hash_password(password, u.password_salt, computed);
    if (memcmp(computed, u.password_hash, SHA256_DIGEST_LEN) != 0)
        return MYDB_ERR_PERM;

    /* Open the partition catalog — only if the user owns one. Users
     * without a partition (analyst accounts) can still log in; their
     * eng->partition_open stays 0. */
    PartitionEntry *p = db_find_by_owner(&eng->database, u.user_id);
    if (p) {
        char cat_path[256];
        if (join1(cat_path, sizeof(cat_path), p->path, "__catalog.mydb") < 0)
            return MYDB_ERR;
        rc = cat_open(cat_path, &eng->active_catalog);
        if (rc != MYDB_OK) return rc;
        eng->partition_open       = 1;
        eng->current_partition_id = p->partition_id;
        strncpy(eng->current_partition_path, p->path,
                sizeof(eng->current_partition_path) - 1);
    }

    /* Stamp last_login on the user record (persists to disk). */
    u.last_login = now_yyyymmddhhmmss();
    rc = users_update(&eng->system_schema.users, &u);
    if (rc != MYDB_OK) {
        if (eng->partition_open) {
            cat_close(&eng->active_catalog);
            eng->partition_open = 0;
        }
        return rc;
    }

    eng->current_user_id = u.user_id;
    eng->logged_in       = 1;
    strncpy(eng->current_username, username,
            sizeof(eng->current_username) - 1);
    eng->current_username[sizeof(eng->current_username) - 1] = '\0';
    return MYDB_OK;
}


/* ====================================================================
 *  USE schema_name
 * ==================================================================== */

int engine_use_schema(EngineState *eng, const char *schema_name)
{
    if (!eng || !schema_name)   return MYDB_ERR;
    if (!eng->logged_in)        return MYDB_ERR_PERM;
    if (schema_name[0] == '\0') return MYDB_ERR;

    /* Flush + close any currently active schema.
     * TODO phase 11: implicit COMMIT of any open transaction. */
    if (eng->schema_active) {
        storage_flush_all_dirty();
        if (schema_close(&eng->active_schema) != MYDB_OK) return MYDB_ERR;
        eng->schema_active = 0;
        eng->current_schema_name[0] = '\0';
        eng->current_partition_id = 0;
    }

    char schema_path[256];

    if (eng->partition_open) {
        /* Owner path: schema must exist in the user's own catalog. */
        if (cat_find_schema(&eng->active_catalog, schema_name) == NULL)
            return MYDB_ERR_NOT_FOUND;

        if (join2(schema_path, sizeof(schema_path),
                  eng->current_partition_path, schema_name, "__schema.mydb") < 0)
            return MYDB_ERR;

        /* current_partition_id was already set during engine_login. */
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
            if (s->grantee_id != eng->current_user_id) continue;
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

        if (join2(schema_path, sizeof(schema_path),
                  pe->path, schema_name, "__schema.mydb") < 0)
            return MYDB_ERR;

        eng->current_partition_id = priv.partition_id;
        strncpy(eng->current_partition_path, pe->path,
                sizeof(eng->current_partition_path) - 1);
        eng->current_partition_path[sizeof(eng->current_partition_path) - 1] = '\0';
    }

    int rc = schema_open(schema_path, &eng->active_schema);
    if (rc != MYDB_OK) return rc;

    strncpy(eng->current_schema_name, schema_name,
            sizeof(eng->current_schema_name) - 1);
    eng->current_schema_name[sizeof(eng->current_schema_name) - 1] = '\0';
    eng->schema_active = 1;
    return MYDB_OK;
}


/* ====================================================================
 *  Authorization
 * ==================================================================== */

int engine_check_access(EngineState *eng, int write_required)
{
    if (!eng || !eng->logged_in || !eng->schema_active)
        return MYDB_ERR_PERM;

    /* Owner of the active partition has full access. */
    if (eng->partition_open)
        return MYDB_OK;

    /* Analyst: verify a privilege grant exists for this (user, partition, schema). */
    PrivilegeSlot slot;
    int rc = privileges_find(&eng->system_schema.privileges,
                             eng->current_user_id,
                             eng->current_partition_id,
                             eng->current_schema_name,
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
    if (!eng || !relation_name) return NULL;
    if (!eng->schema_active)    return NULL;
    return schema_find_relation(&eng->active_schema, relation_name);
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
    if (!eng->logged_in)               return MYDB_ERR_PERM;
    /* Only root (user_id == 1) may create users. */
    if (eng->current_user_id != 1)     return MYDB_ERR_PERM;

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
    if (!eng->logged_in)       return MYDB_ERR_PERM;
    /* Only root may drop users. */
    if (eng->current_user_id != 1) return MYDB_ERR_PERM;
    /* Cannot drop root itself. */
    if (strcmp(username, eng->current_username) == 0) return MYDB_ERR_PERM;

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
    if (!eng->logged_in)                    return MYDB_ERR_PERM;
    /* Phase 1: only root may alter any user's password. */
    if (eng->current_user_id != 1)          return MYDB_ERR_PERM;

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
    if (!eng->logged_in)   return MYDB_ERR_PERM;
    /* Phase 1: only root may alter quotas. */
    if (eng->current_user_id != 1) return MYDB_ERR_PERM;

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
     * in-memory copy consistent. */
    if (u.user_id == eng->current_user_id && eng->partition_open)
        eng->active_catalog.header.quota_bytes = new_quota_bytes;

    return rc;
}


/* ====================================================================
 *  SQL execution
 *
 *  Engine is the single front door for raw SQL coming from bin/REPL.
 *  Two-stage pipeline:
 *    1. parser_parse(sql)               -> opaque AST handle
 *    2. exec_engine_execute(eng, ast)   -> walks AST, writes result
 *
 *  The AST handle is heap-allocated by the parser and freed here on
 *  every path. Parser errors are formatted into result_out and the
 *  parser's PARSER_ERR is returned to the caller.
 * ==================================================================== */

int engine_execute_sql(EngineState *eng, const char *sql,
                       char *result_out, size_t result_cap)
{
    if (!eng || !sql || !result_out || result_cap == 0) return MYDB_ERR;
    if (!eng->logged_in) return MYDB_ERR_PERM;

    ParserAST *ast = NULL;
    char err_buf[256];
    err_buf[0] = '\0';

    int rc = parser_parse(sql, &ast, err_buf, sizeof(err_buf));
    if (rc != PARSER_OK) {
        snprintf(result_out, result_cap, "  Error: %s",
                 err_buf[0] ? err_buf : "parse error");
        return MYDB_ERR;
    }

    /* --- Time the execution pipeline --- */
    struct timespec t0, t1;
    clock_gettime(CLOCK_MONOTONIC, &t0);

    rc = exec_engine_execute(eng, ast, result_out, result_cap);
    parser_free_ast(ast);

    clock_gettime(CLOCK_MONOTONIC, &t1);

    /* Append "  (Xs)" to the last character of a successful result. */
    if (rc == MYDB_OK) {
        double elapsed = (double)(t1.tv_sec  - t0.tv_sec)
                       + (double)(t1.tv_nsec - t0.tv_nsec) * 1e-9;
        size_t len = strlen(result_out);
        snprintf(result_out + len, result_cap - len,
                 "  (%.2fs)", elapsed);
    }

    return rc;
}
