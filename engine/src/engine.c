#include "engine.h"
#include "crypto.h"

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
    return MYDB_OK;
}


/* ====================================================================
 *  USE schema_name
 * ==================================================================== */

int engine_use_schema(EngineState *eng, const char *schema_name)
{
    if (!eng || !schema_name)         return MYDB_ERR;
    if (!eng->logged_in)              return MYDB_ERR_PERM;
    if (!eng->partition_open)         return MYDB_ERR_PERM;
    if (schema_name[0] == '\0')       return MYDB_ERR;

    /* If a schema is already active, close it first.
     *
     * TODO phase 9: flush dirty pages of the old schema's relations
     * out of the buffer pool before closing.
     * TODO phase 11: implicit COMMIT of any open transaction. */
    if (eng->schema_active) {
        if (schema_close(&eng->active_schema) != MYDB_OK) return MYDB_ERR;
        eng->schema_active = 0;
        eng->current_schema_name[0] = '\0';
    }

    /* Verify the schema is registered in the active partition catalog. */
    if (cat_find_schema(&eng->active_catalog, schema_name) == NULL)
        return MYDB_ERR_NOT_FOUND;

    char schema_path[256];
    if (join2(schema_path, sizeof(schema_path),
              eng->current_partition_path, schema_name, "__schema.mydb") < 0)
        return MYDB_ERR;

    int rc = schema_open(schema_path, &eng->active_schema);
    if (rc != MYDB_OK) return rc;

    strncpy(eng->current_schema_name, schema_name,
            sizeof(eng->current_schema_name) - 1);
    eng->current_schema_name[sizeof(eng->current_schema_name) - 1] = '\0';
    eng->schema_active = 1;
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
