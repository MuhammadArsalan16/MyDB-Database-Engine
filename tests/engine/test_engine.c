#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <stdlib.h>

#include "common.h"
#include "engine.h"
#include "schema_file.h"
#include "partition.h"
#include "crypto.h"

#define TEST_ROOT     "/tmp/mydb_test_engine"

static int tests_run    = 0;
static int tests_passed = 0;

#define CHECK(cond, msg) do { \
    tests_run++; \
    if (cond) { tests_passed++; printf("  PASS: %s\n", msg); } \
    else       { printf("  FAIL: %s (line %d)\n", msg, __LINE__); } \
} while(0)


/* ------------------------------------------------------------------ */
/*  Filesystem cleanup — engine creates a small tree under TEST_ROOT  */
/* ------------------------------------------------------------------ */

static void rm_recursive(const char *path)
{
    /* Best-effort: enumerate immediate children, recurse, then rmdir. */
    char cmd[512];
    snprintf(cmd, sizeof(cmd), "rm -rf '%s'", path);
    (void)!system(cmd);
}

static void cleanup(void) { rm_recursive(TEST_ROOT); }

static int file_exists(const char *path)
{
    struct stat st;
    return stat(path, &st) == 0 && S_ISREG(st.st_mode);
}

static int dir_exists(const char *path)
{
    struct stat st;
    return stat(path, &st) == 0 && S_ISDIR(st.st_mode);
}


/* ================================================================== */
/*  Bootstrap                                                         */
/* ================================================================== */

static void test_bootstrap_creates_layout(void)
{
    printf("\n[test_bootstrap_creates_layout]\n");
    cleanup();

    CHECK(engine_bootstrap(TEST_ROOT, "root", "secret") == MYDB_OK,
          "engine_bootstrap succeeds");

    CHECK(file_exists(TEST_ROOT "/__database.mydb"),
          "__database.mydb created");
    CHECK(dir_exists(TEST_ROOT "/system_schema"),
          "system_schema/ dir created");
    CHECK(file_exists(TEST_ROOT "/system_schema/users.mydb"),
          "users.mydb created");
    CHECK(file_exists(TEST_ROOT "/system_schema/privileges.mydb"),
          "privileges.mydb created");
    CHECK(dir_exists(TEST_ROOT "/root"),
          "root partition dir created");
    CHECK(file_exists(TEST_ROOT "/root/__catalog.mydb"),
          "root partition __catalog.mydb created");
}

static void test_bootstrap_rejects_double(void)
{
    printf("\n[test_bootstrap_rejects_double]\n");
    cleanup();

    CHECK(engine_bootstrap(TEST_ROOT, "root", "secret") == MYDB_OK,
          "first bootstrap OK");
    CHECK(engine_bootstrap(TEST_ROOT, "root", "secret") != MYDB_OK,
          "second bootstrap on same root fails");
}

static void test_bootstrap_rejects_empty_args(void)
{
    printf("\n[test_bootstrap_rejects_empty_args]\n");
    cleanup();

    CHECK(engine_bootstrap(NULL, "root", "secret") == MYDB_ERR,
          "NULL root_dir rejected");
    CHECK(engine_bootstrap(TEST_ROOT, "",     "secret") == MYDB_ERR,
          "empty username rejected");
    CHECK(engine_bootstrap(TEST_ROOT, "root", "")       == MYDB_ERR,
          "empty password rejected");
}

static void test_bootstrap_root_user_recorded(void)
{
    printf("\n[test_bootstrap_root_user_recorded]\n");
    cleanup();
    engine_bootstrap(TEST_ROOT, "root", "s3cr3t");

    EngineState eng;
    engine_init(TEST_ROOT, &eng);

    UserSlot u;
    CHECK(users_find_by_name(&eng.system_schema.users, "root", &u) == MYDB_OK,
          "root user present in users.mydb");
    CHECK(u.user_id == 1,         "root user has user_id=1");
    CHECK(u.is_active == 1,       "root user is active");
    CHECK(u.hash_algorithm == 1,  "root user uses SHA-256");

    /* Salt must not be all zeros (must come from /dev/urandom). */
    int any_nonzero = 0;
    for (int i = 0; i < (int)sizeof(u.password_salt); i++)
        if (u.password_salt[i]) { any_nonzero = 1; break; }
    CHECK(any_nonzero, "salt populated with random bytes");

    engine_close(&eng);
}

static void test_bootstrap_root_partition_registered(void)
{
    printf("\n[test_bootstrap_root_partition_registered]\n");
    cleanup();
    engine_bootstrap(TEST_ROOT, "root", "s3cr3t");

    EngineState eng;
    engine_init(TEST_ROOT, &eng);

    PartitionEntry *p = db_find_by_owner(&eng.database, 1);
    CHECK(p != NULL,                        "root user owns a partition");
    CHECK(p && p->partition_id == 1,        "partition_id=1");
    CHECK(p && strstr(p->path, "/root"),    "partition path ends in /root");

    engine_close(&eng);
}


/* ================================================================== */
/*  Init / close                                                       */
/* ================================================================== */

static void test_init_opens_metadata(void)
{
    printf("\n[test_init_opens_metadata]\n");
    cleanup();
    engine_bootstrap(TEST_ROOT, "root", "x");

    EngineState eng;
    CHECK(engine_init(TEST_ROOT, &eng) == MYDB_OK, "engine_init OK");
    CHECK(eng.database.fd            >= 0, "database fd open");
    CHECK(eng.system_schema.users.fd >= 0, "users fd open");
    CHECK(eng.system_schema.privileges.fd >= 0, "privileges fd open");
    CHECK(eng.logged_in == 0,              "no user logged in yet");
    CHECK(eng.partition_open == 0,         "no partition open yet");
    CHECK(eng.schema_active == 0,          "no schema active yet");

    CHECK(engine_close(&eng) == MYDB_OK, "engine_close OK");
    CHECK(eng.database.fd            < 0, "database fd closed");
    CHECK(eng.system_schema.users.fd < 0, "users fd closed");
}

static void test_init_uninitialized_root_fails(void)
{
    printf("\n[test_init_uninitialized_root_fails]\n");
    cleanup();

    EngineState eng;
    CHECK(engine_init(TEST_ROOT, &eng) != MYDB_OK,
          "init on missing root fails");
}


/* ================================================================== */
/*  Login                                                             */
/* ================================================================== */

static void test_login_success(void)
{
    printf("\n[test_login_success]\n");
    cleanup();
    engine_bootstrap(TEST_ROOT, "root", "topsecret");

    EngineState eng;
    engine_init(TEST_ROOT, &eng);

    CHECK(engine_login(&eng, "root", "topsecret") == MYDB_OK,
          "valid creds → login OK");
    CHECK(eng.logged_in == 1,        "logged_in flag set");
    CHECK(eng.current_user_id == 1,  "current_user_id stored");
    CHECK(eng.partition_open == 1,   "owner's partition opened");
    CHECK(eng.current_partition_id == 1, "partition_id=1");
    CHECK(eng.active_catalog.fd >= 0, "active catalog fd open");

    engine_close(&eng);
}

static void test_login_wrong_password(void)
{
    printf("\n[test_login_wrong_password]\n");
    cleanup();
    engine_bootstrap(TEST_ROOT, "root", "right");

    EngineState eng;
    engine_init(TEST_ROOT, &eng);

    CHECK(engine_login(&eng, "root", "wrong") == MYDB_ERR_PERM,
          "bad password → MYDB_ERR_PERM");
    CHECK(eng.logged_in == 0, "logged_in stays 0 after failed login");
    CHECK(eng.partition_open == 0, "no partition opened on failure");

    engine_close(&eng);
}

static void test_login_unknown_user(void)
{
    printf("\n[test_login_unknown_user]\n");
    cleanup();
    engine_bootstrap(TEST_ROOT, "root", "p");

    EngineState eng;
    engine_init(TEST_ROOT, &eng);

    CHECK(engine_login(&eng, "ghost", "p") == MYDB_ERR_NOT_FOUND,
          "unknown username → NOT_FOUND");
    engine_close(&eng);
}

static void test_login_inactive_user(void)
{
    printf("\n[test_login_inactive_user]\n");
    cleanup();
    engine_bootstrap(TEST_ROOT, "root", "p");

    EngineState eng;
    engine_init(TEST_ROOT, &eng);

    /* Manually flip is_active off and re-save. */
    UserSlot u;
    users_find_by_name(&eng.system_schema.users, "root", &u);
    u.is_active = 0;
    users_update(&eng.system_schema.users, &u);

    CHECK(engine_login(&eng, "root", "p") == MYDB_ERR_PERM,
          "inactive user login → MYDB_ERR_PERM");
    engine_close(&eng);
}

static void test_login_double_rejected(void)
{
    printf("\n[test_login_double_rejected]\n");
    cleanup();
    engine_bootstrap(TEST_ROOT, "root", "p");

    EngineState eng;
    engine_init(TEST_ROOT, &eng);
    engine_login(&eng, "root", "p");
    CHECK(engine_login(&eng, "root", "p") == MYDB_ERR,
          "second login on same session rejected");
    engine_close(&eng);
}

static void test_login_stamps_last_login(void)
{
    printf("\n[test_login_stamps_last_login]\n");
    cleanup();
    engine_bootstrap(TEST_ROOT, "root", "p");

    EngineState eng;
    engine_init(TEST_ROOT, &eng);

    UserSlot before;
    users_find_by_name(&eng.system_schema.users, "root", &before);
    CHECK(before.last_login == 0, "last_login starts 0");

    engine_login(&eng, "root", "p");

    UserSlot after;
    users_find_by_name(&eng.system_schema.users, "root", &after);
    CHECK(after.last_login != 0, "last_login populated after login");
    engine_close(&eng);

    /* Persists across reopen. */
    EngineState eng2;
    engine_init(TEST_ROOT, &eng2);
    UserSlot persisted;
    users_find_by_name(&eng2.system_schema.users, "root", &persisted);
    CHECK(persisted.last_login == after.last_login,
          "last_login persists across reopen");
    engine_close(&eng2);
}

static void test_login_analyst_no_partition(void)
{
    printf("\n[test_login_analyst_no_partition]\n");
    cleanup();
    engine_bootstrap(TEST_ROOT, "root", "p");

    EngineState eng;
    engine_init(TEST_ROOT, &eng);

    /* Insert an analyst user with no partition. Hash a known password
     * so we can log in. */
    UserSlot a;
    memset(&a, 0, sizeof(a));
    strncpy(a.username, "analyst", MAX_USERNAME - 1);
    memset(a.password_salt, 0x77, sizeof(a.password_salt));
    crypto_hash_password("pw", a.password_salt, a.password_hash);
    a.hash_algorithm = 1;
    a.is_active = 1;

    uint32_t aid;
    users_insert(&eng.system_schema.users, &a, &aid);

    CHECK(engine_login(&eng, "analyst", "pw") == MYDB_OK,
          "analyst (no partition) can login");
    CHECK(eng.logged_in == 1, "logged_in set");
    CHECK(eng.partition_open == 0, "no partition opened (no ownership)");
    CHECK(eng.current_user_id == aid, "current_user_id stored");

    engine_close(&eng);
}


/* ================================================================== */
/*  USE schema                                                        */
/* ================================================================== */

/* Helper: bootstrap, login, then create one schema dir + __schema.mydb
 * for a fresh schema named `schema_name`. Returns engine in eng with
 * the schema registered in the catalog (but not yet active). */
static void prepare_schema(EngineState *eng, const char *schema_name)
{
    cleanup();
    engine_bootstrap(TEST_ROOT, "root", "p");
    engine_init(TEST_ROOT, eng);
    engine_login(eng, "root", "p");

    /* Manually set up the schema directory + __schema.mydb. The
     * higher-level CREATE DATABASE plumbing lands in phase 9; for
     * phase 8 tests we wire it up directly. */
    char schema_dir[256], schema_path[256];
    snprintf(schema_dir,  sizeof(schema_dir),  "%s/%s", eng->current_partition_path, schema_name);
    snprintf(schema_path, sizeof(schema_path), "%s/__schema.mydb", schema_dir);
    mkdir(schema_dir, 0755);

    SchemaFile sf;
    schema_create(schema_path, eng->current_partition_id, schema_name, &sf);
    schema_close(&sf);

    cat_add_schema(&eng->active_catalog, schema_name);
}

static void test_use_schema_success(void)
{
    printf("\n[test_use_schema_success]\n");
    EngineState eng;
    prepare_schema(&eng, "hr");

    CHECK(engine_use_schema(&eng, "hr") == MYDB_OK, "USE hr OK");
    CHECK(eng.schema_active == 1, "schema_active=1");
    CHECK(strcmp(eng.current_schema_name, "hr") == 0, "schema name stored");
    CHECK(eng.active_schema.fd >= 0, "active schema fd open");

    engine_close(&eng);
}

static void test_use_schema_unknown(void)
{
    printf("\n[test_use_schema_unknown]\n");
    EngineState eng;
    prepare_schema(&eng, "hr");

    CHECK(engine_use_schema(&eng, "nope") == MYDB_ERR_NOT_FOUND,
          "unknown schema → NOT_FOUND");
    CHECK(eng.schema_active == 0, "schema_active stays 0");

    engine_close(&eng);
}

static void test_use_schema_swap(void)
{
    printf("\n[test_use_schema_swap]\n");
    EngineState eng;
    prepare_schema(&eng, "hr");

    /* Add a second schema and verify the swap closes the first one. */
    char schema_dir[256], schema_path[256];
    snprintf(schema_dir,  sizeof(schema_dir),  "%s/sales", eng.current_partition_path);
    snprintf(schema_path, sizeof(schema_path), "%s/__schema.mydb", schema_dir);
    mkdir(schema_dir, 0755);
    SchemaFile sf;
    schema_create(schema_path, eng.current_partition_id, "sales", &sf);
    schema_close(&sf);
    cat_add_schema(&eng.active_catalog, "sales");

    engine_use_schema(&eng, "hr");
    CHECK(eng.active_schema.fd >= 0, "hr schema fd open");
    CHECK(strcmp(eng.active_schema.header.schema_name, "hr") == 0,
          "active schema header name == 'hr'");

    CHECK(engine_use_schema(&eng, "sales") == MYDB_OK, "swap to sales OK");
    CHECK(strcmp(eng.current_schema_name, "sales") == 0, "name now 'sales'");
    CHECK(eng.active_schema.fd >= 0, "sales fd open");
    /* Reading the header proves we have the new file open, not the
     * old one — the OS may reuse fd numbers, so checking fd values
     * isn't reliable. */
    CHECK(strcmp(eng.active_schema.header.schema_name, "sales") == 0,
          "active schema header name == 'sales' after swap");

    engine_close(&eng);
}

static void test_use_schema_requires_login(void)
{
    printf("\n[test_use_schema_requires_login]\n");
    cleanup();
    engine_bootstrap(TEST_ROOT, "root", "p");

    EngineState eng;
    engine_init(TEST_ROOT, &eng);
    /* Did NOT login. */
    CHECK(engine_use_schema(&eng, "hr") == MYDB_ERR_PERM,
          "USE without login → MYDB_ERR_PERM");
    engine_close(&eng);
}


/* ================================================================== */
/*  engine_find_relation — read-only accessor for parser/exec         */
/* ================================================================== */

/* Helper: bootstrap, login, USE schema, and add one relation named
 * `relation_name` to the active schema's __schema.mydb. */
static void prepare_schema_with_relation(EngineState *eng,
                                         const char *schema_name,
                                         const char *relation_name)
{
    prepare_schema(eng, schema_name);
    engine_use_schema(eng, schema_name);

    RelationDef d;
    memset(&d, 0, sizeof(d));
    strncpy(d.relation_name, relation_name, MAX_TABLE_NAME - 1);
    d.num_columns = 1;
    d.columns[0].type = TYPE_INT;
    d.columns[0].is_primary_key = 1;
    d.columns[0].is_not_null = 1;
    strncpy(d.columns[0].name, "id", MAX_COLUMN_NAME - 1);
    d.pk_col_idx = 0;
    schema_add_relation(&eng->active_schema, &d);
}

static void test_find_relation_hit(void)
{
    printf("\n[test_find_relation_hit]\n");
    EngineState eng;
    prepare_schema_with_relation(&eng, "hr", "employees");

    const RelationDef *r = engine_find_relation(&eng, "employees");
    CHECK(r != NULL, "find_relation hit returns non-NULL");
    CHECK(r && strcmp(r->relation_name, "employees") == 0,
          "relation_name matches");

    engine_close(&eng);
}

static void test_find_relation_miss(void)
{
    printf("\n[test_find_relation_miss]\n");
    EngineState eng;
    prepare_schema_with_relation(&eng, "hr", "employees");

    CHECK(engine_find_relation(&eng, "ghost") == NULL,
          "unknown relation → NULL");
    engine_close(&eng);
}

static void test_find_relation_requires_active_schema(void)
{
    printf("\n[test_find_relation_requires_active_schema]\n");
    cleanup();
    engine_bootstrap(TEST_ROOT, "root", "p");

    EngineState eng;
    engine_init(TEST_ROOT, &eng);
    engine_login(&eng, "root", "p");
    /* did NOT call engine_use_schema */

    CHECK(engine_find_relation(&eng, "anything") == NULL,
          "no active schema → NULL");
    engine_close(&eng);
}

static void test_find_relation_null_args(void)
{
    printf("\n[test_find_relation_null_args]\n");
    EngineState eng;
    prepare_schema_with_relation(&eng, "hr", "employees");

    CHECK(engine_find_relation(NULL, "employees") == NULL, "NULL eng → NULL");
    CHECK(engine_find_relation(&eng, NULL) == NULL, "NULL name → NULL");
    engine_close(&eng);
}


/* ================================================================== */
/*  Argument validation                                               */
/* ================================================================== */

static void test_null_args(void)
{
    printf("\n[test_null_args]\n");
    cleanup();

    EngineState eng;
    CHECK(engine_init(NULL, &eng) == MYDB_ERR, "init NULL root");
    CHECK(engine_init(TEST_ROOT, NULL) == MYDB_ERR, "init NULL out");

    engine_bootstrap(TEST_ROOT, "root", "p");
    engine_init(TEST_ROOT, &eng);

    CHECK(engine_login(NULL, "x", "y")    == MYDB_ERR, "login NULL eng");
    CHECK(engine_login(&eng, NULL, "y")   == MYDB_ERR, "login NULL user");
    CHECK(engine_login(&eng, "x", NULL)   == MYDB_ERR, "login NULL pw");

    CHECK(engine_use_schema(NULL, "x") == MYDB_ERR, "use_schema NULL eng");
    CHECK(engine_use_schema(&eng, NULL) == MYDB_ERR, "use_schema NULL name");

    engine_close(&eng);
}


/* ================================================================== */
/*  Driver                                                            */
/* ================================================================== */

int main(void)
{
    printf("=== test_engine ===\n");

    test_bootstrap_creates_layout();
    test_bootstrap_rejects_double();
    test_bootstrap_rejects_empty_args();
    test_bootstrap_root_user_recorded();
    test_bootstrap_root_partition_registered();

    test_init_opens_metadata();
    test_init_uninitialized_root_fails();

    test_login_success();
    test_login_wrong_password();
    test_login_unknown_user();
    test_login_inactive_user();
    test_login_double_rejected();
    test_login_stamps_last_login();
    test_login_analyst_no_partition();

    test_use_schema_success();
    test_use_schema_unknown();
    test_use_schema_swap();
    test_use_schema_requires_login();

    test_find_relation_hit();
    test_find_relation_miss();
    test_find_relation_requires_active_schema();
    test_find_relation_null_args();

    test_null_args();

    printf("\n%d/%d tests passed\n", tests_passed, tests_run);
    cleanup();
    return (tests_passed == tests_run) ? 0 : 1;
}
