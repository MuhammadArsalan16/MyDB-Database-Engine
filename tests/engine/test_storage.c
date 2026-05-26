#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/stat.h>

#include "common.h"
#include "engine.h"
#include "storage.h"
#include "schema_file.h"
#include "partition.h"

#define TEST_ROOT      "/tmp/mydb_test_storage"
#define TEST_SCHEMA    "main"
#define TEST_USERNAME  "root"
#define TEST_PASSWORD  "p"

static int tests_run    = 0;
static int tests_passed = 0;

#define CHECK(cond, msg) do { \
    tests_run++; \
    if (cond) { tests_passed++; printf("  PASS: %s\n", msg); } \
    else       { printf("  FAIL: %s (line %d)\n", msg, __LINE__); } \
} while(0)


/* ------------------------------------------------------------------ */
/*  Session helpers                                                    */
/*                                                                    */
/*  Each test owns its EngineState. setup_session() fully wipes the   */
/*  test root, bootstraps a fresh engine, logs in as root, registers  */
/*  one schema named TEST_SCHEMA, and calls storage_init().           */
/* ------------------------------------------------------------------ */

static EngineState g_eng;

static void rm_recursive(const char *path)
{
    char cmd[512];
    snprintf(cmd, sizeof(cmd), "rm -rf '%s'", path);
    (void)!system(cmd);
}

static void cleanup(void) { rm_recursive(TEST_ROOT); }

/* Open an already-bootstrapped engine and rejoin: login + use the
 * test schema + storage_init. Used by reopen-persistence tests. */
static void reopen_session(void)
{
    engine_init(TEST_ROOT, &g_eng);
    engine_login(&g_eng, TEST_USERNAME, TEST_PASSWORD);
    engine_use_schema(&g_eng, TEST_SCHEMA);
    storage_init(&g_eng);
}

static void setup_session(void)
{
    storage_shutdown();
    /* engine_close is safe on a never-initialised state — fields zeroed
     * by the previous engine_init/close cycle, or by the static init. */
    engine_close(&g_eng);
    cleanup();

    engine_bootstrap(TEST_ROOT, TEST_USERNAME, TEST_PASSWORD);
    engine_init(TEST_ROOT, &g_eng);
    engine_login(&g_eng, TEST_USERNAME, TEST_PASSWORD);

    /* Register one schema and create its __schema.mydb. The CREATE
     * DATABASE plumbing is a higher layer; for storage tests we wire
     * it up directly the same way test_engine does. */
    char schema_dir[256], schema_path[256];
    snprintf(schema_dir,  sizeof(schema_dir),  "%s/%s",
             g_eng.current_partition_path, TEST_SCHEMA);
    snprintf(schema_path, sizeof(schema_path), "%s/__schema.mydb", schema_dir);
    mkdir(schema_dir, 0755);

    SchemaFile sf;
    schema_create(schema_path, g_eng.current_partition_id, TEST_SCHEMA, &sf);
    schema_close(&sf);
    cat_add_schema(&g_eng.active_catalog, TEST_SCHEMA);

    engine_use_schema(&g_eng, TEST_SCHEMA);
    storage_init(&g_eng);
}

static void teardown_session(void)
{
    storage_shutdown();
    engine_close(&g_eng);
}

/* ------------------------------------------------------------------ */
/*  Schema + row builders                                              */
/* ------------------------------------------------------------------ */

/* "users" schema: id INT PK AUTOINCR, name VARCHAR(32) NOT NULL */
static RelationDef make_users_schema(void)
{
    RelationDef r;
    memset(&r, 0, sizeof(r));
    strncpy(r.relation_name, "users", MAX_TABLE_NAME - 1);
    r.num_columns = 2;
    r.pk_col_idx  = 0;
    r.auto_incr_counter = 1;

    ColumnDef *id = &r.columns[0];
    strncpy(id->name, "id", MAX_COLUMN_NAME - 1);
    id->type = TYPE_INT; id->max_len = 4;
    id->is_not_null = 1; id->is_primary_key = 1; id->is_auto_increment = 1;

    ColumnDef *nm = &r.columns[1];
    strncpy(nm->name, "name", MAX_COLUMN_NAME - 1);
    nm->type = TYPE_VARCHAR; nm->max_len = 32; nm->is_not_null = 1;

    return r;
}

static Row make_user_row(int id, const char *name)
{
    Row r;
    memset(&r, 0, sizeof(r));
    r.num_cols = 2;

    r.cols[0].type      = TYPE_INT;
    r.cols[0].is_null   = (id == 0);
    r.cols[0].v.int_val = id;

    r.cols[1].type    = TYPE_VARCHAR;
    r.cols[1].is_null = 0;
    r.cols[1].v.varchar_val.len = (uint16_t)strlen(name);
    strncpy(r.cols[1].v.varchar_val.data, name, MAX_VARCHAR_LEN - 1);

    return r;
}

/* Convenience: create the users table and return a writable RelationDef
 * pointer (into the active schema's defs[] slot, mutated by storage). */
static RelationDef *create_users_table(void)
{
    RelationDef tmp = make_users_schema();
    storage_create_table(&tmp);
    return schema_find_relation(&g_eng.active_schema, "users");
}

/* ------------------------------------------------------------------ */

static void test_init_shutdown(void)
{
    printf("\n[test_init_shutdown]\n");
    setup_session();

    /* setup_session already called storage_init; calling again is a no-op. */
    CHECK(storage_init(&g_eng) == MYDB_OK, "storage_init returns OK");
    CHECK(storage_shutdown() == MYDB_OK, "storage_shutdown returns OK");
    CHECK(storage_shutdown() == MYDB_OK, "double shutdown is safe");

    engine_close(&g_eng);
}

static void test_create_schema_basic(void)
{
    printf("\n[test_create_schema_basic]\n");
    setup_session();

    int rc = storage_create_schema("alt");
    CHECK(rc == MYDB_OK, "storage_create_schema returns OK");
    CHECK(cat_find_schema(&g_eng.active_catalog, "alt") != NULL,
          "schema is registered in the catalog");

    /* Filesystem side-effects: dir + __schema.mydb both exist. */
    char dir[256], path[256];
    snprintf(dir,  sizeof(dir),  "%s/alt", g_eng.current_partition_path);
    snprintf(path, sizeof(path), "%s/__schema.mydb", dir);
    struct stat st;
    CHECK(stat(dir, &st) == 0 && S_ISDIR(st.st_mode),
          "schema directory created");
    CHECK(stat(path, &st) == 0 && S_ISREG(st.st_mode),
          "__schema.mydb created");

    teardown_session();
}

static void test_create_schema_duplicate(void)
{
    printf("\n[test_create_schema_duplicate]\n");
    setup_session();

    CHECK(storage_create_schema("alt") == MYDB_OK, "first create OK");
    CHECK(storage_create_schema("alt") == MYDB_ERR_DUPLICATE,
          "duplicate schema rejected");

    teardown_session();
}

static void test_create_schema_then_use(void)
{
    printf("\n[test_create_schema_then_use]\n");
    setup_session();

    CHECK(storage_create_schema("alt") == MYDB_OK, "create OK");
    CHECK(engine_use_schema(&g_eng, "alt") == MYDB_OK,
          "engine_use_schema after create OK");
    CHECK(g_eng.schema_active == 1, "schema_active after USE");
    CHECK(strcmp(g_eng.current_schema_name, "alt") == 0,
          "current_schema_name is 'alt'");

    teardown_session();
}

static void test_create_schema_bad_args(void)
{
    printf("\n[test_create_schema_bad_args]\n");
    setup_session();

    CHECK(storage_create_schema(NULL) == MYDB_ERR, "NULL name rejected");
    CHECK(storage_create_schema("")   == MYDB_ERR, "empty name rejected");

    teardown_session();
}

static void test_create_drop_table(void)
{
    printf("\n[test_create_drop_table]\n");
    setup_session();

    RelationDef s = make_users_schema();
    int rc = storage_create_table(&s);
    CHECK(rc == MYDB_OK, "create_table returns OK");
    CHECK(s.root_page_no != INVALID_PAGE, "root_page_no set by create_table");

    /* duplicate create */
    RelationDef s2 = make_users_schema();
    rc = storage_create_table(&s2);
    CHECK(rc == MYDB_ERR_DUPLICATE, "duplicate create returns ERR_DUPLICATE");

    rc = storage_drop_table(&s);
    CHECK(rc == MYDB_OK, "drop_table returns OK");

    /* after drop, create again should succeed */
    RelationDef s3 = make_users_schema();
    rc = storage_create_table(&s3);
    CHECK(rc == MYDB_OK, "create after drop succeeds");

    teardown_session();
}

static void test_insert_get_by_pk(void)
{
    printf("\n[test_insert_get_by_pk]\n");
    setup_session();
    RelationDef *rel = create_users_table();

    Row r = make_user_row(1, "Alice");
    int rc = storage_insert(rel, &r);
    CHECK(rc == MYDB_OK, "insert returns OK");

    Value pk; pk.type = TYPE_INT; pk.is_null = 0; pk.v.int_val = 1;
    Row *found = storage_get_by_pk(rel, &pk);
    CHECK(found != NULL,                                   "get_by_pk finds row");
    CHECK(found && found->cols[0].v.int_val == 1,          "PK value correct");
    CHECK(found && strcmp(found->cols[1].v.varchar_val.data, "Alice") == 0,
          "name correct");
    CHECK(found && found->cols[1].v.varchar_val.len == 5,  "name length correct");

    pk.v.int_val = 99;
    Row *missing = storage_get_by_pk(rel, &pk);
    CHECK(missing == NULL, "get_by_pk returns NULL for missing row");

    teardown_session();
}

static void test_auto_increment(void)
{
    printf("\n[test_auto_increment]\n");
    setup_session();
    RelationDef *rel = create_users_table();

    Row r1 = make_user_row(0, "Alice");
    Row r2 = make_user_row(0, "Bob");
    Row r3 = make_user_row(0, "Carol");

    storage_insert(rel, &r1);
    storage_insert(rel, &r2);
    storage_insert(rel, &r3);

    Value pk; pk.type = TYPE_INT; pk.is_null = 0;

    pk.v.int_val = 1;
    Row *a = storage_get_by_pk(rel, &pk);
    CHECK(a && strcmp(a->cols[1].v.varchar_val.data, "Alice") == 0,
          "auto_incr id=1 → Alice");

    pk.v.int_val = 2;
    Row *b = storage_get_by_pk(rel, &pk);
    CHECK(b && strcmp(b->cols[1].v.varchar_val.data, "Bob") == 0,
          "auto_incr id=2 → Bob");

    pk.v.int_val = 3;
    Row *c = storage_get_by_pk(rel, &pk);
    CHECK(c && strcmp(c->cols[1].v.varchar_val.data, "Carol") == 0,
          "auto_incr id=3 → Carol");

    teardown_session();
}

static void test_duplicate_pk(void)
{
    printf("\n[test_duplicate_pk]\n");
    setup_session();
    RelationDef *rel = create_users_table();

    Row r1 = make_user_row(5, "Alice");
    storage_insert(rel, &r1);

    Row r2 = make_user_row(5, "Bob");
    int rc = storage_insert(rel, &r2);
    CHECK(rc == MYDB_ERR_DUPLICATE, "duplicate PK returns ERR_DUPLICATE");

    Value pk; pk.type = TYPE_INT; pk.is_null = 0; pk.v.int_val = 5;
    Row *found = storage_get_by_pk(rel, &pk);
    CHECK(found && strcmp(found->cols[1].v.varchar_val.data, "Alice") == 0,
          "original row unchanged after duplicate insert");

    teardown_session();
}

static void test_not_null_violation(void)
{
    printf("\n[test_not_null_violation]\n");
    setup_session();
    RelationDef *rel = create_users_table();

    Row r;
    memset(&r, 0, sizeof(r));
    r.num_cols = 2;
    r.cols[0].type = TYPE_INT; r.cols[0].is_null = 0; r.cols[0].v.int_val = 1;
    r.cols[1].type = TYPE_VARCHAR; r.cols[1].is_null = 1;  /* NULL! */

    int rc = storage_insert(rel, &r);
    CHECK(rc == MYDB_ERR_NULL_VIOLATION, "null in NOT NULL column returns violation");

    teardown_session();
}

static void test_delete(void)
{
    printf("\n[test_delete]\n");
    setup_session();
    RelationDef *rel = create_users_table();

    Row r1 = make_user_row(1, "Alice");
    Row r2 = make_user_row(2, "Bob");
    storage_insert(rel, &r1);
    storage_insert(rel, &r2);

    Value pk; pk.type = TYPE_INT; pk.is_null = 0; pk.v.int_val = 1;
    Row *found = storage_get_by_pk(rel, &pk);
    CHECK(found != NULL, "row exists before delete");

    RID rid = found->rid;
    int rc = storage_delete(rel, rid);
    CHECK(rc == MYDB_OK, "storage_delete returns OK");

    Row *gone = storage_get_by_pk(rel, &pk);
    CHECK(gone == NULL, "deleted row not found by PK");

    pk.v.int_val = 2;
    Row *bob = storage_get_by_pk(rel, &pk);
    CHECK(bob && strcmp(bob->cols[1].v.varchar_val.data, "Bob") == 0,
          "other row unaffected by delete");

    teardown_session();
}

static void test_update(void)
{
    printf("\n[test_update]\n");
    setup_session();
    RelationDef *rel = create_users_table();

    Row r = make_user_row(1, "Alice");
    storage_insert(rel, &r);

    Value pk; pk.type = TYPE_INT; pk.is_null = 0; pk.v.int_val = 1;
    Row *found = storage_get_by_pk(rel, &pk);
    CHECK(found != NULL, "row found before update");
    RID rid = found->rid;

    Row updated = make_user_row(1, "Alicia");
    int rc = storage_update(rel, rid, &updated);
    CHECK(rc == MYDB_OK, "storage_update returns OK");

    Row *after = storage_get_by_pk(rel, &pk);
    CHECK(after != NULL, "row found after update");
    CHECK(after && strcmp(after->cols[1].v.varchar_val.data, "Alicia") == 0,
          "name updated to Alicia");

    teardown_session();
}

static void test_scan(void)
{
    printf("\n[test_scan]\n");
    setup_session();
    RelationDef *rel = create_users_table();

    Row r1 = make_user_row(1, "Alice");
    Row r2 = make_user_row(2, "Bob");
    Row r3 = make_user_row(3, "Carol");
    storage_insert(rel, &r1);
    storage_insert(rel, &r2);
    storage_insert(rel, &r3);

    Cursor *cur = storage_scan(rel);
    CHECK(cur != NULL, "storage_scan returns cursor");

    int count = 0;
    Row *row;
    while ((row = cursor_next(cur)) != NULL) {
        count++;
        CHECK(row->num_cols == 2, "scanned row has 2 columns");
    }
    CHECK(count == 3, "scan returned 3 rows");

    cursor_close(cur);
    teardown_session();
}

static void test_scan_order(void)
{
    printf("\n[test_scan_order]\n");
    setup_session();
    RelationDef *rel = create_users_table();

    /* insert in reverse — scan returns in PK order */
    Row r3 = make_user_row(3, "Carol");
    Row r1 = make_user_row(1, "Alice");
    Row r2 = make_user_row(2, "Bob");
    storage_insert(rel, &r3);
    storage_insert(rel, &r1);
    storage_insert(rel, &r2);

    Cursor *cur = storage_scan(rel);
    int ids[3]; int n = 0;
    Row *row;
    while ((row = cursor_next(cur)) != NULL && n < 3)
        ids[n++] = row->cols[0].v.int_val;
    cursor_close(cur);

    CHECK(n == 3,         "scan returned 3 rows");
    CHECK(ids[0] == 1,    "first row: id=1");
    CHECK(ids[1] == 2,    "second row: id=2");
    CHECK(ids[2] == 3,    "third row: id=3");

    teardown_session();
}

static void test_transaction_rollback(void)
{
    printf("\n[test_transaction_rollback]\n");
    setup_session();
    RelationDef *rel = create_users_table();

    Row seed = make_user_row(1, "Seed");
    storage_insert(rel, &seed);

    storage_begin();
    Row r = make_user_row(2, "Transient");
    storage_insert(rel, &r);

    Value pk; pk.type = TYPE_INT; pk.is_null = 0; pk.v.int_val = 2;
    Row *during = storage_get_by_pk(rel, &pk);
    CHECK(during != NULL, "row visible within transaction");

    storage_rollback();

    Row *after = storage_get_by_pk(rel, &pk);
    CHECK(after == NULL, "row gone after rollback");

    teardown_session();
}

static void test_transaction_commit_persists(void)
{
    printf("\n[test_transaction_commit_persists]\n");
    setup_session();
    RelationDef *rel = create_users_table();

    storage_begin();
    Row r = make_user_row(1, "Persistent");
    storage_insert(rel, &r);
    storage_commit();

    /* shut down storage + engine, then reopen and verify row survives */
    storage_shutdown();
    engine_close(&g_eng);

    reopen_session();
    RelationDef *rel2 = schema_find_relation(&g_eng.active_schema, "users");
    CHECK(rel2 != NULL, "relation reloaded after reopen");

    Value pk; pk.type = TYPE_INT; pk.is_null = 0; pk.v.int_val = 1;
    Row *found = storage_get_by_pk(rel2, &pk);
    CHECK(found != NULL, "committed row survives shutdown/reopen");
    if (found)
        CHECK(strcmp(found->cols[1].v.varchar_val.data, "Persistent") == 0,
              "name correct after reopen");

    teardown_session();
}

static void test_many_rows(void)
{
    printf("\n[test_many_rows]\n");
    setup_session();
    RelationDef *rel = create_users_table();

    /* insert 200 rows — forces B+ Tree splits */
    for (int i = 1; i <= 200; i++) {
        char name[32];
        snprintf(name, sizeof(name), "user%d", i);
        Row r = make_user_row(i, name);
        int rc = storage_insert(rel, &r);
        CHECK(rc == MYDB_OK, "insert row in bulk");
    }

    Value pk; pk.type = TYPE_INT; pk.is_null = 0;

    pk.v.int_val = 1;
    Row *r1 = storage_get_by_pk(rel, &pk);
    CHECK(r1 && strcmp(r1->cols[1].v.varchar_val.data, "user1") == 0,
          "get row 1 after splits");

    pk.v.int_val = 100;
    Row *r100 = storage_get_by_pk(rel, &pk);
    CHECK(r100 && strcmp(r100->cols[1].v.varchar_val.data, "user100") == 0,
          "get row 100 after splits");

    pk.v.int_val = 200;
    Row *r200 = storage_get_by_pk(rel, &pk);
    CHECK(r200 && strcmp(r200->cols[1].v.varchar_val.data, "user200") == 0,
          "get row 200 after splits");

    Cursor *cur = storage_scan(rel);
    int count = 0;
    while (cursor_next(cur) != NULL) count++;
    cursor_close(cur);
    CHECK(count == 200, "full scan returns 200 rows after splits");

    teardown_session();
}

/* ------------------------------------------------------------------ */
/*  Phase 9 step 2 — quota + RelationEntry.num_pages bump              */
/* ------------------------------------------------------------------ */

static void test_create_table_bumps_num_pages(void)
{
    printf("\n[test_create_table_bumps_num_pages]\n");
    setup_session();

    RelationDef s = make_users_schema();
    storage_create_table(&s);

    /* clustered root page = 1, no secondary indexes → num_pages=1. */
    RelationEntry *e = schema_find_relation_stat(&g_eng.active_schema, "users");
    CHECK(e != NULL, "stats entry exists");
    CHECK(e && e->num_pages == 1, "num_pages=1 after create (clustered root only)");

    /* The partition's used_bytes was bumped by partition_alloc_page. */
    CHECK(g_eng.active_catalog.header.used_bytes == PAGE_SIZE,
          "partition used_bytes = 1 page after create");

    teardown_session();
}

static void test_insert_bumps_num_pages_eventually(void)
{
    printf("\n[test_insert_bumps_num_pages_eventually]\n");
    setup_session();
    RelationDef *rel = create_users_table();

    /* Push enough payload through to force at least one B+ tree split.
     * 32-byte VARCHAR + 4-byte INT → ~60 B per record incl. header /
     * page-dir slot, so ~250 records fit per leaf. 1500 records is well
     * past one split. */
    uint32_t pages_at_start = 0;
    RelationEntry *e0 = schema_find_relation_stat(&g_eng.active_schema, "users");
    if (e0) pages_at_start = e0->num_pages;

    for (int i = 1; i <= 1500; i++) {
        char nm[32]; snprintf(nm, sizeof(nm), "user%010d", i);
        Row r = make_user_row(i, nm);
        storage_insert(rel, &r);
    }

    RelationEntry *e1 = schema_find_relation_stat(&g_eng.active_schema, "users");
    CHECK(e1 && e1->num_pages > pages_at_start,
          "bulk inserts grew num_pages past create-time count");

    /* used_bytes on the partition catalog tracks num_pages * PAGE_SIZE. */
    CHECK(g_eng.active_catalog.header.used_bytes ==
          (uint64_t)e1->num_pages * PAGE_SIZE,
          "partition used_bytes matches num_pages * PAGE_SIZE");

    teardown_session();
}

static void test_drop_table_releases_quota(void)
{
    printf("\n[test_drop_table_releases_quota]\n");
    setup_session();

    RelationDef s = make_users_schema();
    storage_create_table(&s);

    uint64_t after_create = g_eng.active_catalog.header.used_bytes;
    CHECK(after_create > 0, "used_bytes nonzero after create");

    storage_drop_table(&s);

    CHECK(g_eng.active_catalog.header.used_bytes == 0,
          "used_bytes back to 0 after drop");

    teardown_session();
}

static void test_create_table_quota_exhausted(void)
{
    printf("\n[test_create_table_quota_exhausted]\n");
    setup_session();

    /* Force the partition into a tight quota by manually editing the
     * catalog. Quota = 0 → first create must fail at the headroom
     * pre-check, leaving the schema empty. */
    g_eng.active_catalog.header.quota_bytes = 0;
    cat_save(&g_eng.active_catalog);

    RelationDef s = make_users_schema();
    int rc = storage_create_table(&s);
    CHECK(rc == MYDB_ERR_FULL, "create with zero quota → MYDB_ERR_FULL");
    CHECK(schema_find_relation(&g_eng.active_schema, "users") == NULL,
          "no relation registered when create rejected");

    teardown_session();
}

/* ================================================================== */
/*  FK constraint enforcement                                         */
/* ================================================================== */

/* departments: id INT PK (not auto-increment) */
static RelationDef make_departments_schema(void)
{
    RelationDef r;
    memset(&r, 0, sizeof(r));
    strncpy(r.relation_name, "departments", MAX_TABLE_NAME - 1);
    r.num_columns = 1;
    r.pk_col_idx  = 0;

    ColumnDef *id = &r.columns[0];
    strncpy(id->name, "id", MAX_COLUMN_NAME - 1);
    id->type = TYPE_INT; id->max_len = 4;
    id->is_not_null = 1; id->is_primary_key = 1;

    return r;
}

/* employees: id INT PK AUTOINCR, name VARCHAR(32) NOT NULL,
 *            dept_id INT FK → departments.id (nullable) */
static RelationDef make_employees_fk_schema(void)
{
    RelationDef r;
    memset(&r, 0, sizeof(r));
    strncpy(r.relation_name, "employees", MAX_TABLE_NAME - 1);
    r.num_columns       = 3;
    r.pk_col_idx        = 0;
    r.auto_incr_counter = 1;

    ColumnDef *id = &r.columns[0];
    strncpy(id->name, "id", MAX_COLUMN_NAME - 1);
    id->type = TYPE_INT; id->max_len = 4;
    id->is_not_null = 1; id->is_primary_key = 1; id->is_auto_increment = 1;

    ColumnDef *nm = &r.columns[1];
    strncpy(nm->name, "name", MAX_COLUMN_NAME - 1);
    nm->type = TYPE_VARCHAR; nm->max_len = 32; nm->is_not_null = 1;

    ColumnDef *dept = &r.columns[2];
    strncpy(dept->name, "dept_id", MAX_COLUMN_NAME - 1);
    dept->type = TYPE_INT; dept->max_len = 4;
    /* nullable — so NULL FK inserts are valid */

    r.num_foreign_keys = 1;
    strncpy(r.foreign_keys[0].constraint_name, "fk_dept",      MAX_COLUMN_NAME - 1);
    strncpy(r.foreign_keys[0].column_name,     "dept_id",      MAX_COLUMN_NAME - 1);
    strncpy(r.foreign_keys[0].ref_relation_name, "departments", MAX_TABLE_NAME  - 1);
    strncpy(r.foreign_keys[0].ref_column_name, "id",           MAX_COLUMN_NAME - 1);

    return r;
}

static Row make_dept_row(int id)
{
    Row r;
    memset(&r, 0, sizeof(r));
    r.num_cols          = 1;
    r.cols[0].type      = TYPE_INT;
    r.cols[0].is_null   = 0;
    r.cols[0].v.int_val = id;
    return r;
}

static Row make_emp_row(const char *name, int dept_id, int dept_null)
{
    Row r;
    memset(&r, 0, sizeof(r));
    r.num_cols = 3;

    /* id — let AUTOINCR fill it in */
    r.cols[0].type      = TYPE_INT;
    r.cols[0].is_null   = 1;

    r.cols[1].type    = TYPE_VARCHAR;
    r.cols[1].is_null = 0;
    r.cols[1].v.varchar_val.len = (uint16_t)strlen(name);
    strncpy(r.cols[1].v.varchar_val.data, name, MAX_VARCHAR_LEN - 1);

    r.cols[2].type      = TYPE_INT;
    r.cols[2].is_null   = dept_null;
    r.cols[2].v.int_val = dept_id;
    return r;
}

static Value pk_int(int v)
{
    Value pk;
    pk.type      = TYPE_INT;
    pk.is_null   = 0;
    pk.v.int_val = v;
    return pk;
}

/* Create both FK-linked tables and return their live RelationDef pointers. */
static void setup_fk_tables(RelationDef **dept_out, RelationDef **emp_out)
{
    RelationDef d = make_departments_schema();
    storage_create_table(&d);
    *dept_out = schema_find_relation(&g_eng.active_schema, "departments");

    RelationDef e = make_employees_fk_schema();
    storage_create_table(&e);
    *emp_out = schema_find_relation(&g_eng.active_schema, "employees");
}

static void test_fk_insert_valid_ref(void)
{
    printf("\n[test_fk_insert_valid_ref]\n");
    setup_session();
    RelationDef *dept, *emp;
    setup_fk_tables(&dept, &emp);

    Row d = make_dept_row(1);
    CHECK(storage_insert(dept, &d) == MYDB_OK, "insert dept id=1 OK");

    Row e = make_emp_row("Alice", 1, 0);
    CHECK(storage_insert(emp, &e) == MYDB_OK,
          "insert employee with valid dept_id=1 → OK");

    teardown_session();
}

static void test_fk_insert_invalid_ref(void)
{
    printf("\n[test_fk_insert_invalid_ref]\n");
    setup_session();
    RelationDef *dept, *emp;
    setup_fk_tables(&dept, &emp);

    /* No department inserted — dept_id=99 references nothing. */
    Row e = make_emp_row("Bob", 99, 0);
    CHECK(storage_insert(emp, &e) == MYDB_ERR_FK_VIOLATION,
          "insert with non-existent FK value → MYDB_ERR_FK_VIOLATION");

    teardown_session();
}

static void test_fk_insert_null_fk_allowed(void)
{
    printf("\n[test_fk_insert_null_fk_allowed]\n");
    setup_session();
    RelationDef *dept, *emp;
    setup_fk_tables(&dept, &emp);

    /* NULL dept_id — no FK check required. */
    Row e = make_emp_row("Charlie", 0, 1 /* dept_null=1 */);
    CHECK(storage_insert(emp, &e) == MYDB_OK,
          "insert with NULL FK column → OK (NULL skips FK check)");

    teardown_session();
}

static void test_fk_delete_referenced_row(void)
{
    printf("\n[test_fk_delete_referenced_row]\n");
    setup_session();
    RelationDef *dept, *emp;
    setup_fk_tables(&dept, &emp);

    Row d = make_dept_row(1);
    storage_insert(dept, &d);

    Row e = make_emp_row("Alice", 1, 0);
    storage_insert(emp, &e);

    /* Try to delete the department that Alice references. */
    Value pk = pk_int(1);
    Row *found = storage_get_by_pk(dept, &pk);
    CHECK(found != NULL, "get_by_pk finds dept id=1");
    if (found) {
        RID rid = found->rid;
        CHECK(storage_delete(dept, rid) == MYDB_ERR_FK_VIOLATION,
              "delete referenced dept → MYDB_ERR_FK_VIOLATION");
    }

    teardown_session();
}

static void test_fk_delete_after_removing_ref(void)
{
    printf("\n[test_fk_delete_after_removing_ref]\n");
    setup_session();
    RelationDef *dept, *emp;
    setup_fk_tables(&dept, &emp);

    Row d = make_dept_row(2);
    storage_insert(dept, &d);

    Row e = make_emp_row("Dave", 2, 0);
    storage_insert(emp, &e);

    /* First delete the referencing employee. */
    Value emp_pk = pk_int(1); /* auto-increment started at 1 */
    Row *emp_row = storage_get_by_pk(emp, &emp_pk);
    CHECK(emp_row != NULL, "get_by_pk finds employee");
    if (emp_row) {
        CHECK(storage_delete(emp, emp_row->rid) == MYDB_OK,
              "delete employee OK");
    }

    /* Now the department has no references — delete must succeed. */
    Value dept_pk = pk_int(2);
    Row *dept_row = storage_get_by_pk(dept, &dept_pk);
    CHECK(dept_row != NULL, "get_by_pk finds dept id=2");
    if (dept_row) {
        CHECK(storage_delete(dept, dept_row->rid) == MYDB_OK,
              "delete unreferenced dept → OK");
    }

    teardown_session();
}

static void test_fk_update_invalid_ref(void)
{
    printf("\n[test_fk_update_invalid_ref]\n");
    setup_session();
    RelationDef *dept, *emp;
    setup_fk_tables(&dept, &emp);

    Row d = make_dept_row(1);
    storage_insert(dept, &d);

    Row e = make_emp_row("Eve", 1, 0);
    storage_insert(emp, &e);

    /* Update employee to point at dept_id=99 which does not exist. */
    Value emp_pk = pk_int(1);
    Row *emp_row = storage_get_by_pk(emp, &emp_pk);
    CHECK(emp_row != NULL, "get_by_pk finds employee");
    if (emp_row) {
        RID rid      = emp_row->rid;
        Row new_row  = make_emp_row("Eve", 99, 0);
        new_row.cols[0].is_null   = 0;
        new_row.cols[0].v.int_val = 1; /* keep same PK */
        CHECK(storage_update(emp, rid, &new_row) == MYDB_ERR_FK_VIOLATION,
              "update FK to non-existent target → MYDB_ERR_FK_VIOLATION");
    }

    teardown_session();
}


/* ------------------------------------------------------------------ */
/*  storage_range_by_index tests                                        */
/*                                                                      */
/*  Table: products(id INT PK AUTOINCR, code INT NOT NULL UNIQUE,       */
/*                  name VARCHAR(32) NOT NULL)                           */
/*  Rows inserted with code = 10, 20, 30, 40, 50.                       */
/* ------------------------------------------------------------------ */

static RelationDef *create_products_table(void)
{
    RelationDef r;
    memset(&r, 0, sizeof(r));
    strncpy(r.relation_name, "products", MAX_TABLE_NAME - 1);
    r.num_columns       = 3;
    r.pk_col_idx        = 0;
    r.auto_incr_counter = 1;

    ColumnDef *id = &r.columns[0];
    strncpy(id->name, "id", MAX_COLUMN_NAME - 1);
    id->type = TYPE_INT; id->max_len = 4;
    id->is_not_null = 1; id->is_primary_key = 1; id->is_auto_increment = 1;

    ColumnDef *code = &r.columns[1];
    strncpy(code->name, "code", MAX_COLUMN_NAME - 1);
    code->type = TYPE_INT; code->max_len = 4;
    code->is_not_null = 1; code->is_unique = 1;

    ColumnDef *nm = &r.columns[2];
    strncpy(nm->name, "name", MAX_COLUMN_NAME - 1);
    nm->type = TYPE_VARCHAR; nm->max_len = 32; nm->is_not_null = 1;

    r.num_secondary_indexes  = 1;
    r.secondary_col_idx[0]   = 1;   /* secondary index on code */

    storage_create_table(&r);
    return schema_find_relation(&g_eng.active_schema, "products");
}

static Row make_product_row(int code, const char *name)
{
    Row r;
    memset(&r, 0, sizeof(r));
    r.num_cols = 3;

    r.cols[0].type    = TYPE_INT; r.cols[0].is_null = 1;  /* AUTO_INCREMENT */

    r.cols[1].type = TYPE_INT; r.cols[1].is_null = 0;
    r.cols[1].v.int_val = code;

    r.cols[2].type = TYPE_VARCHAR; r.cols[2].is_null = 0;
    r.cols[2].v.varchar_val.len = (uint16_t)strlen(name);
    strncpy(r.cols[2].v.varchar_val.data, name, MAX_VARCHAR_LEN - 1);

    return r;
}

static Value int_val(int v)
{
    Value val;
    memset(&val, 0, sizeof(val));
    val.type = TYPE_INT; val.is_null = 0; val.v.int_val = v;
    return val;
}

/* Count rows returned by a cursor, applying an optional INT upper bound
 * on col_idx (pass hi_val = INT_MIN to disable the upper bound check). */
static int count_cursor_rows(Cursor *cur, int col_idx, int hi_val, int hi_incl)
{
    int n = 0;
    Row *row;
    while ((row = cursor_next(cur)) != NULL) {
        if (hi_val != (int)0x80000000) {
            int v = row->cols[col_idx].v.int_val;
            if (v > hi_val)              break;
            if (v == hi_val && !hi_incl) break;
        }
        n++;
    }
    cursor_close(cur);
    return n;
}

static void test_scan_by_index(void)
{
    printf("\n[test_scan_by_index]\n");
    setup_session();

    RelationDef *rel = create_products_table();

    /* Insert 5 rows with code = 10, 20, 30, 40, 50. */
    Row p1 = make_product_row(10, "alpha");
    Row p2 = make_product_row(20, "beta");
    Row p3 = make_product_row(30, "gamma");
    Row p4 = make_product_row(40, "delta");
    Row p5 = make_product_row(50, "epsilon");
    storage_insert(rel, &p1);
    storage_insert(rel, &p2);
    storage_insert(rel, &p3);
    storage_insert(rel, &p4);
    storage_insert(rel, &p5);

    Cursor *cur;
    Row    *row;

    /* Full scan: lo=NULL → all 5 rows in key order */
    cur = storage_scan_by_index(rel, 1, NULL);
    CHECK(cur != NULL, "full index scan cursor opens");
    CHECK(count_cursor_rows(cur, 1, (int)0x80000000, 1) == 5,
          "full index scan yields 5 rows");

    /* Range [20, 40]: open at 20, stop when code > 40 */
    Value lo20 = int_val(20);
    cur = storage_scan_by_index(rel, 1, &lo20);
    CHECK(cur != NULL, "[20,inf) cursor opens");
    CHECK(count_cursor_rows(cur, 1, 40, 1) == 3,
          "[20,40] yields 3 rows (20,30,40)");

    /* Range [30, ∞): open at 30, no upper bound */
    Value lo30 = int_val(30);
    cur = storage_scan_by_index(rel, 1, &lo30);
    CHECK(cur != NULL, "[30,inf) cursor opens");
    CHECK(count_cursor_rows(cur, 1, (int)0x80000000, 1) == 3,
          "[30,inf) yields 3 rows (30,40,50)");

    /* Upper bound only (−∞, 30]: open at start, stop after code 30 */
    cur = storage_scan_by_index(rel, 1, NULL);
    CHECK(cur != NULL, "(-inf,30] cursor opens");
    CHECK(count_cursor_rows(cur, 1, 30, 1) == 3,
          "(-inf,30] yields 3 rows (10,20,30)");

    /* Exclusive upper bound (−∞, 30): stop before code 30 */
    cur = storage_scan_by_index(rel, 1, NULL);
    CHECK(cur != NULL, "(-inf,30) cursor opens");
    CHECK(count_cursor_rows(cur, 1, 30, 0) == 2,
          "(-inf,30) yields 2 rows (10,20)");

    /* Start beyond all values [60, ∞): cursor opens but yields 0 rows */
    Value lo60 = int_val(60);
    cur = storage_scan_by_index(rel, 1, &lo60);
    CHECK(cur != NULL, "[60,inf) cursor opens (done state)");
    CHECK(count_cursor_rows(cur, 1, (int)0x80000000, 1) == 0,
          "[60,inf) yields 0 rows");

    /* Verify full row data is returned correctly */
    cur = storage_scan_by_index(rel, 1, &lo30);
    CHECK(cur != NULL, "cursor for data verify opens");
    row = cursor_next(cur);
    CHECK(row != NULL,                              "first row not NULL");
    CHECK(row && row->cols[1].v.int_val == 30,      "first row code == 30");
    CHECK(row && strcmp(row->cols[2].v.varchar_val.data, "gamma") == 0,
          "first row name == gamma");
    cursor_close(cur);

    /* Non-indexed column → NULL */
    cur = storage_scan_by_index(rel, 2, NULL);
    CHECK(cur == NULL, "non-indexed column returns NULL cursor");

    /* NULL rel → NULL */
    cur = storage_scan_by_index(NULL, 1, NULL);
    CHECK(cur == NULL, "NULL rel returns NULL cursor");

    teardown_session();
}

int main(void)
{
    printf("=== test_storage ===\n");

    test_init_shutdown();
    test_create_schema_basic();
    test_create_schema_duplicate();
    test_create_schema_then_use();
    test_create_schema_bad_args();
    test_create_drop_table();
    test_insert_get_by_pk();
    test_auto_increment();
    test_duplicate_pk();
    test_not_null_violation();
    test_delete();
    test_update();
    test_scan();
    test_scan_order();
    test_transaction_rollback();
    test_transaction_commit_persists();
    test_many_rows();

    test_create_table_bumps_num_pages();
    test_insert_bumps_num_pages_eventually();
    test_drop_table_releases_quota();
    test_create_table_quota_exhausted();

    test_fk_insert_valid_ref();
    test_fk_insert_invalid_ref();
    test_fk_insert_null_fk_allowed();
    test_fk_delete_referenced_row();
    test_fk_delete_after_removing_ref();
    test_fk_update_invalid_ref();

    test_scan_by_index();

    storage_shutdown();
    engine_close(&g_eng);
    cleanup();

    printf("\n%d / %d tests passed\n", tests_passed, tests_run);
    return (tests_passed == tests_run) ? 0 : 1;
}
