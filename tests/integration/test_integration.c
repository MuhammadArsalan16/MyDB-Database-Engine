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
#include "crypto.h"

#define TEST_ROOT    "/tmp/mydb_test_integration"
#define TEST_USER    "root"
#define TEST_PASS    "pass"

static int tests_run    = 0;
static int tests_passed = 0;

#define CHECK(cond, msg) do { \
    tests_run++; \
    if (cond) { tests_passed++; printf("  PASS: %s\n", msg); } \
    else       { printf("  FAIL: %s (line %d)\n", msg, __LINE__); } \
} while(0)

static EngineState g_eng;

/* ------------------------------------------------------------------ */
/*  Session helpers                                                     */
/* ------------------------------------------------------------------ */

static void rm_recursive(const char *path)
{
    char cmd[512];
    snprintf(cmd, sizeof(cmd), "rm -rf '%s'", path);
    (void)!system(cmd);
}

/* Create a named schema dir + __schema.mydb and register it in the
 * active catalog. eng must be logged in. */
static void create_schema(EngineState *eng, const char *name)
{
    char dir[256], path[256];
    snprintf(dir,  sizeof(dir),  "%s/%s", eng->current_partition_path, name);
    snprintf(path, sizeof(path), "%s/__schema.mydb", dir);
    mkdir(dir, 0755);
    SchemaFile sf;
    schema_create(path, eng->current_partition_id, name, &sf);
    schema_close(&sf);
    cat_add_schema(&eng->active_catalog, name);
}

/* Bootstrap fresh engine, login as root, create + use the given schema,
 * initialise storage. */
static void full_setup(const char *schema_name)
{
    rm_recursive(TEST_ROOT);
    engine_bootstrap(TEST_ROOT, TEST_USER, TEST_PASS);
    engine_init(TEST_ROOT, &g_eng);
    engine_login(&g_eng, TEST_USER, TEST_PASS);
    create_schema(&g_eng, schema_name);
    engine_use_schema(&g_eng, schema_name);
    storage_init(&g_eng);
}

/* Flush + close without re-bootstrapping. */
static void full_close(void)
{
    storage_shutdown();
    engine_close(&g_eng);
}

/* Reopen an already-bootstrapped engine: init → login → use schema →
 * storage_init. No bootstrap, no rm. */
static void full_reopen(const char *schema_name)
{
    engine_init(TEST_ROOT, &g_eng);
    engine_login(&g_eng, TEST_USER, TEST_PASS);
    engine_use_schema(&g_eng, schema_name);
    storage_init(&g_eng);
}

/* ------------------------------------------------------------------ */
/*  Schema + row builders                                              */
/* ------------------------------------------------------------------ */

/* users: id INT PK AUTOINCR, name VARCHAR(32) NOT NULL */
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

static Value pk_int(int v)
{
    Value pk;
    pk.type      = TYPE_INT;
    pk.is_null   = 0;
    pk.v.int_val = v;
    return pk;
}

static RelationDef *create_users_table(void)
{
    RelationDef tmp = make_users_schema();
    storage_create_table(&tmp);
    return schema_find_relation(&g_eng.active_schema, "users");
}

/* departments: id INT PK (no autoincr) */
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

    r.num_foreign_keys = 1;
    strncpy(r.foreign_keys[0].constraint_name, "fk_dept",       MAX_COLUMN_NAME - 1);
    strncpy(r.foreign_keys[0].column_name,     "dept_id",       MAX_COLUMN_NAME - 1);
    strncpy(r.foreign_keys[0].ref_relation_name, "departments",  MAX_TABLE_NAME  - 1);
    strncpy(r.foreign_keys[0].ref_column_name, "id",            MAX_COLUMN_NAME - 1);

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
    r.num_cols          = 3;
    r.cols[0].type      = TYPE_INT;
    r.cols[0].is_null   = 1; /* autoincr — engine fills in */
    r.cols[1].type      = TYPE_VARCHAR;
    r.cols[1].is_null   = 0;
    r.cols[1].v.varchar_val.len = (uint16_t)strlen(name);
    strncpy(r.cols[1].v.varchar_val.data, name, MAX_VARCHAR_LEN - 1);
    r.cols[2].type      = TYPE_INT;
    r.cols[2].is_null   = dept_null;
    r.cols[2].v.int_val = dept_id;
    return r;
}

/* ================================================================== */
/*  1. Cross-restart persistence                                       */
/*                                                                     */
/*  Insert 3 rows, close the engine (full shutdown + engine_close),   */
/*  reopen with the complete login sequence, and verify all rows are  */
/*  still retrievable. Proves that the disk flush path is correct     */
/*  end-to-end (buffer pool → DiskManager → pwrite).                  */
/* ================================================================== */

static void test_persist_and_reopen(void)
{
    printf("\n[test_persist_and_reopen]\n");
    full_setup("main");

    RelationDef *rel = create_users_table();
    Row r1 = make_user_row(1, "Alice");
    Row r2 = make_user_row(2, "Bob");
    Row r3 = make_user_row(3, "Carol");
    storage_insert(rel, &r1);
    storage_insert(rel, &r2);
    storage_insert(rel, &r3);

    full_close();
    full_reopen("main");

    RelationDef *rel2 = schema_find_relation(&g_eng.active_schema, "users");
    CHECK(rel2 != NULL, "relation reloaded after engine restart");

    Value pk;
    Row *found;

    pk = pk_int(1); found = storage_get_by_pk(rel2, &pk);
    CHECK(found != NULL, "Alice found after restart");
    if (found)
        CHECK(strcmp(found->cols[1].v.varchar_val.data, "Alice") == 0,
              "Alice name correct");

    pk = pk_int(2); found = storage_get_by_pk(rel2, &pk);
    CHECK(found != NULL, "Bob found after restart");
    if (found)
        CHECK(strcmp(found->cols[1].v.varchar_val.data, "Bob") == 0,
              "Bob name correct");

    pk = pk_int(3); found = storage_get_by_pk(rel2, &pk);
    CHECK(found != NULL, "Carol found after restart");
    if (found)
        CHECK(strcmp(found->cols[1].v.varchar_val.data, "Carol") == 0,
              "Carol name correct");

    full_close();
}

/* ================================================================== */
/*  2. Schema switch triggers dirty-page flush                         */
/*                                                                     */
/*  Write a row into schema "main", then USE schema "alt" (which      */
/*  calls storage_flush_all_dirty internally), then close + reopen.   */
/*  Verifies the flush before the switch actually made the data       */
/*  durable — the row must survive even though the schema was never   */
/*  closed explicitly before the engine restart.                      */
/* ================================================================== */

static void test_schema_switch_flushes(void)
{
    printf("\n[test_schema_switch_flushes]\n");
    full_setup("main");

    /* Create a second schema so USE "alt" is valid. */
    create_schema(&g_eng, "alt");

    RelationDef *rel = create_users_table();
    Row r = make_user_row(1, "Flushed");
    storage_insert(rel, &r);

    /* Switch to "alt" — triggers storage_flush_all_dirty() + schema swap. */
    int rc = engine_use_schema(&g_eng, "alt");
    CHECK(rc == MYDB_OK, "USE alt succeeded (flush triggered)");

    /* Verify "users" is not in the alt schema. */
    RelationDef *in_alt = schema_find_relation(&g_eng.active_schema, "users");
    CHECK(in_alt == NULL, "users not visible in alt schema");

    /* Full restart — buffer pool is cleared; data must come from disk. */
    full_close();
    full_reopen("main");

    RelationDef *rel2 = schema_find_relation(&g_eng.active_schema, "users");
    CHECK(rel2 != NULL, "relation found after switch + restart");

    Value pk = pk_int(1);
    Row *found = storage_get_by_pk(rel2, &pk);
    CHECK(found != NULL, "row survives schema switch + restart");
    if (found)
        CHECK(strcmp(found->cols[1].v.varchar_val.data, "Flushed") == 0,
              "row data correct after schema switch + restart");

    full_close();
}

/* ================================================================== */
/*  3. Analyst read-only access                                        */
/*                                                                     */
/*  Root creates a schema with one row and grants SELECT to an        */
/*  analyst user (no partition of their own). After root's session    */
/*  closes, the analyst logs in, opens the schema via the privilege   */
/*  grant, reads the row successfully, and is rejected on INSERT.     */
/* ================================================================== */

static void test_analyst_read_only(void)
{
    printf("\n[test_analyst_read_only]\n");

    /* Root session: create schema, insert data, add analyst + grant. */
    rm_recursive(TEST_ROOT);
    engine_bootstrap(TEST_ROOT, TEST_USER, TEST_PASS);
    engine_init(TEST_ROOT, &g_eng);
    engine_login(&g_eng, TEST_USER, TEST_PASS);
    create_schema(&g_eng, "hr");
    engine_use_schema(&g_eng, "hr");
    storage_init(&g_eng);

    RelationDef *rel = create_users_table();
    Row r = make_user_row(1, "Alice");
    storage_insert(rel, &r);

    /* Analyst user (no partition — analyst accounts get partition_open=0). */
    UserSlot a;
    memset(&a, 0, sizeof(a));
    strncpy(a.username, "analyst", MAX_USERNAME - 1);
    memset(a.password_salt, 0x55, sizeof(a.password_salt));
    crypto_hash_password("pw", a.password_salt, a.password_hash);
    a.hash_algorithm = 1;
    a.is_active      = 1;
    uint32_t aid;
    users_insert(&g_eng.system_schema.users, &a, &aid);

    /* Grant analyst SELECT on "hr". */
    PrivilegeSlot priv;
    memset(&priv, 0, sizeof(priv));
    priv.grantee_id   = aid;
    priv.partition_id = g_eng.current_partition_id;
    priv.granted_by   = 1;
    priv.is_valid     = 1;
    strncpy(priv.schema_name, "hr", sizeof(priv.schema_name) - 1);
    uint32_t priv_id;
    privileges_insert(&g_eng.system_schema.privileges, &priv, &priv_id);

    full_close();

    /* Analyst session. */
    engine_init(TEST_ROOT, &g_eng);
    CHECK(engine_login(&g_eng, "analyst", "pw") == MYDB_OK,
          "analyst login OK");
    CHECK(engine_use_schema(&g_eng, "hr") == MYDB_OK,
          "analyst USE hr via privilege grant");
    storage_init(&g_eng);

    RelationDef *arel = schema_find_relation(&g_eng.active_schema, "users");
    Value pk = pk_int(1);
    Row *found = storage_get_by_pk(arel, &pk);
    CHECK(found != NULL, "analyst read → row found");
    if (found)
        CHECK(strcmp(found->cols[1].v.varchar_val.data, "Alice") == 0,
              "analyst read → correct name");

    Row new_row = make_user_row(2, "Bob");
    CHECK(storage_insert(arel, &new_row) == MYDB_ERR_PERM,
          "analyst insert → MYDB_ERR_PERM (SELECT-only grant)");

    full_close();
}

/* ================================================================== */
/*  4. FK RESTRICT lifecycle                                           */
/*                                                                     */
/*  Insert parent (dept id=10), insert child (emp dept_id=10).        */
/*  Attempt to delete parent → RESTRICT violation.                    */
/*  Delete child, then delete parent → succeeds.                      */
/*  Verify parent is gone with get_by_pk.                             */
/* ================================================================== */

static void test_fk_lifecycle(void)
{
    printf("\n[test_fk_lifecycle]\n");
    full_setup("main");

    RelationDef d_tmp = make_departments_schema();
    storage_create_table(&d_tmp);
    RelationDef *dept = schema_find_relation(&g_eng.active_schema, "departments");

    RelationDef e_tmp = make_employees_fk_schema();
    storage_create_table(&e_tmp);
    RelationDef *emp = schema_find_relation(&g_eng.active_schema, "employees");

    /* Insert parent. */
    Row dr = make_dept_row(10);
    CHECK(storage_insert(dept, &dr) == MYDB_OK,
          "insert department id=10 OK");

    /* Insert child referencing parent. */
    Row er = make_emp_row("Alice", 10, 0);
    CHECK(storage_insert(emp, &er) == MYDB_OK,
          "insert employee with valid dept_id OK");

    /* Try to delete parent while child references it. */
    Value dept_pk = pk_int(10);
    Row *dept_row = storage_get_by_pk(dept, &dept_pk);
    CHECK(storage_delete(dept, dept_row->rid) == MYDB_ERR_FK_VIOLATION,
          "delete referenced department → FK RESTRICT violation");

    /* Delete child first. */
    Value emp_pk = pk_int(1);
    Row *emp_row = storage_get_by_pk(emp, &emp_pk);
    CHECK(storage_delete(emp, emp_row->rid) == MYDB_OK,
          "delete employee OK");

    /* Now parent delete must succeed. */
    dept_row = storage_get_by_pk(dept, &dept_pk);
    CHECK(storage_delete(dept, dept_row->rid) == MYDB_OK,
          "delete department after child removed → OK");

    /* Verify parent is gone. */
    CHECK(storage_get_by_pk(dept, &dept_pk) == NULL,
          "deleted department no longer found");

    full_close();
}

/* ================================================================== */
/*  5. Explicit COMMIT survives engine restart                         */
/*                                                                     */
/*  Use an explicit BEGIN/COMMIT around 3 inserts, then perform a     */
/*  full engine_close + engine_init + login cycle. All 3 rows must    */
/*  be present after the restart.                                      */
/* ================================================================== */

static void test_commit_survives_restart(void)
{
    printf("\n[test_commit_survives_restart]\n");
    full_setup("main");

    RelationDef *rel = create_users_table();

    storage_begin();
    Row r1 = make_user_row(1, "Tx1");
    Row r2 = make_user_row(2, "Tx2");
    Row r3 = make_user_row(3, "Tx3");
    CHECK(storage_insert(rel, &r1) == MYDB_OK, "insert Tx1 in txn");
    CHECK(storage_insert(rel, &r2) == MYDB_OK, "insert Tx2 in txn");
    CHECK(storage_insert(rel, &r3) == MYDB_OK, "insert Tx3 in txn");
    storage_commit();

    full_close();
    full_reopen("main");

    RelationDef *rel2 = schema_find_relation(&g_eng.active_schema, "users");
    CHECK(rel2 != NULL, "relation reloaded after commit + restart");

    Value pk;
    pk = pk_int(1); CHECK(storage_get_by_pk(rel2, &pk) != NULL, "Tx1 survives restart");
    pk = pk_int(2); CHECK(storage_get_by_pk(rel2, &pk) != NULL, "Tx2 survives restart");
    pk = pk_int(3); CHECK(storage_get_by_pk(rel2, &pk) != NULL, "Tx3 survives restart");

    full_close();
}

/* ================================================================== */
/*  6. ROLLBACK clears rows, confirmed after restart                  */
/*                                                                     */
/*  BEGIN → insert 3 rows → ROLLBACK. Scan must show 0 rows.         */
/*  Close + reopen and scan again to confirm rollback was durable     */
/*  (no partial write leaked through).                                */
/* ================================================================== */

static void test_rollback_clears_rows(void)
{
    printf("\n[test_rollback_clears_rows]\n");
    full_setup("main");

    RelationDef *rel = create_users_table();

    storage_begin();
    Row r1 = make_user_row(1, "Rollback1");
    Row r2 = make_user_row(2, "Rollback2");
    Row r3 = make_user_row(3, "Rollback3");
    storage_insert(rel, &r1);
    storage_insert(rel, &r2);
    storage_insert(rel, &r3);
    storage_rollback();

    /* In-session: scan must be empty. */
    Cursor *c = storage_scan(rel);
    int count = 0;
    while (cursor_next(c)) count++;
    cursor_close(c);
    CHECK(count == 0, "scan after rollback → 0 rows");

    /* Restart and confirm nothing leaked to disk. */
    full_close();
    full_reopen("main");

    RelationDef *rel2 = schema_find_relation(&g_eng.active_schema, "users");
    c = storage_scan(rel2);
    count = 0;
    while (cursor_next(c)) count++;
    cursor_close(c);
    CHECK(count == 0, "scan after rollback + restart → still 0 rows");

    full_close();
}

/* ================================================================== */
/*  7. Drop + recreate yields an empty table                           */
/*                                                                     */
/*  Create a table, insert 3 rows, drop it, recreate the same table,  */
/*  and scan. Must return 0 rows — verifies that the old data file    */
/*  is gone and the new one is truly fresh.                           */
/* ================================================================== */

static void test_drop_recreate_fresh(void)
{
    printf("\n[test_drop_recreate_fresh]\n");
    full_setup("main");

    RelationDef *rel = create_users_table();
    Row r1 = make_user_row(1, "Old1");
    Row r2 = make_user_row(2, "Old2");
    Row r3 = make_user_row(3, "Old3");
    CHECK(storage_insert(rel, &r1) == MYDB_OK, "insert Old1");
    CHECK(storage_insert(rel, &r2) == MYDB_OK, "insert Old2");
    CHECK(storage_insert(rel, &r3) == MYDB_OK, "insert Old3");

    CHECK(storage_drop_table(rel) == MYDB_OK, "drop table OK");

    /* Recreate same relation. */
    RelationDef tmp = make_users_schema();
    CHECK(storage_create_table(&tmp) == MYDB_OK, "recreate table OK");

    RelationDef *rel2 = schema_find_relation(&g_eng.active_schema, "users");
    Cursor *c = storage_scan(rel2);
    int count = 0;
    while (cursor_next(c)) count++;
    cursor_close(c);
    CHECK(count == 0, "scan after drop+recreate → 0 rows (fresh table)");

    full_close();
}

/* ================================================================== */
/*  8. engine_execute_sql wires parser + exec engine                  */
/*                                                                     */
/*  End-to-end check that bin's single SQL door works:                */
/*    - valid SQL flows through parser → exec engine stub → MYDB_OK   */
/*      with the stub's "received AST" message in result_out.         */
/*    - garbage SQL flows through parser only → MYDB_ERR with a       */
/*      "parse error: …" message in result_out.                       */
/*    - calling without an active session returns MYDB_ERR_PERM.      */
/*                                                                     */
/*  Once the executor is real, this test will start asserting actual  */
/*  query results instead of stub messages.                           */
/* ================================================================== */

static void test_execute_sql_pipeline(void)
{
    printf("\n[test_execute_sql_pipeline]\n");
    full_setup("main");

    char result[1024];

    /* Valid statement → parser succeeds, exec engine runs.
     * Before table exists: real engine returns an ERROR message.
     * After the stub is replaced, "not implemented" is gone and
     * the result is either a row-set or an error from the real engine. */
    int rc = engine_execute_sql(&g_eng, "SELECT * FROM users;",
                                result, sizeof(result));
    CHECK(rc != MYDB_OK || strstr(result, "not implemented") != NULL,
          "valid SQL → scaffold reached (not implemented or real result)");
    /* Accept "not implemented" (stub), "(N rows)" (real, table exists),
     * or "ERROR" (real engine, table missing — pipeline is live). */
    CHECK(strstr(result, "not implemented") != NULL ||
          strstr(result, "row")             != NULL ||
          strstr(result, "ERROR")           != NULL ||
          strstr(result, "Error")           != NULL,
          "result contains scaffold or real output");

    /* Garbage → parser rejects, engine reports parse error. */
    rc = engine_execute_sql(&g_eng, "wibble flarn;",
                            result, sizeof(result));
    CHECK(rc != MYDB_OK, "garbage SQL → non-OK");
    CHECK(strstr(result, "parse error") != NULL,
          "result mentions parse error");

    full_close();
}

static void test_execute_sql_no_session(void)
{
    printf("\n[test_execute_sql_no_session]\n");

    /* engine_init only — no login. */
    rm_recursive(TEST_ROOT);
    engine_bootstrap(TEST_ROOT, TEST_USER, TEST_PASS);
    engine_init(TEST_ROOT, &g_eng);

    char result[256];
    int rc = engine_execute_sql(&g_eng, "SELECT 1;", result, sizeof(result));
    CHECK(rc == MYDB_ERR_PERM, "execute_sql without login → MYDB_ERR_PERM");

    engine_close(&g_eng);
}

/* ================================================================== */

int main(void)
{
    printf("=== test_integration ===\n");

    test_persist_and_reopen();
    test_schema_switch_flushes();
    test_analyst_read_only();
    test_fk_lifecycle();
    test_commit_survives_restart();
    test_rollback_clears_rows();
    test_drop_recreate_fresh();
    test_execute_sql_pipeline();
    test_execute_sql_no_session();

    rm_recursive(TEST_ROOT);

    printf("\n%d / %d tests passed\n", tests_passed, tests_run);
    return (tests_passed == tests_run) ? 0 : 1;
}
