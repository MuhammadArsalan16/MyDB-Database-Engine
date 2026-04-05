#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/stat.h>

#include "storage.h"

#define TEST_DIR "/tmp/mydb_test_storage"

static int tests_run    = 0;
static int tests_passed = 0;

#define CHECK(cond, msg) do { \
    tests_run++; \
    if (cond) { tests_passed++; printf("  PASS: %s\n", msg); } \
    else       { printf("  FAIL: %s (line %d)\n", msg, __LINE__); } \
} while(0)

/* ------------------------------------------------------------------ */
/*  Helpers                                                             */
/* ------------------------------------------------------------------ */

static void cleanup(void)
{
    /* remove every file in the test directory */
    char cmd[512];
    snprintf(cmd, sizeof(cmd), "rm -rf %s", TEST_DIR);
    system(cmd);
}

static void ensure_dir(void)
{
    mkdir(TEST_DIR, 0755);
}

static void reinit(void)
{
    storage_shutdown();
    cleanup();
    ensure_dir();
    storage_init(TEST_DIR);
}

/* Build a simple "users" schema: id INT PK AUTOINCR, name VARCHAR(32) NOT NULL */
static Schema make_users_schema(void)
{
    Schema s;
    memset(&s, 0, sizeof(s));
    s.num_columns  = 2;
    s.pk_col_idx   = 0;
    s.auto_incr_counter = 1;

    ColumnDef *id = &s.columns[0];
    strncpy(id->name, "id", MAX_COLUMN_NAME - 1);
    id->type = TYPE_INT; id->max_len = 4;
    id->is_not_null = 1; id->is_primary_key = 1; id->is_auto_increment = 1;

    ColumnDef *nm = &s.columns[1];
    strncpy(nm->name, "name", MAX_COLUMN_NAME - 1);
    nm->type = TYPE_VARCHAR; nm->max_len = 32; nm->is_not_null = 1;

    return s;
}

/* Build a row for the users table */
static Row make_user_row(int id, const char *name)
{
    Row r;
    memset(&r, 0, sizeof(r));
    r.num_cols = 2;

    r.cols[0].type       = TYPE_INT;
    r.cols[0].is_null    = (id == 0);
    r.cols[0].v.int_val  = id;

    r.cols[1].type = TYPE_VARCHAR;
    r.cols[1].is_null = 0;
    r.cols[1].v.varchar_val.len = (uint16_t)strlen(name);
    strncpy(r.cols[1].v.varchar_val.data, name, MAX_VARCHAR_LEN - 1);

    return r;
}

/* ------------------------------------------------------------------ */

static void test_init_shutdown(void)
{
    printf("\n[test_init_shutdown]\n");
    cleanup(); ensure_dir();

    int rc = storage_init(TEST_DIR);
    CHECK(rc == MYDB_OK, "storage_init returns OK");

    rc = storage_shutdown();
    CHECK(rc == MYDB_OK, "storage_shutdown returns OK");

    /* double shutdown is safe */
    rc = storage_shutdown();
    CHECK(rc == MYDB_OK, "double shutdown is safe");
}

static void test_create_drop_table(void)
{
    printf("\n[test_create_drop_table]\n");
    reinit();

    Schema s = make_users_schema();
    int rc = storage_create_table("users", &s);
    CHECK(rc == MYDB_OK, "create_table returns OK");
    CHECK(s.root_page_no != INVALID_PAGE, "root_page_no set by create_table");

    /* duplicate create */
    Schema s2 = make_users_schema();
    rc = storage_create_table("users", &s2);
    CHECK(rc == MYDB_ERR_DUPLICATE, "duplicate create returns ERR_DUPLICATE");

    rc = storage_drop_table("users");
    CHECK(rc == MYDB_OK, "drop_table returns OK");

    /* after drop, create again should succeed */
    Schema s3 = make_users_schema();
    rc = storage_create_table("users", &s3);
    CHECK(rc == MYDB_OK, "create after drop succeeds");
}

static void test_insert_get_by_pk(void)
{
    printf("\n[test_insert_get_by_pk]\n");
    reinit();

    Schema s = make_users_schema();
    storage_create_table("users", &s);

    Row r = make_user_row(1, "Alice");
    int rc = storage_insert("users", &r);
    CHECK(rc == MYDB_OK, "insert returns OK");

    /* get by PK */
    Value pk; pk.type = TYPE_INT; pk.is_null = 0; pk.v.int_val = 1;
    Row *found = storage_get_by_pk("users", &pk);
    CHECK(found != NULL,                                   "get_by_pk finds row");
    CHECK(found->cols[0].v.int_val == 1,                   "PK value correct");
    CHECK(strcmp(found->cols[1].v.varchar_val.data, "Alice") == 0, "name correct");
    CHECK(found->cols[1].v.varchar_val.len == 5,           "name length correct");

    /* get non-existent */
    pk.v.int_val = 99;
    Row *missing = storage_get_by_pk("users", &pk);
    CHECK(missing == NULL, "get_by_pk returns NULL for missing row");
}

static void test_auto_increment(void)
{
    printf("\n[test_auto_increment]\n");
    reinit();

    Schema s = make_users_schema();
    storage_create_table("users", &s);

    /* insert with id=0 → auto-assigned */
    Row r1 = make_user_row(0, "Alice");
    Row r2 = make_user_row(0, "Bob");
    Row r3 = make_user_row(0, "Carol");

    storage_insert("users", &r1);
    storage_insert("users", &r2);
    storage_insert("users", &r3);

    Value pk; pk.type = TYPE_INT; pk.is_null = 0;

    pk.v.int_val = 1;
    Row *a = storage_get_by_pk("users", &pk);
    CHECK(a != NULL && strcmp(a->cols[1].v.varchar_val.data, "Alice") == 0,
          "auto_incr id=1 → Alice");

    pk.v.int_val = 2;
    Row *b = storage_get_by_pk("users", &pk);
    CHECK(b != NULL && strcmp(b->cols[1].v.varchar_val.data, "Bob") == 0,
          "auto_incr id=2 → Bob");

    pk.v.int_val = 3;
    Row *c = storage_get_by_pk("users", &pk);
    CHECK(c != NULL && strcmp(c->cols[1].v.varchar_val.data, "Carol") == 0,
          "auto_incr id=3 → Carol");
}

static void test_duplicate_pk(void)
{
    printf("\n[test_duplicate_pk]\n");
    reinit();

    Schema s = make_users_schema();
    storage_create_table("users", &s);

    Row r1 = make_user_row(5, "Alice");
    storage_insert("users", &r1);

    Row r2 = make_user_row(5, "Bob");
    int rc = storage_insert("users", &r2);
    CHECK(rc == MYDB_ERR_DUPLICATE, "duplicate PK returns ERR_DUPLICATE");

    /* original row still intact */
    Value pk; pk.type = TYPE_INT; pk.is_null = 0; pk.v.int_val = 5;
    Row *found = storage_get_by_pk("users", &pk);
    CHECK(found != NULL && strcmp(found->cols[1].v.varchar_val.data, "Alice") == 0,
          "original row unchanged after duplicate insert");
}

static void test_not_null_violation(void)
{
    printf("\n[test_not_null_violation]\n");
    reinit();

    Schema s = make_users_schema();
    storage_create_table("users", &s);

    /* name column is NOT NULL — pass a null value */
    Row r;
    memset(&r, 0, sizeof(r));
    r.num_cols = 2;
    r.cols[0].type = TYPE_INT; r.cols[0].is_null = 0; r.cols[0].v.int_val = 1;
    r.cols[1].type = TYPE_VARCHAR; r.cols[1].is_null = 1;  /* NULL! */

    int rc = storage_insert("users", &r);
    CHECK(rc == MYDB_ERR_NULL_VIOLATION, "null in NOT NULL column returns violation");
}

static void test_delete(void)
{
    printf("\n[test_delete]\n");
    reinit();

    Schema s = make_users_schema();
    storage_create_table("users", &s);

    Row r1 = make_user_row(1, "Alice");
    Row r2 = make_user_row(2, "Bob");
    storage_insert("users", &r1);
    storage_insert("users", &r2);

    /* get the RID via get_by_pk, then delete */
    Value pk; pk.type = TYPE_INT; pk.is_null = 0; pk.v.int_val = 1;
    Row *found = storage_get_by_pk("users", &pk);
    CHECK(found != NULL, "row exists before delete");

    RID rid = found->rid;
    int rc = storage_delete("users", rid);
    CHECK(rc == MYDB_OK, "storage_delete returns OK");

    /* row should be gone */
    Row *gone = storage_get_by_pk("users", &pk);
    CHECK(gone == NULL, "deleted row not found by PK");

    /* other row still present */
    pk.v.int_val = 2;
    Row *bob = storage_get_by_pk("users", &pk);
    CHECK(bob != NULL && strcmp(bob->cols[1].v.varchar_val.data, "Bob") == 0,
          "other row unaffected by delete");
}

static void test_update(void)
{
    printf("\n[test_update]\n");
    reinit();

    Schema s = make_users_schema();
    storage_create_table("users", &s);

    Row r = make_user_row(1, "Alice");
    storage_insert("users", &r);

    Value pk; pk.type = TYPE_INT; pk.is_null = 0; pk.v.int_val = 1;
    Row *found = storage_get_by_pk("users", &pk);
    CHECK(found != NULL, "row found before update");
    RID rid = found->rid;

    /* update name to "Alicia" */
    Row updated = make_user_row(1, "Alicia");
    int rc = storage_update("users", rid, &updated);
    CHECK(rc == MYDB_OK, "storage_update returns OK");

    Row *after = storage_get_by_pk("users", &pk);
    CHECK(after != NULL, "row found after update");
    CHECK(after != NULL &&
          strcmp(after->cols[1].v.varchar_val.data, "Alicia") == 0,
          "name updated to Alicia");
}

static void test_scan(void)
{
    printf("\n[test_scan]\n");
    reinit();

    Schema s = make_users_schema();
    storage_create_table("users", &s);

    Row r1 = make_user_row(1, "Alice");
    Row r2 = make_user_row(2, "Bob");
    Row r3 = make_user_row(3, "Carol");
    storage_insert("users", &r1);
    storage_insert("users", &r2);
    storage_insert("users", &r3);

    Cursor *cur = storage_scan("users");
    CHECK(cur != NULL, "storage_scan returns cursor");

    int count = 0;
    Row *row;
    while ((row = cursor_next(cur)) != NULL) {
        count++;
        CHECK(row->num_cols == 2, "scanned row has 2 columns");
    }
    CHECK(count == 3, "scan returned 3 rows");

    cursor_close(cur);
}

static void test_scan_order(void)
{
    printf("\n[test_scan_order]\n");
    reinit();

    Schema s = make_users_schema();
    storage_create_table("users", &s);

    /* insert in reverse order — scan should return in PK (key) order */
    Row r3 = make_user_row(3, "Carol");
    Row r1 = make_user_row(1, "Alice");
    Row r2 = make_user_row(2, "Bob");
    storage_insert("users", &r3);
    storage_insert("users", &r1);
    storage_insert("users", &r2);

    Cursor *cur = storage_scan("users");
    int ids[3]; int n = 0;
    Row *row;
    while ((row = cursor_next(cur)) != NULL && n < 3)
        ids[n++] = row->cols[0].v.int_val;
    cursor_close(cur);

    CHECK(n == 3,         "scan returned 3 rows");
    CHECK(ids[0] == 1,    "first row: id=1");
    CHECK(ids[1] == 2,    "second row: id=2");
    CHECK(ids[2] == 3,    "third row: id=3");
}

static void test_transaction_rollback(void)
{
    printf("\n[test_transaction_rollback]\n");
    reinit();

    Schema s = make_users_schema();
    storage_create_table("users", &s);

    /* insert one row so the file is opened and registered */
    Row seed = make_user_row(1, "Seed");
    storage_insert("users", &seed);

    storage_begin();
    Row r = make_user_row(2, "Transient");
    storage_insert("users", &r);

    /* row is visible within the transaction */
    Value pk; pk.type = TYPE_INT; pk.is_null = 0; pk.v.int_val = 2;
    Row *during = storage_get_by_pk("users", &pk);
    CHECK(during != NULL, "row visible within transaction");

    storage_rollback();

    /* after rollback, buffer pool was evicted — row should not be on disk */
    Row *after = storage_get_by_pk("users", &pk);
    CHECK(after == NULL, "row gone after rollback");
}

static void test_transaction_commit_persists(void)
{
    printf("\n[test_transaction_commit_persists]\n");
    reinit();

    Schema s = make_users_schema();
    storage_create_table("users", &s);

    storage_begin();
    Row r = make_user_row(1, "Persistent");
    storage_insert("users", &r);
    storage_commit();

    /* shut down and reopen — row must survive */
    storage_shutdown();
    storage_init(TEST_DIR);

    Value pk; pk.type = TYPE_INT; pk.is_null = 0; pk.v.int_val = 1;
    Row *found = storage_get_by_pk("users", &pk);
    CHECK(found != NULL, "committed row survives shutdown/reopen");
    if (found)
        CHECK(strcmp(found->cols[1].v.varchar_val.data, "Persistent") == 0,
              "name correct after reopen");
}

static void test_many_rows(void)
{
    printf("\n[test_many_rows]\n");
    reinit();

    Schema s = make_users_schema();
    storage_create_table("users", &s);

    /* insert 200 rows — forces B+ Tree splits */
    for (int i = 1; i <= 200; i++) {
        char name[32];
        snprintf(name, sizeof(name), "user%d", i);
        Row r = make_user_row(i, name);
        int rc = storage_insert("users", &r);
        CHECK(rc == MYDB_OK, "insert row in bulk");
    }

    /* verify a few by PK */
    Value pk; pk.type = TYPE_INT; pk.is_null = 0;

    pk.v.int_val = 1;
    Row *r1 = storage_get_by_pk("users", &pk);
    CHECK(r1 != NULL && strcmp(r1->cols[1].v.varchar_val.data, "user1") == 0,
          "get row 1 after splits");

    pk.v.int_val = 100;
    Row *r100 = storage_get_by_pk("users", &pk);
    CHECK(r100 != NULL && strcmp(r100->cols[1].v.varchar_val.data, "user100") == 0,
          "get row 100 after splits");

    pk.v.int_val = 200;
    Row *r200 = storage_get_by_pk("users", &pk);
    CHECK(r200 != NULL && strcmp(r200->cols[1].v.varchar_val.data, "user200") == 0,
          "get row 200 after splits");

    /* full scan should return all 200 */
    Cursor *cur = storage_scan("users");
    int count = 0;
    while (cursor_next(cur) != NULL) count++;
    cursor_close(cur);
    CHECK(count == 200, "full scan returns 200 rows after splits");
}

/* ------------------------------------------------------------------ */

int main(void)
{
    printf("=== test_storage ===\n");

    test_init_shutdown();
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

    storage_shutdown();
    cleanup();

    printf("\n%d / %d tests passed\n", tests_passed, tests_run);
    return (tests_passed == tests_run) ? 0 : 1;
}
