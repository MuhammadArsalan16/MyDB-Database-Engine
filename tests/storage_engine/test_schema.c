#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <unistd.h>

#include "schema.h"

#define TEST_DIR "/tmp/mydb_test_schema"

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

/* Remove the catalog file so each test group starts fresh. */
static void cleanup(void)
{
    remove(TEST_DIR "/" CATALOG_FILENAME);
    rmdir(TEST_DIR);
}

static void ensure_dir(void)
{
    mkdir(TEST_DIR, 0755);
}

/* Build a minimal schema with one INT PK column. */
static Schema make_simple_schema(const char *name)
{
    Schema s;
    memset(&s, 0, sizeof(s));
    strncpy(s.table_name, name, MAX_TABLE_NAME - 1);

    s.num_columns  = 1;
    s.pk_col_idx   = 0;
    s.root_page_no = INVALID_PAGE;
    s.auto_incr_counter = 1;

    ColumnDef *c = &s.columns[0];
    strncpy(c->name, "id", MAX_COLUMN_NAME - 1);
    c->type           = TYPE_INT;
    c->max_len        = 4;
    c->is_not_null    = 1;
    c->is_primary_key = 1;
    c->is_auto_increment = 1;

    return s;
}

/* Build a richer schema: id INT PK, name VARCHAR(50), score DECIMAL, active BOOL */
static Schema make_rich_schema(const char *tname)
{
    Schema s;
    memset(&s, 0, sizeof(s));
    strncpy(s.table_name, tname, MAX_TABLE_NAME - 1);

    s.num_columns  = 4;
    s.pk_col_idx   = 0;
    s.root_page_no = 42;
    s.auto_incr_counter = 10;

    /* col 0: id INT PK */
    ColumnDef *c = &s.columns[0];
    strncpy(c->name, "id", MAX_COLUMN_NAME - 1);
    c->type = TYPE_INT; c->max_len = 4;
    c->is_not_null = 1; c->is_primary_key = 1; c->is_auto_increment = 1;

    /* col 1: name VARCHAR(50) NOT NULL */
    c = &s.columns[1];
    strncpy(c->name, "username", MAX_COLUMN_NAME - 1);
    c->type = TYPE_VARCHAR; c->max_len = 50; c->is_not_null = 1;

    /* col 2: score DECIMAL(10,2) DEFAULT 0 */
    c = &s.columns[2];
    strncpy(c->name, "score", MAX_COLUMN_NAME - 1);
    c->type = TYPE_DECIMAL; c->max_len = 10; c->scale = 2;
    c->has_default = 1; c->default_value.type = TYPE_DECIMAL;
    c->default_value.is_null = 0; c->default_value.v.decimal_val = 0;

    /* col 3: active BOOL DEFAULT TRUE */
    c = &s.columns[3];
    strncpy(c->name, "active", MAX_COLUMN_NAME - 1);
    c->type = TYPE_BOOL; c->max_len = 1;
    c->has_default = 1; c->default_value.type = TYPE_BOOL;
    c->default_value.is_null = 0; c->default_value.v.bool_val = 1;

    return s;
}

/* Build a schema with an ENUM column. */
static Schema make_enum_schema(const char *tname)
{
    Schema s;
    memset(&s, 0, sizeof(s));
    strncpy(s.table_name, tname, MAX_TABLE_NAME - 1);

    s.num_columns = 2;
    s.pk_col_idx  = 0;
    s.root_page_no = INVALID_PAGE;

    /* col 0: id INT PK */
    ColumnDef *c = &s.columns[0];
    strncpy(c->name, "id", MAX_COLUMN_NAME - 1);
    c->type = TYPE_INT; c->max_len = 4;
    c->is_not_null = 1; c->is_primary_key = 1;

    /* col 1: status ENUM(active, inactive, pending) DEFAULT active */
    c = &s.columns[1];
    strncpy(c->name, "status", MAX_COLUMN_NAME - 1);
    c->type = TYPE_ENUM; c->max_len = 1;
    c->num_enum_values = 3;
    strncpy(c->enum_values[0], "active",   MAX_ENUM_STR_LEN - 1);
    strncpy(c->enum_values[1], "inactive", MAX_ENUM_STR_LEN - 1);
    strncpy(c->enum_values[2], "pending",  MAX_ENUM_STR_LEN - 1);
    c->has_default = 1; c->default_value.type = TYPE_ENUM;
    c->default_value.is_null = 0; c->default_value.v.enum_val = 0; /* "active" */

    return s;
}

/* ------------------------------------------------------------------ */
/*  Test groups                                                         */
/* ------------------------------------------------------------------ */

static void test_open_empty(void)
{
    printf("\n[test_open_empty]\n");
    cleanup(); ensure_dir();

    SchemaCatalog cat;
    int rc = schema_catalog_open(&cat, TEST_DIR);
    CHECK(rc == MYDB_OK,       "catalog opens on empty dir");
    CHECK(cat.is_loaded == 1,  "is_loaded set");
    CHECK(cat.num_tables == 0, "zero tables on fresh catalog");

    schema_catalog_close(&cat);
}

static void test_add_and_get(void)
{
    printf("\n[test_add_and_get]\n");
    cleanup(); ensure_dir();

    SchemaCatalog cat;
    schema_catalog_open(&cat, TEST_DIR);

    Schema s = make_simple_schema("users");
    int rc = schema_add(&cat, &s);
    CHECK(rc == MYDB_OK,       "schema_add returns OK");
    CHECK(cat.num_tables == 1, "num_tables is 1 after add");

    Schema *found = schema_get(&cat, "users");
    CHECK(found != NULL,                              "schema_get finds table");
    CHECK(strcmp(found->table_name, "users") == 0,   "table name matches");
    CHECK(found->num_columns == 1,                   "column count matches");
    CHECK(found->columns[0].type == TYPE_INT,        "column type matches");
    CHECK(found->columns[0].is_primary_key == 1,     "PK flag preserved");

    /* non-existent lookup returns NULL */
    Schema *missing = schema_get(&cat, "orders");
    CHECK(missing == NULL, "schema_get returns NULL for unknown table");

    schema_catalog_close(&cat);
}

static void test_duplicate(void)
{
    printf("\n[test_duplicate]\n");
    cleanup(); ensure_dir();

    SchemaCatalog cat;
    schema_catalog_open(&cat, TEST_DIR);

    Schema s = make_simple_schema("users");
    schema_add(&cat, &s);

    int rc = schema_add(&cat, &s);  /* add same name again */
    CHECK(rc == MYDB_ERR_DUPLICATE, "duplicate add returns MYDB_ERR_DUPLICATE");
    CHECK(cat.num_tables == 1,      "num_tables still 1 after duplicate");

    schema_catalog_close(&cat);
}

static void test_remove(void)
{
    printf("\n[test_remove]\n");
    cleanup(); ensure_dir();

    SchemaCatalog cat;
    schema_catalog_open(&cat, TEST_DIR);

    Schema s1 = make_simple_schema("users");
    Schema s2 = make_simple_schema("orders");
    schema_add(&cat, &s1);
    schema_add(&cat, &s2);
    CHECK(cat.num_tables == 2, "two tables added");

    int rc = schema_remove(&cat, "users");
    CHECK(rc == MYDB_OK,         "schema_remove returns OK");
    CHECK(cat.num_tables == 1,   "num_tables decremented");
    CHECK(schema_get(&cat, "users")  == NULL, "removed table not found");
    CHECK(schema_get(&cat, "orders") != NULL, "other table still present");

    /* removing non-existent */
    rc = schema_remove(&cat, "nonexistent");
    CHECK(rc == MYDB_ERR_NOT_FOUND, "remove non-existent returns NOT_FOUND");

    schema_catalog_close(&cat);
}

static void test_persistence(void)
{
    printf("\n[test_persistence]\n");
    cleanup(); ensure_dir();

    /* add two schemas */
    {
        SchemaCatalog cat;
        schema_catalog_open(&cat, TEST_DIR);
        Schema s1 = make_simple_schema("users");
        Schema s2 = make_rich_schema("products");
        schema_add(&cat, &s1);
        schema_add(&cat, &s2);
        schema_catalog_close(&cat);
    }

    /* reopen and verify they survived */
    {
        SchemaCatalog cat;
        int rc = schema_catalog_open(&cat, TEST_DIR);
        CHECK(rc == MYDB_OK,           "reopen succeeds");
        CHECK(cat.num_tables == 2,     "both schemas survived close/reopen");

        Schema *u = schema_get(&cat, "users");
        Schema *p = schema_get(&cat, "products");
        CHECK(u != NULL, "users table reloaded");
        CHECK(p != NULL, "products table reloaded");

        if (u) {
            CHECK(u->num_columns == 1,         "users: column count persisted");
            CHECK(u->columns[0].type == TYPE_INT, "users: column type persisted");
        }
        if (p) {
            CHECK(p->num_columns == 4,              "products: column count persisted");
            CHECK(p->root_page_no == 42,            "products: root_page_no persisted");
            CHECK(p->auto_incr_counter == 10,       "products: auto_incr persisted");

            /* verify BOOL default value */
            ColumnDef *active = &p->columns[3];
            CHECK(active->has_default == 1,           "active: has_default persisted");
            CHECK(active->default_value.v.bool_val == 1, "active: bool default persisted");
        }
        schema_catalog_close(&cat);
    }
}

static void test_persistence_after_remove(void)
{
    printf("\n[test_persistence_after_remove]\n");
    cleanup(); ensure_dir();

    {
        SchemaCatalog cat;
        schema_catalog_open(&cat, TEST_DIR);
        Schema s1 = make_simple_schema("a");
        Schema s2 = make_simple_schema("b");
        Schema s3 = make_simple_schema("c");
        schema_add(&cat, &s1);
        schema_add(&cat, &s2);
        schema_add(&cat, &s3);
        schema_remove(&cat, "b");        /* remove middle table */
        schema_catalog_close(&cat);
    }

    {
        SchemaCatalog cat;
        schema_catalog_open(&cat, TEST_DIR);
        CHECK(cat.num_tables == 2,         "2 tables after removing 1");
        CHECK(schema_get(&cat, "a") != NULL, "table a still present");
        CHECK(schema_get(&cat, "b") == NULL, "table b gone");
        CHECK(schema_get(&cat, "c") != NULL, "table c still present");
        schema_catalog_close(&cat);
    }
}

static void test_enum_persistence(void)
{
    printf("\n[test_enum_persistence]\n");
    cleanup(); ensure_dir();

    {
        SchemaCatalog cat;
        schema_catalog_open(&cat, TEST_DIR);
        Schema s = make_enum_schema("tickets");
        schema_add(&cat, &s);
        schema_catalog_close(&cat);
    }

    {
        SchemaCatalog cat;
        schema_catalog_open(&cat, TEST_DIR);

        Schema *t = schema_get(&cat, "tickets");
        CHECK(t != NULL, "tickets table reloaded");
        if (t) {
            ColumnDef *status = &t->columns[1];
            CHECK(status->type == TYPE_ENUM,           "enum type persisted");
            CHECK(status->num_enum_values == 3,        "enum count persisted");
            CHECK(strcmp(status->enum_values[0], "active")   == 0, "enum[0] persisted");
            CHECK(strcmp(status->enum_values[1], "inactive") == 0, "enum[1] persisted");
            CHECK(strcmp(status->enum_values[2], "pending")  == 0, "enum[2] persisted");
            CHECK(status->default_value.v.enum_val == 0,   "enum default persisted");
        }
        schema_catalog_close(&cat);
    }
}

static void test_flush(void)
{
    printf("\n[test_flush]\n");
    cleanup(); ensure_dir();

    SchemaCatalog cat;
    schema_catalog_open(&cat, TEST_DIR);
    Schema s = make_simple_schema("users");
    schema_add(&cat, &s);

    /* mutate in-memory and flush */
    Schema *live = schema_get(&cat, "users");
    live->auto_incr_counter = 99;
    live->root_page_no      = 7;

    /* find index */
    int idx = -1;
    for (int i = 0; i < cat.num_tables; i++) {
        if (strcmp(cat.tables[i].table_name, "users") == 0) { idx = i; break; }
    }
    int rc = schema_flush(&cat, idx);
    CHECK(rc == MYDB_OK, "schema_flush returns OK");
    schema_catalog_close(&cat);

    /* reopen and check the flushed values */
    schema_catalog_open(&cat, TEST_DIR);
    Schema *reloaded = schema_get(&cat, "users");
    CHECK(reloaded != NULL,                        "table reloaded after flush");
    if (reloaded) {
        CHECK(reloaded->auto_incr_counter == 99,   "flushed auto_incr persisted");
        CHECK(reloaded->root_page_no == 7,         "flushed root_page_no persisted");
    }
    schema_catalog_close(&cat);
}

static void test_col_size(void)
{
    printf("\n[test_col_size]\n");

    ColumnDef c;
    memset(&c, 0, sizeof(c));

    c.type = TYPE_INT;      CHECK(schema_col_size(&c) == 4,  "INT size = 4");
    c.type = TYPE_DECIMAL;  CHECK(schema_col_size(&c) == 8,  "DECIMAL size = 8");
    c.type = TYPE_BOOL;     CHECK(schema_col_size(&c) == 1,  "BOOL size = 1");
    c.type = TYPE_ENUM;     CHECK(schema_col_size(&c) == 1,  "ENUM size = 1");
    c.type = TYPE_DATE;     CHECK(schema_col_size(&c) == 4,  "DATE size = 4");
    c.type = TYPE_DATETIME; CHECK(schema_col_size(&c) == 8,  "DATETIME size = 8");

    c.type = TYPE_VARCHAR; c.max_len = 50;
    CHECK(schema_col_size(&c) == 52, "VARCHAR(50) size = 52 (2 + 50)");

    c.type = TYPE_VARCHAR; c.max_len = 150;
    CHECK(schema_col_size(&c) == 152, "VARCHAR(150) size = 152 (2 + 150)");
}

static void test_row_size(void)
{
    printf("\n[test_row_size]\n");

    Schema s = make_rich_schema("t");
    /* INT(4) + VARCHAR(50)→52 + DECIMAL(8) + BOOL(1) = 65 */
    uint32_t sz = schema_row_size(&s);
    CHECK(sz == 65, "row size for rich schema = 65");
}

static void test_multiple_add_reuse_slot(void)
{
    printf("\n[test_multiple_add_reuse_slot]\n");
    cleanup(); ensure_dir();

    SchemaCatalog cat;
    schema_catalog_open(&cat, TEST_DIR);

    /* add 3, remove middle, add a new one — it should reuse the freed slot */
    Schema s1 = make_simple_schema("t1");
    Schema s2 = make_simple_schema("t2");
    Schema s3 = make_simple_schema("t3");
    schema_add(&cat, &s1);
    schema_add(&cat, &s2);
    schema_add(&cat, &s3);
    schema_remove(&cat, "t2");

    Schema s4 = make_simple_schema("t4");
    int rc = schema_add(&cat, &s4);
    CHECK(rc == MYDB_OK,         "add after remove succeeds");
    CHECK(cat.num_tables == 3,   "3 tables after add+remove+add");

    schema_catalog_close(&cat);

    /* verify persistence */
    schema_catalog_open(&cat, TEST_DIR);
    CHECK(cat.num_tables == 3,              "3 tables reloaded");
    CHECK(schema_get(&cat, "t1") != NULL,  "t1 present");
    CHECK(schema_get(&cat, "t2") == NULL,  "t2 absent");
    CHECK(schema_get(&cat, "t3") != NULL,  "t3 present");
    CHECK(schema_get(&cat, "t4") != NULL,  "t4 present");
    schema_catalog_close(&cat);
}

/* ------------------------------------------------------------------ */
/*  main                                                                */
/* ------------------------------------------------------------------ */

int main(void)
{
    printf("=== test_schema ===\n");

    test_open_empty();
    test_add_and_get();
    test_duplicate();
    test_remove();
    test_persistence();
    test_persistence_after_remove();
    test_enum_persistence();
    test_flush();
    test_col_size();
    test_row_size();
    test_multiple_add_reuse_slot();

    cleanup();

    printf("\n%d / %d tests passed\n", tests_passed, tests_run);
    return (tests_passed == tests_run) ? 0 : 1;
}
