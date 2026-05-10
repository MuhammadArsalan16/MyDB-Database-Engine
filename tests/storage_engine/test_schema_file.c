#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>

#include "common.h"
#include "schema_file.h"

#define TEST_FILE  "/tmp/mydb_test_schema_file.mydb"
#define TEST_PID   17
#define TEST_NAME  "mydb"

static int tests_run    = 0;
static int tests_passed = 0;

#define CHECK(cond, msg) do { \
    tests_run++; \
    if (cond) { tests_passed++; printf("  PASS: %s\n", msg); } \
    else       { printf("  FAIL: %s (line %d)\n", msg, __LINE__); } \
} while(0)

static void cleanup(void) { unlink(TEST_FILE); }

static void corrupt_byte(const char *path, off_t offset, uint8_t value)
{
    int fd = open(path, O_RDWR);
    if (fd < 0) return;
    pwrite(fd, &value, 1, offset);
    fsync(fd);
    close(fd);
}

/* Build a minimal RelationDef with the given name, two int columns,
 * pk on column 0. Returns by value. */
static RelationDef make_simple_def(const char *name)
{
    RelationDef r;
    memset(&r, 0, sizeof(r));
    strncpy(r.relation_name, name, MAX_TABLE_NAME - 1);
    r.num_columns = 2;
    strncpy(r.columns[0].name, "id",   MAX_COLUMN_NAME - 1);
    r.columns[0].type           = TYPE_INT;
    r.columns[0].is_primary_key = 1;
    r.columns[0].is_not_null    = 1;
    strncpy(r.columns[1].name, "age",  MAX_COLUMN_NAME - 1);
    r.columns[1].type           = TYPE_INT;
    r.pk_col_idx = 0;
    r.root_page_no = 99;
    r.auto_incr_counter = 1;
    return r;
}

/* ------------------------------------------------------------------ */
/*  Lifecycle                                                          */
/* ------------------------------------------------------------------ */

static void test_create_open_round_trip(void)
{
    printf("\n[test_create_open_round_trip]\n");
    cleanup();

    SchemaFile sf;
    CHECK(schema_create(TEST_FILE, TEST_PID, TEST_NAME, &sf) == MYDB_OK,
          "schema_create succeeds");
    CHECK(sf.header.partition_id == TEST_PID,        "partition_id stored");
    CHECK(strcmp(sf.header.schema_name, TEST_NAME) == 0,
                                                     "schema_name stored");
    CHECK(sf.header.num_relations == 0,              "num_relations starts 0");
    CHECK(sf.header.size_bytes == 0,                 "size_bytes starts 0");
    schema_close(&sf);

    SchemaFile sf2;
    CHECK(schema_open(TEST_FILE, &sf2) == MYDB_OK,   "schema_open succeeds");
    CHECK(sf2.header.partition_id == TEST_PID,       "partition_id persisted");
    CHECK(strcmp(sf2.header.schema_name, TEST_NAME) == 0,
                                                     "schema_name persisted");
    CHECK(sf2.header.num_relations == 0,             "num_relations persisted");
    schema_close(&sf2);
}

static void test_create_existing_fails(void)
{
    printf("\n[test_create_existing_fails]\n");
    cleanup();

    SchemaFile sf;
    schema_create(TEST_FILE, TEST_PID, TEST_NAME, &sf);
    schema_close(&sf);

    SchemaFile sf2;
    CHECK(schema_create(TEST_FILE, TEST_PID, TEST_NAME, &sf2) == MYDB_ERR,
          "schema_create on existing file fails");
}

static void test_open_missing_fails(void)
{
    printf("\n[test_open_missing_fails]\n");
    cleanup();

    SchemaFile sf;
    CHECK(schema_open(TEST_FILE, &sf) == MYDB_ERR, "open missing file fails");
}

/* ------------------------------------------------------------------ */
/*  File header negative cases                                         */
/* ------------------------------------------------------------------ */

static void test_bad_magic_rejected(void)
{
    printf("\n[test_bad_magic_rejected]\n");
    cleanup();

    SchemaFile sf;
    schema_create(TEST_FILE, TEST_PID, TEST_NAME, &sf);
    schema_close(&sf);

    corrupt_byte(TEST_FILE, 0, 0xFF);                /* break magic[0] */
    CHECK(schema_open(TEST_FILE, &sf) == MYDB_ERR_BAD_MAGIC,
          "bad magic returns MYDB_ERR_BAD_MAGIC");
}

static void test_bad_file_type_rejected(void)
{
    printf("\n[test_bad_file_type_rejected]\n");
    cleanup();

    SchemaFile sf;
    schema_create(TEST_FILE, TEST_PID, TEST_NAME, &sf);
    schema_close(&sf);

    corrupt_byte(TEST_FILE, 6, 99);                  /* file_type byte */
    CHECK(schema_open(TEST_FILE, &sf) == MYDB_ERR_BAD_FILE_TYPE,
          "bad file_type returns MYDB_ERR_BAD_FILE_TYPE");
}

static void test_bad_version_rejected(void)
{
    printf("\n[test_bad_version_rejected]\n");
    cleanup();

    SchemaFile sf;
    schema_create(TEST_FILE, TEST_PID, TEST_NAME, &sf);
    schema_close(&sf);

    corrupt_byte(TEST_FILE, 4, 99);                  /* version byte */
    CHECK(schema_open(TEST_FILE, &sf) == MYDB_ERR_BAD_VERSION,
          "bad version returns MYDB_ERR_BAD_VERSION");
}

static void test_bad_checksum_rejected(void)
{
    printf("\n[test_bad_checksum_rejected]\n");
    cleanup();

    SchemaFile sf;
    schema_create(TEST_FILE, TEST_PID, TEST_NAME, &sf);
    schema_close(&sf);

    /* tamper with a reserved byte inside Page 0 — checksum should catch it */
    corrupt_byte(TEST_FILE, 4000, 0xAB);
    CHECK(schema_open(TEST_FILE, &sf) == MYDB_ERR_BAD_CHECKSUM,
          "tampered Page 0 returns MYDB_ERR_BAD_CHECKSUM");
}

/* ------------------------------------------------------------------ */
/*  Add / remove / find                                                */
/* ------------------------------------------------------------------ */

static void test_add_relation_basic(void)
{
    printf("\n[test_add_relation_basic]\n");
    cleanup();

    SchemaFile sf;
    schema_create(TEST_FILE, TEST_PID, TEST_NAME, &sf);

    RelationDef def = make_simple_def("students");
    CHECK(schema_add_relation(&sf, &def) == MYDB_OK, "add_relation succeeds");
    CHECK(sf.header.num_relations == 1,              "num_relations bumped");

    RelationDef *got = schema_find_relation(&sf, "students");
    CHECK(got != NULL,                                "find_relation returns def");
    CHECK(got->num_columns == 2,                      "num_columns round-trip");
    CHECK(got->root_page_no == 99,                    "root_page_no round-trip");
    CHECK(got->auto_incr_counter == 1,                "auto_incr_counter round-trip");
    CHECK(strcmp(got->columns[0].name, "id") == 0,    "col[0] name round-trip");
    CHECK(got->columns[0].is_primary_key == 1,        "col[0] pk flag round-trip");

    RelationEntry *stat = schema_find_relation_stat(&sf, "students");
    CHECK(stat != NULL,                               "find_relation_stat works");
    CHECK(stat->is_valid == 1,                        "stat slot is valid");
    CHECK(stat->page_no >= 1 && stat->page_no < SCHEMA_FILE_PAGES,
                                                      "page_no in valid range");
    CHECK(stat->num_columns == 2,                     "stat num_columns set");

    schema_close(&sf);
}

static void test_add_duplicate_rejected(void)
{
    printf("\n[test_add_duplicate_rejected]\n");
    cleanup();

    SchemaFile sf;
    schema_create(TEST_FILE, TEST_PID, TEST_NAME, &sf);

    RelationDef d1 = make_simple_def("orders");
    RelationDef d2 = make_simple_def("orders");
    CHECK(schema_add_relation(&sf, &d1) == MYDB_OK,            "first add ok");
    CHECK(schema_add_relation(&sf, &d2) == MYDB_ERR_DUPLICATE, "duplicate rejected");
    CHECK(sf.header.num_relations == 1,                        "count unchanged");

    schema_close(&sf);
}

static void test_full_capacity(void)
{
    printf("\n[test_full_capacity]\n");
    cleanup();

    SchemaFile sf;
    schema_create(TEST_FILE, TEST_PID, TEST_NAME, &sf);

    /* fill every slot */
    for (int i = 0; i < MAX_RELATIONS_PER_SCHEMA; i++) {
        char name[32];
        snprintf(name, sizeof(name), "rel_%d", i);
        RelationDef d = make_simple_def(name);
        if (schema_add_relation(&sf, &d) != MYDB_OK) {
            CHECK(0, "all MAX_RELATIONS_PER_SCHEMA inserts should succeed");
            schema_close(&sf);
            return;
        }
    }
    CHECK(sf.header.num_relations == MAX_RELATIONS_PER_SCHEMA,
          "every slot filled");

    /* one more should fail */
    RelationDef extra = make_simple_def("overflow");
    CHECK(schema_add_relation(&sf, &extra) == MYDB_ERR_FULL,
          "overflow returns MYDB_ERR_FULL");

    schema_close(&sf);
}

static void test_remove_and_reuse(void)
{
    printf("\n[test_remove_and_reuse]\n");
    cleanup();

    SchemaFile sf;
    schema_create(TEST_FILE, TEST_PID, TEST_NAME, &sf);

    RelationDef a = make_simple_def("alpha");
    RelationDef b = make_simple_def("beta");
    schema_add_relation(&sf, &a);
    schema_add_relation(&sf, &b);

    RelationEntry *e_a = schema_find_relation_stat(&sf, "alpha");
    uint8_t alpha_page = e_a->page_no;

    CHECK(schema_remove_relation(&sf, "alpha") == MYDB_OK,    "remove succeeds");
    CHECK(sf.header.num_relations == 1,                       "count drops");
    CHECK(schema_find_relation(&sf, "alpha") == NULL,         "removed disappears");
    CHECK(schema_find_relation(&sf, "beta")  != NULL,         "sibling intact");

    /* re-add: should reuse the freed slot/page */
    RelationDef c = make_simple_def("gamma");
    CHECK(schema_add_relation(&sf, &c) == MYDB_OK,            "re-add succeeds");
    RelationEntry *e_c = schema_find_relation_stat(&sf, "gamma");
    CHECK(e_c->page_no == alpha_page,
          "freed def page reused (lowest-free policy)");

    schema_close(&sf);
}

static void test_remove_missing(void)
{
    printf("\n[test_remove_missing]\n");
    cleanup();

    SchemaFile sf;
    schema_create(TEST_FILE, TEST_PID, TEST_NAME, &sf);
    CHECK(schema_remove_relation(&sf, "ghost") == MYDB_ERR_NOT_FOUND,
          "remove missing returns NOT_FOUND");
    schema_close(&sf);
}

/* ------------------------------------------------------------------ */
/*  Persistence: round-trip through close/open                         */
/* ------------------------------------------------------------------ */

static void test_persistence_round_trip(void)
{
    printf("\n[test_persistence_round_trip]\n");
    cleanup();

    SchemaFile sf;
    schema_create(TEST_FILE, TEST_PID, TEST_NAME, &sf);
    RelationDef d = make_simple_def("persisted");
    schema_add_relation(&sf, &d);
    schema_update_stats(&sf, "persisted", 100, 5, 80);
    schema_close(&sf);

    SchemaFile sf2;
    CHECK(schema_open(TEST_FILE, &sf2) == MYDB_OK,
          "reopen after add+stats succeeds");
    CHECK(sf2.header.num_relations == 1, "num_relations persisted");

    RelationEntry *stat = schema_find_relation_stat(&sf2, "persisted");
    CHECK(stat != NULL,                  "relation slot recovered");
    CHECK(stat->num_rows == 100,         "num_rows persisted");
    CHECK(stat->num_pages == 5,          "num_pages persisted");
    CHECK(stat->avg_row_size == 80,      "avg_row_size persisted");

    /* size_bytes is computed at load — verify */
    CHECK(sf2.header.size_bytes == (uint64_t)5 * PAGE_SIZE,
          "size_bytes recomputed on load");

    RelationDef *def = schema_find_relation(&sf2, "persisted");
    CHECK(def != NULL,                   "RelationDef recovered");
    CHECK(def->root_page_no == 99,       "root_page_no persisted in def page");

    schema_close(&sf2);
}

/* ------------------------------------------------------------------ */
/*  RelationDef round-trip across all column types & features         */
/* ------------------------------------------------------------------ */

static void test_full_relation_def_round_trip(void)
{
    printf("\n[test_full_relation_def_round_trip]\n");
    cleanup();

    RelationDef r;
    memset(&r, 0, sizeof(r));
    strncpy(r.relation_name, "kitchen_sink", MAX_TABLE_NAME - 1);
    r.num_columns = 6;

    /* col 0: INT pk auto_incr */
    strncpy(r.columns[0].name, "id", MAX_COLUMN_NAME - 1);
    r.columns[0].type = TYPE_INT;
    r.columns[0].is_primary_key = 1;
    r.columns[0].is_auto_increment = 1;
    r.columns[0].is_not_null = 1;
    r.pk_col_idx = 0;

    /* col 1: VARCHAR(50) NOT NULL DEFAULT 'hello' */
    strncpy(r.columns[1].name, "title", MAX_COLUMN_NAME - 1);
    r.columns[1].type    = TYPE_VARCHAR;
    r.columns[1].max_len = 50;
    r.columns[1].is_not_null = 1;
    r.columns[1].has_default = 1;
    r.columns[1].default_value.type = TYPE_VARCHAR;
    r.columns[1].default_value.v.varchar_val.len = 5;
    memcpy(r.columns[1].default_value.v.varchar_val.data, "hello", 5);

    /* col 2: DECIMAL(10,3) DEFAULT 12345 (= 12.345) */
    strncpy(r.columns[2].name, "price", MAX_COLUMN_NAME - 1);
    r.columns[2].type    = TYPE_DECIMAL;
    r.columns[2].max_len = 10;
    r.columns[2].scale   = 3;
    r.columns[2].has_default = 1;
    r.columns[2].default_value.type = TYPE_DECIMAL;
    r.columns[2].default_value.v.decimal_val = 12345;

    /* col 3: ENUM(a,b,c) DEFAULT a */
    strncpy(r.columns[3].name, "status", MAX_COLUMN_NAME - 1);
    r.columns[3].type = TYPE_ENUM;
    r.columns[3].num_enum_values = 3;
    strcpy(r.columns[3].enum_values[0], "active");
    strcpy(r.columns[3].enum_values[1], "inactive");
    strcpy(r.columns[3].enum_values[2], "pending");
    r.columns[3].has_default = 1;
    r.columns[3].default_value.type = TYPE_ENUM;
    r.columns[3].default_value.v.enum_val = 0;

    /* col 4: BOOL DEFAULT TRUE */
    strncpy(r.columns[4].name, "active", MAX_COLUMN_NAME - 1);
    r.columns[4].type = TYPE_BOOL;
    r.columns[4].has_default = 1;
    r.columns[4].default_value.type = TYPE_BOOL;
    r.columns[4].default_value.v.bool_val = 1;

    /* col 5: DATETIME, no default */
    strncpy(r.columns[5].name, "created_at", MAX_COLUMN_NAME - 1);
    r.columns[5].type = TYPE_DATETIME;

    /* one foreign key */
    r.num_foreign_keys = 1;
    strncpy(r.foreign_keys[0].constraint_name, "fk_owner", MAX_COLUMN_NAME - 1);
    strncpy(r.foreign_keys[0].column_name,     "id",       MAX_COLUMN_NAME - 1);
    strncpy(r.foreign_keys[0].ref_relation_name, "users",   MAX_TABLE_NAME  - 1);
    strncpy(r.foreign_keys[0].ref_column_name, "user_id",  MAX_COLUMN_NAME - 1);

    /* secondary indexes */
    r.num_secondary_indexes  = 2;
    r.secondary_col_idx[0]   = 1;       /* on title */
    r.secondary_col_idx[1]   = 2;       /* on price */
    r.secondary_root_page_no[0] = 100;
    r.secondary_root_page_no[1] = 200;
    r.auto_incr_counter = 42;
    r.root_page_no      = 7;

    SchemaFile sf;
    schema_create(TEST_FILE, TEST_PID, TEST_NAME, &sf);
    CHECK(schema_add_relation(&sf, &r) == MYDB_OK,
          "add full RelationDef succeeds");
    schema_close(&sf);

    SchemaFile sf2;
    CHECK(schema_open(TEST_FILE, &sf2) == MYDB_OK, "reopen succeeds");
    RelationDef *got = schema_find_relation(&sf2, "kitchen_sink");
    CHECK(got != NULL, "RelationDef recovered");

    /* deep equality — memcmp because both structs are zeroed in their padding */
    CHECK(memcmp(got, &r, sizeof(RelationDef)) == 0,
          "full RelationDef byte-equal after round-trip");

    schema_close(&sf2);
}

/* ------------------------------------------------------------------ */
/*  flush_relation: edits to defs[] persist to def page                */
/* ------------------------------------------------------------------ */

static void test_flush_relation(void)
{
    printf("\n[test_flush_relation]\n");
    cleanup();

    SchemaFile sf;
    schema_create(TEST_FILE, TEST_PID, TEST_NAME, &sf);
    RelationDef d = make_simple_def("counters");
    schema_add_relation(&sf, &d);

    /* mutate in-memory and flush */
    RelationDef *got = schema_find_relation(&sf, "counters");
    got->auto_incr_counter = 555;
    got->root_page_no      = 1234;
    CHECK(schema_flush_relation(&sf, "counters") == MYDB_OK,
          "flush_relation succeeds");
    schema_close(&sf);

    SchemaFile sf2;
    schema_open(TEST_FILE, &sf2);
    RelationDef *got2 = schema_find_relation(&sf2, "counters");
    CHECK(got2 != NULL,                          "relation recovered");
    CHECK(got2->auto_incr_counter == 555,        "auto_incr_counter persisted");
    CHECK(got2->root_page_no      == 1234,       "root_page_no persisted");
    schema_close(&sf2);
}

static void test_flush_missing(void)
{
    printf("\n[test_flush_missing]\n");
    cleanup();

    SchemaFile sf;
    schema_create(TEST_FILE, TEST_PID, TEST_NAME, &sf);
    CHECK(schema_flush_relation(&sf, "nope") == MYDB_ERR_NOT_FOUND,
          "flush missing returns NOT_FOUND");
    schema_close(&sf);
}

/* ------------------------------------------------------------------ */
/*  size_bytes computed across multiple relations                      */
/* ------------------------------------------------------------------ */

static void test_size_bytes_multi(void)
{
    printf("\n[test_size_bytes_multi]\n");
    cleanup();

    SchemaFile sf;
    schema_create(TEST_FILE, TEST_PID, TEST_NAME, &sf);
    RelationDef a = make_simple_def("a");
    RelationDef b = make_simple_def("b");
    RelationDef c = make_simple_def("c");
    schema_add_relation(&sf, &a);
    schema_add_relation(&sf, &b);
    schema_add_relation(&sf, &c);

    schema_update_stats(&sf, "a", 0, 3, 0);
    schema_update_stats(&sf, "b", 0, 7, 0);
    schema_update_stats(&sf, "c", 0, 1, 0);

    CHECK(sf.header.size_bytes == (uint64_t)(3 + 7 + 1) * PAGE_SIZE,
          "size_bytes sums num_pages * PAGE_SIZE across slots");

    schema_close(&sf);

    SchemaFile sf2;
    schema_open(TEST_FILE, &sf2);
    CHECK(sf2.header.size_bytes == (uint64_t)11 * PAGE_SIZE,
          "size_bytes recomputed identically on reopen");
    schema_close(&sf2);
}

/* ------------------------------------------------------------------ */
/*  schema_bump_relation_pages — phase 9                               */
/* ------------------------------------------------------------------ */

static void test_bump_pages_positive(void)
{
    printf("\n[test_bump_pages_positive]\n");
    cleanup();

    SchemaFile sf;
    schema_create(TEST_FILE, TEST_PID, TEST_NAME, &sf);
    RelationDef d = make_simple_def("users");
    schema_add_relation(&sf, &d);

    CHECK(schema_bump_relation_pages(&sf, "users", +5) == MYDB_OK,
          "+5 pages succeeds");
    RelationEntry *e = schema_find_relation_stat(&sf, "users");
    CHECK(e != NULL && e->num_pages == 5, "num_pages now 5");
    CHECK(sf.header.size_bytes == (uint64_t)5 * PAGE_SIZE,
          "size_bytes recomputed after bump");

    /* Persists across reopen. */
    schema_close(&sf);
    SchemaFile sf2;
    schema_open(TEST_FILE, &sf2);
    RelationEntry *e2 = schema_find_relation_stat(&sf2, "users");
    CHECK(e2 != NULL && e2->num_pages == 5, "bump persists across reopen");
    schema_close(&sf2);
}

static void test_bump_pages_negative_clamps(void)
{
    printf("\n[test_bump_pages_negative_clamps]\n");
    cleanup();

    SchemaFile sf;
    schema_create(TEST_FILE, TEST_PID, TEST_NAME, &sf);
    RelationDef d = make_simple_def("orders");
    schema_add_relation(&sf, &d);
    schema_bump_relation_pages(&sf, "orders", +3);

    CHECK(schema_bump_relation_pages(&sf, "orders", -2) == MYDB_OK,
          "-2 from 3 succeeds");
    RelationEntry *e = schema_find_relation_stat(&sf, "orders");
    CHECK(e && e->num_pages == 1, "num_pages now 1");

    /* Driving below zero is refused, leaves the counter intact. */
    CHECK(schema_bump_relation_pages(&sf, "orders", -5) == MYDB_ERR,
          "-5 from 1 refused");
    e = schema_find_relation_stat(&sf, "orders");
    CHECK(e && e->num_pages == 1, "num_pages unchanged after refused bump");

    schema_close(&sf);
}

static void test_bump_pages_unknown_relation(void)
{
    printf("\n[test_bump_pages_unknown_relation]\n");
    cleanup();

    SchemaFile sf;
    schema_create(TEST_FILE, TEST_PID, TEST_NAME, &sf);

    CHECK(schema_bump_relation_pages(&sf, "ghost", +1) == MYDB_ERR_NOT_FOUND,
          "unknown relation → MYDB_ERR_NOT_FOUND");
    schema_close(&sf);
}

/* ------------------------------------------------------------------ */
/*  Main                                                               */
/* ------------------------------------------------------------------ */

int main(void)
{
    printf("=== test_schema_file ===\n");

    test_create_open_round_trip();
    test_create_existing_fails();
    test_open_missing_fails();

    test_bad_magic_rejected();
    test_bad_file_type_rejected();
    test_bad_version_rejected();
    test_bad_checksum_rejected();

    test_add_relation_basic();
    test_add_duplicate_rejected();
    test_full_capacity();
    test_remove_and_reuse();
    test_remove_missing();

    test_persistence_round_trip();
    test_full_relation_def_round_trip();

    test_flush_relation();
    test_flush_missing();

    test_size_bytes_multi();

    test_bump_pages_positive();
    test_bump_pages_negative_clamps();
    test_bump_pages_unknown_relation();

    cleanup();

    printf("\nResults: %d/%d passed\n", tests_passed, tests_run);
    return (tests_passed == tests_run) ? 0 : 1;
}
