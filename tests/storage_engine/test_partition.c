#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>

#include "common.h"
#include "partition.h"
#include "disk_manager.h"

#define TEST_FILE      "/tmp/mydb_test_partition.mydb"
#define TEST_RELATION  "/tmp/mydb_test_partition_relation.mydb"
#define TEST_PID   42
#define TEST_OWNER 7
#define TEST_QUOTA (1024ULL * 1024ULL)   /* 1 MB default for most tests */

static int tests_run    = 0;
static int tests_passed = 0;

#define CHECK(cond, msg) do { \
    tests_run++; \
    if (cond) { tests_passed++; printf("  PASS: %s\n", msg); } \
    else       { printf("  FAIL: %s (line %d)\n", msg, __LINE__); } \
} while(0)

static void cleanup(void) { unlink(TEST_FILE); unlink(TEST_RELATION); }

static void corrupt_byte(const char *path, off_t offset, uint8_t value)
{
    int fd = open(path, O_RDWR);
    if (fd < 0) return;
    pwrite(fd, &value, 1, offset);
    fsync(fd);
    close(fd);
}

/* ------------------------------------------------------------------ */
/*  Lifecycle                                                          */
/* ------------------------------------------------------------------ */

static void test_create_open_round_trip(void)
{
    printf("\n[test_create_open_round_trip]\n");
    cleanup();

    Catalog cat;
    CHECK(cat_create(TEST_FILE, TEST_PID, TEST_OWNER, TEST_QUOTA, &cat) == MYDB_OK,
          "cat_create succeeds");
    CHECK(cat.header.partition_id == TEST_PID,    "partition_id stored");
    CHECK(cat.header.owner_id     == TEST_OWNER,  "owner_id stored");
    CHECK(cat.header.quota_bytes  == TEST_QUOTA,  "quota_bytes stored");
    CHECK(cat.header.used_bytes   == 0,           "used_bytes starts 0");
    CHECK(cat.header.num_schemas  == 0,           "num_schemas starts 0");
    cat_close(&cat);

    Catalog cat2;
    CHECK(cat_open(TEST_FILE, &cat2) == MYDB_OK, "cat_open succeeds");
    CHECK(cat2.header.partition_id == TEST_PID,   "partition_id persisted");
    CHECK(cat2.header.owner_id     == TEST_OWNER, "owner_id persisted");
    CHECK(cat2.header.quota_bytes  == TEST_QUOTA, "quota_bytes persisted");
    CHECK(cat2.header.used_bytes   == 0,          "used_bytes persisted");
    cat_close(&cat2);
}

static void test_create_rejects_existing(void)
{
    printf("\n[test_create_rejects_existing]\n");
    cleanup();

    Catalog cat;
    CHECK(cat_create(TEST_FILE, TEST_PID, TEST_OWNER, TEST_QUOTA, &cat) == MYDB_OK,
          "first create succeeds");
    cat_close(&cat);

    Catalog cat2;
    CHECK(cat_create(TEST_FILE, TEST_PID, TEST_OWNER, TEST_QUOTA, &cat2) != MYDB_OK,
          "second create on same path fails");
}

/* ------------------------------------------------------------------ */
/*  Format guards                                                      */
/* ------------------------------------------------------------------ */

static void test_open_rejects_bad_magic(void)
{
    printf("\n[test_open_rejects_bad_magic]\n");
    cleanup();

    Catalog cat;
    cat_create(TEST_FILE, TEST_PID, TEST_OWNER, TEST_QUOTA, &cat);
    cat_close(&cat);

    corrupt_byte(TEST_FILE, 0, 0xFF);

    Catalog cat2;
    CHECK(cat_open(TEST_FILE, &cat2) == MYDB_ERR_BAD_MAGIC,
          "cat_open rejects bad magic");
}

static void test_open_rejects_wrong_file_type(void)
{
    printf("\n[test_open_rejects_wrong_file_type]\n");
    cleanup();

    Catalog cat;
    cat_create(TEST_FILE, TEST_PID, TEST_OWNER, TEST_QUOTA, &cat);
    cat_close(&cat);

    uint16_t wrong = FILETYPE_DATABASE;
    int fd = open(TEST_FILE, O_RDWR);
    pwrite(fd, &wrong, 2, 6);
    fsync(fd);
    close(fd);

    Catalog cat2;
    CHECK(cat_open(TEST_FILE, &cat2) == MYDB_ERR_BAD_FILE_TYPE,
          "cat_open rejects wrong file_type");
}

static void test_open_rejects_bad_checksum(void)
{
    printf("\n[test_open_rejects_bad_checksum]\n");
    cleanup();

    Catalog cat;
    cat_create(TEST_FILE, TEST_PID, TEST_OWNER, TEST_QUOTA, &cat);
    cat_close(&cat);

    /* Flip a byte in the schema-slot region to leave magic/version/filetype
     * intact and force checksum failure. */
    corrupt_byte(TEST_FILE, 100, 0xAA);

    Catalog cat2;
    CHECK(cat_open(TEST_FILE, &cat2) == MYDB_ERR_BAD_CHECKSUM,
          "cat_open rejects corrupted file");
}

/* ------------------------------------------------------------------ */
/*  Schema management                                                  */
/* ------------------------------------------------------------------ */

static void test_add_schema_persists(void)
{
    printf("\n[test_add_schema_persists]\n");
    cleanup();

    Catalog cat;
    cat_create(TEST_FILE, TEST_PID, TEST_OWNER, TEST_QUOTA, &cat);

    CHECK(cat_add_schema(&cat, "mydb") == MYDB_OK, "add 'mydb' succeeds");
    CHECK(cat.header.num_schemas == 1,             "num_schemas = 1");

    SchemaEntry *s = cat_find_schema(&cat, "mydb");
    CHECK(s != NULL,                               "find returns entry");
    CHECK(s && s->is_valid,                         "is_valid is set");

    cat_close(&cat);

    Catalog cat2;
    cat_open(TEST_FILE, &cat2);
    CHECK(cat_find_schema(&cat2, "mydb") != NULL,  "schema survived reopen");
    CHECK(cat2.header.num_schemas == 1,             "num_schemas survived reopen");
    cat_close(&cat2);
}

static void test_add_schema_duplicate(void)
{
    printf("\n[test_add_schema_duplicate]\n");
    cleanup();

    Catalog cat;
    cat_create(TEST_FILE, TEST_PID, TEST_OWNER, TEST_QUOTA, &cat);

    CHECK(cat_add_schema(&cat, "mydb") == MYDB_OK,           "first add succeeds");
    CHECK(cat_add_schema(&cat, "mydb") == MYDB_ERR_DUPLICATE,
          "duplicate add returns MYDB_ERR_DUPLICATE");

    cat_close(&cat);
}

static void test_add_schema_full(void)
{
    printf("\n[test_add_schema_full]\n");
    cleanup();

    Catalog cat;
    cat_create(TEST_FILE, TEST_PID, TEST_OWNER, TEST_QUOTA, &cat);

    int all_ok = 1;
    for (int i = 0; i < MAX_SCHEMAS_PER_PARTITION; i++) {
        char name[32];
        snprintf(name, sizeof(name), "schema_%d", i);
        if (cat_add_schema(&cat, name) != MYDB_OK) all_ok = 0;
    }
    CHECK(all_ok, "MAX_SCHEMAS_PER_PARTITION schemas added");

    CHECK(cat_add_schema(&cat, "one_more") == MYDB_ERR_FULL,
          "next add returns MYDB_ERR_FULL");

    cat_close(&cat);
}

static void test_remove_schema(void)
{
    printf("\n[test_remove_schema]\n");
    cleanup();

    Catalog cat;
    cat_create(TEST_FILE, TEST_PID, TEST_OWNER, TEST_QUOTA, &cat);
    cat_add_schema(&cat, "mydb");

    CHECK(cat_remove_schema(&cat, "mydb") == MYDB_OK,    "remove succeeds");
    CHECK(cat_find_schema(&cat, "mydb")   == NULL,        "removed schema not findable");
    CHECK(cat.header.num_schemas == 0,                    "num_schemas decremented");

    cat_close(&cat);
}

static void test_remove_unknown_schema(void)
{
    printf("\n[test_remove_unknown_schema]\n");
    cleanup();

    Catalog cat;
    cat_create(TEST_FILE, TEST_PID, TEST_OWNER, TEST_QUOTA, &cat);

    CHECK(cat_remove_schema(&cat, "ghost") == MYDB_ERR_NOT_FOUND,
          "remove unknown name → NOT_FOUND");

    cat_close(&cat);
}

static void test_remove_then_add_reuses_slot(void)
{
    printf("\n[test_remove_then_add_reuses_slot]\n");
    cleanup();

    Catalog cat;
    cat_create(TEST_FILE, TEST_PID, TEST_OWNER, TEST_QUOTA, &cat);

    for (int i = 0; i < MAX_SCHEMAS_PER_PARTITION; i++) {
        char name[32];
        snprintf(name, sizeof(name), "schema_%d", i);
        cat_add_schema(&cat, name);
    }
    cat_remove_schema(&cat, "schema_5");

    CHECK(cat_add_schema(&cat, "fresh") == MYDB_OK,
          "add into freed slot succeeds");

    cat_close(&cat);
}

static void test_find_schema_miss(void)
{
    printf("\n[test_find_schema_miss]\n");
    cleanup();

    Catalog cat;
    cat_create(TEST_FILE, TEST_PID, TEST_OWNER, TEST_QUOTA, &cat);
    cat_add_schema(&cat, "mydb");

    CHECK(cat_find_schema(&cat, "mydb")  != NULL, "known name finds entry");
    CHECK(cat_find_schema(&cat, "ghost") == NULL, "unknown name → NULL");

    cat_close(&cat);
}

/* ------------------------------------------------------------------ */
/*  Quota tracking                                                     */
/* ------------------------------------------------------------------ */

static void test_track_alloc_positive(void)
{
    printf("\n[test_track_alloc_positive]\n");
    cleanup();

    Catalog cat;
    cat_create(TEST_FILE, TEST_PID, TEST_OWNER, TEST_QUOTA, &cat);

    CHECK(cat_track_alloc(&cat,  PAGE_SIZE) == MYDB_OK,    "+PAGE_SIZE succeeds");
    CHECK(cat.header.used_bytes == PAGE_SIZE,              "used_bytes = PAGE_SIZE");
    CHECK(cat_track_alloc(&cat,  PAGE_SIZE) == MYDB_OK,    "+PAGE_SIZE again succeeds");
    CHECK(cat.header.used_bytes == 2 * PAGE_SIZE,          "used_bytes = 2*PAGE_SIZE");

    cat_close(&cat);
}

static void test_track_alloc_quota_exceeded(void)
{
    printf("\n[test_track_alloc_quota_exceeded]\n");
    cleanup();

    /* Tight quota: exactly one page. Second alloc must fail. */
    Catalog cat;
    cat_create(TEST_FILE, TEST_PID, TEST_OWNER, PAGE_SIZE, &cat);

    CHECK(cat_track_alloc(&cat, PAGE_SIZE) == MYDB_OK,        "fill exact quota");
    CHECK(cat_track_alloc(&cat, 1)         == MYDB_ERR_FULL,  "1 byte over quota → FULL");
    CHECK(cat.header.used_bytes == PAGE_SIZE,
          "used_bytes unchanged after rejected alloc");

    cat_close(&cat);
}

static void test_track_alloc_negative(void)
{
    printf("\n[test_track_alloc_negative]\n");
    cleanup();

    Catalog cat;
    cat_create(TEST_FILE, TEST_PID, TEST_OWNER, TEST_QUOTA, &cat);
    cat_track_alloc(&cat, 4 * PAGE_SIZE);

    CHECK(cat_track_alloc(&cat, -PAGE_SIZE) == MYDB_OK,
          "free 1 page succeeds");
    CHECK(cat.header.used_bytes == 3 * PAGE_SIZE,
          "used_bytes decreased by PAGE_SIZE");

    cat_close(&cat);
}

static void test_track_alloc_underflow_protection(void)
{
    printf("\n[test_track_alloc_underflow_protection]\n");
    cleanup();

    Catalog cat;
    cat_create(TEST_FILE, TEST_PID, TEST_OWNER, TEST_QUOTA, &cat);
    cat_track_alloc(&cat, PAGE_SIZE);

    CHECK(cat_track_alloc(&cat, -2 * PAGE_SIZE) == MYDB_ERR,
          "free more than used → MYDB_ERR");
    CHECK(cat.header.used_bytes == PAGE_SIZE,
          "used_bytes unchanged after rejected free");

    cat_close(&cat);
}

static void test_track_alloc_zero_noop(void)
{
    printf("\n[test_track_alloc_zero_noop]\n");
    cleanup();

    Catalog cat;
    cat_create(TEST_FILE, TEST_PID, TEST_OWNER, TEST_QUOTA, &cat);
    cat_track_alloc(&cat, PAGE_SIZE);

    CHECK(cat_track_alloc(&cat, 0) == MYDB_OK,    "zero delta returns OK");
    CHECK(cat.header.used_bytes == PAGE_SIZE,     "used_bytes unchanged");

    cat_close(&cat);
}

static void test_track_alloc_persists(void)
{
    printf("\n[test_track_alloc_persists]\n");
    cleanup();

    Catalog cat;
    cat_create(TEST_FILE, TEST_PID, TEST_OWNER, TEST_QUOTA, &cat);
    cat_track_alloc(&cat, 5 * PAGE_SIZE);
    cat_close(&cat);

    Catalog cat2;
    cat_open(TEST_FILE, &cat2);
    CHECK(cat2.header.used_bytes == 5 * PAGE_SIZE,
          "used_bytes persisted across close+reopen");
    cat_close(&cat2);
}

/* ------------------------------------------------------------------ */
/*  Bookkeeping                                                        */
/* ------------------------------------------------------------------ */

static void test_last_modified_refreshes(void)
{
    printf("\n[test_last_modified_refreshes]\n");
    cleanup();

    Catalog cat;
    cat_create(TEST_FILE, TEST_PID, TEST_OWNER, TEST_QUOTA, &cat);

    /* Force last_modified backwards in time, then trigger a save. */
    cat.header.last_modified = 19700101000000ULL;
    cat_save(&cat);
    CHECK(cat.header.last_modified > 19700101000000ULL,
          "cat_save refreshes last_modified");

    cat_close(&cat);
}

/* ------------------------------------------------------------------ */
/*  partition_alloc_page — quota-aware page allocation                */
/* ------------------------------------------------------------------ */

static void test_alloc_page_basic(void)
{
    printf("\n[test_alloc_page_basic]\n");
    cleanup();

    Catalog cat;
    DiskManager dm;
    cat_create(TEST_FILE, TEST_PID, TEST_OWNER, TEST_QUOTA, &cat);
    disk_create(&dm, TEST_RELATION);

    uint64_t before_used  = cat.header.used_bytes;
    uint32_t before_pages = dm.num_pages;

    uint32_t pno = 0;
    CHECK(partition_alloc_page(&cat, &dm, TEST_OWNER, &pno) == MYDB_OK,
          "partition_alloc_page succeeds");
    CHECK(pno == before_pages,
          "out_pno equals pre-alloc num_pages (page appended at end)");
    CHECK(cat.header.used_bytes == before_used + PAGE_SIZE,
          "used_bytes bumped by PAGE_SIZE");
    CHECK(dm.num_pages == before_pages + 1,
          "DiskManager num_pages incremented");

    disk_close(&dm);
    cat_close(&cat);
}

static void test_alloc_page_repeated(void)
{
    printf("\n[test_alloc_page_repeated]\n");
    cleanup();

    Catalog cat;
    DiskManager dm;
    cat_create(TEST_FILE, TEST_PID, TEST_OWNER, TEST_QUOTA, &cat);
    disk_create(&dm, TEST_RELATION);

    uint32_t pno;
    int ok = 1;
    for (int i = 0; i < 5; i++) {
        if (partition_alloc_page(&cat, &dm, TEST_OWNER, &pno) != MYDB_OK) { ok = 0; break; }
    }
    CHECK(ok, "5 sequential allocs all succeed");
    CHECK(cat.header.used_bytes == (uint64_t)5 * PAGE_SIZE,
          "used_bytes accumulates exactly 5 pages");

    disk_close(&dm);
    cat_close(&cat);
}

static void test_alloc_page_quota_exceeded(void)
{
    printf("\n[test_alloc_page_quota_exceeded]\n");
    cleanup();

    /* Quota exactly fits 2 pages — third must fail. */
    Catalog cat;
    DiskManager dm;
    cat_create(TEST_FILE, TEST_PID, TEST_OWNER,
               (uint64_t)2 * PAGE_SIZE, &cat);
    disk_create(&dm, TEST_RELATION);

    uint32_t pno;
    CHECK(partition_alloc_page(&cat, &dm, TEST_OWNER, &pno) == MYDB_OK, "page 1 fits");
    CHECK(partition_alloc_page(&cat, &dm, TEST_OWNER, &pno) == MYDB_OK, "page 2 fits exact");

    uint32_t pages_before = dm.num_pages;
    uint64_t used_before  = cat.header.used_bytes;

    CHECK(partition_alloc_page(&cat, &dm, TEST_OWNER, &pno) == MYDB_ERR_FULL,
          "page 3 exceeds quota → MYDB_ERR_FULL");
    CHECK(dm.num_pages == pages_before,
          "rejected alloc did NOT grow the relation file");
    CHECK(cat.header.used_bytes == used_before,
          "rejected alloc did NOT change used_bytes");

    disk_close(&dm);
    cat_close(&cat);
}

static void test_alloc_page_quota_zero(void)
{
    printf("\n[test_alloc_page_quota_zero]\n");
    cleanup();

    Catalog cat;
    DiskManager dm;
    cat_create(TEST_FILE, TEST_PID, TEST_OWNER, 0, &cat);
    disk_create(&dm, TEST_RELATION);

    uint32_t pages_before = dm.num_pages;
    uint32_t pno;
    CHECK(partition_alloc_page(&cat, &dm, TEST_OWNER, &pno) == MYDB_ERR_FULL,
          "quota=0 rejects every alloc");
    CHECK(dm.num_pages == pages_before, "no growth on rejected alloc");

    disk_close(&dm);
    cat_close(&cat);
}

static void test_alloc_page_corrupted_used_bytes(void)
{
    printf("\n[test_alloc_page_corrupted_used_bytes]\n");
    cleanup();

    Catalog cat;
    DiskManager dm;
    cat_create(TEST_FILE, TEST_PID, TEST_OWNER, TEST_QUOTA, &cat);
    disk_create(&dm, TEST_RELATION);

    /* Simulate a corrupted catalog where used_bytes already exceeds quota.
     * Pre-check must reject without touching the relation file. */
    cat.header.used_bytes = TEST_QUOTA + PAGE_SIZE;

    uint32_t pages_before = dm.num_pages;
    uint32_t pno;
    CHECK(partition_alloc_page(&cat, &dm, TEST_OWNER, &pno) == MYDB_ERR_FULL,
          "used_bytes > quota_bytes is rejected");
    CHECK(dm.num_pages == pages_before, "corrupted state does not grow file");

    disk_close(&dm);
    cat_close(&cat);
}

static void test_alloc_page_persists_used_bytes(void)
{
    printf("\n[test_alloc_page_persists_used_bytes]\n");
    cleanup();

    Catalog cat;
    DiskManager dm;
    cat_create(TEST_FILE, TEST_PID, TEST_OWNER, TEST_QUOTA, &cat);
    disk_create(&dm, TEST_RELATION);

    uint32_t pno;
    partition_alloc_page(&cat, &dm, TEST_OWNER, &pno);
    partition_alloc_page(&cat, &dm, TEST_OWNER, &pno);
    partition_alloc_page(&cat, &dm, TEST_OWNER, &pno);

    disk_close(&dm);
    cat_close(&cat);

    Catalog cat2;
    CHECK(cat_open(TEST_FILE, &cat2) == MYDB_OK, "catalog reopens cleanly");
    CHECK(cat2.header.used_bytes == (uint64_t)3 * PAGE_SIZE,
          "used_bytes survives close+reopen");
    cat_close(&cat2);
}

static void test_alloc_page_null_params(void)
{
    printf("\n[test_alloc_page_null_params]\n");
    cleanup();

    Catalog cat;
    DiskManager dm;
    cat_create(TEST_FILE, TEST_PID, TEST_OWNER, TEST_QUOTA, &cat);
    disk_create(&dm, TEST_RELATION);

    uint32_t pno;
    CHECK(partition_alloc_page(NULL, &dm,  TEST_OWNER, &pno) == MYDB_ERR, "NULL cat → MYDB_ERR");
    CHECK(partition_alloc_page(&cat, NULL, TEST_OWNER, &pno) == MYDB_ERR, "NULL dm → MYDB_ERR");
    CHECK(partition_alloc_page(&cat, &dm,  TEST_OWNER, NULL) == MYDB_ERR, "NULL out_pno → MYDB_ERR");

    disk_close(&dm);
    cat_close(&cat);
}

/* Phase 8: ownership gate. Only the catalog's owner may grow user
 * relations through the wrapper; all other user_ids are rejected
 * before the quota check or any disk I/O. */
static void test_alloc_page_owner_match(void)
{
    printf("\n[test_alloc_page_owner_match]\n");
    cleanup();

    Catalog cat;
    DiskManager dm;
    cat_create(TEST_FILE, TEST_PID, TEST_OWNER, TEST_QUOTA, &cat);
    disk_create(&dm, TEST_RELATION);

    uint32_t pno;
    CHECK(partition_alloc_page(&cat, &dm, TEST_OWNER, &pno) == MYDB_OK,
          "matching owner allowed");
    CHECK(cat.header.used_bytes == PAGE_SIZE,
          "used_bytes bumped on accepted alloc");

    disk_close(&dm);
    cat_close(&cat);
}

static void test_alloc_page_owner_mismatch(void)
{
    printf("\n[test_alloc_page_owner_mismatch]\n");
    cleanup();

    Catalog cat;
    DiskManager dm;
    cat_create(TEST_FILE, TEST_PID, TEST_OWNER, TEST_QUOTA, &cat);
    disk_create(&dm, TEST_RELATION);

    uint32_t pages_before = dm.num_pages;
    uint64_t used_before  = cat.header.used_bytes;

    uint32_t pno;
    CHECK(partition_alloc_page(&cat, &dm, TEST_OWNER + 1, &pno) == MYDB_ERR_PERM,
          "non-owner rejected with MYDB_ERR_PERM");
    CHECK(dm.num_pages == pages_before,
          "rejected non-owner did NOT grow the relation file");
    CHECK(cat.header.used_bytes == used_before,
          "rejected non-owner did NOT change used_bytes");

    /* user_id 0 is also not the owner — should be rejected. */
    CHECK(partition_alloc_page(&cat, &dm, 0, &pno) == MYDB_ERR_PERM,
          "user_id=0 rejected (not owner)");

    disk_close(&dm);
    cat_close(&cat);
}

static void test_alloc_page_ownership_before_quota(void)
{
    printf("\n[test_alloc_page_ownership_before_quota]\n");
    cleanup();

    /* Quota=0 would normally trigger MYDB_ERR_FULL. With a wrong
     * user_id, ownership must reject FIRST (MYDB_ERR_PERM) — proving
     * the gates are checked in the documented order. */
    Catalog cat;
    DiskManager dm;
    cat_create(TEST_FILE, TEST_PID, TEST_OWNER, 0, &cat);
    disk_create(&dm, TEST_RELATION);

    uint32_t pno;
    CHECK(partition_alloc_page(&cat, &dm, TEST_OWNER + 1, &pno) == MYDB_ERR_PERM,
          "ownership checked before quota");

    disk_close(&dm);
    cat_close(&cat);
}

/* ------------------------------------------------------------------ */

int main(void)
{
    printf("=== test_partition ===\n");

    test_create_open_round_trip();
    test_create_rejects_existing();
    test_open_rejects_bad_magic();
    test_open_rejects_wrong_file_type();
    test_open_rejects_bad_checksum();

    test_add_schema_persists();
    test_add_schema_duplicate();
    test_add_schema_full();
    test_remove_schema();
    test_remove_unknown_schema();
    test_remove_then_add_reuses_slot();
    test_find_schema_miss();

    test_track_alloc_positive();
    test_track_alloc_quota_exceeded();
    test_track_alloc_negative();
    test_track_alloc_underflow_protection();
    test_track_alloc_zero_noop();
    test_track_alloc_persists();

    test_last_modified_refreshes();

    test_alloc_page_basic();
    test_alloc_page_repeated();
    test_alloc_page_quota_exceeded();
    test_alloc_page_quota_zero();
    test_alloc_page_corrupted_used_bytes();
    test_alloc_page_persists_used_bytes();
    test_alloc_page_null_params();
    test_alloc_page_owner_match();
    test_alloc_page_owner_mismatch();
    test_alloc_page_ownership_before_quota();

    printf("\nResults: %d/%d passed\n", tests_passed, tests_run);
    cleanup();
    return (tests_passed == tests_run) ? 0 : 1;
}
