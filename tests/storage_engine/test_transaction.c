#include <stdio.h>
#include <string.h>
#include <unistd.h>

#include "transaction.h"

#define TEST_FILE_A  "/tmp/mydb_test_trx_a.mydb"
#define TEST_FILE_B  "/tmp/mydb_test_trx_b.mydb"
#define TABLE_A  0
#define TABLE_B  1

static int tests_run    = 0;
static int tests_passed = 0;

#define CHECK(cond, msg) do { \
    tests_run++; \
    if (cond) { tests_passed++; printf("  PASS: %s\n", msg); } \
    else       { printf("  FAIL: %s (line %d)\n", msg, __LINE__); } \
} while(0)

/* ------------------------------------------------------------------ */
/*  Shared fixtures                                                     */
/* ------------------------------------------------------------------ */

static DiskManager       dm_a, dm_b;
static BufferPool        bp;
static TransactionManager tm;

static void setup_one(void)
{
    unlink(TEST_FILE_A);
    disk_create(&dm_a, TEST_FILE_A);
    /* pre-allocate page 1 so tests can fetch it */
    uint32_t pno; disk_alloc_page(&dm_a, &pno);

    bp_init(&bp);
    trx_init(&tm, &bp);
    trx_register_table(&tm, TABLE_A, &dm_a);
}

static void setup_two(void)
{
    unlink(TEST_FILE_A);
    unlink(TEST_FILE_B);
    disk_create(&dm_a, TEST_FILE_A);
    disk_create(&dm_b, TEST_FILE_B);
    uint32_t pno;
    disk_alloc_page(&dm_a, &pno);
    disk_alloc_page(&dm_b, &pno);

    bp_init(&bp);
    trx_init(&tm, &bp);
    trx_register_table(&tm, TABLE_A, &dm_a);
    trx_register_table(&tm, TABLE_B, &dm_b);
}

static void teardown(void)
{
    disk_close(&dm_a);
    disk_close(&dm_b);
    unlink(TEST_FILE_A);
    unlink(TEST_FILE_B);
}

/* ------------------------------------------------------------------ */

static void test_init(void)
{
    printf("\n[test_init]\n");

    BufferPool  local_bp;
    TransactionManager local_tm;
    bp_init(&local_bp);
    trx_init(&local_tm, &local_bp);

    CHECK(local_tm.active   == 0,      "not active after init");
    CHECK(local_tm.trx_id   == 0,      "trx_id 0 after init");
    CHECK(local_tm.next_id  == 1,      "next_id starts at 1");
    CHECK(local_tm.num_open == 0,      "no open tables after init");
    CHECK(trx_is_active(&local_tm) == 0, "trx_is_active returns 0");
    CHECK(trx_current_id(&local_tm) == 0, "current_id returns 0");
}

static void test_begin_commit(void)
{
    printf("\n[test_begin_commit]\n");
    setup_one();

    int rc = trx_begin(&tm);
    CHECK(rc == MYDB_OK,          "trx_begin returns OK");
    CHECK(tm.active == 1,         "active after begin");
    CHECK(tm.trx_id == 1,         "first trx_id is 1");
    CHECK(trx_is_active(&tm) == 1, "trx_is_active true");
    CHECK(trx_current_id(&tm) == 1, "current_id == 1");

    rc = trx_commit(&tm);
    CHECK(rc == MYDB_OK,           "trx_commit returns OK");
    CHECK(tm.active == 0,          "inactive after commit");
    CHECK(trx_is_active(&tm) == 0, "trx_is_active false after commit");
    CHECK(trx_current_id(&tm) == 0,"current_id 0 after commit");

    teardown();
}

static void test_begin_rollback(void)
{
    printf("\n[test_begin_rollback]\n");
    setup_one();

    trx_begin(&tm);
    int rc = trx_rollback(&tm);
    CHECK(rc == MYDB_OK,           "trx_rollback returns OK");
    CHECK(tm.active == 0,          "inactive after rollback");
    CHECK(trx_is_active(&tm) == 0, "trx_is_active false after rollback");
    CHECK(trx_current_id(&tm) == 0,"current_id 0 after rollback");

    teardown();
}

static void test_monotonic_ids(void)
{
    printf("\n[test_monotonic_ids]\n");
    setup_one();

    trx_begin(&tm);
    uint64_t id1 = trx_current_id(&tm);
    trx_commit(&tm);

    trx_begin(&tm);
    uint64_t id2 = trx_current_id(&tm);
    trx_commit(&tm);

    trx_begin(&tm);
    uint64_t id3 = trx_current_id(&tm);
    trx_rollback(&tm);

    CHECK(id1 == 1,      "first transaction ID = 1");
    CHECK(id2 == 2,      "second transaction ID = 2");
    CHECK(id3 == 3,      "third transaction ID = 3");
    CHECK(id1 < id2,     "IDs are strictly increasing");
    CHECK(id2 < id3,     "IDs increase across rollback too");

    teardown();
}

static void test_double_begin_error(void)
{
    printf("\n[test_double_begin_error]\n");
    setup_one();

    trx_begin(&tm);
    int rc = trx_begin(&tm);   /* second BEGIN while one is active */
    CHECK(rc != MYDB_OK,       "nested BEGIN returns error");
    CHECK(tm.active == 1,      "still active after failed nested BEGIN");

    trx_rollback(&tm);
    teardown();
}

static void test_commit_without_begin(void)
{
    printf("\n[test_commit_without_begin]\n");
    setup_one();

    int rc = trx_commit(&tm);
    CHECK(rc == MYDB_ERR_NO_TXN, "commit without begin returns MYDB_ERR_NO_TXN");

    teardown();
}

static void test_rollback_without_begin(void)
{
    printf("\n[test_rollback_without_begin]\n");
    setup_one();

    int rc = trx_rollback(&tm);
    CHECK(rc == MYDB_ERR_NO_TXN, "rollback without begin returns MYDB_ERR_NO_TXN");

    teardown();
}

static void test_register_unregister(void)
{
    printf("\n[test_register_unregister]\n");
    setup_two();

    CHECK(tm.num_open == 2, "two tables registered");

    int rc = trx_unregister_table(&tm, TABLE_A);
    CHECK(rc == MYDB_OK,       "unregister TABLE_A returns OK");
    CHECK(tm.num_open == 1,    "num_open decremented");

    rc = trx_unregister_table(&tm, TABLE_A);
    CHECK(rc == MYDB_ERR_NOT_FOUND, "unregister again returns NOT_FOUND");

    /* re-register TABLE_A */
    rc = trx_register_table(&tm, TABLE_A, &dm_a);
    CHECK(rc == MYDB_OK,       "re-register returns OK");
    CHECK(tm.num_open == 2,    "back to 2 after re-register");

    /* duplicate registration */
    rc = trx_register_table(&tm, TABLE_A, &dm_a);
    CHECK(rc == MYDB_ERR,      "duplicate register returns error");

    teardown();
}

/*
 * Test that COMMIT actually writes dirty pages to disk.
 * Modify a page in the buffer pool, commit, then read
 * the page directly via disk_read_page and verify the change landed.
 */
static void test_commit_flushes_to_disk(void)
{
    printf("\n[test_commit_flushes_to_disk]\n");
    setup_one();

    trx_begin(&tm);

    /* write a marker into page 1 through the buffer pool */
    uint8_t *page = bp_fetch_page(&bp, &dm_a, TABLE_A, 1);
    memcpy(page, "COMMITTED", 9);
    bp_unpin_page(&bp, TABLE_A, 1, /*dirty=*/1);

    trx_commit(&tm);

    /* read page 1 directly from disk — bypassing the buffer pool */
    uint8_t disk_buf[PAGE_SIZE];
    int rc = disk_read_page(&dm_a, 1, disk_buf);
    CHECK(rc == MYDB_OK,                         "disk_read_page OK after commit");
    CHECK(memcmp(disk_buf, "COMMITTED", 9) == 0, "committed data is on disk");

    teardown();
}

/*
 * Test that ROLLBACK discards dirty pages.
 * Write an initial value to disk directly, then modify it through the
 * buffer pool, rollback, and confirm the disk still holds the original.
 */
static void test_rollback_discards_dirty(void)
{
    printf("\n[test_rollback_discards_dirty]\n");
    setup_one();

    /* write known content to page 1 on disk directly (pre-transaction state) */
    uint8_t original[PAGE_SIZE];
    memset(original, 0, PAGE_SIZE);
    memcpy(original, "ORIGINAL", 8);
    disk_write_page(&dm_a, 1, original);

    trx_begin(&tm);

    /* overwrite through the buffer pool (in-memory only so far) */
    uint8_t *page = bp_fetch_page(&bp, &dm_a, TABLE_A, 1);
    memcpy(page, "MODIFIED", 8);
    bp_unpin_page(&bp, TABLE_A, 1, /*dirty=*/1);

    trx_rollback(&tm);

    /* page 1 should have been evicted; disk should still hold "ORIGINAL" */
    uint8_t disk_buf[PAGE_SIZE];
    disk_read_page(&dm_a, 1, disk_buf);
    CHECK(memcmp(disk_buf, "ORIGINAL", 8) == 0, "disk unchanged after rollback");

    /* fetching the page again from buffer pool should reload "ORIGINAL" */
    trx_begin(&tm);
    page = bp_fetch_page(&bp, &dm_a, TABLE_A, 1);
    CHECK(memcmp(page, "ORIGINAL", 8) == 0, "buffer pool reloads original after rollback");
    bp_unpin_page(&bp, TABLE_A, 1, 0);
    trx_commit(&tm);

    teardown();
}

/* Commit across two tables — both should be flushed. */
static void test_commit_multiple_tables(void)
{
    printf("\n[test_commit_multiple_tables]\n");
    setup_two();

    trx_begin(&tm);

    uint8_t *pa = bp_fetch_page(&bp, &dm_a, TABLE_A, 1);
    uint8_t *pb = bp_fetch_page(&bp, &dm_b, TABLE_B, 1);
    memcpy(pa, "TABLE_A", 7);
    memcpy(pb, "TABLE_B", 7);
    bp_unpin_page(&bp, TABLE_A, 1, 1);
    bp_unpin_page(&bp, TABLE_B, 1, 1);

    int rc = trx_commit(&tm);
    CHECK(rc == MYDB_OK, "commit with two tables OK");

    uint8_t buf[PAGE_SIZE];
    disk_read_page(&dm_a, 1, buf);
    CHECK(memcmp(buf, "TABLE_A", 7) == 0, "table A data flushed to disk");

    disk_read_page(&dm_b, 1, buf);
    CHECK(memcmp(buf, "TABLE_B", 7) == 0, "table B data flushed to disk");

    teardown();
}

/* Rollback across two tables — both dirty pages should be discarded. */
static void test_rollback_multiple_tables(void)
{
    printf("\n[test_rollback_multiple_tables]\n");
    setup_two();

    /* lay down original content on disk for both tables */
    uint8_t orig[PAGE_SIZE]; memset(orig, 0, PAGE_SIZE);
    memcpy(orig, "ORIG_A", 6); disk_write_page(&dm_a, 1, orig);
    memcpy(orig, "ORIG_B", 6); disk_write_page(&dm_b, 1, orig);

    trx_begin(&tm);

    uint8_t *pa = bp_fetch_page(&bp, &dm_a, TABLE_A, 1);
    uint8_t *pb = bp_fetch_page(&bp, &dm_b, TABLE_B, 1);
    memcpy(pa, "MOD_A", 5);
    memcpy(pb, "MOD_B", 5);
    bp_unpin_page(&bp, TABLE_A, 1, 1);
    bp_unpin_page(&bp, TABLE_B, 1, 1);

    int rc = trx_rollback(&tm);
    CHECK(rc == MYDB_OK, "rollback with two tables OK");

    uint8_t buf[PAGE_SIZE];
    disk_read_page(&dm_a, 1, buf);
    CHECK(memcmp(buf, "ORIG_A", 6) == 0, "table A disk unchanged after rollback");

    disk_read_page(&dm_b, 1, buf);
    CHECK(memcmp(buf, "ORIG_B", 6) == 0, "table B disk unchanged after rollback");

    teardown();
}

/* Sequential transactions — IDs advance, each commit is independent. */
static void test_sequential_transactions(void)
{
    printf("\n[test_sequential_transactions]\n");
    setup_one();

    /* txn 1: write and commit */
    trx_begin(&tm);
    uint8_t *p = bp_fetch_page(&bp, &dm_a, TABLE_A, 1);
    memcpy(p, "TXN1", 4);
    bp_unpin_page(&bp, TABLE_A, 1, 1);
    trx_commit(&tm);

    /* txn 2: write and commit */
    trx_begin(&tm);
    p = bp_fetch_page(&bp, &dm_a, TABLE_A, 1);
    memcpy(p, "TXN2", 4);
    bp_unpin_page(&bp, TABLE_A, 1, 1);
    trx_commit(&tm);

    uint8_t buf[PAGE_SIZE];
    disk_read_page(&dm_a, 1, buf);
    CHECK(memcmp(buf, "TXN2", 4) == 0, "last committed value is on disk");
    CHECK(tm.next_id == 3,             "next_id advanced to 3 after two transactions");

    teardown();
}

/* ------------------------------------------------------------------ */

int main(void)
{
    printf("=== test_transaction ===\n");

    test_init();
    test_begin_commit();
    test_begin_rollback();
    test_monotonic_ids();
    test_double_begin_error();
    test_commit_without_begin();
    test_rollback_without_begin();
    test_register_unregister();
    test_commit_flushes_to_disk();
    test_rollback_discards_dirty();
    test_commit_multiple_tables();
    test_rollback_multiple_tables();
    test_sequential_transactions();

    printf("\n%d / %d tests passed\n", tests_passed, tests_run);
    return (tests_passed == tests_run) ? 0 : 1;
}
