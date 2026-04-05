#include <stdio.h>
#include <assert.h>
#include <string.h>
#include <unistd.h>

#include "disk_manager.h"

#define TEST_FILE "/tmp/mydb_test_disk.mydb"

/* Simple test runner helpers */
static int tests_run    = 0;
static int tests_passed = 0;

#define CHECK(cond, msg) do { \
    tests_run++; \
    if (cond) { tests_passed++; printf("  PASS: %s\n", msg); } \
    else       { printf("  FAIL: %s (line %d)\n", msg, __LINE__); } \
} while(0)

/* Remove leftover test file before each test group */
static void cleanup(void) { unlink(TEST_FILE); }

/* ------------------------------------------------------------------ */

static void test_create_and_open(void)
{
    printf("\n[test_create_and_open]\n");
    cleanup();

    DiskManager dm;

    /* Create a new file */
    CHECK(disk_create(&dm, TEST_FILE) == MYDB_OK, "disk_create succeeds");
    CHECK(dm.num_pages == 1, "num_pages == 1 after create");
    CHECK(dm.fd >= 0, "fd is valid");
    disk_close(&dm);

    /* Open the same file and check the header persisted */
    CHECK(disk_open(&dm, TEST_FILE) == MYDB_OK, "disk_open succeeds");
    CHECK(dm.num_pages == 1, "num_pages == 1 after reopen");

    FileHeader fh;
    disk_read_header(&dm, &fh);
    CHECK(fh.magic == MYDB_MAGIC, "magic number matches");
    CHECK(fh.version == 1, "version == 1");
    CHECK(fh.root_page_no == INVALID_PAGE, "root_page_no is INVALID_PAGE");
    CHECK(fh.auto_incr == 1, "auto_incr starts at 1");

    disk_close(&dm);

    /* Creating the same file again must fail */
    CHECK(disk_create(&dm, TEST_FILE) == MYDB_ERR, "disk_create fails if file exists");
}

static void test_alloc_and_rw(void)
{
    printf("\n[test_alloc_and_rw]\n");
    cleanup();

    DiskManager dm;
    disk_create(&dm, TEST_FILE);

    /* Allocate 3 pages */
    uint32_t p1, p2, p3;
    CHECK(disk_alloc_page(&dm, &p1) == MYDB_OK && p1 == 1, "alloc page 1");
    CHECK(disk_alloc_page(&dm, &p2) == MYDB_OK && p2 == 2, "alloc page 2");
    CHECK(disk_alloc_page(&dm, &p3) == MYDB_OK && p3 == 3, "alloc page 3");
    CHECK(dm.num_pages == 4, "num_pages == 4 after 3 allocs");

    /* Write a known pattern to page 2 */
    uint8_t wbuf[PAGE_SIZE];
    memset(wbuf, 0xAB, PAGE_SIZE);
    CHECK(disk_write_page(&dm, 2, wbuf) == MYDB_OK, "write page 2");

    /* Read it back */
    uint8_t rbuf[PAGE_SIZE];
    CHECK(disk_read_page(&dm, 2, rbuf) == MYDB_OK, "read page 2");
    CHECK(memcmp(wbuf, rbuf, PAGE_SIZE) == 0, "read matches write");

    /* Verify page 1 is still zeroed */
    memset(wbuf, 0, PAGE_SIZE);
    CHECK(disk_read_page(&dm, 1, rbuf) == MYDB_OK, "read page 1");
    CHECK(memcmp(wbuf, rbuf, PAGE_SIZE) == 0, "page 1 is zeroed");

    disk_close(&dm);

    /* Reopen and verify num_pages persisted */
    disk_open(&dm, TEST_FILE);
    CHECK(dm.num_pages == 4, "num_pages persists across close/open");
    disk_close(&dm);
}

static void test_header_update(void)
{
    printf("\n[test_header_update]\n");
    cleanup();

    DiskManager dm;
    disk_create(&dm, TEST_FILE);

    /* Update the header (e.g., set root_page_no after creating the root) */
    FileHeader fh;
    disk_read_header(&dm, &fh);
    fh.root_page_no = 1;
    fh.auto_incr    = 42;
    disk_write_header(&dm, &fh);
    disk_close(&dm);

    /* Verify changes persisted */
    disk_open(&dm, TEST_FILE);
    disk_read_header(&dm, &fh);
    CHECK(fh.root_page_no == 1, "root_page_no persisted");
    CHECK(fh.auto_incr == 42, "auto_incr persisted");
    disk_close(&dm);
}

static void test_out_of_range_read(void)
{
    printf("\n[test_out_of_range_read]\n");
    cleanup();

    DiskManager dm;
    disk_create(&dm, TEST_FILE);

    uint8_t buf[PAGE_SIZE];
    /* page 5 does not exist (only page 0 was created) */
    CHECK(disk_read_page(&dm, 5, buf) == MYDB_ERR, "read out-of-range page fails");

    disk_close(&dm);
}

static void test_destroy(void)
{
    printf("\n[test_destroy]\n");
    cleanup();

    DiskManager dm;
    disk_create(&dm, TEST_FILE);
    disk_close(&dm);

    CHECK(disk_destroy(TEST_FILE) == MYDB_OK, "disk_destroy succeeds");
    /* Trying to open a destroyed file must fail */
    CHECK(disk_open(&dm, TEST_FILE) == MYDB_ERR, "open after destroy fails");
}

/* ------------------------------------------------------------------ */

int main(void)
{
    printf("=== test_disk_manager ===\n");

    test_create_and_open();
    test_alloc_and_rw();
    test_header_update();
    test_out_of_range_read();
    test_destroy();

    printf("\nResults: %d/%d passed\n", tests_passed, tests_run);
    return (tests_passed == tests_run) ? 0 : 1;
}
