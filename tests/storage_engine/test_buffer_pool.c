#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <assert.h>

#include "buffer_pool.h"
#include "page.h"

#define TEST_FILE "/tmp/mydb_test_bp.mydb"
#define TABLE_ID  0

static int tests_run    = 0;
static int tests_passed = 0;

#define CHECK(cond, msg) do { \
    tests_run++; \
    if (cond) { tests_passed++; printf("  PASS: %s\n", msg); } \
    else       { printf("  FAIL: %s (line %d)\n", msg, __LINE__); } \
} while(0)

static void cleanup(void) { unlink(TEST_FILE); }

/* ------------------------------------------------------------------ */

static void test_fetch_and_unpin(void)
{
    printf("\n[test_fetch_and_unpin]\n");
    cleanup();

    DiskManager dm;
    disk_create(&dm, TEST_FILE);

    /* Allocate a page and write a known pattern to it */
    uint32_t pno;
    disk_alloc_page(&dm, &pno); /* page 1 */
    uint8_t wbuf[PAGE_SIZE];
    memset(wbuf, 0xAB, PAGE_SIZE);
    disk_write_page(&dm, pno, wbuf);

    BufferPool bp;
    bp_init(&bp);

    /* Fetch the page — should load from disk */
    uint8_t *page = bp_fetch_page(&bp, &dm, TABLE_ID, pno);
    CHECK(page != NULL, "bp_fetch_page returns non-NULL");
    CHECK(memcmp(page, wbuf, PAGE_SIZE) == 0, "fetched data matches disk");

    /* Unpin without marking dirty */
    CHECK(bp_unpin_page(&bp, TABLE_ID, pno, 0) == MYDB_OK, "unpin succeeds");

    disk_close(&dm);
}

static void test_dirty_flush(void)
{
    printf("\n[test_dirty_flush]\n");
    cleanup();

    DiskManager dm;
    disk_create(&dm, TEST_FILE);
    uint32_t pno;
    disk_alloc_page(&dm, &pno);

    BufferPool bp;
    bp_init(&bp);

    /* Fetch, modify in the frame, unpin as dirty */
    uint8_t *page = bp_fetch_page(&bp, &dm, TABLE_ID, pno);
    memset(page, 0xCD, PAGE_SIZE);
    bp_unpin_page(&bp, TABLE_ID, pno, 1); /* dirty=1 */

    /* Flush to disk explicitly */
    CHECK(bp_flush_page(&bp, &dm, TABLE_ID, pno) == MYDB_OK, "bp_flush_page succeeds");

    /* Read back from disk directly to verify the flush worked */
    uint8_t rbuf[PAGE_SIZE];
    disk_read_page(&dm, pno, rbuf);
    uint8_t expected[PAGE_SIZE];
    memset(expected, 0xCD, PAGE_SIZE);
    CHECK(memcmp(rbuf, expected, PAGE_SIZE) == 0, "dirty data persisted to disk");

    disk_close(&dm);
}

static void test_cache_hit(void)
{
    printf("\n[test_cache_hit]\n");
    cleanup();

    DiskManager dm;
    disk_create(&dm, TEST_FILE);
    uint32_t pno;
    disk_alloc_page(&dm, &pno);

    BufferPool bp;
    bp_init(&bp);

    uint8_t *p1 = bp_fetch_page(&bp, &dm, TABLE_ID, pno);
    memset(p1, 0x77, PAGE_SIZE);
    bp_unpin_page(&bp, TABLE_ID, pno, 0);

    /* Fetch the same page again — must return the same frame pointer */
    uint8_t *p2 = bp_fetch_page(&bp, &dm, TABLE_ID, pno);
    CHECK(p1 == p2, "second fetch returns same frame (cache hit)");
    bp_unpin_page(&bp, TABLE_ID, pno, 0);

    disk_close(&dm);
}

static void test_lru_eviction(void)
{
    printf("\n[test_lru_eviction]\n");
    cleanup();

    DiskManager dm;
    disk_create(&dm, TEST_FILE);

    /* Allocate BUFFER_POOL_SIZE + 1 pages */
    for (int i = 0; i < BUFFER_POOL_SIZE + 1; i++) {
        uint32_t pno;
        disk_alloc_page(&dm, &pno);
    }

    BufferPool bp;
    bp_init(&bp);

    /* Fill all 64 frames by fetching pages 1..64 and immediately unpinning */
    for (int i = 1; i <= BUFFER_POOL_SIZE; i++) {
        uint8_t *p = bp_fetch_page(&bp, &dm, TABLE_ID, (uint32_t)i);
        CHECK(p != NULL, "fetch succeeds while frames available");
        bp_unpin_page(&bp, TABLE_ID, (uint32_t)i, 0);
    }

    /* Fetching one more page (65) should evict the LRU frame (page 1) */
    uint8_t *p65 = bp_fetch_page(&bp, &dm, TABLE_ID, BUFFER_POOL_SIZE + 1);
    CHECK(p65 != NULL, "fetch page 65 succeeds via LRU eviction");
    bp_unpin_page(&bp, TABLE_ID, BUFFER_POOL_SIZE + 1, 0);

    disk_close(&dm);
}

static void test_page0_rejected(void)
{
    printf("\n[test_page0_rejected]\n");
    cleanup();

    DiskManager dm;
    disk_create(&dm, TEST_FILE);

    BufferPool bp;
    bp_init(&bp);

    /* page 0 must never go through the buffer pool */
    CHECK(bp_fetch_page(&bp, &dm, TABLE_ID, 0) == NULL,
          "bp_fetch_page(page 0) returns NULL");

    disk_close(&dm);
}

static void test_alloc_page(void)
{
    printf("\n[test_alloc_page]\n");
    cleanup();

    DiskManager dm;
    disk_create(&dm, TEST_FILE);

    BufferPool bp;
    bp_init(&bp);

    uint32_t new_pno;
    uint8_t *p = bp_alloc_page(&bp, &dm, TABLE_ID, &new_pno);
    CHECK(p != NULL,    "bp_alloc_page returns non-NULL");
    CHECK(new_pno == 1, "first allocated page is page 1");

    /* Write something and flush */
    memset(p, 0xEE, PAGE_SIZE);
    bp_unpin_page(&bp, TABLE_ID, new_pno, 1);
    bp_flush_page(&bp, &dm, TABLE_ID, new_pno);

    /* Verify on disk */
    uint8_t rbuf[PAGE_SIZE];
    disk_read_page(&dm, new_pno, rbuf);
    uint8_t expected[PAGE_SIZE];
    memset(expected, 0xEE, PAGE_SIZE);
    CHECK(memcmp(rbuf, expected, PAGE_SIZE) == 0, "alloc_page data flushed correctly");

    disk_close(&dm);
}

/* ------------------------------------------------------------------ */

int main(void)
{
    printf("=== test_buffer_pool ===\n");

    test_fetch_and_unpin();
    test_dirty_flush();
    test_cache_hit();
    test_lru_eviction();
    test_page0_rejected();
    test_alloc_page();

    printf("\nResults: %d/%d passed\n", tests_passed, tests_run);
    cleanup();
    return (tests_passed == tests_run) ? 0 : 1;
}
