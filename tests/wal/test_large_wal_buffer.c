#include <stdio.h>
#include <stdint.h>

#include "common.h"
#include "large_wal/large_wal_buffer.h"

static int tests_run    = 0;
static int tests_passed = 0;

#define CHECK(cond, msg) do { \
    tests_run++; \
    if (cond) { tests_passed++; printf("  PASS: %s\n", msg); } \
    else       { printf("  FAIL: %s (line %d)\n", msg, __LINE__); } \
} while(0)

static void test_static_buffer_for_small_sizes(void)
{
    printf("\n[test_static_buffer_for_small_sizes]\n");

    for (uint32_t pages = 1; pages <= LARGE_WAL_STATIC_PAGES; pages++) {
        LargeWalBuffer buf;
        uint32_t total_size = pages * LARGE_WAL_PAGE_USABLE;   /* exact multiple */
        CHECK(large_wal_buffer_acquire(&buf, total_size) == MYDB_OK, "acquire succeeds");
        CHECK(buf.page_count == pages, "page_count matches the exact page-multiple size");
        CHECK(buf.buf == buf.static_buf, "buf points at the embedded static buffer");
        CHECK(buf.is_heap == 0, "is_heap is 0 for the static case");
        large_wal_buffer_release(&buf);
    }
}

static void test_heap_buffer_for_large_sizes(void)
{
    printf("\n[test_heap_buffer_for_large_sizes]\n");

    LargeWalBuffer buf;
    uint32_t total_size = (LARGE_WAL_STATIC_PAGES + 1) * LARGE_WAL_PAGE_USABLE;
    CHECK(large_wal_buffer_acquire(&buf, total_size) == MYDB_OK, "acquire succeeds");
    CHECK(buf.page_count == LARGE_WAL_STATIC_PAGES + 1, "page_count is one more than the static cap");
    CHECK(buf.buf != NULL && buf.buf != buf.static_buf, "buf points at a distinct heap block, not static_buf");
    CHECK(buf.is_heap == 1, "is_heap is 1 for the heap case");

    large_wal_buffer_release(&buf);
    CHECK(buf.buf == NULL, "release clears buf to NULL after freeing the heap block");
    CHECK(buf.is_heap == 0, "release resets is_heap");
}

static void test_release_safe_on_static_and_idempotent(void)
{
    printf("\n[test_release_safe_on_static_and_idempotent]\n");

    LargeWalBuffer buf;
    large_wal_buffer_acquire(&buf, LARGE_WAL_PAGE_USABLE);   /* 1 page, static case */

    large_wal_buffer_release(&buf);
    CHECK(1, "release on a static-buffer acquire does not crash");
    large_wal_buffer_release(&buf);
    CHECK(1, "calling release a second time (idempotent) does not crash or double-free");

    LargeWalBuffer heap_buf;
    large_wal_buffer_acquire(&heap_buf, (LARGE_WAL_STATIC_PAGES + 2) * LARGE_WAL_PAGE_USABLE);
    large_wal_buffer_release(&heap_buf);
    large_wal_buffer_release(&heap_buf);
    CHECK(1, "calling release twice on a heap acquire does not double-free");
}

static void test_acquire_rejects_zero_size(void)
{
    printf("\n[test_acquire_rejects_zero_size]\n");

    LargeWalBuffer buf;
    CHECK(large_wal_buffer_acquire(&buf, 0) == MYDB_ERR, "acquire rejects total_size == 0");
}

static void test_acquire_rejects_oversized_record(void)
{
    printf("\n[test_acquire_rejects_oversized_record]\n");

    LargeWalBuffer buf;
    uint32_t too_big = 256 * LARGE_WAL_PAGE_USABLE;   /* page_count == 256, exceeds uint8_t range */
    CHECK(large_wal_buffer_acquire(&buf, too_big) == MYDB_ERR,
          "acquire rejects a size whose page_count exceeds 255");

    uint32_t just_fits = 255 * LARGE_WAL_PAGE_USABLE;
    CHECK(large_wal_buffer_acquire(&buf, just_fits) == MYDB_OK,
          "acquire accepts page_count == 255 exactly, at the ceiling");
    CHECK(buf.page_count == 255, "page_count is exactly 255 at the ceiling");
    large_wal_buffer_release(&buf);
}

static void test_page_count_boundary_arithmetic(void)
{
    printf("\n[test_page_count_boundary_arithmetic]\n");

    LargeWalBuffer buf;

    CHECK(large_wal_buffer_acquire(&buf, LARGE_WAL_PAGE_USABLE) == MYDB_OK, "acquire at exact page boundary succeeds");
    CHECK(buf.page_count == 1, "exactly one page's worth of usable bytes -> page_count == 1, not 2");
    large_wal_buffer_release(&buf);

    CHECK(large_wal_buffer_acquire(&buf, LARGE_WAL_PAGE_USABLE + 1) == MYDB_OK, "acquire one byte over a boundary succeeds");
    CHECK(buf.page_count == 2, "one byte past a page boundary rolls page_count to 2");
    large_wal_buffer_release(&buf);

    CHECK(large_wal_buffer_acquire(&buf, 1) == MYDB_OK, "acquire for a 1-byte record succeeds");
    CHECK(buf.page_count == 1, "the smallest possible record still occupies page_count == 1");
    large_wal_buffer_release(&buf);
}

int main(void)
{
    printf("=== test_large_wal_buffer ===\n");

    test_static_buffer_for_small_sizes();
    test_heap_buffer_for_large_sizes();
    test_release_safe_on_static_and_idempotent();
    test_acquire_rejects_zero_size();
    test_acquire_rejects_oversized_record();
    test_page_count_boundary_arithmetic();

    printf("\nResults: %d/%d passed\n", tests_passed, tests_run);
    return (tests_passed == tests_run) ? 0 : 1;
}
