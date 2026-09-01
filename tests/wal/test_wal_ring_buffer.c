#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "common.h"
#include "normal_wal/wal_types.h"
#include "wal_page.h"
#include "normal_wal/wal_segment_pool.h"
#include "normal_wal/wal_ring_buffer.h"

#define TEST_WAL_DIR "/tmp/mydb_test_wal_ring_buffer"

static int tests_run    = 0;
static int tests_passed = 0;

#define CHECK(cond, msg) do { \
    tests_run++; \
    if (cond) { tests_passed++; printf("  PASS: %s\n", msg); } \
    else       { printf("  FAIL: %s (line %d)\n", msg, __LINE__); } \
} while(0)

static void cleanup(void)
{
    char path[300];
    for (uint32_t i = 0; i < WAL_SEGMENT_POOL_SLOTS; i++) {
        snprintf(path, sizeof(path), "%s/wal_%u.mydb", TEST_WAL_DIR, i);
        unlink(path);
    }
    rmdir(TEST_WAL_DIR);
}

static WalRecordHeader make_header(uint64_t prev_lsn, uint64_t txn_id)
{
    WalRecordHeader hdr;
    memset(&hdr, 0, sizeof(hdr));
    hdr.prev_lsn  = prev_lsn;
    hdr.txn_id    = txn_id;
    hdr.table_id  = 5;
    hdr.page_no   = 7;
    hdr.rec_type  = WAL_REC_INSERT;
    return hdr;
}

/* ------------------------------------------------------------------ */

static void test_single_append(void)
{
    printf("\n[test_single_append]\n");
    WalRingBuffer rb;
    CHECK(wal_ring_buffer_init(&rb) == MYDB_OK, "init succeeds");
    CHECK(rb.current_lsn == 0 && rb.flush_lsn == 0, "current_lsn/flush_lsn start at 0");

    WalRecordHeader hdr = make_header(0, 1);
    uint8_t body[20] = {0};
    uint64_t lsn;
    int rc = wal_ring_buffer_append(&rb, &hdr, body, sizeof(body), &lsn);

    CHECK(rc == MYDB_OK, "append succeeds");
    CHECK(lsn == 0, "first record gets lsn 0");
    uint32_t total_len = WAL_RECORD_HEADER_SIZE + sizeof(body);
    CHECK(rb.current_lsn == total_len, "current_lsn advances by exactly this record's total_len");

    wal_ring_buffer_shutdown(&rb);
}

static void test_byte_offset_lsn(void)
{
    printf("\n[test_byte_offset_lsn]\n");
    WalRingBuffer rb;
    wal_ring_buffer_init(&rb);

    WalRecordHeader hdr1 = make_header(0, 1);
    uint8_t body1[30] = {0};
    uint64_t lsn1;
    wal_ring_buffer_append(&rb, &hdr1, body1, sizeof(body1), &lsn1);

    WalRecordHeader hdr2 = make_header(lsn1, 1);
    uint8_t body2[10] = {0};
    uint64_t lsn2;
    wal_ring_buffer_append(&rb, &hdr2, body2, sizeof(body2), &lsn2);

    uint32_t first_total_len = WAL_RECORD_HEADER_SIZE + sizeof(body1);
    CHECK(lsn2 == first_total_len,
          "second record's lsn == first record's exact serialized size, not a fixed stride");

    wal_ring_buffer_shutdown(&rb);
}

static void test_packing_and_no_spanning(void)
{
    printf("\n[test_packing_and_no_spanning]\n");
    WalRingBuffer rb;
    wal_ring_buffer_init(&rb);

    /* Several small records should land in the same frame. */
    uint32_t frame_after_first = 0;
    for (int i = 0; i < 5; i++) {
        WalRecordHeader hdr = make_header(0, 1);
        uint8_t body[50] = {0};
        uint64_t lsn;
        wal_ring_buffer_append(&rb, &hdr, body, sizeof(body), &lsn);
        if (i == 0) frame_after_first = rb.write_frame;
    }
    CHECK(rb.write_frame == frame_after_first,
          "5 small records (well under WAL_PAGE_USABLE total) stay in the same frame");
    CHECK(rb.write_frame_used == 5 * (WAL_RECORD_HEADER_SIZE + 50),
          "write_frame_used reflects all 5 records packed so far");

    /* Now append a record sized so it can't possibly fit in whatever's
     * left of this frame (5 * 94 = 470 bytes used, ~3590 remain) but is
     * still well under WAL_MAX_RECORD_SIZE on its own — must start a
     * fresh frame, not split, and not be rejected as oversized. */
    uint32_t before_frame = rb.write_frame;
    WalRecordHeader big_hdr = make_header(0, 1);
    size_t big_body_len = WAL_MAX_ROW_BODY - 100;
    uint8_t *big_body = malloc(big_body_len);
    memset(big_body, 0xCC, big_body_len);
    uint64_t big_lsn;
    int rc = wal_ring_buffer_append(&rb, &big_hdr, big_body, big_body_len, &big_lsn);

    CHECK(rc == MYDB_OK, "the big record itself is appended successfully");
    CHECK(rb.write_frame != before_frame, "it started a fresh frame rather than splitting");
    CHECK(rb.write_frame_used == WAL_RECORD_HEADER_SIZE + big_body_len,
          "the new frame holds exactly this one record so far");

    free(big_body);
    wal_ring_buffer_shutdown(&rb);
}

static void test_oversized_record_rejected(void)
{
    printf("\n[test_oversized_record_rejected]\n");
    WalRingBuffer rb;
    wal_ring_buffer_init(&rb);

    WalRecordHeader hdr = make_header(0, 1);
    size_t body_len = WAL_MAX_ROW_BODY + 1;   /* pushes total_len 1 byte over WAL_MAX_RECORD_SIZE */
    uint8_t *body = malloc(body_len);
    memset(body, 0, body_len);
    uint64_t lsn;
    int rc = wal_ring_buffer_append(&rb, &hdr, body, body_len, &lsn);

    CHECK(rc == MYDB_ERR, "a record exceeding WAL_MAX_RECORD_SIZE is rejected (LARGE_WAL not built yet)");

    free(body);
    wal_ring_buffer_shutdown(&rb);
}

static void test_end_to_end_drain(void)
{
    printf("\n[test_end_to_end_drain]\n");
    cleanup();

    WalRingBuffer rb;
    wal_ring_buffer_init(&rb);

    WalSegmentPool pool;
    CHECK(wal_segment_pool_init(&pool, TEST_WAL_DIR, 1) == MYDB_OK, "segment pool init succeeds");
    uint32_t seg_slot, seg_page = 1, seg_offset = 0;
    CHECK(wal_segment_pool_claim_next(&pool, &seg_slot) == MYDB_OK, "segment claim succeeds");

    /* Append enough records to span roughly 2-3 frames. */
    uint64_t last_lsn = 0;
    for (int i = 0; i < 200; i++) {
        WalRecordHeader hdr = make_header(0, 1);
        uint8_t body[80];
        memset(body, (uint8_t)i, sizeof(body));
        uint64_t lsn;
        int rc = wal_ring_buffer_append(&rb, &hdr, body, sizeof(body), &lsn);
        CHECK(rc == MYDB_OK, "append in the drain-test loop succeeds");
        last_lsn = lsn;
    }
    CHECK(rb.write_frame >= 2, "200 records of 124 bytes each spans at least 3 frames");

    uint32_t frames_before_drain = rb.write_frame;   /* closed frames drainable */
    uint32_t drain_frame = 0;                         /* caller-owned drain position */
    int rc = wal_ring_buffer_drain(&rb, &pool, &drain_frame, &seg_slot, &seg_page, &seg_offset);
    CHECK(rc == MYDB_OK, "drain succeeds");
    CHECK(drain_frame == frames_before_drain,
          "drain position caught up to write_frame (every closed frame drained)");
    CHECK(rb.flush_lsn > 0, "flush_lsn advanced past 0");
    CHECK(wal_ring_buffer_lsn_is_flushed(&rb, rb.flush_lsn), "lsn_is_flushed agrees at flush_lsn itself");
    CHECK(!wal_ring_buffer_lsn_is_flushed(&rb, last_lsn) || drain_frame == rb.write_frame,
          "the still-open last frame's own last record isn't claimed flushed unless it was also drained");

    /* Spot-check: frame 0's content, read back from the segment, matches
     * what's still sitting in the ring buffer at that same frame index. */
    uint8_t from_disk[WAL_PAGE_SIZE];
    CHECK(wal_segment_pool_read_page(&pool, seg_slot, 1, from_disk) == MYDB_OK,
          "reading segment page 1 back succeeds");
    CHECK(memcmp(from_disk, rb.buf, WAL_PAGE_SIZE) == 0,
          "frame 0's bytes on disk match the ring buffer's own copy exactly");

    wal_segment_pool_shutdown(&pool);
    wal_ring_buffer_shutdown(&rb);
    cleanup();
}

/* ------------------------------------------------------------------ */

int main(void)
{
    printf("=== test_wal_ring_buffer ===\n");

    test_single_append();
    test_byte_offset_lsn();
    test_packing_and_no_spanning();
    test_oversized_record_rejected();
    test_end_to_end_drain();

    printf("\nResults: %d/%d passed\n", tests_passed, tests_run);
    return (tests_passed == tests_run) ? 0 : 1;
}
