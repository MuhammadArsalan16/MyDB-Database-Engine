#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#include "common.h"
#include "normal_wal/wal_types.h"
#include "normal_wal/wal_page.h"
#include "normal_wal/wal_segment_pool.h"
#include "normal_wal/wal_ring_buffer.h"
#include "normal_wal/wal_flusher.h"

#define TEST_WAL_DIR "/tmp/mydb_test_wal_flusher"

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

static WalRecordHeader make_header(void)
{
    WalRecordHeader hdr;
    memset(&hdr, 0, sizeof(hdr));
    hdr.table_id = 5;
    hdr.page_no  = 7;
    hdr.rec_type = WAL_REC_INSERT;
    return hdr;
}

/* Appends one record forcing the current ring frame to close (a body
 * far bigger than any plausible remaining space) — used to make a
 * still-open frame's earlier records drainable, since this phase's
 * Flusher only ever drains fully-closed frames. */
static uint64_t force_frame_close(WalRingBuffer *rb)
{
    WalRecordHeader hdr = make_header();
    uint8_t body[WAL_MAX_ROW_BODY];
    memset(body, 0xEE, sizeof(body));
    uint64_t lsn;
    wal_ring_buffer_append(rb, &hdr, body, sizeof(body), &lsn);
    return lsn;
}

/* Validates every actually-written page in the given segment slot:
 * checksum must pass, and every record packed into it must itself
 * deserialize (checksum-valid). next_empty_page is wal_segment_pool_
 * write's own out-param after the last write — since each Flusher call
 * writes an exact whole WAL_PAGE_SIZE chunk, that always lands exactly
 * on a page boundary, leaving *page_no pointing at the next, still-
 * empty page (never itself written) — so the scan is exclusive of it.
 * Returns 1 if every page/record checked out, 0 on the first failure. */
static int verify_segment_integrity(WalSegmentPool *pool, uint32_t slot_index, uint32_t next_empty_page)
{
    for (uint32_t p = 1; p < next_empty_page; p++) {
        uint8_t buf[WAL_PAGE_SIZE];
        if (wal_segment_pool_read_page(pool, slot_index, p, buf) != MYDB_OK) return 0;

        WalPageHeader phdr;
        if (wal_page_header_deserialize(buf, &phdr) != MYDB_OK) return 0;

        uint32_t off = 0;
        while (off < phdr.data_len) {
            uint32_t total_len;
            memcpy(&total_len, buf + WAL_PAGE_HEADER_SIZE + off + 16, 4);   /* total_len field offset within WalRecordHeader */
            if (total_len < WAL_RECORD_HEADER_SIZE || off + total_len > phdr.data_len) return 0;

            WalRecordHeader rhdr;
            size_t body_len = total_len - WAL_RECORD_HEADER_SIZE;
            if (wal_record_header_deserialize(buf + WAL_PAGE_HEADER_SIZE + off, body_len, &rhdr) != MYDB_OK)
                return 0;

            off += total_len;
        }
    }
    return 1;
}

/* ------------------------------------------------------------------ */

static void test_demand_signal(void)
{
    printf("\n[test_demand_signal]\n");
    cleanup();

    WalRingBuffer rb;
    wal_ring_buffer_init(&rb);
    WalSegmentPool pool;
    CHECK(wal_segment_pool_init(&pool, TEST_WAL_DIR, 1) == MYDB_OK, "segment pool init succeeds");
    uint32_t slot;
    CHECK(wal_segment_pool_claim_next(&pool, &slot) == MYDB_OK, "claim succeeds");

    WalFlusher flusher;
    CHECK(wal_flusher_start(&flusher, &rb, &pool, slot, NULL) == MYDB_OK, "flusher starts");

    uint64_t last_lsn = 0;
    for (int i = 0; i < 10; i++) {
        WalRecordHeader hdr = make_header();
        uint8_t body[50] = {0};
        uint64_t lsn;
        wal_ring_buffer_append(&rb, &hdr, body, sizeof(body), &lsn);
        last_lsn = lsn;
    }
    uint64_t closer_lsn = force_frame_close(&rb);
    (void)closer_lsn;

    wal_flusher_signal(&flusher);
    CHECK(wal_ring_buffer_wait_until_flushed(&rb, last_lsn) == MYDB_OK,
          "wait_until_flushed returns once the demand-signaled flusher catches up");
    CHECK(wal_ring_buffer_lsn_is_flushed(&rb, last_lsn), "lsn_is_flushed agrees");

    wal_flusher_stop(&flusher);
    CHECK(verify_segment_integrity(&pool, slot, flusher.seg_cursor.page_no),
          "every page/record the flusher wrote deserializes and checksums cleanly");

    wal_segment_pool_shutdown(&pool);
    wal_ring_buffer_shutdown(&rb);
    cleanup();
}

static void test_periodic_timeout(void)
{
    printf("\n[test_periodic_timeout]\n");
    cleanup();

    WalRingBuffer rb;
    wal_ring_buffer_init(&rb);
    WalSegmentPool pool;
    wal_segment_pool_init(&pool, TEST_WAL_DIR, 1);
    uint32_t slot;
    wal_segment_pool_claim_next(&pool, &slot);

    WalFlusher flusher;
    CHECK(wal_flusher_start(&flusher, &rb, &pool, slot, NULL) == MYDB_OK, "flusher starts");

    /* Append enough 144-byte records (no signal at all) to close at
     * least frame 0 — track the last lsn that landed in frame 0. */
    uint64_t frame0_last_lsn = 0;
    uint32_t initial_frame = rb.write_frame;
    for (int i = 0; i < 40 && rb.write_frame == initial_frame; i++) {
        WalRecordHeader hdr = make_header();
        uint8_t body[100] = {0};
        uint64_t lsn;
        wal_ring_buffer_append(&rb, &hdr, body, sizeof(body), &lsn);
        if (rb.write_frame == initial_frame) frame0_last_lsn = lsn;
    }
    CHECK(rb.write_frame != initial_frame, "enough records were appended to close frame 0");

    /* No wal_flusher_signal call here — this test is specifically about
     * the periodic WAL_FLUSHER_PERIODIC_MS timeout path. */
    struct timespec wait_ts = { 0, 200 * 1000 * 1000L };   /* 200ms, 10x the interval */
    nanosleep(&wait_ts, NULL);

    CHECK(wal_ring_buffer_lsn_is_flushed(&rb, frame0_last_lsn),
          "the periodic timeout drained frame 0 with no explicit signal");

    wal_flusher_stop(&flusher);
    wal_segment_pool_shutdown(&pool);
    wal_ring_buffer_shutdown(&rb);
    cleanup();
}

#define APPENDER_THREADS 4
#define APPENDS_PER_THREAD 50

typedef struct {
    WalRingBuffer *rb;
    uint64_t       last_lsn;
} AppenderArg;

static void *appender_thread(void *arg)
{
    AppenderArg *a = (AppenderArg *)arg;
    for (int i = 0; i < APPENDS_PER_THREAD; i++) {
        WalRecordHeader hdr = make_header();
        uint8_t body[100];
        memset(body, (uint8_t)i, sizeof(body));
        uint64_t lsn;
        if (wal_ring_buffer_append(a->rb, &hdr, body, sizeof(body), &lsn) == MYDB_OK)
            a->last_lsn = lsn;
    }
    return NULL;
}

static void test_concurrent_appenders(void)
{
    printf("\n[test_concurrent_appenders]\n");
    cleanup();

    WalRingBuffer rb;
    wal_ring_buffer_init(&rb);
    WalSegmentPool pool;
    wal_segment_pool_init(&pool, TEST_WAL_DIR, 1);
    uint32_t slot;
    wal_segment_pool_claim_next(&pool, &slot);

    WalFlusher flusher;
    CHECK(wal_flusher_start(&flusher, &rb, &pool, slot, NULL) == MYDB_OK, "flusher starts");

    pthread_t threads[APPENDER_THREADS];
    AppenderArg args[APPENDER_THREADS];
    for (int i = 0; i < APPENDER_THREADS; i++) {
        args[i].rb = &rb;
        args[i].last_lsn = 0;
        pthread_create(&threads[i], NULL, appender_thread, &args[i]);
    }
    for (int i = 0; i < APPENDER_THREADS; i++)
        pthread_join(threads[i], NULL);

    uint64_t expected_total = (uint64_t)APPENDER_THREADS * APPENDS_PER_THREAD
                             * (WAL_RECORD_HEADER_SIZE + 100);
    CHECK(rb.current_lsn == expected_total,
          "current_lsn reflects every concurrent append exactly once (no lost/duplicated records)");

    uint64_t max_last_lsn = 0;
    for (int i = 0; i < APPENDER_THREADS; i++)
        if (args[i].last_lsn > max_last_lsn) max_last_lsn = args[i].last_lsn;

    force_frame_close(&rb);   /* so the frame holding max_last_lsn is drainable */
    wal_flusher_signal(&flusher);
    CHECK(wal_ring_buffer_wait_until_flushed(&rb, max_last_lsn) == MYDB_OK,
          "wait_until_flushed returns after concurrent appends settle");

    wal_flusher_stop(&flusher);
    CHECK(verify_segment_integrity(&pool, slot, flusher.seg_cursor.page_no),
          "every page/record survives concurrent appends + real threaded draining intact — "
          "this is the correctness property the lock/snapshot design exists for");

    wal_segment_pool_shutdown(&pool);
    wal_ring_buffer_shutdown(&rb);
    cleanup();
}

static void test_stop_join(void)
{
    printf("\n[test_stop_join]\n");
    cleanup();

    WalRingBuffer rb;
    wal_ring_buffer_init(&rb);
    WalSegmentPool pool;
    wal_segment_pool_init(&pool, TEST_WAL_DIR, 1);
    uint32_t slot;
    wal_segment_pool_claim_next(&pool, &slot);

    WalFlusher flusher;
    wal_flusher_start(&flusher, &rb, &pool, slot, NULL);

    int rc = wal_flusher_stop(&flusher);
    CHECK(rc == MYDB_OK, "stop returns promptly, well under a full periodic cycle's worth of waiting");
    CHECK(flusher.started == 0, "started flag cleared after stop");

    int rc2 = wal_flusher_stop(&flusher);
    CHECK(rc2 == MYDB_OK, "a second stop() call is a harmless no-op, not a crash");

    wal_segment_pool_shutdown(&pool);
    wal_ring_buffer_shutdown(&rb);
    cleanup();
}

/* ------------------------------------------------------------------ */

int main(void)
{
    printf("=== test_wal_flusher ===\n");

    test_demand_signal();
    test_periodic_timeout();
    test_concurrent_appenders();
    test_stop_join();

    printf("\nResults: %d/%d passed\n", tests_passed, tests_run);
    return (tests_passed == tests_run) ? 0 : 1;
}