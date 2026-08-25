#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <dirent.h>

#include "common.h"
#include "wal_manager.h"
#include "large_wal/large_wal_api.h"

#define TEST_WAL_DIR "/tmp/mydb_test_wal_manager"

static int tests_run    = 0;
static int tests_passed = 0;

#define CHECK(cond, msg) do { \
    tests_run++; \
    if (cond) { tests_passed++; printf("  PASS: %s\n", msg); } \
    else       { printf("  FAIL: %s (line %d)\n", msg, __LINE__); } \
} while(0)

static void cleanup(void)
{
    DIR *d = opendir(TEST_WAL_DIR);
    if (d) {
        struct dirent *ent;
        char path[400];
        while ((ent = readdir(d)) != NULL) {
            if (strcmp(ent->d_name, ".") == 0 || strcmp(ent->d_name, "..") == 0) continue;
            snprintf(path, sizeof(path), "%s/%s", TEST_WAL_DIR, ent->d_name);
            unlink(path);
        }
        closedir(d);
    }
    rmdir(TEST_WAL_DIR);
}

static void test_init_and_shutdown_wire_the_shared_worker(void)
{
    printf("\n[test_init_and_shutdown_wire_the_shared_worker]\n");
    cleanup();

    WalManager wm;
    CHECK(wal_manager_init(&wm, TEST_WAL_DIR, 1) == MYDB_OK,
          "a single init() call brings up the shared worker + both sub-managers");
    CHECK(wm.worker.started == 1, "the shared worker thread was started");
    CHECK(wm.nwm.flusher.worker == &wm.worker,
          "normal_wal's Flusher was handed a pointer to the same shared worker");
    CHECK(wm.lwm.lw_writer.worker == &wm.worker,
          "large_wal's writer was handed a pointer to the same shared worker");

    CHECK(wal_manager_shutdown(&wm) == MYDB_OK, "shutdown() succeeds cleanly");
    CHECK(wm.worker.started == 0, "the shared worker was stopped by shutdown()");

    cleanup();
}

static void test_large_wal_rollover_with_the_real_shared_worker(void)
{
    printf("\n[test_large_wal_rollover_with_the_real_shared_worker]\n");
    cleanup();

    WalManager wm;
    CHECK(wal_manager_init(&wm, TEST_WAL_DIR, 1) == MYDB_OK, "manager init succeeds");

    uint8_t seed[100];
    memset(seed, 0x11, sizeof(seed));
    LargeWalIndexEntry seed_entry;
    CHECK(large_wal_write(&wm.lwm, seed, sizeof(seed), 500, WAL_REC_LARGE_REF, &seed_entry) == MYDB_OK,
          "seed record written through the real api, real worker attached");

    uint32_t old_slot = wm.lwm.lw_writer.cur_slot_index;

    /* Force "nearly full" the same way test_large_wal_writer.c's own
     * rollover test does, but this time with a real (not NULL) worker
     * actually wired in — exercising mark_done()'s async-offload path
     * and write()'s wal_worker_wait() for real, not just the mechanics
     * with worker == NULL. */
    wm.lwm.lw_writer.cur_page_no = LARGE_WAL_SEGMENT_PAGES_PER_FILE - 1;

    uint8_t two_pages[LARGE_WAL_PAGE_USABLE + 1];
    memset(two_pages, 0x22, sizeof(two_pages));
    LargeWalIndexEntry entry;
    CHECK(large_wal_write(&wm.lwm, two_pages, sizeof(two_pages), 600, WAL_REC_LARGE_REF, &entry) == MYDB_OK,
          "a rollover-triggering write succeeds with the real shared worker doing the old segment's fsync");

    CHECK(wm.lwm.lw_pool.slots[old_slot].header.state == LSEG_DONE,
          "the old segment reached LSEG_DONE (its fsync was confirmed via wal_worker_wait before this call returned)");
    CHECK(entry.segment_no != wm.lwm.lw_pool.slots[old_slot].header.segment_no,
          "the record landed in a genuinely new segment");

    uint8_t out_buf[2 * PAGE_SIZE];
    uint32_t out_len = 0;
    CHECK(large_wal_get(&wm.lwm, 600, out_buf, &out_len) == MYDB_OK,
          "the rolled-over record resolves correctly");
    CHECK(out_len == sizeof(two_pages) && memcmp(out_buf, two_pages, sizeof(two_pages)) == 0,
          "the record reads back byte-for-byte correct after a real-worker-assisted rollover");

    wal_manager_shutdown(&wm);
    cleanup();
}

int main(void)
{
    printf("=== test_wal_manager ===\n");

    test_init_and_shutdown_wire_the_shared_worker();
    test_large_wal_rollover_with_the_real_shared_worker();

    printf("\nResults: %d/%d passed\n", tests_passed, tests_run);
    return (tests_passed == tests_run) ? 0 : 1;
}
