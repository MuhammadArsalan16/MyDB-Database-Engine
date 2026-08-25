#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <dirent.h>

#include "common.h"
#include "large_wal/large_wal_manager.h"
#include "large_wal/large_wal_api.h"

#define TEST_WAL_DIR "/tmp/mydb_test_large_wal_manager"

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

static void test_init_and_shutdown_bring_up_all_subpieces(void)
{
    printf("\n[test_init_and_shutdown_bring_up_all_subpieces]\n");
    cleanup();

    LargeWalManager mgr;
    CHECK(large_wal_manager_init(&mgr, TEST_WAL_DIR, 1, NULL) == MYDB_OK,
          "a single init() call brings up pool/registry/index/state/archiver/writer");
    CHECK(mgr.partition_id == 1, "mgr's own partition_id is set");
    CHECK(strcmp(mgr.wal_dir, TEST_WAL_DIR) == 0, "mgr's own wal_dir is set");
    CHECK(mgr.lw_writer.started == 1, "the writer thread was started as part of init()");
    CHECK(mgr.lw_pool.slots[mgr.lw_writer.cur_slot_index].header.state == LSEG_ACTIVE,
          "init() claimed an active segment for the writer to use");

    CHECK(large_wal_manager_shutdown(&mgr) == MYDB_OK, "shutdown() succeeds cleanly");
    CHECK(mgr.lw_writer.started == 0, "the writer thread was stopped by shutdown()");

    cleanup();
}

static void test_write_and_get_round_trip_through_api(void)
{
    printf("\n[test_write_and_get_round_trip_through_api]\n");
    cleanup();

    LargeWalManager mgr;
    CHECK(large_wal_manager_init(&mgr, TEST_WAL_DIR, 1, NULL) == MYDB_OK, "manager init succeeds");

    uint8_t content[2000];
    memset(content, 0x7A, sizeof(content));

    LargeWalIndexEntry entry;
    CHECK(large_wal_write(&mgr, content, sizeof(content), 42, WAL_REC_LARGE_REF, &entry) == MYDB_OK,
          "large_wal_write succeeds through the api layer");
    CHECK(entry.content_lsn == 42 && entry.total_size == sizeof(content),
          "the returned entry matches the submitted record");

    uint8_t out_buf[2000];
    uint32_t out_len = 0;
    CHECK(large_wal_get(&mgr, 42, out_buf, &out_len) == MYDB_OK,
          "large_wal_get resolves the record through the api layer");
    CHECK(out_len == sizeof(content) && memcmp(out_buf, content, sizeof(content)) == 0,
          "the resolved bytes match exactly");

    CHECK(large_wal_get(&mgr, 9999, out_buf, &out_len) == MYDB_ERR_NOT_FOUND,
          "large_wal_get on an unindexed content_lsn misses cleanly");

    large_wal_manager_shutdown(&mgr);
    cleanup();
}

/* ------------------------------------------------------------------ */

int main(void)
{
    printf("=== test_large_wal_manager ===\n");

    test_init_and_shutdown_bring_up_all_subpieces();
    test_write_and_get_round_trip_through_api();

    printf("\nResults: %d/%d passed\n", tests_passed, tests_run);
    return (tests_passed == tests_run) ? 0 : 1;
}
