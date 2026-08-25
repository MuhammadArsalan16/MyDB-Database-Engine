#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <dirent.h>

#include "common.h"
#include "normal_wal/normal_wal_manager.h"

#define TEST_WAL_DIR "/tmp/mydb_test_normal_wal_manager"

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

    NormalWalManager nwm;
    CHECK(normal_wal_manager_init(&nwm, TEST_WAL_DIR, 1, NULL) == MYDB_OK,
          "a single init() call brings up the ring buffer + pool + Flusher");
    CHECK(nwm.flusher.started == 1, "the Flusher thread was started as part of init()");
    CHECK(nwm.pool.slots[nwm.flusher.seg_slot_index].header.state == SEG_ACTIVE,
          "init() claimed an active segment for the Flusher to use");

    CHECK(normal_wal_manager_shutdown(&nwm) == MYDB_OK, "shutdown() succeeds cleanly");
    CHECK(nwm.flusher.started == 0, "the Flusher thread was stopped by shutdown()");

    cleanup();
}

int main(void)
{
    printf("=== test_normal_wal_manager ===\n");

    test_init_and_shutdown_bring_up_all_subpieces();

    printf("\nResults: %d/%d passed\n", tests_passed, tests_run);
    return (tests_passed == tests_run) ? 0 : 1;
}
