#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <sys/stat.h>

#include "common.h"
#include "large_wal/large_wal_state.h"

#define TEST_WAL_DIR "/tmp/mydb_test_large_wal_state"

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
    snprintf(path, sizeof(path), "%s/large_wal_state.mydb", TEST_WAL_DIR);
    unlink(path);
    rmdir(TEST_WAL_DIR);
}

static void test_fresh_open_starts_at_zero(void)
{
    printf("\n[test_fresh_open_starts_at_zero]\n");
    cleanup();
    mkdir(TEST_WAL_DIR, 0755);

    LargeWalState st;
    CHECK(large_wal_state_open(&st, TEST_WAL_DIR) == MYDB_OK, "fresh open/create succeeds");
    CHECK(st.flush_lsn == 0, "flush_lsn starts at 0");

    char path[300];
    snprintf(path, sizeof(path), "%s/large_wal_state.mydb", TEST_WAL_DIR);
    struct stat s;
    CHECK(stat(path, &s) == 0, "the file was actually created on disk");

    large_wal_state_close(&st);
    cleanup();
}

static void test_advance_persists_and_rejects_backward(void)
{
    printf("\n[test_advance_persists_and_rejects_backward]\n");
    cleanup();
    mkdir(TEST_WAL_DIR, 0755);

    LargeWalState st;
    large_wal_state_open(&st, TEST_WAL_DIR);

    CHECK(large_wal_state_advance(&st, 100) == MYDB_OK, "advance to 100 succeeds");
    CHECK(st.flush_lsn == 100, "flush_lsn is now 100");

    CHECK(large_wal_state_advance(&st, 50) == MYDB_ERR, "advancing backward (50 < 100) is rejected");
    CHECK(st.flush_lsn == 100, "flush_lsn unchanged after the rejected backward advance");

    CHECK(large_wal_state_advance(&st, 100) == MYDB_OK, "advancing to the same value is accepted");
    CHECK(large_wal_state_advance(&st, 250) == MYDB_OK, "advancing forward again succeeds");
    CHECK(st.flush_lsn == 250, "flush_lsn is now 250");

    large_wal_state_close(&st);
    cleanup();
}

static void test_close_reopen_reloads(void)
{
    printf("\n[test_close_reopen_reloads]\n");
    cleanup();
    mkdir(TEST_WAL_DIR, 0755);

    LargeWalState st;
    large_wal_state_open(&st, TEST_WAL_DIR);
    large_wal_state_advance(&st, 777);
    large_wal_state_close(&st);

    LargeWalState reloaded;
    CHECK(large_wal_state_open(&reloaded, TEST_WAL_DIR) == MYDB_OK, "reopen succeeds");
    CHECK(reloaded.flush_lsn == 777, "flush_lsn survives close+reopen");

    large_wal_state_close(&reloaded);
    cleanup();
}

int main(void)
{
    printf("=== test_large_wal_state ===\n");

    test_fresh_open_starts_at_zero();
    test_advance_persists_and_rejects_backward();
    test_close_reopen_reloads();

    printf("\nResults: %d/%d passed\n", tests_passed, tests_run);
    return (tests_passed == tests_run) ? 0 : 1;
}
