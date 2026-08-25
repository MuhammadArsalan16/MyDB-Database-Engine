#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>

#include "common.h"
#include "wal_worker.h"

#define TEST_FILE "/tmp/mydb_test_wal_worker.tmp"

static int tests_run    = 0;
static int tests_passed = 0;

#define CHECK(cond, msg) do { \
    tests_run++; \
    if (cond) { tests_passed++; printf("  PASS: %s\n", msg); } \
    else       { printf("  FAIL: %s (line %d)\n", msg, __LINE__); } \
} while(0)

static void test_start_stop(void)
{
    printf("\n[test_start_stop]\n");

    WalWorker w;
    CHECK(wal_worker_start(&w) == MYDB_OK, "start succeeds");
    CHECK(wal_worker_stop(&w) == MYDB_OK, "stop succeeds");
    CHECK(wal_worker_stop(&w) == MYDB_OK, "calling stop() a second time is a harmless no-op");
}

static void test_wait_with_nothing_pending_is_a_noop(void)
{
    printf("\n[test_wait_with_nothing_pending_is_a_noop]\n");

    WalWorker w;
    wal_worker_start(&w);

    CHECK(wal_worker_wait(&w) == MYDB_OK, "wait() with nothing ever submitted is a cheap no-op");

    wal_worker_stop(&w);
}

static void test_async_fdatasync_round_trip(void)
{
    printf("\n[test_async_fdatasync_round_trip]\n");

    int fd = open(TEST_FILE, O_CREAT | O_RDWR, 0644);
    CHECK(fd >= 0, "test file opened");

    uint8_t buf[16] = {1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16};
    CHECK(write(fd, buf, sizeof(buf)) == (ssize_t)sizeof(buf), "wrote some bytes to sync");

    WalWorker w;
    wal_worker_start(&w);

    CHECK(wal_worker_async_fdatasync(&w, fd) == MYDB_OK, "async_fdatasync accepted");
    CHECK(wal_worker_wait(&w) == MYDB_OK, "wait() returns the fdatasync's result");

    wal_worker_stop(&w);
    close(fd);
    unlink(TEST_FILE);
}

static void test_back_to_back_submissions(void)
{
    printf("\n[test_back_to_back_submissions]\n");

    int fd = open(TEST_FILE, O_CREAT | O_RDWR, 0644);

    WalWorker w;
    wal_worker_start(&w);

    int all_ok = 1;
    for (int i = 0; i < 20; i++) {
        if (wal_worker_async_fdatasync(&w, fd) != MYDB_OK) all_ok = 0;
        if (wal_worker_wait(&w) != MYDB_OK) all_ok = 0;
    }
    CHECK(all_ok, "20 sequential submit+wait round trips from the same thread all succeed");

    wal_worker_stop(&w);
    close(fd);
    unlink(TEST_FILE);
}

int main(void)
{
    printf("=== test_wal_worker ===\n");

    test_start_stop();
    test_wait_with_nothing_pending_is_a_noop();
    test_async_fdatasync_round_trip();
    test_back_to_back_submissions();

    printf("\nResults: %d/%d passed\n", tests_passed, tests_run);
    return (tests_passed == tests_run) ? 0 : 1;
}
