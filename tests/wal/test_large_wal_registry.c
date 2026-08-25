#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>

#include "common.h"
#include "large_wal/large_wal_registry.h"

#define TEST_FILE_A "/tmp/mydb_test_large_wal_registry_a.tmp"
#define TEST_FILE_B "/tmp/mydb_test_large_wal_registry_b.tmp"

static int tests_run    = 0;
static int tests_passed = 0;

#define CHECK(cond, msg) do { \
    tests_run++; \
    if (cond) { tests_passed++; printf("  PASS: %s\n", msg); } \
    else       { printf("  FAIL: %s (line %d)\n", msg, __LINE__); } \
} while(0)

static int fd_is_valid(int fd)
{
    return fcntl(fd, F_GETFD) != -1 || errno != EBADF;
}

static void test_register_insert_and_update(void)
{
    printf("\n[test_register_insert_and_update]\n");

    LargeWalRegistry reg;
    large_wal_registry_init(&reg);

    CHECK(large_wal_registry_register(&reg, 7, 42, 0) == MYDB_OK, "register a new segment_no succeeds");
    CHECK(reg.count == 1, "count is 1 after one register");

    int fd;
    CHECK(large_wal_registry_lookup(&reg, 7, &fd) == MYDB_OK && fd == 42, "lookup returns the registered fd");

    CHECK(large_wal_registry_register(&reg, 7, 99, 1) == MYDB_OK, "re-registering the same segment_no succeeds");
    CHECK(reg.count == 1, "count stays 1 -- update, not a second insert");
    CHECK(large_wal_registry_lookup(&reg, 7, &fd) == MYDB_OK && fd == 99, "lookup now returns the updated fd");

    reg.entries[0].fd = -1;   /* avoid a real close() on shutdown for this synthetic fd */
    large_wal_registry_shutdown(&reg);
}

static void test_lookup_miss(void)
{
    printf("\n[test_lookup_miss]\n");

    LargeWalRegistry reg;
    large_wal_registry_init(&reg);
    large_wal_registry_register(&reg, 1, 10, 0);

    int fd;
    CHECK(large_wal_registry_lookup(&reg, 999, &fd) == MYDB_ERR_NOT_FOUND,
          "looking up a segment_no that was never registered misses");

    reg.entries[0].fd = -1;
    large_wal_registry_shutdown(&reg);
}

static void test_remove(void)
{
    printf("\n[test_remove]\n");

    LargeWalRegistry reg;
    large_wal_registry_init(&reg);
    large_wal_registry_register(&reg, 1, 10, 0);
    large_wal_registry_register(&reg, 2, 20, 0);
    large_wal_registry_register(&reg, 3, 30, 0);
    CHECK(reg.count == 3, "3 entries present before remove");

    CHECK(large_wal_registry_remove(&reg, 2) == MYDB_OK, "remove succeeds");
    CHECK(reg.count == 2, "count drops to 2");

    int fd;
    CHECK(large_wal_registry_lookup(&reg, 2, &fd) == MYDB_ERR_NOT_FOUND, "segment 2 is gone");
    CHECK(large_wal_registry_lookup(&reg, 1, &fd) == MYDB_OK && fd == 10, "segment 1 survives untouched");
    CHECK(large_wal_registry_lookup(&reg, 3, &fd) == MYDB_OK && fd == 30, "segment 3 survives untouched");

    CHECK(large_wal_registry_remove(&reg, 999) == MYDB_OK,
          "removing a segment_no that isn't present is a harmless no-op");
    CHECK(reg.count == 2, "count unchanged by the no-op remove");

    reg.entries[0].fd = -1;
    reg.entries[1].fd = -1;
    large_wal_registry_shutdown(&reg);
}

static void test_shutdown_closes_only_owned_fds(void)
{
    printf("\n[test_shutdown_closes_only_owned_fds]\n");

    int fd_owned    = open(TEST_FILE_A, O_CREAT | O_RDWR, 0644);
    int fd_borrowed = open(TEST_FILE_B, O_CREAT | O_RDWR, 0644);
    CHECK(fd_owned >= 0 && fd_borrowed >= 0, "both real fds opened for the test");

    LargeWalRegistry reg;
    large_wal_registry_init(&reg);
    large_wal_registry_register(&reg, 1, fd_owned,    /*owns_fd=*/1);
    large_wal_registry_register(&reg, 2, fd_borrowed, /*owns_fd=*/0);

    large_wal_registry_shutdown(&reg);

    CHECK(!fd_is_valid(fd_owned), "the owns_fd=1 entry's fd was actually closed by shutdown");
    CHECK(fd_is_valid(fd_borrowed), "the owns_fd=0 (borrowed) entry's fd was left open by shutdown");

    close(fd_borrowed);
    unlink(TEST_FILE_A);
    unlink(TEST_FILE_B);
}

/* ------------------------------------------------------------------ */

int main(void)
{
    printf("=== test_large_wal_registry ===\n");

    test_register_insert_and_update();
    test_lookup_miss();
    test_remove();
    test_shutdown_closes_only_owned_fds();

    printf("\nResults: %d/%d passed\n", tests_passed, tests_run);
    return (tests_passed == tests_run) ? 0 : 1;
}
