#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/stat.h>

#include "common.h"
#include "large_wal/large_wal_index.h"

#define TEST_WAL_DIR "/tmp/mydb_test_large_wal_index"

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
    snprintf(path, sizeof(path), "%s/large_wal_index.mydb", TEST_WAL_DIR);
    unlink(path);
    rmdir(TEST_WAL_DIR);
}

static LargeWalIndexEntry make_entry(uint64_t content_lsn, uint64_t segment_no)
{
    LargeWalIndexEntry e;
    memset(&e, 0, sizeof(e));
    e.content_lsn   = content_lsn;
    e.rec_type      = WAL_REC_LARGE_REF;
    e.segment_no    = segment_no;
    e.start_page_no = 1;
    e.offset        = 0;
    e.page_count    = 2;
    e.total_size    = 30000;
    return e;
}

/* ------------------------------------------------------------------ */

static void test_open_creates_fresh_empty_file(void)
{
    printf("\n[test_open_creates_fresh_empty_file]\n");
    cleanup();
    mkdir(TEST_WAL_DIR, 0755);

    LargeWalIndex idx;
    CHECK(large_wal_index_open(&idx, TEST_WAL_DIR) == MYDB_OK, "fresh open/create succeeds");
    CHECK(idx.count == 0, "starts with zero entries");

    char path[300];
    snprintf(path, sizeof(path), "%s/large_wal_index.mydb", TEST_WAL_DIR);
    struct stat st;
    CHECK(stat(path, &st) == 0, "the file was actually created on disk");

    large_wal_index_close(&idx);
    cleanup();
}

static void test_insert_and_lookup(void)
{
    printf("\n[test_insert_and_lookup]\n");
    cleanup();
    mkdir(TEST_WAL_DIR, 0755);

    LargeWalIndex idx;
    CHECK(large_wal_index_open(&idx, TEST_WAL_DIR) == MYDB_OK, "open succeeds");

    LargeWalIndexEntry e = make_entry(500, 7);
    CHECK(large_wal_index_insert(&idx, &e) == MYDB_OK, "insert succeeds");
    CHECK(idx.count == 1, "count is 1 after one insert");

    LargeWalIndexEntry out;
    CHECK(large_wal_index_lookup(&idx, 500, &out) == MYDB_OK, "lookup finds the inserted entry");
    CHECK(out.segment_no == 7 && out.page_count == 2 && out.total_size == 30000,
          "the looked-up entry's fields match what was inserted");

    CHECK(large_wal_index_lookup(&idx, 999, &out) == MYDB_ERR_NOT_FOUND,
          "looking up a content_lsn that was never inserted misses");

    large_wal_index_close(&idx);
    cleanup();
}

static void test_insert_duplicate_rejected(void)
{
    printf("\n[test_insert_duplicate_rejected]\n");
    cleanup();
    mkdir(TEST_WAL_DIR, 0755);

    LargeWalIndex idx;
    large_wal_index_open(&idx, TEST_WAL_DIR);

    LargeWalIndexEntry e = make_entry(42, 1);
    CHECK(large_wal_index_insert(&idx, &e) == MYDB_OK, "first insert succeeds");

    LargeWalIndexEntry dup = make_entry(42, 2);   /* same content_lsn, different segment */
    CHECK(large_wal_index_insert(&idx, &dup) == MYDB_ERR_DUPLICATE,
          "inserting a second entry with the same content_lsn is rejected");
    CHECK(idx.count == 1, "the rejected duplicate did not get added");

    large_wal_index_close(&idx);
    cleanup();
}

static void test_delete_by_segment_prunes_only_matching(void)
{
    printf("\n[test_delete_by_segment_prunes_only_matching]\n");
    cleanup();
    mkdir(TEST_WAL_DIR, 0755);

    LargeWalIndex idx;
    large_wal_index_open(&idx, TEST_WAL_DIR);

    LargeWalIndexEntry a = make_entry(1, 10);
    LargeWalIndexEntry b = make_entry(2, 10);
    LargeWalIndexEntry c = make_entry(3, 20);
    large_wal_index_insert(&idx, &a);
    large_wal_index_insert(&idx, &b);
    large_wal_index_insert(&idx, &c);
    CHECK(idx.count == 3, "3 entries present before delete");

    CHECK(large_wal_index_delete_by_segment(&idx, 10) == MYDB_OK, "delete_by_segment succeeds");
    CHECK(idx.count == 1, "only the 2 entries for segment 10 were removed");

    LargeWalIndexEntry out;
    CHECK(large_wal_index_lookup(&idx, 1, &out) == MYDB_ERR_NOT_FOUND, "entry 1 (segment 10) is gone");
    CHECK(large_wal_index_lookup(&idx, 2, &out) == MYDB_ERR_NOT_FOUND, "entry 2 (segment 10) is gone");
    CHECK(large_wal_index_lookup(&idx, 3, &out) == MYDB_OK, "entry 3 (segment 20) survives untouched");

    CHECK(large_wal_index_delete_by_segment(&idx, 999) == MYDB_OK,
          "deleting a segment_no with no matching entries is a harmless no-op");
    CHECK(idx.count == 1, "count unchanged by the no-op delete");

    large_wal_index_close(&idx);
    cleanup();
}

static void test_close_reopen_reloads(void)
{
    printf("\n[test_close_reopen_reloads]\n");
    cleanup();
    mkdir(TEST_WAL_DIR, 0755);

    LargeWalIndex idx;
    large_wal_index_open(&idx, TEST_WAL_DIR);
    for (uint64_t i = 0; i < 20; i++) {
        LargeWalIndexEntry e = make_entry(1000 + i, i % 3);
        large_wal_index_insert(&idx, &e);
    }
    CHECK(idx.count == 20, "20 entries inserted");
    large_wal_index_close(&idx);

    LargeWalIndex reloaded;
    CHECK(large_wal_index_open(&reloaded, TEST_WAL_DIR) == MYDB_OK, "reopen succeeds");
    CHECK(reloaded.count == 20, "all 20 entries survive close+reopen");

    int all_found = 1;
    for (uint64_t i = 0; i < 20; i++) {
        LargeWalIndexEntry out;
        if (large_wal_index_lookup(&reloaded, 1000 + i, &out) != MYDB_OK ||
            out.segment_no != i % 3) {
            all_found = 0;
            break;
        }
    }
    CHECK(all_found,
          "every entry is found by lookup after reload with correct fields "
          "(hash map rebuilt correctly from the reloaded entries)");

    large_wal_index_close(&reloaded);
    cleanup();
}

/* ------------------------------------------------------------------ */

int main(void)
{
    printf("=== test_large_wal_index ===\n");

    test_open_creates_fresh_empty_file();
    test_insert_and_lookup();
    test_insert_duplicate_rejected();
    test_delete_by_segment_prunes_only_matching();
    test_close_reopen_reloads();

    printf("\nResults: %d/%d passed\n", tests_passed, tests_run);
    return (tests_passed == tests_run) ? 0 : 1;
}
