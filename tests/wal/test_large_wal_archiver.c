#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/stat.h>
#include <dirent.h>

#include "common.h"
#include "large_wal/large_wal_page.h"
#include "large_wal/large_wal_segment.h"
#include "large_wal/large_wal_segment_pool.h"
#include "large_wal/large_wal_index.h"
#include "large_wal/large_wal_archiver.h"

#define TEST_WAL_DIR "/tmp/mydb_test_large_wal_archiver"

static int tests_run    = 0;
static int tests_passed = 0;

#define CHECK(cond, msg) do { \
    tests_run++; \
    if (cond) { tests_passed++; printf("  PASS: %s\n", msg); } \
    else       { printf("  FAIL: %s (line %d)\n", msg, __LINE__); } \
} while(0)

/* Removes everything under TEST_WAL_DIR (segment-pool slots, archival
 * copies whose segment_no varies run to run, and the index file), then
 * the directory itself. Safe whether or not the directory exists yet. */
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

/* Builds one complete PAGE_SIZE page buffer (real header + content) —
 * same helper test_large_wal_segment_pool.c uses. */
static void build_page(uint8_t *out, uint64_t content_lsn, uint8_t page_index, uint8_t fill, uint16_t data_len)
{
    LargeWalPageHeader hdr;
    memset(&hdr, 0, sizeof(hdr));
    hdr.id.file_type = FILETYPE_LARGE_WAL_PAGE;
    hdr.content_lsn  = content_lsn;
    hdr.page_index   = page_index;
    hdr.data_len     = data_len;
    memset(out, 0, PAGE_SIZE);
    memset(out + LARGE_WAL_PAGE_HEADER_ON_DISK_SIZE, fill, data_len);
    large_wal_page_header_serialize(&hdr, out);
}

/* Common fixture: init pool + archiver + index in the same wal_dir,
 * claim a segment, write a synthetic 2-page record (content_lsn=500),
 * mark_done it. Returns the segment's segment_no and end_lsn, and
 * leaves slot_index pointing at the DONE slot ready for copy_out. */
static void build_done_segment(LargeWalSegmentPool *pool, uint32_t *out_slot_index,
                                uint64_t *out_segment_no, uint64_t *out_end_lsn,
                                uint16_t *out_len_a, uint16_t *out_len_b)
{
    uint32_t slot_index;
    CHECK(large_wal_segment_pool_claim_next(pool, &slot_index) == MYDB_OK, "claim_next succeeds");

    uint16_t len_a = 6000, len_b = 7000;
    uint8_t page_a[PAGE_SIZE], page_b[PAGE_SIZE];
    build_page(page_a, 500, 0, 0xAA, len_a);
    build_page(page_b, 500, 1, 0xBB, len_b);

    CHECK(large_wal_segment_pool_write_page(pool, slot_index, 1, page_a) == MYDB_OK, "write page 1");
    CHECK(large_wal_segment_pool_write_page(pool, slot_index, 2, page_b) == MYDB_OK, "write page 2");

    uint64_t segment_no = pool->slots[slot_index].header.segment_no;
    CHECK(large_wal_segment_pool_mark_done(pool, slot_index, 500, 2) == MYDB_OK, "mark_done succeeds");

    *out_slot_index = slot_index;
    *out_segment_no = segment_no;
    *out_end_lsn    = 500;
    *out_len_a      = len_a;
    *out_len_b      = len_b;
}

/* ------------------------------------------------------------------ */

static void test_copy_out_frees_slot_and_content_readable_via_get(void)
{
    printf("\n[test_copy_out_frees_slot_and_content_readable_via_get]\n");
    cleanup();

    LargeWalSegmentPool pool;
    LargeWalArchiver    arc;
    LargeWalIndex        idx;
    CHECK(large_wal_segment_pool_init(&pool, TEST_WAL_DIR, 1) == MYDB_OK, "pool init succeeds");
    CHECK(large_wal_archiver_init(&arc, TEST_WAL_DIR) == MYDB_OK, "archiver init succeeds");
    CHECK(large_wal_index_open(&idx, TEST_WAL_DIR) == MYDB_OK, "index open succeeds");

    uint32_t slot_index; uint64_t segment_no, end_lsn; uint16_t len_a, len_b;
    build_done_segment(&pool, &slot_index, &segment_no, &end_lsn, &len_a, &len_b);

    CHECK(large_wal_archiver_copy_out(&arc, &pool, slot_index) == MYDB_OK, "copy_out succeeds");
    CHECK(pool.slots[slot_index].header.state == LSEG_FREE,
          "the rotation slot was freed back to LSEG_FREE");

    char archival_path[400];
    snprintf(archival_path, sizeof(archival_path), "%s/large_wal_archival_%llu.mydb",
             TEST_WAL_DIR, (unsigned long long)segment_no);
    struct stat st;
    CHECK(stat(archival_path, &st) == 0 && st.st_size == LARGE_WAL_SEGMENT_FILE_SIZE,
          "the holding-area copy exists on disk at the full segment file size");

    LargeWalIndexEntry e;
    memset(&e, 0, sizeof(e));
    e.content_lsn   = 500;
    e.rec_type      = WAL_REC_LARGE_REF;
    e.segment_no    = segment_no;
    e.start_page_no = 1;
    e.offset        = 0;
    e.page_count    = 2;
    e.total_size    = (uint32_t)len_a + len_b;
    CHECK(large_wal_index_insert(&idx, &e) == MYDB_OK, "index entry inserted for content_lsn 500");

    uint8_t out_buf[2 * PAGE_SIZE];
    uint32_t out_len = 0;
    CHECK(large_wal_get(&arc, &idx, 500, out_buf, &out_len) == MYDB_OK,
          "large_wal_get resolves content_lsn 500 after copy_out");
    CHECK(out_len == e.total_size, "the returned length matches the record's total_size");

    int bytes_ok = 1;
    for (uint16_t i = 0; i < len_a; i++) if (out_buf[i] != 0xAA) bytes_ok = 0;
    for (uint16_t i = 0; i < len_b; i++) if (out_buf[len_a + i] != 0xBB) bytes_ok = 0;
    CHECK(bytes_ok, "the reassembled bytes match the two original pages exactly, headers stripped");

    large_wal_index_close(&idx);
    large_wal_archiver_shutdown(&arc);
    large_wal_segment_pool_shutdown(&pool);
    cleanup();
}

static void test_try_free_gated_on_both_conditions(void)
{
    printf("\n[test_try_free_gated_on_both_conditions]\n");
    cleanup();

    LargeWalSegmentPool pool;
    LargeWalArchiver    arc;
    LargeWalIndex        idx;
    large_wal_segment_pool_init(&pool, TEST_WAL_DIR, 1);
    large_wal_archiver_init(&arc, TEST_WAL_DIR);
    large_wal_index_open(&idx, TEST_WAL_DIR);

    uint32_t slot_index; uint64_t segment_no, end_lsn; uint16_t len_a, len_b;
    build_done_segment(&pool, &slot_index, &segment_no, &end_lsn, &len_a, &len_b);
    large_wal_archiver_copy_out(&arc, &pool, slot_index);

    LargeWalIndexEntry e;
    memset(&e, 0, sizeof(e));
    e.content_lsn = 500; e.rec_type = WAL_REC_LARGE_REF; e.segment_no = segment_no;
    e.start_page_no = 1; e.page_count = 2; e.total_size = (uint32_t)len_a + len_b;
    large_wal_index_insert(&idx, &e);

    char archival_path[400];
    snprintf(archival_path, sizeof(archival_path), "%s/large_wal_archival_%llu.mydb",
             TEST_WAL_DIR, (unsigned long long)segment_no);

    int freed;
    CHECK(large_wal_archiver_try_free(&arc, &idx, segment_no, end_lsn,
                                       /*checkpoint_lsn=*/end_lsn, /*gate_b_cleared=*/1, &freed) == MYDB_OK,
          "try_free with checkpoint_lsn == end_lsn (Gate A fails: not strictly greater) succeeds");
    CHECK(freed == 0, "not freed: Gate A didn't clear");

    CHECK(large_wal_archiver_try_free(&arc, &idx, segment_no, end_lsn,
                                       /*checkpoint_lsn=*/end_lsn + 100, /*gate_b_cleared=*/0, &freed) == MYDB_OK,
          "try_free with Gate A clear but Gate B not cleared succeeds");
    CHECK(freed == 0, "not freed: Gate B didn't clear");

    struct stat st;
    CHECK(stat(archival_path, &st) == 0, "the holding-area file still exists — neither partial attempt freed it");

    LargeWalIndexEntry out;
    CHECK(large_wal_index_lookup(&idx, 500, &out) == MYDB_OK, "the index entry still resolves");

    CHECK(large_wal_archiver_try_free(&arc, &idx, segment_no, end_lsn,
                                       /*checkpoint_lsn=*/end_lsn + 100, /*gate_b_cleared=*/1, &freed) == MYDB_OK,
          "try_free with both gates clear succeeds");
    CHECK(freed == 1, "freed: both gates cleared");

    CHECK(stat(archival_path, &st) != 0, "the holding-area file was actually unlinked");
    CHECK(large_wal_index_lookup(&idx, 500, &out) == MYDB_ERR_NOT_FOUND,
          "the index entry was pruned by delete_by_segment");

    uint8_t out_buf[2 * PAGE_SIZE];
    uint32_t out_len;
    CHECK(large_wal_get(&arc, &idx, 500, out_buf, &out_len) == MYDB_ERR_NOT_FOUND,
          "large_wal_get can no longer resolve the freed content_lsn");

    large_wal_index_close(&idx);
    large_wal_archiver_shutdown(&arc);
    large_wal_segment_pool_shutdown(&pool);
    cleanup();
}

/* ------------------------------------------------------------------ */

int main(void)
{
    printf("=== test_large_wal_archiver ===\n");

    test_copy_out_frees_slot_and_content_readable_via_get();
    test_try_free_gated_on_both_conditions();

    printf("\nResults: %d/%d passed\n", tests_passed, tests_run);
    return (tests_passed == tests_run) ? 0 : 1;
}
