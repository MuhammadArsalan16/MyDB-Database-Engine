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
#include "large_wal/large_wal_registry.h"
#include "large_wal/large_wal_index.h"
#include "large_wal/large_wal_state.h"
#include "large_wal/large_wal_writer.h"

#define TEST_WAL_DIR "/tmp/mydb_test_large_wal_writer"

static int tests_run    = 0;
static int tests_passed = 0;

#define CHECK(cond, msg) do { \
    tests_run++; \
    if (cond) { tests_passed++; printf("  PASS: %s\n", msg); } \
    else       { printf("  FAIL: %s (line %d)\n", msg, __LINE__); } \
} while(0)

typedef struct {
    LargeWalSegmentPool pool;
    LargeWalRegistry      registry;
    LargeWalIndex           idx;
    LargeWalState             state;
} Deps;

static void cleanup_dir(void)
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

static void setup_deps(Deps *d)
{
    cleanup_dir();
    large_wal_segment_pool_init(&d->pool, TEST_WAL_DIR, 1);
    large_wal_registry_init(&d->registry);
    large_wal_index_open(&d->idx, TEST_WAL_DIR);
    large_wal_state_open(&d->state, TEST_WAL_DIR);
}

static void teardown_deps(Deps *d)
{
    large_wal_state_close(&d->state);
    large_wal_index_close(&d->idx);
    large_wal_registry_shutdown(&d->registry);
    large_wal_segment_pool_shutdown(&d->pool);
    cleanup_dir();
}

/* Builds one complete PAGE_SIZE page (real header + content) — for
 * tests that write directly through the pool API, bypassing the writer,
 * to set up a fixture the writer's own registration should still make
 * resolvable. */
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

/* Resolves segment_no -> fd via the registry directly and reassembles
 * page_count pages starting at start_page_no. Kept local and decoupled
 * from the manager/api layer on purpose — this test stays scoped to the
 * writer's own mechanics, same as test_large_wal_archiver.c's own copy. */
static int verify_content(LargeWalRegistry *reg, uint64_t segment_no,
                           uint32_t start_page_no, uint8_t page_count,
                           uint8_t *out_buf, uint32_t *out_len)
{
    int fd;
    if (large_wal_registry_lookup(reg, segment_no, &fd) != MYDB_OK) return MYDB_ERR;

    uint32_t written = 0;
    for (uint8_t p = 0; p < page_count; p++) {
        uint32_t page_no = start_page_no + p;
        uint8_t  page_buf[PAGE_SIZE];
        if (pread(fd, page_buf, PAGE_SIZE, (off_t)page_no * PAGE_SIZE) != PAGE_SIZE) return MYDB_ERR;

        LargeWalPageHeader hdr;
        if (large_wal_page_header_deserialize(page_buf, &hdr) != MYDB_OK) return MYDB_ERR;

        memcpy(out_buf + written, page_buf + LARGE_WAL_PAGE_HEADER_ON_DISK_SIZE, hdr.data_len);
        written += hdr.data_len;
    }
    *out_len = written;
    return MYDB_OK;
}

/* ------------------------------------------------------------------ */

static void test_submit_single_record_resolves_and_advances_state(void)
{
    printf("\n[test_submit_single_record_resolves_and_advances_state]\n");
    Deps d;
    setup_deps(&d);

    LargeWalWriter w;
    CHECK(large_wal_writer_init(&w, &d.pool, &d.registry, &d.idx, &d.state, NULL) == MYDB_OK, "writer init succeeds");
    CHECK(large_wal_writer_start(&w) == MYDB_OK, "writer start succeeds");

    uint8_t content[500];
    memset(content, 0xAB, sizeof(content));

    LargeWalIndexEntry entry;
    CHECK(large_wal_writer_submit(&w, content, sizeof(content), 100, WAL_REC_LARGE_REF, &entry) == MYDB_OK,
          "submit of a single small record succeeds");
    CHECK(entry.content_lsn == 100 && entry.page_count == 1 && entry.total_size == sizeof(content),
          "the returned index entry's fields match the submitted record");

    uint8_t out_buf[PAGE_SIZE];
    uint32_t out_len = 0;
    CHECK(verify_content(&d.registry, entry.segment_no, entry.start_page_no, entry.page_count,
                          out_buf, &out_len) == MYDB_OK,
          "the just-written record resolves via the registry");
    CHECK(out_len == sizeof(content) && memcmp(out_buf, content, sizeof(content)) == 0,
          "the resolved bytes match exactly");

    CHECK(d.state.flush_lsn == 100, "large_wal_state.flush_lsn advanced to the record's content_lsn");

    large_wal_writer_stop(&w);
    teardown_deps(&d);
}

static void test_registry_widening_resolves_rotation_resident_segment(void)
{
    printf("\n[test_registry_widening_resolves_rotation_resident_segment]\n");
    Deps d;
    setup_deps(&d);

    LargeWalWriter w;
    CHECK(large_wal_writer_init(&w, &d.pool, &d.registry, &d.idx, &d.state, NULL) == MYDB_OK,
          "writer init succeeds (claims a fresh segment, registers it)");

    /* Write directly through the pool API -- the writer thread was
     * never started, so this content did NOT come from submit(). Proves
     * registration happened purely as a side effect of init(). */
    uint16_t data_len = 1000;
    uint8_t page[PAGE_SIZE];
    build_page(page, 999, 0, 0xCD, data_len);
    CHECK(large_wal_segment_pool_write_page(&d.pool, w.cur_slot_index, w.cur_page_no, page) == MYDB_OK,
          "direct pool write succeeds");

    uint8_t out_buf[PAGE_SIZE];
    uint32_t out_len = 0;
    CHECK(verify_content(&d.registry, d.pool.slots[w.cur_slot_index].header.segment_no,
                          w.cur_page_no, 1, out_buf, &out_len) == MYDB_OK,
          "the registry resolves a rotation-resident segment with zero submits made — "
          "proves init()'s registration, not copy_out(), made this resolvable");
    CHECK(out_len == data_len, "resolved length matches");

    teardown_deps(&d);
}

static void test_sequential_submits_land_on_consecutive_pages(void)
{
    printf("\n[test_sequential_submits_land_on_consecutive_pages]\n");
    Deps d;
    setup_deps(&d);

    LargeWalWriter w;
    large_wal_writer_init(&w, &d.pool, &d.registry, &d.idx, &d.state, NULL);
    large_wal_writer_start(&w);

    uint64_t initial_segment_no = d.pool.slots[w.cur_slot_index].header.segment_no;

    uint8_t small[50];
    memset(small, 0x11, sizeof(small));

    LargeWalIndexEntry e1, e2, e3;
    large_wal_writer_submit(&w, small, sizeof(small), 200, WAL_REC_LARGE_REF, &e1);
    CHECK(e1.start_page_no == 1, "first submit starts at page 1");

    large_wal_writer_submit(&w, small, sizeof(small), 201, WAL_REC_LARGE_REF, &e2);
    CHECK(e2.start_page_no == 2, "second submit starts right after the first (page 2)");

    uint8_t two_pages[LARGE_WAL_PAGE_USABLE + 10];
    memset(two_pages, 0x22, sizeof(two_pages));
    large_wal_writer_submit(&w, two_pages, sizeof(two_pages), 202, WAL_REC_LARGE_REF, &e3);
    CHECK(e3.start_page_no == 3 && e3.page_count == 2, "third submit (2 pages) starts at page 3");

    CHECK(e1.segment_no == initial_segment_no && e2.segment_no == initial_segment_no &&
          e3.segment_no == initial_segment_no, "all three submits stayed in the same segment");
    CHECK(w.cur_page_no == 5, "cursor now sits at page 5, right after all three records");

    large_wal_writer_stop(&w);
    teardown_deps(&d);
}

static void test_submit_triggers_rollover_when_segment_full(void)
{
    printf("\n[test_submit_triggers_rollover_when_segment_full]\n");
    Deps d;
    setup_deps(&d);

    LargeWalWriter w;
    large_wal_writer_init(&w, &d.pool, &d.registry, &d.idx, &d.state, NULL);
    large_wal_writer_start(&w);

    uint8_t seed[100];
    memset(seed, 0x33, sizeof(seed));
    LargeWalIndexEntry seed_entry;
    large_wal_writer_submit(&w, seed, sizeof(seed), 500, WAL_REC_LARGE_REF, &seed_entry);

    uint32_t old_slot        = w.cur_slot_index;
    uint64_t old_segment_no  = d.pool.slots[old_slot].header.segment_no;

    /* Simulate "this segment is nearly full" without actually writing
     * 126 pages of filler — only 1 page of room left. */
    w.cur_page_no = LARGE_WAL_SEGMENT_PAGES_PER_FILE - 1;

    uint8_t two_pages[LARGE_WAL_PAGE_USABLE + 1];
    memset(two_pages, 0x44, sizeof(two_pages));
    LargeWalIndexEntry entry;
    CHECK(large_wal_writer_submit(&w, two_pages, sizeof(two_pages), 600, WAL_REC_LARGE_REF, &entry) == MYDB_OK,
          "a 2-page submit that doesn't fit the 1 remaining page succeeds (via rollover)");

    CHECK(d.pool.slots[old_slot].header.state == LSEG_DONE, "the old segment was closed out to LSEG_DONE");
    CHECK(d.pool.slots[old_slot].header.data_pages == LARGE_WAL_SEGMENT_PAGES_PER_FILE - 2,
          "the old segment's data_pages reflects exactly how far it had actually been written");
    CHECK(d.pool.slots[old_slot].header.end_lsn == 500,
          "the old segment's end_lsn is the last real content_lsn written into it before rollover");

    CHECK(entry.segment_no != old_segment_no, "the record landed in a genuinely new segment");
    CHECK(entry.start_page_no == 1, "the new segment's record starts fresh at page 1");

    uint8_t out_buf[2 * PAGE_SIZE];
    uint32_t out_len = 0;
    CHECK(verify_content(&d.registry, entry.segment_no, entry.start_page_no, entry.page_count,
                          out_buf, &out_len) == MYDB_OK,
          "the rolled-over record resolves immediately (new segment already registered)");
    CHECK(out_len == sizeof(two_pages) && memcmp(out_buf, two_pages, sizeof(two_pages)) == 0,
          "the record reads back as one contiguous whole, not split across segments");

    large_wal_writer_stop(&w);
    teardown_deps(&d);
}

static void test_stop_joins_cleanly_and_submit_after_stop_fails(void)
{
    printf("\n[test_stop_joins_cleanly_and_submit_after_stop_fails]\n");
    Deps d;
    setup_deps(&d);

    LargeWalWriter w;
    large_wal_writer_init(&w, &d.pool, &d.registry, &d.idx, &d.state, NULL);
    large_wal_writer_start(&w);

    uint8_t content[10];
    memset(content, 0x55, sizeof(content));
    LargeWalIndexEntry entry;
    CHECK(large_wal_writer_submit(&w, content, sizeof(content), 700, WAL_REC_LARGE_REF, &entry) == MYDB_OK,
          "one submit before stopping succeeds");

    CHECK(large_wal_writer_stop(&w) == MYDB_OK, "stop() joins the thread cleanly");
    CHECK(large_wal_writer_stop(&w) == MYDB_OK, "calling stop() a second time is a harmless no-op");

    CHECK(large_wal_writer_submit(&w, content, sizeof(content), 701, WAL_REC_LARGE_REF, &entry) == MYDB_ERR,
          "submit() after stop() fails immediately instead of hanging");

    teardown_deps(&d);
}

static void test_reload_recovers_cursor_and_registry(void)
{
    printf("\n[test_reload_recovers_cursor_and_registry]\n");
    Deps d;
    setup_deps(&d);

    LargeWalWriter w;
    large_wal_writer_init(&w, &d.pool, &d.registry, &d.idx, &d.state, NULL);
    large_wal_writer_start(&w);

    uint8_t small[50];
    memset(small, 0x66, sizeof(small));
    LargeWalIndexEntry e1, e2;
    large_wal_writer_submit(&w, small, sizeof(small), 300, WAL_REC_LARGE_REF, &e1);
    large_wal_writer_submit(&w, small, sizeof(small), 301, WAL_REC_LARGE_REF, &e2);

    large_wal_writer_stop(&w);
    large_wal_state_close(&d.state);
    large_wal_index_close(&d.idx);
    large_wal_registry_shutdown(&d.registry);
    large_wal_segment_pool_shutdown(&d.pool);

    /* Reopen everything fresh -- same on-disk directory, new in-memory
     * structures (registry starts empty again). */
    Deps d2;
    memset(&d2, 0, sizeof(d2));
    CHECK(large_wal_segment_pool_init(&d2.pool, TEST_WAL_DIR, 1) == MYDB_OK, "pool reload succeeds");
    CHECK(large_wal_registry_init(&d2.registry) == MYDB_OK, "registry reinit succeeds (fresh, empty table)");
    CHECK(large_wal_index_open(&d2.idx, TEST_WAL_DIR) == MYDB_OK, "index reload succeeds");
    CHECK(large_wal_state_open(&d2.state, TEST_WAL_DIR) == MYDB_OK, "state reload succeeds");

    LargeWalWriter w2;
    CHECK(large_wal_writer_init(&w2, &d2.pool, &d2.registry, &d2.idx, &d2.state, NULL) == MYDB_OK,
          "a fresh writer init against the reloaded deps succeeds");
    CHECK(w2.cur_slot_index == w.cur_slot_index, "the recovered cursor's slot matches where the old writer left off");
    CHECK(w2.cur_page_no == w.cur_page_no, "the recovered cursor's page_no matches where the old writer left off");

    uint8_t out_buf[PAGE_SIZE];
    uint32_t out_len = 0;
    LargeWalIndexEntry reloaded_e1;
    CHECK(large_wal_index_lookup(&d2.idx, 300, &reloaded_e1) == MYDB_OK, "the index entry for 300 survives reload");
    CHECK(verify_content(&d2.registry, reloaded_e1.segment_no, reloaded_e1.start_page_no,
                          reloaded_e1.page_count, out_buf, &out_len) == MYDB_OK,
          "content_lsn 300 resolves after reload, with no new writes -- "
          "both the reloaded index and the freshly re-registered rotation segment work");
    CHECK(out_len == sizeof(small) && memcmp(out_buf, small, sizeof(small)) == 0,
          "resolved bytes match the original record");

    large_wal_state_close(&d2.state);
    large_wal_index_close(&d2.idx);
    large_wal_registry_shutdown(&d2.registry);
    large_wal_segment_pool_shutdown(&d2.pool);
    cleanup_dir();
}

/* ------------------------------------------------------------------ */

int main(void)
{
    printf("=== test_large_wal_writer ===\n");

    test_submit_single_record_resolves_and_advances_state();
    test_registry_widening_resolves_rotation_resident_segment();
    test_sequential_submits_land_on_consecutive_pages();
    test_submit_triggers_rollover_when_segment_full();
    test_stop_joins_cleanly_and_submit_after_stop_fails();
    test_reload_recovers_cursor_and_registry();

    printf("\nResults: %d/%d passed\n", tests_passed, tests_run);
    return (tests_passed == tests_run) ? 0 : 1;
}
