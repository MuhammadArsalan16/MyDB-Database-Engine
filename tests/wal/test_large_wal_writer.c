#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/stat.h>
#include <dirent.h>

#include "common.h"
#include "wal_page.h"
#include "normal_wal/wal_types.h"
#include "large_wal/large_wal_page.h"
#include "large_wal/large_wal_segment.h"
#include "large_wal/large_wal_segment_pool.h"
#include "large_wal/large_wal_registry.h"
#include "large_wal/large_wal_index.h"
#include "large_wal/large_wal_state.h"
#include "large_wal/large_wal_buffer.h"
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
    large_wal_registry_init(&d->registry);          /* before the pool: it holds a pointer to this */
    large_wal_segment_pool_init(&d->pool, TEST_WAL_DIR, 1, &d->registry);
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

/* Builds one real, serialized WAL record (header + filled body) at out.
 * Submits are record-aware now — the writer walks the blob by each
 * header's own total_len — so tests must hand it genuine records rather
 * than the raw filler bytes earlier phases used. Returns total_len. */
static uint32_t build_record(uint8_t *out, uint64_t lsn, uint8_t rec_type,
                              uint32_t body_len, uint8_t fill)
{
    uint8_t *body = malloc(body_len ? body_len : 1);
    memset(body, fill, body_len);

    WalRecordHeader hdr;
    memset(&hdr, 0, sizeof(hdr));
    hdr.lsn       = lsn;
    hdr.total_len = WAL_RECORD_HEADER_SIZE + body_len;
    hdr.rec_type  = rec_type;

    wal_record_header_serialize(&hdr, body, body_len, out);
    free(body);
    return WAL_RECORD_HEADER_SIZE + body_len;
}

/* Resolves segment_no -> fd via the registry and reassembles exactly
 * total_size bytes starting at (start_page_no, offset) — the same walk
 * large_wal_get performs, kept local so this test stays scoped to the
 * writer's own mechanics rather than the api layer. */
static int verify_content(LargeWalRegistry *reg, const LargeWalIndexEntry *e,
                           uint8_t *out_buf, uint32_t *out_len)
{
    int fd;
    if (large_wal_registry_lookup(reg, e->segment_no, &fd) != MYDB_OK) return MYDB_ERR;

    uint32_t written = 0, remaining = e->total_size;
    uint32_t page_no = e->start_page_no, pos = e->offset;

    while (remaining > 0) {
        uint8_t page_buf[PAGE_SIZE];
        if (pread(fd, page_buf, PAGE_SIZE, (off_t)page_no * PAGE_SIZE) != PAGE_SIZE)
            return MYDB_ERR;

        WalPageHeader hdr;
        if (large_wal_page_header_deserialize(page_buf, &hdr) != MYDB_OK) return MYDB_ERR;

        uint32_t room  = LARGE_WAL_PAGE_USABLE - pos;
        uint32_t chunk = (remaining < room) ? remaining : room;
        memcpy(out_buf + written, page_buf + LARGE_WAL_PAGE_HEADER_ON_DISK_SIZE + pos, chunk);

        written += chunk; remaining -= chunk; page_no++; pos = 0;
    }
    *out_len = written;
    return MYDB_OK;
}

/* Reads one page's header straight off disk, for flag assertions. */
static int read_page_header(LargeWalRegistry *reg, uint64_t segment_no,
                             uint32_t page_no, WalPageHeader *out)
{
    int fd;
    if (large_wal_registry_lookup(reg, segment_no, &fd) != MYDB_OK) return MYDB_ERR;

    uint8_t page_buf[PAGE_SIZE];
    if (pread(fd, page_buf, PAGE_SIZE, (off_t)page_no * PAGE_SIZE) != PAGE_SIZE) return MYDB_ERR;
    return large_wal_page_header_deserialize(page_buf, out);
}

/* ------------------------------------------------------------------ */

static void test_single_record_resolves_and_advances_state(void)
{
    printf("\n[test_single_record_resolves_and_advances_state]\n");
    Deps d;
    setup_deps(&d);

    LargeWalWriter w;
    CHECK(large_wal_writer_init(&w, &d.pool, &d.registry, &d.idx, &d.state, NULL) == MYDB_OK, "writer init succeeds");
    CHECK(large_wal_writer_start(&w) == MYDB_OK, "writer start succeeds");

    uint8_t blob[1024];
    uint32_t len = build_record(blob, 100, WAL_REC_SCHEMA_UPDATE, 500, 0xAB);

    LargeWalIndexEntry entries[4];
    uint32_t count = 0;
    CHECK(large_wal_writer_submit(&w, blob, len, entries, 4, &count) == MYDB_OK,
          "submit of a single-record blob succeeds");
    CHECK(count == 1, "one record reported");
    CHECK(entries[0].content_lsn == 100 && entries[0].rec_type == WAL_REC_SCHEMA_UPDATE,
          "lsn and rec_type were read out of the record's own header");
    CHECK(entries[0].total_size == len && entries[0].page_count == 1 && entries[0].offset == 0,
          "entry describes a single page starting at offset 0");

    uint8_t out[PAGE_SIZE];
    uint32_t out_len = 0;
    CHECK(verify_content(&d.registry, &entries[0], out, &out_len) == MYDB_OK, "record resolves");
    CHECK(out_len == len && memcmp(out, blob, len) == 0, "bytes round-trip exactly");
    CHECK(d.state.flush_lsn == 100, "flush_lsn advanced to the record's lsn");

    large_wal_writer_stop(&w);
    teardown_deps(&d);
}

static void test_two_records_share_one_page(void)
{
    printf("\n[test_two_records_share_one_page]\n");
    Deps d;
    setup_deps(&d);

    LargeWalWriter w;
    large_wal_writer_init(&w, &d.pool, &d.registry, &d.idx, &d.state, NULL);
    large_wal_writer_start(&w);

    uint8_t blob[2048];
    uint32_t l1 = build_record(blob, 200, WAL_REC_SCHEMA_UPDATE, 100, 0x11);
    uint32_t l2 = build_record(blob + l1, 201, WAL_REC_SCHEMA_UPDATE, 100, 0x22);

    LargeWalIndexEntry e[4];
    uint32_t count = 0;
    CHECK(large_wal_writer_submit(&w, blob, l1 + l2, e, 4, &count) == MYDB_OK, "two-record submit succeeds");
    CHECK(count == 2, "both records reported");

    CHECK(e[0].start_page_no == e[1].start_page_no,
          "both records landed on the SAME physical page — tight packing");
    CHECK(e[0].offset == 0, "first record starts at offset 0");
    CHECK(e[1].offset == l1,
          "second record's offset is non-zero and equals the first record's length");

    uint8_t out[PAGE_SIZE]; uint32_t out_len = 0;
    CHECK(verify_content(&d.registry, &e[0], out, &out_len) == MYDB_OK &&
          out_len == l1 && memcmp(out, blob, l1) == 0, "first record round-trips");
    CHECK(verify_content(&d.registry, &e[1], out, &out_len) == MYDB_OK &&
          out_len == l2 && memcmp(out, blob + l1, l2) == 0, "second record round-trips");

    /* One page holding two records with different LSNs is exactly why
     * the shared page header carries start_lsn AND end_lsn. */
    WalPageHeader hdr;
    CHECK(read_page_header(&d.registry, e[0].segment_no, e[0].start_page_no, &hdr) == MYDB_OK,
          "the shared page's header reads back");
    CHECK(hdr.start_lsn == 200 && hdr.end_lsn == 201,
          "page start_lsn/end_lsn span the two records it holds");
    CHECK(hdr.data_len == l1 + l2, "page data_len counts both records' bytes");

    /* A further submit must continue into that same partially-filled
     * page rather than starting a fresh one. */
    uint8_t blob2[1024];
    uint32_t l3 = build_record(blob2, 202, WAL_REC_SCHEMA_UPDATE, 100, 0x33);
    LargeWalIndexEntry e3[2]; uint32_t c3 = 0;
    CHECK(large_wal_writer_submit(&w, blob2, l3, e3, 2, &c3) == MYDB_OK, "third submit succeeds");
    CHECK(e3[0].start_page_no == e[0].start_page_no && e3[0].offset == l1 + l2,
          "a later submit keeps appending into the same page (cursor persists across calls)");

    large_wal_writer_stop(&w);
    teardown_deps(&d);
}

static void test_record_spanning_pages(void)
{
    printf("\n[test_record_spanning_pages]\n");
    Deps d;
    setup_deps(&d);

    LargeWalWriter w;
    large_wal_writer_init(&w, &d.pool, &d.registry, &d.idx, &d.state, NULL);
    large_wal_writer_start(&w);

    /* Body sized so header+body exceeds one page's usable space. */
    uint32_t body = LARGE_WAL_PAGE_USABLE;
    uint8_t *blob = malloc(WAL_RECORD_HEADER_SIZE + body);
    uint32_t len = build_record(blob, 300, WAL_REC_SCHEMA_UPDATE, body, 0x5A);

    LargeWalIndexEntry e[2]; uint32_t count = 0;
    CHECK(large_wal_writer_submit(&w, blob, len, e, 2, &count) == MYDB_OK, "spanning submit succeeds");
    CHECK(count == 1 && e[0].page_count == 2, "one record occupying two pages");

    WalPageHeader p1, p2;
    read_page_header(&d.registry, e[0].segment_no, e[0].start_page_no,     &p1);
    read_page_header(&d.registry, e[0].segment_no, e[0].start_page_no + 1, &p2);
    CHECK((p1.flags & WAL_PAGE_FLAG_OUTGOING_CONTINUATION) != 0,
          "the first page is flagged OUTGOING — its last record runs on");
    CHECK((p2.flags & WAL_PAGE_FLAG_INCOMING_CONTINUATION) != 0,
          "the second page is flagged INCOMING — it opens mid-record");

    uint8_t *out = malloc(len); uint32_t out_len = 0;
    CHECK(verify_content(&d.registry, &e[0], out, &out_len) == MYDB_OK, "spanning record resolves");
    CHECK(out_len == len, "resolved length matches");

    /* The reassembled record must still be a valid record — the writer
     * patched its flags byte and recomputed its checksum. */
    WalRecordHeader rh;
    CHECK(wal_record_header_deserialize(out, body, &rh) == MYDB_OK,
          "the reassembled record still passes its own checksum after the flag patch");
    CHECK((rh.flags & WAL_RECORD_FLAG_CONTINUES_ON_NEW_PAGE) != 0,
          "the record header carries CONTINUES_ON_NEW_PAGE");
    CHECK(memcmp(out + WAL_RECORD_HEADER_SIZE, blob + WAL_RECORD_HEADER_SIZE, body) == 0,
          "the body survives the split byte-for-byte");

    free(out); free(blob);
    large_wal_writer_stop(&w);
    teardown_deps(&d);
}

static void test_oversized_record_rejected(void)
{
    printf("\n[test_oversized_record_rejected]\n");
    Deps d;
    setup_deps(&d);

    LargeWalWriter w;
    large_wal_writer_init(&w, &d.pool, &d.registry, &d.idx, &d.state, NULL);
    large_wal_writer_start(&w);

    /* One page more than a whole segment's content capacity. */
    uint32_t body = (uint32_t)LARGE_WAL_SEGMENT_PAGES_PER_FILE * LARGE_WAL_PAGE_USABLE;
    uint8_t *blob = malloc(WAL_RECORD_HEADER_SIZE + body);
    uint32_t len = build_record(blob, 400, WAL_REC_SCHEMA_UPDATE, body, 0x77);

    LargeWalIndexEntry e[2]; uint32_t count = 0;
    CHECK(large_wal_writer_submit(&w, blob, len, e, 2, &count) == MYDB_ERR,
          "a record too large for any segment is rejected outright");
    CHECK(count == 0, "nothing was written");

    free(blob);
    large_wal_writer_stop(&w);
    teardown_deps(&d);
}

static void test_batch_spans_segments(void)
{
    printf("\n[test_batch_spans_segments]\n");
    Deps d;
    setup_deps(&d);

    LargeWalWriter w;
    large_wal_writer_init(&w, &d.pool, &d.registry, &d.idx, &d.state, NULL);
    large_wal_writer_start(&w);

    uint64_t first_segment = d.pool.slots[w.cur_slot_index].header.segment_no;

    /* Park the cursor on the segment's last usable page so a two-record
     * batch cannot fit in one segment — the batch must split into runs
     * while each record stays wholly inside one segment. */
    w.cur_page_no = LARGE_WAL_SEGMENT_PAGES_PER_FILE - 1;
    w.cur_offset  = 0;

    uint32_t body = LARGE_WAL_PAGE_USABLE - WAL_RECORD_HEADER_SIZE;   /* exactly one page each */
    uint8_t *blob = malloc(2 * LARGE_WAL_PAGE_USABLE);
    uint32_t l1 = build_record(blob, 500, WAL_REC_SCHEMA_UPDATE, body, 0xC1);
    uint32_t l2 = build_record(blob + l1, 501, WAL_REC_SCHEMA_UPDATE, body, 0xC2);

    LargeWalIndexEntry e[4]; uint32_t count = 0;
    CHECK(large_wal_writer_submit(&w, blob, l1 + l2, e, 4, &count) == MYDB_OK,
          "a batch that cannot fit one segment still succeeds");
    CHECK(count == 2, "both records written");
    CHECK(e[0].segment_no == first_segment, "the first record stayed in the original segment");
    CHECK(e[1].segment_no != e[0].segment_no,
          "the batch split across segments — a batch may span, a record may not");
    CHECK(e[1].start_page_no == 1 && e[1].offset == 0,
          "the second record starts cleanly at the head of the new segment");

    uint8_t *out = malloc(LARGE_WAL_PAGE_USABLE); uint32_t out_len = 0;
    CHECK(verify_content(&d.registry, &e[0], out, &out_len) == MYDB_OK &&
          out_len == l1 && memcmp(out, blob, l1) == 0, "first record resolves from its own segment");
    CHECK(verify_content(&d.registry, &e[1], out, &out_len) == MYDB_OK &&
          out_len == l2 && memcmp(out, blob + l1, l2) == 0, "second record resolves from the next segment");

    free(out); free(blob);
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

    int fd;
    CHECK(large_wal_registry_lookup(&d.registry,
                                     d.pool.slots[w.cur_slot_index].header.segment_no, &fd) == MYDB_OK,
          "the freshly claimed rotation segment is resolvable with zero submits made — "
          "init()'s registration, not copy_out(), is what made it so");

    teardown_deps(&d);
}

static void test_stop_and_submit_after_stop(void)
{
    printf("\n[test_stop_and_submit_after_stop]\n");
    Deps d;
    setup_deps(&d);

    LargeWalWriter w;
    large_wal_writer_init(&w, &d.pool, &d.registry, &d.idx, &d.state, NULL);
    large_wal_writer_start(&w);

    uint8_t blob[512];
    uint32_t len = build_record(blob, 700, WAL_REC_SCHEMA_UPDATE, 50, 0x55);
    LargeWalIndexEntry e[2]; uint32_t count = 0;
    CHECK(large_wal_writer_submit(&w, blob, len, e, 2, &count) == MYDB_OK, "submit before stop succeeds");

    CHECK(large_wal_writer_stop(&w) == MYDB_OK, "stop joins the thread cleanly");
    CHECK(large_wal_writer_stop(&w) == MYDB_OK, "a second stop is a harmless no-op");
    CHECK(large_wal_writer_submit(&w, blob, len, e, 2, &count) == MYDB_ERR,
          "submit after stop fails immediately instead of hanging");

    teardown_deps(&d);
}

static void test_malformed_blob_rejected(void)
{
    printf("\n[test_malformed_blob_rejected]\n");
    Deps d;
    setup_deps(&d);

    LargeWalWriter w;
    large_wal_writer_init(&w, &d.pool, &d.registry, &d.idx, &d.state, NULL);
    large_wal_writer_start(&w);

    LargeWalIndexEntry e[4]; uint32_t count = 0;
    uint8_t blob[512];
    uint32_t len = build_record(blob, 800, WAL_REC_SCHEMA_UPDATE, 100, 0x99);

    CHECK(large_wal_writer_submit(&w, blob, WAL_RECORD_HEADER_SIZE - 1, e, 4, &count) == MYDB_ERR,
          "a blob too short to hold even one header is rejected");
    CHECK(large_wal_writer_submit(&w, blob, len - 1, e, 4, &count) == MYDB_ERR,
          "a blob whose record walk overruns its own length is rejected");

    large_wal_writer_stop(&w);
    teardown_deps(&d);
}

static void test_reload_recovers_cursor(void)
{
    printf("\n[test_reload_recovers_cursor]\n");
    Deps d;
    setup_deps(&d);

    LargeWalWriter w;
    large_wal_writer_init(&w, &d.pool, &d.registry, &d.idx, &d.state, NULL);
    large_wal_writer_start(&w);

    uint8_t blob[1024];
    uint32_t len = build_record(blob, 900, WAL_REC_SCHEMA_UPDATE, 200, 0x66);
    LargeWalIndexEntry e[2]; uint32_t count = 0;
    large_wal_writer_submit(&w, blob, len, e, 2, &count);

    large_wal_writer_stop(&w);
    large_wal_state_close(&d.state);
    large_wal_index_close(&d.idx);
    large_wal_registry_shutdown(&d.registry);
    large_wal_segment_pool_shutdown(&d.pool);

    Deps d2;
    memset(&d2, 0, sizeof(d2));
    CHECK(large_wal_registry_init(&d2.registry) == MYDB_OK, "registry reinit succeeds");
    CHECK(large_wal_segment_pool_init(&d2.pool, TEST_WAL_DIR, 1, &d2.registry) == MYDB_OK, "pool reload succeeds");
    CHECK(large_wal_index_open(&d2.idx, TEST_WAL_DIR) == MYDB_OK, "index reload succeeds");
    CHECK(large_wal_state_open(&d2.state, TEST_WAL_DIR) == MYDB_OK, "state reload succeeds");

    LargeWalWriter w2;
    CHECK(large_wal_writer_init(&w2, &d2.pool, &d2.registry, &d2.idx, &d2.state, NULL) == MYDB_OK,
          "a fresh writer init against the reloaded deps succeeds");
    CHECK(w2.cur_slot_index == w.cur_slot_index, "the recovered cursor's slot matches");
    CHECK(w2.cur_offset == 0,
          "the recovered cursor resumes on a fresh page — tail-scan reports whole pages, "
          "not how full the last one was");

    LargeWalIndexEntry reloaded;
    CHECK(large_wal_index_lookup(&d2.idx, 900, &reloaded) == MYDB_OK, "the index entry survives reload");
    uint8_t out[PAGE_SIZE]; uint32_t out_len = 0;
    CHECK(verify_content(&d2.registry, &reloaded, out, &out_len) == MYDB_OK &&
          out_len == len && memcmp(out, blob, len) == 0,
          "the record still resolves after reload, with no new writes");

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

    test_single_record_resolves_and_advances_state();
    test_two_records_share_one_page();
    test_record_spanning_pages();
    test_oversized_record_rejected();
    test_batch_spans_segments();
    test_registry_widening_resolves_rotation_resident_segment();
    test_stop_and_submit_after_stop();
    test_malformed_blob_rejected();
    test_reload_recovers_cursor();

    printf("\nResults: %d/%d passed\n", tests_passed, tests_run);
    return (tests_passed == tests_run) ? 0 : 1;
}
