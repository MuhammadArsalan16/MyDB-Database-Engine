#include <stdio.h>
#include <string.h>

#include "common.h"
#include "large_wal/large_wal_page.h"
#include "large_wal/large_wal_segment.h"

static int tests_run    = 0;
static int tests_passed = 0;

#define CHECK(cond, msg) do { \
    tests_run++; \
    if (cond) { tests_passed++; printf("  PASS: %s\n", msg); } \
    else       { printf("  FAIL: %s (line %d)\n", msg, __LINE__); } \
} while(0)

/* ------------------------------------------------------------------ */

/* LARGE_WAL pages carry the shared WalPageHeader (wal_page.h) — same
 * struct and byte layout normal_wal's pages use, differing only in the
 * file_type stamped into id. These tests cover LARGE_WAL's own
 * serialize/deserialize pair (large_wal_page.h), which is what enforces
 * that file_type distinction. */
static void test_page_header_round_trip(void)
{
    printf("\n[test_page_header_round_trip]\n");
    WalPageHeader hdr;
    memset(&hdr, 0, sizeof(hdr));
    hdr.id.file_type = FILETYPE_LARGE_WAL_PAGE;
    hdr.start_lsn    = 123456789ull;
    hdr.end_lsn      = 123456999ull;
    hdr.data_len     = 16000;
    hdr.flags        = WAL_PAGE_FLAG_INCOMING_CONTINUATION |
                       WAL_PAGE_FLAG_OUTGOING_CONTINUATION;

    uint8_t buf[LARGE_WAL_PAGE_HEADER_ON_DISK_SIZE];
    large_wal_page_header_serialize(&hdr, buf);

    WalPageHeader out;
    memset(&out, 0xAA, sizeof(out));
    int rc = large_wal_page_header_deserialize(buf, &out);

    CHECK(rc == MYDB_OK,                          "deserialize of a freshly-serialized page header succeeds");
    CHECK(out.id.magic == MYDB_MAGIC,             "magic round-trips");
    CHECK(out.id.file_type == FILETYPE_LARGE_WAL_PAGE, "file_type round-trips");
    CHECK(out.start_lsn == hdr.start_lsn,         "start_lsn round-trips");
    CHECK(out.end_lsn == hdr.end_lsn,             "end_lsn round-trips");
    CHECK(out.data_len == hdr.data_len,           "data_len round-trips");
    CHECK(out.flags == hdr.flags,                 "flags (both continuation bits) round-trip");
}

static void test_page_header_detects_tamper(void)
{
    printf("\n[test_page_header_detects_tamper]\n");
    WalPageHeader hdr;
    memset(&hdr, 0, sizeof(hdr));
    hdr.id.file_type = FILETYPE_LARGE_WAL_PAGE;
    hdr.start_lsn    = 42;

    uint8_t buf[LARGE_WAL_PAGE_HEADER_ON_DISK_SIZE];
    large_wal_page_header_serialize(&hdr, buf);
    buf[9] ^= 0x01;   /* flip a byte inside start_lsn */

    WalPageHeader out;
    int rc = large_wal_page_header_deserialize(buf, &out);
    CHECK(rc == MYDB_ERR_BAD_CHECKSUM, "corrupted page header fails checksum verification");
}

static void test_page_header_wrong_file_type(void)
{
    printf("\n[test_page_header_wrong_file_type]\n");
    WalPageHeader hdr;
    memset(&hdr, 0, sizeof(hdr));
    hdr.id.file_type = FILETYPE_LARGE_WAL_SEGMENT;   /* wrong type on purpose */

    uint8_t buf[LARGE_WAL_PAGE_HEADER_ON_DISK_SIZE];
    large_wal_page_header_serialize(&hdr, buf);

    WalPageHeader out;
    int rc = large_wal_page_header_deserialize(buf, &out);
    CHECK(rc == MYDB_ERR_BAD_FILE_TYPE, "a page header stamped with the wrong file_type is rejected");
}

/* A normal_wal page header must not deserialize as a LARGE_WAL one and
 * vice versa — the single thing that still distinguishes the two now
 * that they share a struct and byte layout. */
static void test_page_header_types_do_not_cross(void)
{
    printf("\n[test_page_header_types_do_not_cross]\n");
    WalPageHeader hdr;
    memset(&hdr, 0, sizeof(hdr));
    hdr.start_lsn = 7;

    uint8_t buf[LARGE_WAL_PAGE_HEADER_ON_DISK_SIZE];
    WalPageHeader out;

    hdr.id.file_type = FILETYPE_LARGE_WAL_PAGE;
    large_wal_page_header_serialize(&hdr, buf);
    CHECK(wal_page_header_deserialize(buf, &out) == MYDB_ERR_BAD_FILE_TYPE,
          "a LARGE_WAL page is rejected by normal_wal's deserializer");
    CHECK(large_wal_page_header_deserialize(buf, &out) == MYDB_OK,
          "...and accepted by LARGE_WAL's own");

    hdr.id.file_type = FILETYPE_WAL_PAGE;
    wal_page_header_serialize(&hdr, buf);
    CHECK(large_wal_page_header_deserialize(buf, &out) == MYDB_ERR_BAD_FILE_TYPE,
          "a normal_wal page is rejected by LARGE_WAL's deserializer");
    CHECK(wal_page_header_deserialize(buf, &out) == MYDB_OK,
          "...and accepted by normal_wal's own");
}

static void test_segment_header_round_trip(void)
{
    printf("\n[test_segment_header_round_trip]\n");
    LargeWalSegmentHeader hdr;
    memset(&hdr, 0, sizeof(hdr));
    hdr.id.file_type = FILETYPE_LARGE_WAL_SEGMENT;
    hdr.segment_no   = 3;
    hdr.start_lsn    = 500;
    hdr.end_lsn      = 900;
    hdr.partition_id = 2;
    hdr.data_pages   = 64;
    hdr.state        = LSEG_ACTIVE;

    uint8_t buf[LARGE_WAL_SEGMENT_HEADER_ON_DISK_SIZE];
    large_wal_segment_header_serialize(&hdr, buf);

    LargeWalSegmentHeader out;
    memset(&out, 0xAA, sizeof(out));
    int rc = large_wal_segment_header_deserialize(buf, &out);

    CHECK(rc == MYDB_OK,                        "deserialize of a freshly-serialized segment header succeeds");
    CHECK(out.segment_no == hdr.segment_no,      "segment_no round-trips");
    CHECK(out.start_lsn == hdr.start_lsn,        "start_lsn round-trips");
    CHECK(out.end_lsn == hdr.end_lsn,            "end_lsn round-trips");
    CHECK(out.partition_id == hdr.partition_id,  "partition_id round-trips");
    CHECK(out.data_pages == hdr.data_pages,      "data_pages round-trips");
    CHECK(out.state == (uint8_t)LSEG_ACTIVE,     "state round-trips");

    int padding_zeroed = 1;
    for (int i = 48; i < LARGE_WAL_SEGMENT_HEADER_ON_DISK_SIZE; i++)
        if (buf[i] != 0) padding_zeroed = 0;
    CHECK(padding_zeroed, "trailing 16 padding bytes are zeroed");
}

static void test_segment_header_detects_tamper(void)
{
    printf("\n[test_segment_header_detects_tamper]\n");
    LargeWalSegmentHeader hdr;
    memset(&hdr, 0, sizeof(hdr));
    hdr.id.file_type = FILETYPE_LARGE_WAL_SEGMENT;
    hdr.segment_no   = 1;
    hdr.state        = LSEG_DONE;

    uint8_t buf[LARGE_WAL_SEGMENT_HEADER_ON_DISK_SIZE];
    large_wal_segment_header_serialize(&hdr, buf);
    buf[20] ^= 0x01;   /* flip a byte inside end_lsn */

    LargeWalSegmentHeader out;
    int rc = large_wal_segment_header_deserialize(buf, &out);
    CHECK(rc == MYDB_ERR_BAD_CHECKSUM, "corrupted segment header fails checksum verification");
}

static void test_segment_header_wrong_file_type(void)
{
    printf("\n[test_segment_header_wrong_file_type]\n");
    LargeWalSegmentHeader hdr;
    memset(&hdr, 0, sizeof(hdr));
    hdr.id.file_type = FILETYPE_LARGE_WAL_PAGE;   /* wrong type on purpose */

    uint8_t buf[LARGE_WAL_SEGMENT_HEADER_ON_DISK_SIZE];
    large_wal_segment_header_serialize(&hdr, buf);

    LargeWalSegmentHeader out;
    int rc = large_wal_segment_header_deserialize(buf, &out);
    CHECK(rc == MYDB_ERR_BAD_FILE_TYPE, "a segment header stamped with the wrong file_type is rejected");
}

/* ------------------------------------------------------------------ */

int main(void)
{
    printf("=== test_large_wal_types ===\n");

    test_page_header_round_trip();
    test_page_header_detects_tamper();
    test_page_header_wrong_file_type();
    test_page_header_types_do_not_cross();
    test_segment_header_round_trip();
    test_segment_header_detects_tamper();
    test_segment_header_wrong_file_type();

    printf("\nResults: %d/%d passed\n", tests_passed, tests_run);
    return (tests_passed == tests_run) ? 0 : 1;
}
