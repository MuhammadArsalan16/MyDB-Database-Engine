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

static void test_page_header_round_trip(void)
{
    printf("\n[test_page_header_round_trip]\n");
    LargeWalPageHeader hdr;
    memset(&hdr, 0, sizeof(hdr));
    hdr.id.file_type = FILETYPE_LARGE_WAL_PAGE;
    hdr.content_lsn  = 123456789ull;
    hdr.record_type  = WAL_REC_SCHEMA_UPDATE;
    hdr.page_index   = 1;
    hdr.data_len     = 16000;
    hdr.flags        = LARGE_WAL_PAGE_FLAG_CONTINUATION;

    uint8_t buf[32];
    large_wal_page_header_serialize(&hdr, buf);

    LargeWalPageHeader out;
    memset(&out, 0xAA, sizeof(out));
    int rc = large_wal_page_header_deserialize(buf, &out);

    CHECK(rc == MYDB_OK,                          "deserialize of a freshly-serialized page header succeeds");
    CHECK(out.id.magic == MYDB_MAGIC,             "magic round-trips");
    CHECK(out.id.file_type == FILETYPE_LARGE_WAL_PAGE, "file_type round-trips");
    CHECK(out.content_lsn == hdr.content_lsn,     "content_lsn round-trips");
    CHECK(out.record_type == hdr.record_type,     "record_type round-trips");
    CHECK(out.page_index == hdr.page_index,       "page_index round-trips");
    CHECK(out.data_len == hdr.data_len,           "data_len round-trips");
    CHECK(out.flags == hdr.flags,                 "flags (CONTINUATION) round-trips");
}

static void test_page_header_detects_tamper(void)
{
    printf("\n[test_page_header_detects_tamper]\n");
    LargeWalPageHeader hdr;
    memset(&hdr, 0, sizeof(hdr));
    hdr.id.file_type = FILETYPE_LARGE_WAL_PAGE;
    hdr.content_lsn  = 42;

    uint8_t buf[32];
    large_wal_page_header_serialize(&hdr, buf);
    buf[9] ^= 0x01;   /* flip a byte inside content_lsn */

    LargeWalPageHeader out;
    int rc = large_wal_page_header_deserialize(buf, &out);
    CHECK(rc == MYDB_ERR_BAD_CHECKSUM, "corrupted page header fails checksum verification");
}

static void test_page_header_wrong_file_type(void)
{
    printf("\n[test_page_header_wrong_file_type]\n");
    LargeWalPageHeader hdr;
    memset(&hdr, 0, sizeof(hdr));
    hdr.id.file_type = FILETYPE_LARGE_WAL_SEGMENT;   /* wrong type on purpose */

    uint8_t buf[32];
    large_wal_page_header_serialize(&hdr, buf);

    LargeWalPageHeader out;
    int rc = large_wal_page_header_deserialize(buf, &out);
    CHECK(rc == MYDB_ERR_BAD_FILE_TYPE, "a page header stamped with the wrong file_type is rejected");
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
    test_segment_header_round_trip();
    test_segment_header_detects_tamper();
    test_segment_header_wrong_file_type();

    printf("\nResults: %d/%d passed\n", tests_passed, tests_run);
    return (tests_passed == tests_run) ? 0 : 1;
}
