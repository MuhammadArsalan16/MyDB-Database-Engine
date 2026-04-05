#include <stdio.h>
#include <assert.h>
#include <string.h>
#include <stdint.h>

#include "page.h"

static int tests_run    = 0;
static int tests_passed = 0;

#define CHECK(cond, msg) do { \
    tests_run++; \
    if (cond) { tests_passed++; printf("  PASS: %s\n", msg); } \
    else       { printf("  FAIL: %s (line %d)\n", msg, __LINE__); } \
} while(0)

/* Shared page buffer used across tests */
static uint8_t pg[PAGE_SIZE];

/* ------------------------------------------------------------------ */

static void test_init(void)
{
    printf("\n[test_init]\n");

    CHECK(page_init(pg, 7, PAGE_TYPE_DATA) == MYDB_OK, "page_init succeeds");

    PageHeader hdr;
    page_read_header(pg, &hdr);
    CHECK(hdr.page_no        == 7,                 "page_no set correctly");
    CHECK(hdr.page_type      == PAGE_TYPE_DATA,    "page_type set correctly");
    CHECK(hdr.prev_page      == INVALID_PAGE,      "prev_page is INVALID_PAGE");
    CHECK(hdr.next_page      == INVALID_PAGE,      "next_page is INVALID_PAGE");
    CHECK(hdr.num_records    == 0,                 "num_records == 0");
    CHECK(hdr.free_offset    == USER_RECORDS_OFFSET, "free_offset == 64");
    CHECK(hdr.num_dir_slots  == 0,                 "num_dir_slots == 0");

    /* Infimum should point to Supremum initially */
    RecordHeader inf, sup;
    rec_hdr_decode(pg + INFIMUM_OFFSET, &inf);
    rec_hdr_decode(pg + SUPREMUM_OFFSET, &sup);
    CHECK(inf.next_offset == SUPREMUM_DATA, "infimum->next points to supremum data");
    CHECK(sup.next_offset == 0,             "supremum->next is 0 (end of chain)");
}

static void test_insert_and_get(void)
{
    printf("\n[test_insert_and_get]\n");
    page_init(pg, 1, PAGE_TYPE_DATA);

    /* Insert a 10-byte record */
    uint8_t rec1[10];
    memset(rec1, 0xAA, 10);
    uint16_t slot1;
    CHECK(page_insert_record(pg, rec1, 10, INFIMUM_DATA, &slot1) == MYDB_OK,
          "insert record 1 succeeds");

    PageHeader hdr;
    page_read_header(pg, &hdr);
    CHECK(hdr.num_records   == 1,  "num_records == 1");
    CHECK(hdr.num_dir_slots == 1,  "num_dir_slots == 1");
    CHECK(hdr.free_offset   == USER_RECORDS_OFFSET + RECORD_HEADER_SIZE + 10,
          "free_offset advanced correctly");

    /* Read it back */
    uint16_t doff, dsz;
    CHECK(page_get_record(pg, slot1, &doff, &dsz) == MYDB_OK, "get record 1");
    CHECK(dsz == 10, "record 1 size == 10");
    CHECK(memcmp(pg + doff, rec1, 10) == 0, "record 1 data matches");

    /* Insert a second record (linked after the first) */
    uint8_t rec2[20];
    memset(rec2, 0xBB, 20);
    uint16_t slot2;
    CHECK(page_insert_record(pg, rec2, 20, doff, &slot2) == MYDB_OK,
          "insert record 2 succeeds");

    uint16_t doff2, dsz2;
    CHECK(page_get_record(pg, slot2, &doff2, &dsz2) == MYDB_OK, "get record 2");
    CHECK(dsz2 == 20, "record 2 size == 20");
    CHECK(memcmp(pg + doff2, rec2, 20) == 0, "record 2 data matches");
}

static void test_delete(void)
{
    printf("\n[test_delete]\n");
    page_init(pg, 1, PAGE_TYPE_DATA);

    uint8_t rec[8];
    memset(rec, 0xCC, 8);
    uint16_t slot;
    page_insert_record(pg, rec, 8, INFIMUM_DATA, &slot);

    CHECK(page_delete_record(pg, slot) == MYDB_OK, "delete record succeeds");

    PageHeader hdr;
    page_read_header(pg, &hdr);
    CHECK(hdr.num_records == 0, "num_records == 0 after delete");

    /* Getting a deleted record must fail */
    uint16_t doff, dsz;
    CHECK(page_get_record(pg, slot, &doff, &dsz) == MYDB_ERR_NOT_FOUND,
          "get deleted record returns NOT_FOUND");

    /* Deleting again must fail */
    CHECK(page_delete_record(pg, slot) == MYDB_ERR, "double-delete fails");
}

static void test_compact(void)
{
    printf("\n[test_compact]\n");
    page_init(pg, 1, PAGE_TYPE_DATA);

    /* Insert 3 records */
    uint8_t r[3][8];
    uint16_t slots[3];
    uint16_t prev = INFIMUM_DATA;
    for (int i = 0; i < 3; i++) {
        memset(r[i], (uint8_t)(0x10 + i), 8);
        page_insert_record(pg, r[i], 8, prev, &slots[i]);
        uint16_t doff, dsz;
        page_get_record(pg, slots[i], &doff, &dsz);
        prev = doff;
    }

    /* Delete the middle one */
    page_delete_record(pg, slots[1]);

    PageHeader hdr_before;
    page_read_header(pg, &hdr_before);
    uint16_t free_before = page_free_space(pg);

    /* Compact */
    CHECK(page_compact(pg) == MYDB_OK, "page_compact succeeds");

    PageHeader hdr_after;
    page_read_header(pg, &hdr_after);
    uint16_t free_after = page_free_space(pg);

    CHECK(hdr_after.num_records == 2, "num_records == 2 after compact");
    CHECK(free_after > free_before,   "free space increased after compact");

    /* Remaining records (slot 0 and 1 in the new page) should hold r[0] and r[2] */
    uint16_t doff0, dsz0, doff2, dsz2;
    CHECK(page_get_record(pg, 0, &doff0, &dsz0) == MYDB_OK, "slot 0 live after compact");
    CHECK(page_get_record(pg, 1, &doff2, &dsz2) == MYDB_OK, "slot 1 live after compact");
    CHECK(dsz0 == 8 && memcmp(pg + doff0, r[0], 8) == 0, "slot 0 data is r[0]");
    CHECK(dsz2 == 8 && memcmp(pg + doff2, r[2], 8) == 0, "slot 1 data is r[2]");
}

static void test_full_page(void)
{
    printf("\n[test_full_page]\n");
    page_init(pg, 1, PAGE_TYPE_DATA);

    /* Insert 50-byte records until the page is full */
    uint8_t rec[50];
    memset(rec, 0x55, 50);
    uint16_t slot;
    int count = 0;
    uint16_t prev = INFIMUM_DATA;

    while (page_insert_record(pg, rec, 50, prev, &slot) == MYDB_OK) {
        uint16_t doff, dsz;
        page_get_record(pg, slot, &doff, &dsz);
        prev = doff;
        count++;
    }

    CHECK(count > 0, "inserted at least one record before full");

    /* The next insert must fail with FULL */
    CHECK(page_insert_record(pg, rec, 50, prev, &slot) == MYDB_ERR_FULL,
          "insert into full page returns MYDB_ERR_FULL");

    printf("  INFO: filled page with %d records of 50 bytes each\n", count);
}

static void test_checksum(void)
{
    printf("\n[test_checksum]\n");
    page_init(pg, 3, PAGE_TYPE_DATA);

    page_set_checksum(pg);
    CHECK(page_verify_checksum(pg) == MYDB_OK, "checksum valid after set");

    /* Corrupt one byte in the middle of the page */
    pg[1000] ^= 0xFF;
    CHECK(page_verify_checksum(pg) == MYDB_ERR, "checksum detects corruption");
}

/* ------------------------------------------------------------------ */

int main(void)
{
    printf("=== test_page ===\n");

    test_init();
    test_insert_and_get();
    test_delete();
    test_compact();
    test_full_page();
    test_checksum();

    printf("\nResults: %d/%d passed\n", tests_passed, tests_run);
    return (tests_passed == tests_run) ? 0 : 1;
}
