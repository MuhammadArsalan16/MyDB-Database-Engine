#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <stdlib.h>

#include "btree.h"

#define TEST_FILE "/tmp/mydb_test_btree.mydb"
#define TABLE_ID  0

static int tests_run    = 0;
static int tests_passed = 0;

#define CHECK(cond, msg) do { \
    tests_run++; \
    if (cond) { tests_passed++; printf("  PASS: %s\n", msg); } \
    else       { printf("  FAIL: %s (line %d)\n", msg, __LINE__); } \
} while(0)

static DiskManager dm;
static BufferPool  bp;
static BTree       bt;

static void setup(void)
{
    unlink(TEST_FILE);
    disk_create(&dm, TEST_FILE);
    bp_init(&bp);
    btree_init(&bt, &bp, &dm, TABLE_ID, INVALID_PAGE, TYPE_INT, 0);
}

static void teardown(void)
{
    bp_flush_table(&bp, &dm, TABLE_ID);
    disk_close(&dm);
    unlink(TEST_FILE);
}

/* Build a simple clustered record: [key_len:2][key:4][val_len:2][val:n] */
static uint16_t make_record(int key_int, const char *val, uint8_t *out)
{
    Value kv; kv.type = TYPE_INT; kv.is_null = 0; kv.v.int_val = key_int;
    uint8_t enc[4]; uint16_t klen = btree_key_encode(&kv, enc);

    uint16_t vlen = (uint16_t)strlen(val);
    out[0] = (uint8_t)(klen >> 8); out[1] = (uint8_t)klen;
    memcpy(out + 2, enc, klen);
    out[2 + klen] = (uint8_t)(vlen >> 8); out[3 + klen] = (uint8_t)vlen;
    memcpy(out + 4 + klen, val, vlen);
    return (uint16_t)(4 + klen + vlen);
}

/* ------------------------------------------------------------------ */

static void test_insert_search(void)
{
    printf("\n[test_insert_search]\n");
    setup();

    Value k; k.type = TYPE_INT; k.is_null = 0;
    uint8_t rec[64]; uint16_t rlen;
    RID rid;

    /* Insert key 42 */
    k.v.int_val = 42;
    rlen = make_record(42, "hello", rec);
    CHECK(btree_insert(&bt, &k, rec, rlen, &rid) == MYDB_OK, "insert 42");
    CHECK(bt.root_page_no != INVALID_PAGE, "root page created");

    /* Search for key 42 */
    BTreeSearchResult res;
    CHECK(btree_search(&bt, &k, &res) == MYDB_OK, "search 42");
    CHECK(res.found == 1, "key 42 found");

    /* Search for non-existent key */
    k.v.int_val = 99;
    CHECK(btree_search(&bt, &k, &res) == MYDB_OK, "search 99 ok");
    CHECK(res.found == 0, "key 99 not found");

    /* Duplicate insert must fail */
    k.v.int_val = 42;
    rlen = make_record(42, "dup", rec);
    CHECK(btree_insert(&bt, &k, rec, rlen, &rid) == MYDB_ERR_DUPLICATE,
          "duplicate insert rejected");

    teardown();
}

static void test_delete(void)
{
    printf("\n[test_delete]\n");
    setup();

    Value k; k.type = TYPE_INT; k.is_null = 0;
    uint8_t rec[64]; uint16_t rlen; RID rid;

    k.v.int_val = 10;
    rlen = make_record(10, "ten", rec);
    btree_insert(&bt, &k, rec, rlen, &rid);

    CHECK(btree_delete(&bt, &k) == MYDB_OK, "delete key 10");

    BTreeSearchResult res;
    btree_search(&bt, &k, &res);
    CHECK(res.found == 0, "deleted key 10 not found");

    /* Delete non-existent key */
    k.v.int_val = 999;
    CHECK(btree_delete(&bt, &k) == MYDB_ERR_NOT_FOUND, "delete missing key fails");

    teardown();
}

static void test_scan_ordered(void)
{
    printf("\n[test_scan_ordered]\n");
    setup();

    /* Insert 5 keys out of order */
    int keys[] = {30, 10, 50, 20, 40};
    Value k; k.type = TYPE_INT; k.is_null = 0;
    uint8_t rec[64]; uint16_t rlen; RID rid;

    for (int i = 0; i < 5; i++) {
        k.v.int_val = keys[i];
        rlen = make_record(keys[i], "v", rec);
        btree_insert(&bt, &k, rec, rlen, &rid);
    }

    /* Scan: should come out in ascending key order */
    Cursor cur;
    btree_cursor_open(&bt, &cur);

    int prev = -1;
    int count = 0;
    uint8_t buf[256]; uint16_t len;
    int ordered = 1;

    while (btree_cursor_next(&cur, buf, &len) == MYDB_OK) {
        /* Decode key from the record */
        uint16_t klen = ((uint16_t)buf[0] << 8) | buf[1];
        Value decoded_key;
        btree_key_decode(buf + 2, klen, TYPE_INT, &decoded_key);
        int kval = decoded_key.v.int_val;

        if (kval <= prev) ordered = 0;
        prev = kval;
        count++;
    }
    btree_cursor_close(&cur);

    CHECK(count == 5, "scan returns all 5 records");
    CHECK(ordered,    "scan returns records in ascending order");

    teardown();
}

static void test_splits(void)
{
    printf("\n[test_splits]\n");
    setup();

    /*
     * Insert enough records to force leaf splits.
     * Each record is ~20 bytes; a 16KB page holds ~680 such records.
     * We use 1000 to guarantee at least one split.
     */
    Value k; k.type = TYPE_INT; k.is_null = 0;
    uint8_t rec[32]; uint16_t rlen; RID rid;

    for (int i = 1; i <= 1000; i++) {
        k.v.int_val = i;
        rlen = make_record(i, "data", rec);
        int rc = btree_insert(&bt, &k, rec, rlen, &rid);
        if (rc != MYDB_OK) {
            printf("  FAIL: insert %d failed (rc=%d)\n", i, rc);
            teardown(); return;
        }
    }
    CHECK(1, "inserted 1000 records without error");

    /* Verify all 1000 can be found */
    int all_found = 1;
    BTreeSearchResult res;
    for (int i = 1; i <= 1000; i++) {
        k.v.int_val = i;
        btree_search(&bt, &k, &res);
        if (!res.found) { all_found = 0; break; }
    }
    CHECK(all_found, "all 1000 records found by key search");

    /* Scan must return exactly 1000 records in order */
    Cursor cur;
    btree_cursor_open(&bt, &cur);
    int count = 0, ordered = 1, prev = 0;
    uint8_t buf[256]; uint16_t len;
    while (btree_cursor_next(&cur, buf, &len) == MYDB_OK) {
        uint16_t klen = ((uint16_t)buf[0] << 8) | buf[1];
        Value dkey; btree_key_decode(buf + 2, klen, TYPE_INT, &dkey);
        if (dkey.v.int_val <= prev) ordered = 0;
        prev = dkey.v.int_val;
        count++;
    }
    btree_cursor_close(&cur);
    CHECK(count == 1000, "scan returns all 1000 records");
    CHECK(ordered,       "scan is in ascending order after splits");

    teardown();
}

static void test_varchar_key(void)
{
    printf("\n[test_varchar_key]\n");

    unlink(TEST_FILE);
    disk_create(&dm, TEST_FILE);
    bp_init(&bp);
    btree_init(&bt, &bp, &dm, TABLE_ID, INVALID_PAGE, TYPE_VARCHAR, 0);

    Value k; k.type = TYPE_VARCHAR; k.is_null = 0;
    uint8_t rec[64]; RID rid;

    const char *words[] = {"banana", "apple", "cherry", "date"};
    for (int i = 0; i < 4; i++) {
        uint16_t wlen = (uint16_t)strlen(words[i]);
        k.v.varchar_val.len = wlen;
        memcpy(k.v.varchar_val.data, words[i], wlen);

        uint8_t enc[MAX_VARCHAR_LEN+2]; uint16_t klen = btree_key_encode(&k, enc);
        uint16_t vlen = 0;
        rec[0]=(uint8_t)(klen>>8); rec[1]=(uint8_t)klen;
        memcpy(rec+2, enc, klen);
        rec[2+klen]=0; rec[3+klen]=0; /* empty value */
        uint16_t rlen = (uint16_t)(4 + klen + vlen);
        btree_insert(&bt, &k, rec, rlen, &rid);
    }

    /* Search for "cherry" */
    uint16_t clen = (uint16_t)strlen("cherry");
    k.v.varchar_val.len = clen;
    memcpy(k.v.varchar_val.data, "cherry", clen);
    BTreeSearchResult res;
    btree_search(&bt, &k, &res);
    CHECK(res.found == 1, "varchar key 'cherry' found");

    /* Scan must return in alphabetical order */
    Cursor cur; btree_cursor_open(&bt, &cur);
    char prev_str[32] = "";
    int ordered = 1, count = 0;
    uint8_t buf[128]; uint16_t len;
    while (btree_cursor_next(&cur, buf, &len) == MYDB_OK) {
        uint16_t klen = ((uint16_t)buf[0]<<8)|buf[1];
        Value dkey; btree_key_decode(buf+2, klen, TYPE_VARCHAR, &dkey);
        char cur_str[32];
        memcpy(cur_str, dkey.v.varchar_val.data, dkey.v.varchar_val.len);
        cur_str[dkey.v.varchar_val.len] = '\0';
        if (strcmp(cur_str, prev_str) < 0) ordered = 0;
        strcpy(prev_str, cur_str);
        count++;
    }
    btree_cursor_close(&cur);
    CHECK(count == 4, "varchar scan returns 4 records");
    CHECK(ordered,    "varchar scan is in alphabetical order");

    bp_flush_table(&bp, &dm, TABLE_ID);
    disk_close(&dm);
    unlink(TEST_FILE);
}

/* ------------------------------------------------------------------ */

int main(void)
{
    printf("=== test_btree ===\n");

    test_insert_search();
    test_delete();
    test_scan_ordered();
    test_splits();
    test_varchar_key();

    printf("\nResults: %d/%d passed\n", tests_passed, tests_run);
    return (tests_passed == tests_run) ? 0 : 1;
}
