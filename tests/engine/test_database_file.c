#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>

#include "common.h"
#include "database_file.h"

#define TEST_FILE "/tmp/mydb_test_database.mydb"

static int tests_run    = 0;
static int tests_passed = 0;

#define CHECK(cond, msg) do { \
    tests_run++; \
    if (cond) { tests_passed++; printf("  PASS: %s\n", msg); } \
    else       { printf("  FAIL: %s (line %d)\n", msg, __LINE__); } \
} while(0)

static void cleanup(void) { unlink(TEST_FILE); }

/* Flip a single byte at `offset` to `value`. Used to simulate
 * on-disk corruption for the format-guard tests. */
static void corrupt_byte(const char *path, off_t offset, uint8_t value)
{
    int fd = open(path, O_RDWR);
    if (fd < 0) return;
    pwrite(fd, &value, 1, offset);
    fsync(fd);
    close(fd);
}

/* ------------------------------------------------------------------ */
/*  Lifecycle                                                          */
/* ------------------------------------------------------------------ */

static void test_create_open_round_trip(void)
{
    printf("\n[test_create_open_round_trip]\n");
    cleanup();

    DatabaseFile db;
    CHECK(db_create(TEST_FILE, "TestEngine", &db) == MYDB_OK, "db_create succeeds");
    CHECK(strncmp(db.header.engine_name, "TestEngine", 32) == 0, "engine_name set");
    CHECK(db.header.num_partitions == 0, "num_partitions starts 0");
    db_close(&db);

    DatabaseFile db2;
    CHECK(db_open(TEST_FILE, &db2) == MYDB_OK, "db_open succeeds");
    CHECK(strncmp(db2.header.engine_name, "TestEngine", 32) == 0,
          "engine_name persisted");
    CHECK(db2.header.num_partitions == 0, "num_partitions persisted");
    db_close(&db2);
}

static void test_create_rejects_existing(void)
{
    printf("\n[test_create_rejects_existing]\n");
    cleanup();

    DatabaseFile db;
    CHECK(db_create(TEST_FILE, NULL, &db) == MYDB_OK, "first create succeeds");
    db_close(&db);

    DatabaseFile db2;
    CHECK(db_create(TEST_FILE, NULL, &db2) != MYDB_OK,
          "second create on same path fails");
}

/* ------------------------------------------------------------------ */
/*  Format guards                                                      */
/* ------------------------------------------------------------------ */

static void test_open_rejects_bad_magic(void)
{
    printf("\n[test_open_rejects_bad_magic]\n");
    cleanup();

    DatabaseFile db;
    db_create(TEST_FILE, NULL, &db);
    db_close(&db);

    corrupt_byte(TEST_FILE, 0, 0xFF);  /* clobber low byte of magic */

    DatabaseFile db2;
    CHECK(db_open(TEST_FILE, &db2) == MYDB_ERR_BAD_MAGIC,
          "db_open rejects bad magic");
}

static void test_open_rejects_wrong_file_type(void)
{
    printf("\n[test_open_rejects_wrong_file_type]\n");
    cleanup();

    DatabaseFile db;
    db_create(TEST_FILE, NULL, &db);
    db_close(&db);

    /* Overwrite file_type at offset 6 with FILETYPE_CATALOG */
    uint16_t wrong = FILETYPE_CATALOG;
    int fd = open(TEST_FILE, O_RDWR);
    pwrite(fd, &wrong, 2, 6);
    fsync(fd);
    close(fd);

    DatabaseFile db2;
    CHECK(db_open(TEST_FILE, &db2) == MYDB_ERR_BAD_FILE_TYPE,
          "db_open rejects wrong file_type");
}

static void test_open_rejects_bad_checksum(void)
{
    printf("\n[test_open_rejects_bad_checksum]\n");
    cleanup();

    DatabaseFile db;
    db_create(TEST_FILE, NULL, &db);
    db_close(&db);

    /* Flip a byte in the partition area — leaves header intact so
     * checksum (not magic/version/filetype) is the failing check. */
    corrupt_byte(TEST_FILE, 100, 0xAA);

    DatabaseFile db2;
    CHECK(db_open(TEST_FILE, &db2) == MYDB_ERR_BAD_CHECKSUM,
          "db_open rejects corrupted file (bad checksum)");
}

/* ------------------------------------------------------------------ */
/*  Partition management                                               */
/* ------------------------------------------------------------------ */

static void test_add_partition_assigns_ids(void)
{
    printf("\n[test_add_partition_assigns_ids]\n");
    cleanup();

    DatabaseFile db;
    db_create(TEST_FILE, NULL, &db);

    uint32_t pid1, pid2;
    CHECK(db_add_partition(&db, 100, "/data/root",  &pid1) == MYDB_OK, "add 1 ok");
    CHECK(pid1 == 1, "first partition id is 1");
    CHECK(db_add_partition(&db, 200, "/data/alice", &pid2) == MYDB_OK, "add 2 ok");
    CHECK(pid2 == 2, "second partition id is 2");
    CHECK(db.header.num_partitions == 2, "num_partitions tracks adds");

    db_close(&db);
}

static void test_partition_path_stored(void)
{
    printf("\n[test_partition_path_stored]\n");
    cleanup();

    DatabaseFile db;
    db_create(TEST_FILE, NULL, &db);

    uint32_t pid;
    db_add_partition(&db, 42, "/foo/bar/baz", &pid);

    PartitionEntry *p = db_find_by_id(&db, pid);
    CHECK(p != NULL,                                       "find_by_id non-NULL");
    CHECK(p && strcmp(p->path, "/foo/bar/baz") == 0,       "path stored correctly");
    CHECK(p && p->owner_id  == 42,                          "owner_id stored");
    CHECK(p && p->is_active == 1,                           "is_active set");

    db_close(&db);
}

static void test_add_partition_full(void)
{
    printf("\n[test_add_partition_full]\n");
    cleanup();

    DatabaseFile db;
    db_create(TEST_FILE, NULL, &db);

    int all_ok = 1;
    for (int i = 0; i < MAX_PARTITIONS; i++) {
        uint32_t pid;
        char path[64];
        snprintf(path, sizeof(path), "/p/%d", i);
        if (db_add_partition(&db, (uint32_t)i, path, &pid) != MYDB_OK) all_ok = 0;
    }
    CHECK(all_ok, "16 partitions added successfully");

    uint32_t pid;
    CHECK(db_add_partition(&db, 999, "/p/full", &pid) == MYDB_ERR_FULL,
          "17th partition returns MYDB_ERR_FULL");

    db_close(&db);
}

static void test_partition_persists(void)
{
    printf("\n[test_partition_persists]\n");
    cleanup();

    DatabaseFile db;
    db_create(TEST_FILE, NULL, &db);
    uint32_t pid;
    db_add_partition(&db, 7, "/persisted", &pid);
    db_close(&db);

    DatabaseFile db2;
    db_open(TEST_FILE, &db2);
    PartitionEntry *p = db_find_by_id(&db2, pid);
    CHECK(p != NULL,                                  "partition survived close+reopen");
    CHECK(p && strcmp(p->path, "/persisted") == 0,    "path survived");
    CHECK(p && p->owner_id == 7,                       "owner_id survived");
    db_close(&db2);
}

static void test_path_length_boundary(void)
{
    printf("\n[test_path_length_boundary]\n");
    cleanup();

    DatabaseFile db;
    db_create(TEST_FILE, NULL, &db);

    /* 255 chars + NUL fits in path[256] */
    char path255[256];
    memset(path255, 'a', 255);
    path255[255] = '\0';
    uint32_t pid;
    CHECK(db_add_partition(&db, 1, path255, &pid) == MYDB_OK,
          "255-char path succeeds");

    /* 256 chars + NUL overflows */
    char path256[300];
    memset(path256, 'b', 256);
    path256[256] = '\0';
    CHECK(db_add_partition(&db, 2, path256, &pid) == MYDB_ERR,
          "256-char path rejected");

    db_close(&db);
}

static void test_remove_partition(void)
{
    printf("\n[test_remove_partition]\n");
    cleanup();

    DatabaseFile db;
    db_create(TEST_FILE, NULL, &db);
    uint32_t pid;
    db_add_partition(&db, 1, "/x", &pid);

    CHECK(db_remove_partition(&db, pid) == MYDB_OK,         "remove succeeds");
    CHECK(db_find_by_id(&db, pid)       == NULL,             "removed slot not findable");
    CHECK(db.header.num_partitions      == 0,                "num_partitions decremented");

    db_close(&db);
}

static void test_remove_unknown_id(void)
{
    printf("\n[test_remove_unknown_id]\n");
    cleanup();

    DatabaseFile db;
    db_create(TEST_FILE, NULL, &db);

    CHECK(db_remove_partition(&db, 999) == MYDB_ERR_NOT_FOUND,
          "remove unknown id → NOT_FOUND");

    db_close(&db);
}

static void test_remove_then_add_reuses_slot(void)
{
    printf("\n[test_remove_then_add_reuses_slot]\n");
    cleanup();

    DatabaseFile db;
    db_create(TEST_FILE, NULL, &db);

    /* Fill 16, remove one, add another */
    for (int i = 0; i < MAX_PARTITIONS; i++) {
        uint32_t pid;
        char path[64];
        snprintf(path, sizeof(path), "/p/%d", i);
        db_add_partition(&db, (uint32_t)i, path, &pid);
    }
    db_remove_partition(&db, 1);

    uint32_t pid;
    CHECK(db_add_partition(&db, 99, "/new", &pid) == MYDB_OK,
          "add into freed slot succeeds after remove");

    db_close(&db);
}

static void test_id_allocator_monotonic(void)
{
    printf("\n[test_id_allocator_monotonic]\n");
    cleanup();

    DatabaseFile db;
    db_create(TEST_FILE, NULL, &db);

    uint32_t p1, p2, p3;
    db_add_partition(&db, 1, "/a", &p1);   /* id 1 */
    db_add_partition(&db, 2, "/b", &p2);   /* id 2 */
    db_remove_partition(&db, p2);          /* remove the trailing id */
    db_add_partition(&db, 3, "/c", &p3);   /* expect id 3, NOT 2 */

    CHECK(p1 == 1, "p1 id = 1");
    CHECK(p2 == 2, "p2 id = 2");
    CHECK(p3 == 3, "p3 id is monotonic counter, not reused trailing id");

    db_close(&db);
}

static void test_next_partition_id_persists(void)
{
    printf("\n[test_next_partition_id_persists]\n");
    cleanup();

    /* Add three partitions, close, reopen — counter must survive. */
    {
        DatabaseFile db;
        db_create(TEST_FILE, NULL, &db);
        uint32_t pid;
        db_add_partition(&db, 1, "/a", &pid);
        db_add_partition(&db, 2, "/b", &pid);
        db_add_partition(&db, 3, "/c", &pid);
        db_close(&db);
    }

    DatabaseFile db;
    CHECK(db_open(TEST_FILE, &db) == MYDB_OK, "reopen succeeds");
    CHECK(db.header.next_partition_id == 4,
          "counter resumes at 4 after three adds");

    /* Remove all three, then add — counter still monotonic. */
    db_remove_partition(&db, 1);
    db_remove_partition(&db, 2);
    db_remove_partition(&db, 3);
    uint32_t pid;
    CHECK(db_add_partition(&db, 9, "/d", &pid) == MYDB_OK, "add after wipe");
    CHECK(pid == 4, "post-wipe id is 4, not 1");

    db_close(&db);
}

static void test_find_by_owner(void)
{
    printf("\n[test_find_by_owner]\n");
    cleanup();

    DatabaseFile db;
    db_create(TEST_FILE, NULL, &db);
    uint32_t p1, p2;
    db_add_partition(&db, 100, "/o100", &p1);
    db_add_partition(&db, 200, "/o200", &p2);

    PartitionEntry *e1 = db_find_by_owner(&db, 100);
    PartitionEntry *e2 = db_find_by_owner(&db, 200);
    PartitionEntry *e3 = db_find_by_owner(&db, 999);

    CHECK(e1 && e1->partition_id == p1, "owner 100 → p1");
    CHECK(e2 && e2->partition_id == p2, "owner 200 → p2");
    CHECK(e3 == NULL,                    "unknown owner → NULL");

    db_close(&db);
}

static void test_find_by_id_miss(void)
{
    printf("\n[test_find_by_id_miss]\n");
    cleanup();

    DatabaseFile db;
    db_create(TEST_FILE, NULL, &db);
    uint32_t pid;
    db_add_partition(&db, 1, "/x", &pid);

    CHECK(db_find_by_id(&db, pid)          != NULL, "known id finds entry");
    CHECK(db_find_by_id(&db, pid + 1000)   == NULL, "unknown id → NULL");

    db_close(&db);
}

static void test_last_opened_refreshes(void)
{
    printf("\n[test_last_opened_refreshes]\n");
    cleanup();

    DatabaseFile db;
    db_create(TEST_FILE, NULL, &db);

    /* Force last_opened backwards in time without sleeping the test. */
    db.header.last_opened = 19700101000000ULL;
    db_save(&db);
    db_close(&db);

    DatabaseFile db2;
    db_open(TEST_FILE, &db2);
    CHECK(db2.header.last_opened > 19700101000000ULL,
          "last_opened refreshed on db_open");
    db_close(&db2);
}

/* ------------------------------------------------------------------ */

int main(void)
{
    printf("=== test_database_file ===\n");

    test_create_open_round_trip();
    test_create_rejects_existing();
    test_open_rejects_bad_magic();
    test_open_rejects_wrong_file_type();
    test_open_rejects_bad_checksum();

    test_add_partition_assigns_ids();
    test_partition_path_stored();
    test_add_partition_full();
    test_partition_persists();
    test_path_length_boundary();
    test_remove_partition();
    test_remove_unknown_id();
    test_remove_then_add_reuses_slot();
    test_id_allocator_monotonic();
    test_next_partition_id_persists();
    test_find_by_owner();
    test_find_by_id_miss();
    test_last_opened_refreshes();

    printf("\nResults: %d/%d passed\n", tests_passed, tests_run);
    cleanup();
    return (tests_passed == tests_run) ? 0 : 1;
}
