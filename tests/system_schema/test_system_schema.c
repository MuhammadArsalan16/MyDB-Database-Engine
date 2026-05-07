#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <stdlib.h>

#include "common.h"
#include "system_schema.h"

#define TEST_ROOT      "/tmp/mydb_test_system_schema"
#define TEST_SS_DIR    TEST_ROOT "/system_schema"
#define USERS_PATH     TEST_SS_DIR "/users.mydb"
#define PRIV_PATH      TEST_SS_DIR "/privileges.mydb"

static int tests_run    = 0;
static int tests_passed = 0;

#define CHECK(cond, msg) do { \
    tests_run++; \
    if (cond) { tests_passed++; printf("  PASS: %s\n", msg); } \
    else       { printf("  FAIL: %s (line %d)\n", msg, __LINE__); } \
} while(0)

/* ------------------------------------------------------------------ */
/*  Test scaffolding                                                   */
/* ------------------------------------------------------------------ */

static void rm_rf(const char *path)
{
    /* Best-effort cleanup of the test root. Files may not exist. */
    unlink(USERS_PATH);
    unlink(PRIV_PATH);
    rmdir(TEST_SS_DIR);
    rmdir(path);
}

static void cleanup(void) { rm_rf(TEST_ROOT); }

/* mkdir TEST_ROOT and TEST_ROOT/system_schema. */
static void mk_dirs(void)
{
    mkdir(TEST_ROOT,   0755);
    mkdir(TEST_SS_DIR, 0755);
}

static void corrupt_byte(const char *path, off_t offset, uint8_t value)
{
    int fd = open(path, O_RDWR);
    if (fd < 0) return;
    pwrite(fd, &value, 1, offset);
    fsync(fd);
    close(fd);
}

/* Build a populated UserSlot for tests. The hash + salt are stub
 * bytes — phase 7 stores them verbatim. */
static UserSlot make_user(const char *name, uint8_t is_active)
{
    UserSlot u;
    memset(&u, 0, sizeof(u));
    strncpy(u.username, name, MAX_USERNAME - 1);
    memset(u.password_hash, 0xAB, USER_PASSWORD_HASH_LEN);
    memset(u.password_salt, 0xCD, USER_PASSWORD_SALT_LEN);
    u.hash_algorithm = 1;
    u.is_active = is_active;
    return u;
}

static PrivilegeSlot make_priv(uint32_t grantee, uint32_t partition,
                               const char *schema, uint32_t granter)
{
    PrivilegeSlot p;
    memset(&p, 0, sizeof(p));
    p.grantee_id   = grantee;
    p.partition_id = partition;
    p.granted_by   = granter;
    strncpy(p.schema_name, schema, 31);
    return p;
}


/* ================================================================== */
/*  Lifecycle                                                         */
/* ================================================================== */

static void test_create_round_trip(void)
{
    printf("\n[test_create_round_trip]\n");
    cleanup();
    mk_dirs();

    SystemSchema ss;
    CHECK(system_schema_create(TEST_ROOT, &ss) == MYDB_OK,
          "system_schema_create succeeds");
    CHECK(ss.users.fd >= 0,        "users.mydb fd open");
    CHECK(ss.privileges.fd >= 0,   "privileges.mydb fd open");
    CHECK(ss.users.num_users == 0, "users start empty");
    CHECK(ss.privileges.num_privileges == 0, "privileges start empty");
    CHECK(ss.users.next_user_id == 1, "first user_id will be 1");
    CHECK(ss.privileges.next_privilege_id == 1, "first privilege_id will be 1");

    /* On-disk file sizes match the design. */
    struct stat us, ps;
    CHECK(stat(USERS_PATH, &us) == 0 && us.st_size == USERS_FILE_SIZE,
          "users.mydb is exactly 8 KB");
    CHECK(stat(PRIV_PATH, &ps) == 0 && ps.st_size == PRIVILEGES_FILE_SIZE,
          "privileges.mydb is exactly 16 KB");

    system_schema_close(&ss);
}

static void test_create_rejects_existing(void)
{
    printf("\n[test_create_rejects_existing]\n");
    cleanup();
    mk_dirs();

    SystemSchema ss;
    CHECK(system_schema_create(TEST_ROOT, &ss) == MYDB_OK, "first create OK");
    system_schema_close(&ss);

    SystemSchema ss2;
    CHECK(system_schema_create(TEST_ROOT, &ss2) != MYDB_OK,
          "second create on same dir fails");
}

static void test_open_after_create(void)
{
    printf("\n[test_open_after_create]\n");
    cleanup();
    mk_dirs();

    SystemSchema ss;
    system_schema_create(TEST_ROOT, &ss);
    system_schema_close(&ss);

    SystemSchema ss2;
    CHECK(system_schema_open(TEST_ROOT, &ss2) == MYDB_OK,
          "open after create OK");
    CHECK(ss2.users.num_users == 0,        "no users persisted");
    CHECK(ss2.privileges.num_privileges == 0, "no privileges persisted");
    system_schema_close(&ss2);
}

static void test_open_missing_dir_fails(void)
{
    printf("\n[test_open_missing_dir_fails]\n");
    cleanup();

    SystemSchema ss;
    CHECK(system_schema_open(TEST_ROOT, &ss) != MYDB_OK,
          "open with missing dir fails");
}


/* ================================================================== */
/*  Format guards                                                     */
/* ================================================================== */

static void test_open_rejects_users_bad_magic(void)
{
    printf("\n[test_open_rejects_users_bad_magic]\n");
    cleanup();
    mk_dirs();

    SystemSchema ss;
    system_schema_create(TEST_ROOT, &ss);
    system_schema_close(&ss);

    corrupt_byte(USERS_PATH, 0, 0xFF);

    SystemSchema ss2;
    CHECK(system_schema_open(TEST_ROOT, &ss2) == MYDB_ERR_BAD_MAGIC,
          "open rejects users.mydb bad magic");
}

static void test_open_rejects_users_wrong_filetype(void)
{
    printf("\n[test_open_rejects_users_wrong_filetype]\n");
    cleanup();
    mk_dirs();

    SystemSchema ss;
    system_schema_create(TEST_ROOT, &ss);
    system_schema_close(&ss);

    uint16_t wrong = FILETYPE_PRIVILEGES;
    int fd = open(USERS_PATH, O_RDWR);
    pwrite(fd, &wrong, 2, 6);
    fsync(fd); close(fd);

    SystemSchema ss2;
    CHECK(system_schema_open(TEST_ROOT, &ss2) == MYDB_ERR_BAD_FILE_TYPE,
          "open rejects users.mydb wrong file_type");
}

static void test_open_rejects_users_bad_checksum(void)
{
    printf("\n[test_open_rejects_users_bad_checksum]\n");
    cleanup();
    mk_dirs();

    SystemSchema ss;
    system_schema_create(TEST_ROOT, &ss);
    system_schema_close(&ss);

    /* Flip a byte in the slot region — header stays intact, only the
     * trailer checksum should fail. */
    corrupt_byte(USERS_PATH, 100, 0xAA);

    SystemSchema ss2;
    CHECK(system_schema_open(TEST_ROOT, &ss2) == MYDB_ERR_BAD_CHECKSUM,
          "open rejects users.mydb corrupted bytes");
}

static void test_open_rejects_priv_bad_magic(void)
{
    printf("\n[test_open_rejects_priv_bad_magic]\n");
    cleanup();
    mk_dirs();

    SystemSchema ss;
    system_schema_create(TEST_ROOT, &ss);
    system_schema_close(&ss);

    corrupt_byte(PRIV_PATH, 0, 0xFF);

    SystemSchema ss2;
    CHECK(system_schema_open(TEST_ROOT, &ss2) == MYDB_ERR_BAD_MAGIC,
          "open rejects privileges.mydb bad magic");
}

static void test_open_rejects_priv_wrong_filetype(void)
{
    printf("\n[test_open_rejects_priv_wrong_filetype]\n");
    cleanup();
    mk_dirs();

    SystemSchema ss;
    system_schema_create(TEST_ROOT, &ss);
    system_schema_close(&ss);

    uint16_t wrong = FILETYPE_USERS;
    int fd = open(PRIV_PATH, O_RDWR);
    pwrite(fd, &wrong, 2, 6);
    fsync(fd); close(fd);

    SystemSchema ss2;
    CHECK(system_schema_open(TEST_ROOT, &ss2) == MYDB_ERR_BAD_FILE_TYPE,
          "open rejects privileges.mydb wrong file_type");
}

static void test_open_rejects_priv_bad_checksum(void)
{
    printf("\n[test_open_rejects_priv_bad_checksum]\n");
    cleanup();
    mk_dirs();

    SystemSchema ss;
    system_schema_create(TEST_ROOT, &ss);
    system_schema_close(&ss);

    corrupt_byte(PRIV_PATH, 200, 0x55);

    SystemSchema ss2;
    CHECK(system_schema_open(TEST_ROOT, &ss2) == MYDB_ERR_BAD_CHECKSUM,
          "open rejects privileges.mydb corrupted bytes");
}


/* ================================================================== */
/*  Users CRUD                                                        */
/* ================================================================== */

static void test_users_insert_assigns_ids(void)
{
    printf("\n[test_users_insert_assigns_ids]\n");
    cleanup(); mk_dirs();

    SystemSchema ss;
    system_schema_create(TEST_ROOT, &ss);

    UserSlot a = make_user("alice", 1);
    UserSlot b = make_user("bob",   1);
    uint32_t aid, bid;
    CHECK(users_insert(&ss.users, &a, &aid) == MYDB_OK, "insert alice ok");
    CHECK(aid == 1, "alice gets user_id=1");
    CHECK(users_insert(&ss.users, &b, &bid) == MYDB_OK, "insert bob ok");
    CHECK(bid == 2, "bob gets user_id=2");
    CHECK(ss.users.num_users == 2, "num_users tracks inserts");

    system_schema_close(&ss);
}

static void test_users_find_by_name(void)
{
    printf("\n[test_users_find_by_name]\n");
    cleanup(); mk_dirs();

    SystemSchema ss;
    system_schema_create(TEST_ROOT, &ss);
    UserSlot a = make_user("alice", 1);
    uint32_t aid;
    users_insert(&ss.users, &a, &aid);

    UserSlot got;
    CHECK(users_find_by_name(&ss.users, "alice", &got) == MYDB_OK,
          "find alice by name OK");
    CHECK(got.user_id == aid, "returned user_id matches");
    CHECK(got.is_active == 1, "is_active preserved");
    CHECK(got.hash_algorithm == 1, "hash_algorithm preserved");
    CHECK(got.password_hash[0] == 0xAB, "password_hash bytes preserved");
    CHECK(got.password_salt[0] == 0xCD, "password_salt bytes preserved");
    CHECK(got.created_at != 0, "created_at filled in by insert");

    UserSlot miss;
    CHECK(users_find_by_name(&ss.users, "ghost", &miss) == MYDB_ERR_NOT_FOUND,
          "missing username returns NOT_FOUND");

    system_schema_close(&ss);
}

static void test_users_find_by_id(void)
{
    printf("\n[test_users_find_by_id]\n");
    cleanup(); mk_dirs();

    SystemSchema ss;
    system_schema_create(TEST_ROOT, &ss);
    UserSlot a = make_user("alice", 1);
    UserSlot b = make_user("bob",   1);
    uint32_t aid, bid;
    users_insert(&ss.users, &a, &aid);
    users_insert(&ss.users, &b, &bid);

    UserSlot got;
    CHECK(users_find_by_id(&ss.users, bid, &got) == MYDB_OK,
          "find by id ok");
    CHECK(strcmp(got.username, "bob") == 0, "by-id returned bob");

    CHECK(users_find_by_id(&ss.users, 9999, &got) == MYDB_ERR_NOT_FOUND,
          "missing id returns NOT_FOUND");

    system_schema_close(&ss);
}

static void test_users_duplicate_rejected(void)
{
    printf("\n[test_users_duplicate_rejected]\n");
    cleanup(); mk_dirs();

    SystemSchema ss;
    system_schema_create(TEST_ROOT, &ss);

    UserSlot a = make_user("alice", 1);
    uint32_t aid;
    users_insert(&ss.users, &a, &aid);

    UserSlot dup = make_user("alice", 1);
    uint32_t did;
    CHECK(users_insert(&ss.users, &dup, &did) == MYDB_ERR_DUPLICATE,
          "duplicate username rejected");
    CHECK(ss.users.num_users == 1, "duplicate did not bump num_users");
    CHECK(ss.users.next_user_id == 2, "next_user_id only advanced once");

    system_schema_close(&ss);
}

static void test_users_empty_username_rejected(void)
{
    printf("\n[test_users_empty_username_rejected]\n");
    cleanup(); mk_dirs();

    SystemSchema ss;
    system_schema_create(TEST_ROOT, &ss);

    UserSlot u;
    memset(&u, 0, sizeof(u));   /* no username set */
    uint32_t id;
    CHECK(users_insert(&ss.users, &u, &id) == MYDB_ERR,
          "empty username rejected");
    system_schema_close(&ss);
}

static void test_users_full_file(void)
{
    printf("\n[test_users_full_file]\n");
    cleanup(); mk_dirs();

    SystemSchema ss;
    system_schema_create(TEST_ROOT, &ss);

    int all_ok = 1;
    for (int i = 0; i < USERS_MAX_SLOTS; i++) {
        char name[16];
        snprintf(name, sizeof(name), "u%02d", i);
        UserSlot u = make_user(name, 1);
        uint32_t id;
        if (users_insert(&ss.users, &u, &id) != MYDB_OK) { all_ok = 0; break; }
    }
    CHECK(all_ok, "fill USERS_MAX_SLOTS users OK");
    CHECK(ss.users.num_users == USERS_MAX_SLOTS, "num_users at capacity");

    UserSlot extra = make_user("overflow", 1);
    uint32_t id;
    CHECK(users_insert(&ss.users, &extra, &id) == MYDB_ERR_FULL,
          "insert past capacity returns FULL");

    system_schema_close(&ss);
}

static void test_users_update_same_name(void)
{
    printf("\n[test_users_update_same_name]\n");
    cleanup(); mk_dirs();

    SystemSchema ss;
    system_schema_create(TEST_ROOT, &ss);
    UserSlot a = make_user("alice", 1);
    uint32_t aid;
    users_insert(&ss.users, &a, &aid);

    UserSlot upd;
    users_find_by_id(&ss.users, aid, &upd);
    upd.is_active = 0;
    upd.last_login = 20260506120000ULL;
    CHECK(users_update(&ss.users, &upd) == MYDB_OK, "update OK");

    UserSlot got;
    users_find_by_name(&ss.users, "alice", &got);
    CHECK(got.is_active == 0, "is_active updated");
    CHECK(got.last_login == 20260506120000ULL, "last_login updated");

    system_schema_close(&ss);
}

static void test_users_update_renames(void)
{
    printf("\n[test_users_update_renames]\n");
    cleanup(); mk_dirs();

    SystemSchema ss;
    system_schema_create(TEST_ROOT, &ss);
    UserSlot a = make_user("alice", 1);
    uint32_t aid;
    users_insert(&ss.users, &a, &aid);

    UserSlot upd;
    users_find_by_id(&ss.users, aid, &upd);
    strncpy(upd.username, "alyssa", MAX_USERNAME - 1);
    upd.username[MAX_USERNAME - 1] = '\0';
    CHECK(users_update(&ss.users, &upd) == MYDB_OK, "rename succeeds");

    UserSlot got;
    CHECK(users_find_by_name(&ss.users, "alyssa", &got) == MYDB_OK,
          "find by new name OK");
    CHECK(users_find_by_name(&ss.users, "alice", &got) == MYDB_ERR_NOT_FOUND,
          "old name no longer in index");

    system_schema_close(&ss);
}

static void test_users_update_rename_collides(void)
{
    printf("\n[test_users_update_rename_collides]\n");
    cleanup(); mk_dirs();

    SystemSchema ss;
    system_schema_create(TEST_ROOT, &ss);
    UserSlot a = make_user("alice", 1);
    UserSlot b = make_user("bob",   1);
    uint32_t aid, bid;
    users_insert(&ss.users, &a, &aid);
    users_insert(&ss.users, &b, &bid);

    UserSlot upd;
    users_find_by_id(&ss.users, bid, &upd);
    strncpy(upd.username, "alice", MAX_USERNAME - 1);
    CHECK(users_update(&ss.users, &upd) == MYDB_ERR_DUPLICATE,
          "rename to existing username rejected");

    /* Bob's username should still be intact. */
    UserSlot got;
    users_find_by_id(&ss.users, bid, &got);
    CHECK(strcmp(got.username, "bob") == 0, "bob unchanged after failed rename");

    system_schema_close(&ss);
}

static void test_users_update_unknown_id(void)
{
    printf("\n[test_users_update_unknown_id]\n");
    cleanup(); mk_dirs();

    SystemSchema ss;
    system_schema_create(TEST_ROOT, &ss);

    UserSlot ghost = make_user("ghost", 1);
    ghost.user_id = 999;
    CHECK(users_update(&ss.users, &ghost) == MYDB_ERR_NOT_FOUND,
          "update missing id returns NOT_FOUND");

    system_schema_close(&ss);
}

static void test_users_delete(void)
{
    printf("\n[test_users_delete]\n");
    cleanup(); mk_dirs();

    SystemSchema ss;
    system_schema_create(TEST_ROOT, &ss);
    UserSlot a = make_user("alice", 1);
    UserSlot b = make_user("bob",   1);
    uint32_t aid, bid;
    users_insert(&ss.users, &a, &aid);
    users_insert(&ss.users, &b, &bid);

    CHECK(users_delete(&ss.users, aid) == MYDB_OK, "delete alice OK");
    CHECK(ss.users.num_users == 1, "num_users decremented");

    UserSlot got;
    CHECK(users_find_by_name(&ss.users, "alice", &got) == MYDB_ERR_NOT_FOUND,
          "alice no longer findable");
    CHECK(users_find_by_id(&ss.users, aid, &got) == MYDB_ERR_NOT_FOUND,
          "alice user_id gone");
    CHECK(users_find_by_name(&ss.users, "bob", &got) == MYDB_OK,
          "bob still there");

    CHECK(users_delete(&ss.users, 999) == MYDB_ERR_NOT_FOUND,
          "delete unknown id returns NOT_FOUND");

    system_schema_close(&ss);
}

static void test_users_delete_then_reinsert(void)
{
    printf("\n[test_users_delete_then_reinsert]\n");
    cleanup(); mk_dirs();

    SystemSchema ss;
    system_schema_create(TEST_ROOT, &ss);
    UserSlot a = make_user("alice", 1);
    uint32_t aid;
    users_insert(&ss.users, &a, &aid);
    users_delete(&ss.users, aid);

    UserSlot a2 = make_user("alice", 1);
    uint32_t aid2;
    CHECK(users_insert(&ss.users, &a2, &aid2) == MYDB_OK,
          "reinsert after delete OK");
    CHECK(aid2 == aid + 1, "user_id keeps incrementing (no reuse)");

    system_schema_close(&ss);
}


/* ================================================================== */
/*  Privileges CRUD                                                   */
/* ================================================================== */

static void test_priv_insert_assigns_ids(void)
{
    printf("\n[test_priv_insert_assigns_ids]\n");
    cleanup(); mk_dirs();

    SystemSchema ss;
    system_schema_create(TEST_ROOT, &ss);

    PrivilegeSlot a = make_priv(/*grantee*/2, /*partition*/1, "hr",      /*by*/1);
    PrivilegeSlot b = make_priv(/*grantee*/3, /*partition*/1, "payroll", /*by*/1);
    uint32_t pid1, pid2;
    CHECK(privileges_insert(&ss.privileges, &a, &pid1) == MYDB_OK, "insert 1 ok");
    CHECK(pid1 == 1, "first privilege_id is 1");
    CHECK(privileges_insert(&ss.privileges, &b, &pid2) == MYDB_OK, "insert 2 ok");
    CHECK(pid2 == 2, "second privilege_id is 2");
    CHECK(ss.privileges.num_privileges == 2, "num_privileges tracks inserts");

    system_schema_close(&ss);
}

static void test_priv_find_composite_key(void)
{
    printf("\n[test_priv_find_composite_key]\n");
    cleanup(); mk_dirs();

    SystemSchema ss;
    system_schema_create(TEST_ROOT, &ss);

    PrivilegeSlot a = make_priv(2, 1, "hr",      1);
    uint32_t id;
    privileges_insert(&ss.privileges, &a, &id);

    PrivilegeSlot got;
    CHECK(privileges_find(&ss.privileges, 2, 1, "hr", &got) == MYDB_OK,
          "find by full key OK");
    CHECK(got.privilege_id == id, "returned id matches");
    CHECK(got.granted_at != 0, "granted_at populated");

    /* Different grantee, same schema → miss */
    CHECK(privileges_find(&ss.privileges, 3, 1, "hr", &got) == MYDB_ERR_NOT_FOUND,
          "different grantee → NOT_FOUND");
    /* Different partition → miss */
    CHECK(privileges_find(&ss.privileges, 2, 2, "hr", &got) == MYDB_ERR_NOT_FOUND,
          "different partition → NOT_FOUND");
    /* Different schema → miss */
    CHECK(privileges_find(&ss.privileges, 2, 1, "sales", &got) == MYDB_ERR_NOT_FOUND,
          "different schema → NOT_FOUND");

    system_schema_close(&ss);
}

static void test_priv_find_by_id(void)
{
    printf("\n[test_priv_find_by_id]\n");
    cleanup(); mk_dirs();

    SystemSchema ss;
    system_schema_create(TEST_ROOT, &ss);
    PrivilegeSlot a = make_priv(2, 1, "hr", 1);
    uint32_t id;
    privileges_insert(&ss.privileges, &a, &id);

    PrivilegeSlot got;
    CHECK(privileges_find_by_id(&ss.privileges, id, &got) == MYDB_OK,
          "find by id ok");
    CHECK(got.grantee_id == 2, "grantee preserved");
    CHECK(privileges_find_by_id(&ss.privileges, 9999, &got) == MYDB_ERR_NOT_FOUND,
          "missing id returns NOT_FOUND");

    system_schema_close(&ss);
}

static void test_priv_duplicate_rejected(void)
{
    printf("\n[test_priv_duplicate_rejected]\n");
    cleanup(); mk_dirs();

    SystemSchema ss;
    system_schema_create(TEST_ROOT, &ss);

    PrivilegeSlot a = make_priv(2, 1, "hr", 1);
    uint32_t id;
    privileges_insert(&ss.privileges, &a, &id);

    PrivilegeSlot dup = make_priv(2, 1, "hr", 5);   /* different granted_by */
    uint32_t did;
    CHECK(privileges_insert(&ss.privileges, &dup, &did) == MYDB_ERR_DUPLICATE,
          "same composite key rejected");
    CHECK(ss.privileges.num_privileges == 1, "duplicate did not bump count");

    system_schema_close(&ss);
}

static void test_priv_empty_schema_rejected(void)
{
    printf("\n[test_priv_empty_schema_rejected]\n");
    cleanup(); mk_dirs();

    SystemSchema ss;
    system_schema_create(TEST_ROOT, &ss);

    PrivilegeSlot p;
    memset(&p, 0, sizeof(p));   /* schema_name is empty */
    p.grantee_id = 2; p.partition_id = 1; p.granted_by = 1;
    uint32_t id;
    CHECK(privileges_insert(&ss.privileges, &p, &id) == MYDB_ERR,
          "empty schema_name rejected");
    system_schema_close(&ss);
}

static void test_priv_full_file(void)
{
    printf("\n[test_priv_full_file]\n");
    cleanup(); mk_dirs();

    SystemSchema ss;
    system_schema_create(TEST_ROOT, &ss);

    int all_ok = 1;
    for (int i = 0; i < PRIVILEGES_MAX_SLOTS; i++) {
        char schema[16];
        snprintf(schema, sizeof(schema), "s%03d", i);
        PrivilegeSlot p = make_priv(1, 1, schema, 1);
        uint32_t id;
        if (privileges_insert(&ss.privileges, &p, &id) != MYDB_OK) {
            all_ok = 0; break;
        }
    }
    CHECK(all_ok, "fill PRIVILEGES_MAX_SLOTS privs OK");
    CHECK(ss.privileges.num_privileges == PRIVILEGES_MAX_SLOTS,
          "num_privileges at capacity");

    PrivilegeSlot extra = make_priv(1, 1, "overflow", 1);
    uint32_t id;
    CHECK(privileges_insert(&ss.privileges, &extra, &id) == MYDB_ERR_FULL,
          "insert past capacity returns FULL");

    system_schema_close(&ss);
}

static void test_priv_delete(void)
{
    printf("\n[test_priv_delete]\n");
    cleanup(); mk_dirs();

    SystemSchema ss;
    system_schema_create(TEST_ROOT, &ss);
    PrivilegeSlot a = make_priv(2, 1, "hr",      1);
    PrivilegeSlot b = make_priv(2, 1, "payroll", 1);
    uint32_t pid1, pid2;
    privileges_insert(&ss.privileges, &a, &pid1);
    privileges_insert(&ss.privileges, &b, &pid2);

    CHECK(privileges_delete(&ss.privileges, 2, 1, "hr") == MYDB_OK,
          "delete hr grant OK");
    CHECK(ss.privileges.num_privileges == 1, "num_privileges decremented");

    PrivilegeSlot got;
    CHECK(privileges_find(&ss.privileges, 2, 1, "hr", &got) == MYDB_ERR_NOT_FOUND,
          "deleted grant not findable");
    CHECK(privileges_find(&ss.privileges, 2, 1, "payroll", &got) == MYDB_OK,
          "other grant still findable");

    CHECK(privileges_delete(&ss.privileges, 9, 9, "ghost") == MYDB_ERR_NOT_FOUND,
          "delete missing key returns NOT_FOUND");

    system_schema_close(&ss);
}


/* ================================================================== */
/*  Persistence                                                       */
/* ================================================================== */

static void test_users_persist_across_reopen(void)
{
    printf("\n[test_users_persist_across_reopen]\n");
    cleanup(); mk_dirs();

    SystemSchema ss;
    system_schema_create(TEST_ROOT, &ss);
    UserSlot a = make_user("alice", 1);
    UserSlot b = make_user("bob",   1);
    uint32_t aid, bid;
    users_insert(&ss.users, &a, &aid);
    users_insert(&ss.users, &b, &bid);
    system_schema_close(&ss);

    SystemSchema ss2;
    CHECK(system_schema_open(TEST_ROOT, &ss2) == MYDB_OK, "reopen OK");
    CHECK(ss2.users.num_users == 2, "num_users persisted");
    CHECK(ss2.users.next_user_id == 3, "next_user_id persisted");

    UserSlot got;
    CHECK(users_find_by_name(&ss2.users, "alice", &got) == MYDB_OK,
          "alice found after reopen");
    CHECK(got.user_id == aid, "alice user_id preserved");
    CHECK(got.password_hash[0] == 0xAB, "alice hash bytes preserved");
    CHECK(users_find_by_name(&ss2.users, "bob", &got) == MYDB_OK,
          "bob found after reopen");

    system_schema_close(&ss2);
}

static void test_priv_persist_across_reopen(void)
{
    printf("\n[test_priv_persist_across_reopen]\n");
    cleanup(); mk_dirs();

    SystemSchema ss;
    system_schema_create(TEST_ROOT, &ss);
    PrivilegeSlot a = make_priv(2, 1, "hr",      1);
    PrivilegeSlot b = make_priv(3, 2, "payroll", 1);
    uint32_t pid1, pid2;
    privileges_insert(&ss.privileges, &a, &pid1);
    privileges_insert(&ss.privileges, &b, &pid2);
    system_schema_close(&ss);

    SystemSchema ss2;
    system_schema_open(TEST_ROOT, &ss2);
    CHECK(ss2.privileges.num_privileges == 2, "count persisted");
    CHECK(ss2.privileges.next_privilege_id == 3, "next id persisted");

    PrivilegeSlot got;
    CHECK(privileges_find(&ss2.privileges, 2, 1, "hr", &got) == MYDB_OK,
          "first grant found after reopen");
    CHECK(privileges_find(&ss2.privileges, 3, 2, "payroll", &got) == MYDB_OK,
          "second grant found after reopen");

    system_schema_close(&ss2);
}

static void test_delete_persists(void)
{
    printf("\n[test_delete_persists]\n");
    cleanup(); mk_dirs();

    SystemSchema ss;
    system_schema_create(TEST_ROOT, &ss);
    UserSlot a = make_user("alice", 1);
    UserSlot b = make_user("bob",   1);
    uint32_t aid, bid;
    users_insert(&ss.users, &a, &aid);
    users_insert(&ss.users, &b, &bid);
    users_delete(&ss.users, aid);
    system_schema_close(&ss);

    SystemSchema ss2;
    system_schema_open(TEST_ROOT, &ss2);
    UserSlot got;
    CHECK(users_find_by_name(&ss2.users, "alice", &got) == MYDB_ERR_NOT_FOUND,
          "deleted user stays gone after reopen");
    CHECK(users_find_by_name(&ss2.users, "bob", &got) == MYDB_OK,
          "remaining user still there");
    CHECK(ss2.users.num_users == 1, "num_users persisted post-delete");
    system_schema_close(&ss2);
}


/* ================================================================== */
/*  Hash map stress (collision behaviour)                             */
/* ================================================================== */

static void test_users_hashmap_many(void)
{
    printf("\n[test_users_hashmap_many]\n");
    cleanup(); mk_dirs();

    SystemSchema ss;
    system_schema_create(TEST_ROOT, &ss);

    /* Fill the file. The probe chain must accommodate every entry
     * without false negatives. */
    char names[USERS_MAX_SLOTS][16];
    uint32_t ids[USERS_MAX_SLOTS];
    for (int i = 0; i < USERS_MAX_SLOTS; i++) {
        snprintf(names[i], sizeof(names[i]), "user_%d", i);
        UserSlot u = make_user(names[i], 1);
        users_insert(&ss.users, &u, &ids[i]);
    }

    int all_found = 1;
    for (int i = 0; i < USERS_MAX_SLOTS; i++) {
        UserSlot got;
        if (users_find_by_name(&ss.users, names[i], &got) != MYDB_OK) {
            all_found = 0; break;
        }
        if (got.user_id != ids[i]) { all_found = 0; break; }
    }
    CHECK(all_found, "all 32 users findable via hash map");

    /* Delete every other user, then verify remaining ones still
     * findable — exercises probe-chain rebuild on delete. */
    for (int i = 0; i < USERS_MAX_SLOTS; i += 2) {
        users_delete(&ss.users, ids[i]);
    }
    int still_ok = 1;
    for (int i = 1; i < USERS_MAX_SLOTS; i += 2) {
        UserSlot got;
        if (users_find_by_name(&ss.users, names[i], &got) != MYDB_OK) {
            still_ok = 0; break;
        }
    }
    CHECK(still_ok, "odd-indexed users still findable after deletes");
    int gone_ok = 1;
    for (int i = 0; i < USERS_MAX_SLOTS; i += 2) {
        UserSlot got;
        if (users_find_by_name(&ss.users, names[i], &got) != MYDB_ERR_NOT_FOUND) {
            gone_ok = 0; break;
        }
    }
    CHECK(gone_ok, "even-indexed users no longer findable");

    system_schema_close(&ss);
}

static void test_priv_hashmap_many(void)
{
    printf("\n[test_priv_hashmap_many]\n");
    cleanup(); mk_dirs();

    SystemSchema ss;
    system_schema_create(TEST_ROOT, &ss);

    /* Insert 100 grants: vary grantee, partition, schema. Composite
     * key must distinguish them all. */
    char schemas[100][16];
    uint32_t ids[100];
    for (int i = 0; i < 100; i++) {
        snprintf(schemas[i], sizeof(schemas[i]), "s%03d", i);
        PrivilegeSlot p = make_priv((uint32_t)(i % 7) + 1,
                                    (uint32_t)(i % 3) + 1,
                                    schemas[i], 1);
        privileges_insert(&ss.privileges, &p, &ids[i]);
    }

    int all_found = 1;
    for (int i = 0; i < 100; i++) {
        PrivilegeSlot got;
        if (privileges_find(&ss.privileges,
                            (uint32_t)(i % 7) + 1,
                            (uint32_t)(i % 3) + 1,
                            schemas[i], &got) != MYDB_OK) {
            all_found = 0; break;
        }
        if (got.privilege_id != ids[i]) { all_found = 0; break; }
    }
    CHECK(all_found, "all 100 privileges findable via composite hash");

    /* Delete a chunk in the middle, then verify remaining ones. */
    for (int i = 30; i < 60; i++) {
        privileges_delete(&ss.privileges,
                          (uint32_t)(i % 7) + 1,
                          (uint32_t)(i % 3) + 1,
                          schemas[i]);
    }
    int still_ok = 1;
    for (int i = 0; i < 100; i++) {
        if (i >= 30 && i < 60) continue;
        PrivilegeSlot got;
        if (privileges_find(&ss.privileges,
                            (uint32_t)(i % 7) + 1,
                            (uint32_t)(i % 3) + 1,
                            schemas[i], &got) != MYDB_OK) {
            still_ok = 0; break;
        }
    }
    CHECK(still_ok, "non-deleted privileges still findable after bulk delete");
    CHECK(ss.privileges.num_privileges == 70, "count reflects 30 deletes");

    system_schema_close(&ss);
}


/* ================================================================== */
/*  Independence of users vs. privileges                              */
/* ================================================================== */

static void test_users_and_privs_independent(void)
{
    printf("\n[test_users_and_privs_independent]\n");
    cleanup(); mk_dirs();

    SystemSchema ss;
    system_schema_create(TEST_ROOT, &ss);

    UserSlot a = make_user("alice", 1);
    uint32_t aid;
    users_insert(&ss.users, &a, &aid);

    PrivilegeSlot p = make_priv(aid, 1, "hr", aid);
    uint32_t pid;
    privileges_insert(&ss.privileges, &p, &pid);

    /* Phase 7 does not enforce FK consistency — deleting the user
     * must NOT touch privileges (that's phase 10's job). */
    users_delete(&ss.users, aid);
    PrivilegeSlot got;
    CHECK(privileges_find(&ss.privileges, aid, 1, "hr", &got) == MYDB_OK,
          "privilege survives user delete (FK check is phase 10)");

    system_schema_close(&ss);
}


/* ================================================================== */
/*  Argument validation                                               */
/* ================================================================== */

static void test_null_arguments(void)
{
    printf("\n[test_null_arguments]\n");
    cleanup(); mk_dirs();

    SystemSchema ss;
    system_schema_create(TEST_ROOT, &ss);

    UserSlot u; uint32_t id;
    CHECK(users_insert(NULL, &u, &id)        == MYDB_ERR, "insert NULL uf");
    CHECK(users_insert(&ss.users, NULL, &id) == MYDB_ERR, "insert NULL slot");
    CHECK(users_insert(&ss.users, &u, NULL)  == MYDB_ERR, "insert NULL out");

    UserSlot got;
    CHECK(users_find_by_name(NULL, "x", &got)        == MYDB_ERR, "find NULL uf");
    CHECK(users_find_by_name(&ss.users, NULL, &got)  == MYDB_ERR, "find NULL name");
    CHECK(users_find_by_name(&ss.users, "x", NULL)   == MYDB_ERR, "find NULL out");

    CHECK(system_schema_open(NULL, &ss) == MYDB_ERR, "open NULL root");
    CHECK(system_schema_open(TEST_ROOT, NULL) == MYDB_ERR, "open NULL out");

    system_schema_close(&ss);
}


/* ================================================================== */
/*  Driver                                                            */
/* ================================================================== */

int main(void)
{
    printf("=== test_system_schema ===\n");

    test_create_round_trip();
    test_create_rejects_existing();
    test_open_after_create();
    test_open_missing_dir_fails();

    test_open_rejects_users_bad_magic();
    test_open_rejects_users_wrong_filetype();
    test_open_rejects_users_bad_checksum();
    test_open_rejects_priv_bad_magic();
    test_open_rejects_priv_wrong_filetype();
    test_open_rejects_priv_bad_checksum();

    test_users_insert_assigns_ids();
    test_users_find_by_name();
    test_users_find_by_id();
    test_users_duplicate_rejected();
    test_users_empty_username_rejected();
    test_users_full_file();
    test_users_update_same_name();
    test_users_update_renames();
    test_users_update_rename_collides();
    test_users_update_unknown_id();
    test_users_delete();
    test_users_delete_then_reinsert();

    test_priv_insert_assigns_ids();
    test_priv_find_composite_key();
    test_priv_find_by_id();
    test_priv_duplicate_rejected();
    test_priv_empty_schema_rejected();
    test_priv_full_file();
    test_priv_delete();

    test_users_persist_across_reopen();
    test_priv_persist_across_reopen();
    test_delete_persists();

    test_users_hashmap_many();
    test_priv_hashmap_many();

    test_users_and_privs_independent();
    test_null_arguments();

    printf("\n%d/%d tests passed\n", tests_passed, tests_run);
    cleanup();
    return (tests_passed == tests_run) ? 0 : 1;
}
