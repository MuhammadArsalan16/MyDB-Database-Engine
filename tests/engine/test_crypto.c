#include <stdio.h>
#include <string.h>

#include "common.h"
#include "crypto.h"

static int tests_run    = 0;
static int tests_passed = 0;

#define CHECK(cond, msg) do { \
    tests_run++; \
    if (cond) { tests_passed++; printf("  PASS: %s\n", msg); } \
    else       { printf("  FAIL: %s (line %d)\n", msg, __LINE__); } \
} while(0)

/* Hex compare — only care about case-insensitive bytes. */
static int hex_eq(const uint8_t bytes[32], const char *hex)
{
    char actual[65];
    for (int i = 0; i < 32; i++)
        snprintf(actual + i*2, 3, "%02x", bytes[i]);
    actual[64] = '\0';
    return strcmp(actual, hex) == 0;
}


/* ================================================================== */
/*  SHA-256 known-answer vectors (NIST FIPS-180-4 examples)           */
/* ================================================================== */

static void test_sha256_empty(void)
{
    printf("\n[test_sha256_empty]\n");
    uint8_t out[32];
    sha256("", 0, out);
    CHECK(hex_eq(out,
        "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"),
        "SHA-256(\"\") matches NIST KAT");
}

static void test_sha256_abc(void)
{
    printf("\n[test_sha256_abc]\n");
    uint8_t out[32];
    sha256("abc", 3, out);
    CHECK(hex_eq(out,
        "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"),
        "SHA-256(\"abc\") matches NIST KAT");
}

static void test_sha256_long(void)
{
    printf("\n[test_sha256_long]\n");
    /* 56-character message — exercises the 56-byte padding boundary. */
    const char *m =
        "abcdbcdecdefdefgefghfghighijhijkijkljklmklmnlmnomnopnopq";
    uint8_t out[32];
    sha256(m, strlen(m), out);
    CHECK(hex_eq(out,
        "248d6a61d20638b8e5c026930c3e6039a33ce45964ff2167f6ecedd419db06c1"),
        "SHA-256 56-byte input KAT");
}

static void test_sha256_two_block(void)
{
    printf("\n[test_sha256_two_block]\n");
    /* Length crosses one block — exercises the multi-block path. */
    const char *m =
        "abcdefghbcdefghicdefghijdefghijkefghijklfghijklmghijklmn"
        "hijklmnoijklmnopjklmnopqklmnopqrlmnopqrsmnopqrstnopqrstu";
    uint8_t out[32];
    sha256(m, strlen(m), out);
    CHECK(hex_eq(out,
        "cf5b16a778af8380036ce59e7b0492370b249b11e8f07a51afac45037afee9d1"),
        "SHA-256 two-block input KAT");
}

static void test_sha256_million_a(void)
{
    printf("\n[test_sha256_million_a]\n");
    /* The classic 1,000,000 'a' KAT — exercises long-message padding. */
    static char buf[1000000];
    memset(buf, 'a', sizeof(buf));
    uint8_t out[32];
    sha256(buf, sizeof(buf), out);
    CHECK(hex_eq(out,
        "cdc76e5c9914fb9281a1c7e284d73e67f1809a48a497200e046d39ccc7112cd0"),
        "SHA-256(1M 'a') matches NIST KAT");
}


/* ================================================================== */
/*  Password hashing                                                   */
/* ================================================================== */

static void test_password_deterministic(void)
{
    printf("\n[test_password_deterministic]\n");
    uint8_t salt[16];
    memset(salt, 0x42, sizeof(salt));

    uint8_t a[32], b[32];
    crypto_hash_password("hunter2", salt, a);
    crypto_hash_password("hunter2", salt, b);
    CHECK(memcmp(a, b, 32) == 0, "same password+salt → same hash");
}

static void test_password_salt_changes_output(void)
{
    printf("\n[test_password_salt_changes_output]\n");
    uint8_t s1[16] = {0}, s2[16];
    memset(s2, 0xff, sizeof(s2));

    uint8_t a[32], b[32];
    crypto_hash_password("p", s1, a);
    crypto_hash_password("p", s2, b);
    CHECK(memcmp(a, b, 32) != 0, "different salt → different hash");
}

static void test_password_different_pw(void)
{
    printf("\n[test_password_different_pw]\n");
    uint8_t salt[16];
    memset(salt, 0xaa, sizeof(salt));

    uint8_t a[32], b[32];
    crypto_hash_password("alpha", salt, a);
    crypto_hash_password("alphb", salt, b);
    CHECK(memcmp(a, b, 32) != 0, "different password → different hash");
}

static void test_password_known_value(void)
{
    printf("\n[test_password_known_value]\n");
    /* salt = 16 zero bytes, password = "abc"
     * Expected = SHA-256(0x00 * 16 || "abc") */
    uint8_t salt[16] = {0};
    uint8_t out[32];
    crypto_hash_password("abc", salt, out);

    /* Cross-check: feed the same bytes manually through sha256(). */
    uint8_t buf[19] = {0};
    memcpy(buf + 16, "abc", 3);
    uint8_t expected[32];
    sha256(buf, sizeof(buf), expected);

    CHECK(memcmp(out, expected, 32) == 0,
          "crypto_hash_password matches manual SHA-256(salt||password)");
}


/* ================================================================== */
/*  Random salt                                                        */
/* ================================================================== */

static void test_random_salt_nonzero(void)
{
    printf("\n[test_random_salt_nonzero]\n");
    uint8_t s[16];
    CHECK(crypto_random_salt(s) == MYDB_OK, "crypto_random_salt OK");

    int any_nonzero = 0;
    for (int i = 0; i < 16; i++) if (s[i] != 0) { any_nonzero = 1; break; }
    CHECK(any_nonzero, "salt has at least one non-zero byte");
}

static void test_random_salt_changes(void)
{
    printf("\n[test_random_salt_changes]\n");
    uint8_t a[16], b[16];
    crypto_random_salt(a);
    crypto_random_salt(b);
    CHECK(memcmp(a, b, 16) != 0, "consecutive salts differ");
}


int main(void)
{
    printf("=== test_crypto ===\n");

    test_sha256_empty();
    test_sha256_abc();
    test_sha256_long();
    test_sha256_two_block();
    test_sha256_million_a();

    test_password_deterministic();
    test_password_salt_changes_output();
    test_password_different_pw();
    test_password_known_value();

    test_random_salt_nonzero();
    test_random_salt_changes();

    printf("\n%d/%d tests passed\n", tests_passed, tests_run);
    return (tests_passed == tests_run) ? 0 : 1;
}
