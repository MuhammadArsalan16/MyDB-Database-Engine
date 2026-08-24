#include <stdio.h>
#include <string.h>

#include "checksum.h"

static int tests_run    = 0;
static int tests_passed = 0;

#define CHECK(cond, msg) do { \
    tests_run++; \
    if (cond) { tests_passed++; printf("  PASS: %s\n", msg); } \
    else       { printf("  FAIL: %s (line %d)\n", msg, __LINE__); } \
} while(0)

/* ------------------------------------------------------------------ */

static void test_crc32_check_value(void)
{
    printf("\n[test_crc32_check_value]\n");
    /* Standard CRC-32/ISO-HDLC check value: CRC-32 of the ASCII string
     * "123456789" is 0xCBF43926 — used across every CRC-32 implementation
     * (zlib, InnoDB, etc.) as the canonical self-test vector. */
    const char *check_str = "123456789";
    uint32_t cs = crc32(check_str, strlen(check_str));
    CHECK(cs == 0xCBF43926u, "crc32(\"123456789\") == 0xCBF43926");
}

static void test_crc32_empty(void)
{
    printf("\n[test_crc32_empty]\n");
    uint32_t cs = crc32("", 0);
    CHECK(cs == 0x00000000u, "crc32 of zero-length input is 0");
}

static void test_crc32_round_trip(void)
{
    printf("\n[test_crc32_round_trip]\n");
    uint8_t buf[64];
    for (int i = 0; i < 64; i++) buf[i] = (uint8_t)(i * 7 + 3);

    uint32_t a = crc32(buf, sizeof(buf));
    uint32_t b = crc32(buf, sizeof(buf));
    CHECK(a == b, "same input produces the same checksum");
}

static void test_crc32_detects_tamper(void)
{
    printf("\n[test_crc32_detects_tamper]\n");
    uint8_t buf[64];
    for (int i = 0; i < 64; i++) buf[i] = (uint8_t)(i * 7 + 3);

    uint32_t before = crc32(buf, sizeof(buf));
    buf[31] ^= 0x01;   /* flip one bit */
    uint32_t after = crc32(buf, sizeof(buf));
    CHECK(before != after, "flipping one bit changes the checksum");
}

static void test_fnv1a_still_works(void)
{
    printf("\n[test_fnv1a_still_works]\n");
    /* fnv1a() stays in checksum.h for the hash-table use cases
     * (system_schema.c's name_hash()/priv_key_hash()) even though every
     * corruption-detection checksum in the codebase moved to crc32(). */
    uint32_t a = fnv1a("hello", 5);
    uint32_t b = fnv1a("hello", 5);
    uint32_t c = fnv1a("world", 5);
    CHECK(a == b, "fnv1a is deterministic");
    CHECK(a != c, "fnv1a distinguishes different inputs");
}

/* ------------------------------------------------------------------ */

int main(void)
{
    printf("=== test_checksum ===\n");

    test_crc32_check_value();
    test_crc32_empty();
    test_crc32_round_trip();
    test_crc32_detects_tamper();
    test_fnv1a_still_works();

    printf("\nResults: %d/%d passed\n", tests_passed, tests_run);
    return (tests_passed == tests_run) ? 0 : 1;
}
