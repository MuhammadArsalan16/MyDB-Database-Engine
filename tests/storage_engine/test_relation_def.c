#include <stdio.h>
#include <string.h>

#include "common.h"
#include "relation_def.h"

static int tests_run    = 0;
static int tests_passed = 0;

#define CHECK(cond, msg) do { \
    tests_run++; \
    if (cond) { tests_passed++; printf("  PASS: %s\n", msg); } \
    else       { printf("  FAIL: %s (line %d)\n", msg, __LINE__); } \
} while(0)


static void test_col_size_fixed_widths(void)
{
    printf("\n[test_col_size_fixed_widths]\n");
    ColumnDef c;
    memset(&c, 0, sizeof(c));

    c.type = TYPE_INT;      CHECK(relation_col_size(&c) == 4,  "INT size = 4");
    c.type = TYPE_DECIMAL;  CHECK(relation_col_size(&c) == 8,  "DECIMAL size = 8");
    c.type = TYPE_BOOL;     CHECK(relation_col_size(&c) == 1,  "BOOL size = 1");
    c.type = TYPE_ENUM;     CHECK(relation_col_size(&c) == 1,  "ENUM size = 1");
    c.type = TYPE_DATE;     CHECK(relation_col_size(&c) == 4,  "DATE size = 4");
    c.type = TYPE_DATETIME; CHECK(relation_col_size(&c) == 8,  "DATETIME size = 8");
}

static void test_col_size_varchar_uses_max_len(void)
{
    printf("\n[test_col_size_varchar_uses_max_len]\n");
    ColumnDef c;
    memset(&c, 0, sizeof(c));
    c.type = TYPE_VARCHAR;

    c.max_len = 50;
    CHECK(relation_col_size(&c) == 52,
          "VARCHAR(50) size = 52 (2-byte len prefix + 50)");

    c.max_len = MAX_VARCHAR_LEN;
    CHECK(relation_col_size(&c) == 152,
          "VARCHAR(150) size = 152 (2-byte len prefix + 150)");

    c.max_len = 0;
    CHECK(relation_col_size(&c) == 2,
          "VARCHAR(0) size = 2 (just the length prefix)");
}

static void test_col_size_unknown_type(void)
{
    printf("\n[test_col_size_unknown_type]\n");
    ColumnDef c;
    memset(&c, 0, sizeof(c));
    c.type = (DataType)99;     /* outside the enum */
    CHECK(relation_col_size(&c) == 0, "unknown type → 0");
}

static void test_row_size_sums_columns(void)
{
    printf("\n[test_row_size_sums_columns]\n");
    RelationDef r;
    memset(&r, 0, sizeof(r));
    r.num_columns = 3;
    r.columns[0].type = TYPE_INT;                     /* 4 */
    r.columns[1].type = TYPE_VARCHAR; r.columns[1].max_len = 32;  /* 34 */
    r.columns[2].type = TYPE_BOOL;                    /* 1 */

    CHECK(relation_row_size(&r) == 4 + 34 + 1,
          "row_size = sum of col sizes (INT + VARCHAR(32) + BOOL = 39)");
}

static void test_row_size_zero_columns(void)
{
    printf("\n[test_row_size_zero_columns]\n");
    RelationDef r;
    memset(&r, 0, sizeof(r));
    /* num_columns left at 0 */
    CHECK(relation_row_size(&r) == 0, "row_size with 0 columns = 0");
}

static void test_row_size_max_columns(void)
{
    printf("\n[test_row_size_max_columns]\n");
    RelationDef r;
    memset(&r, 0, sizeof(r));
    r.num_columns = MAX_COLUMNS;
    for (int i = 0; i < MAX_COLUMNS; i++) r.columns[i].type = TYPE_INT;
    CHECK(relation_row_size(&r) == (uint32_t)MAX_COLUMNS * 4,
          "row_size scales linearly to MAX_COLUMNS");
}


int main(void)
{
    printf("=== test_relation_def ===\n");

    test_col_size_fixed_widths();
    test_col_size_varchar_uses_max_len();
    test_col_size_unknown_type();
    test_row_size_sums_columns();
    test_row_size_zero_columns();
    test_row_size_max_columns();

    printf("\nResults: %d/%d passed\n", tests_passed, tests_run);
    return (tests_passed == tests_run) ? 0 : 1;
}
