/*
 * test_exec_engine.cpp — tests for execution engine phases 1–5.1.
 *
 *  Phase 1   — TCL     : BEGIN / COMMIT / ROLLBACK via engine_execute_sql.
 *  Phase 2   — Helpers : unit tests for value_cast and expr_eval.
 *  Phase 3   — DDL     : CREATE/DROP DATABASE, USE, CREATE/DROP TABLE,
 *                        SHOW TABLES / SHOW DATABASES via engine_execute_sql.
 *  Phase 4   — INSERT  : full INSERT coverage via engine_execute_sql.
 *  Phase 5.1 — SELECT  : single-table SELECT, projection, WHERE filtering,
 *                        access-path selection, LIMIT / OFFSET.
 *
 * Test isolation: every test group that touches the disk calls full_setup()
 * which wipes /tmp/mydb_test_exec and bootstraps a clean engine.
 */

#include <cstdio>
#include <cstring>
#include <cstdlib>
#include <memory>
#include <unistd.h>
#include <sys/stat.h>

extern "C" {
#include "engine.h"
#include "storage.h"
#include "schema_file.h"
#include "partition.h"
#include "common.h"
}

/* execution engine helpers — included directly for unit tests */
#include "value_cast.hpp"
#include "expr_eval.hpp"

/* ======================================================================
 * Globals and test harness
 * ====================================================================== */

#define TEST_ROOT  "/tmp/mydb_test_exec"
#define TEST_USER  "root"
#define TEST_PASS  "pass"

static int tests_run    = 0;
static int tests_passed = 0;
static EngineState g_eng;

/* result buffer shared between all SQL helpers */
static char g_res[4096];

#define CHECK(cond, msg) do {                                           \
    tests_run++;                                                        \
    if (cond) { tests_passed++; printf("  PASS: %s\n", msg); }        \
    else       { printf("  FAIL: %s  (line %d)\n", msg, __LINE__); }  \
} while (0)

/* ======================================================================
 * Session helpers
 * ====================================================================== */

static void rm_rf(const char *path)
{
    char cmd[512];
    snprintf(cmd, sizeof(cmd), "rm -rf '%s'", path);
    (void)!system(cmd);
}

/*
 * Bootstrap a clean engine, log in, and call storage_init.
 * No schema is made active — DDL tests create their own via SQL.
 */
static void engine_setup(void)
{
    rm_rf(TEST_ROOT);
    engine_bootstrap(TEST_ROOT, TEST_USER, TEST_PASS);
    engine_init(TEST_ROOT, &g_eng);
    engine_login(&g_eng, TEST_USER, TEST_PASS);
    storage_init(&g_eng);
}

/*
 * Bootstrap + login + manually create a schema at the C level + USE it.
 * Used when tests need an active schema without going through SQL.
 */
static void engine_setup_with_schema(const char *name)
{
    rm_rf(TEST_ROOT);
    engine_bootstrap(TEST_ROOT, TEST_USER, TEST_PASS);
    engine_init(TEST_ROOT, &g_eng);
    engine_login(&g_eng, TEST_USER, TEST_PASS);

    /* create schema dir + __schema.mydb + register in catalog */
    char dir[256], path[256];
    snprintf(dir,  sizeof(dir),  "%s/%s", g_eng.current_partition_path, name);
    snprintf(path, sizeof(path), "%s/__schema.mydb", dir);
    mkdir(dir, 0755);
    SchemaFile sf;
    schema_create(path, g_eng.current_partition_id, name, &sf);
    schema_close(&sf);
    cat_add_schema(&g_eng.active_catalog, name);

    engine_use_schema(&g_eng, name);
    storage_init(&g_eng);
}

static void engine_teardown(void)
{
    storage_shutdown();
    engine_close(&g_eng);
}

/* Execute a SQL string, store result in g_res, return the rc. */
static int sql(const char *query)
{
    memset(g_res, 0, sizeof(g_res));
    return engine_execute_sql(&g_eng, query, g_res, sizeof(g_res));
}

/* ======================================================================
 * PHASE 1 — TCL
 * ====================================================================== */

static void test_tcl(void)
{
    printf("\n[test_tcl]\n");
    engine_setup_with_schema("main");   /* TCL calls storage_ functions → need storage_init */

    int rc;

    /* Normal BEGIN → COMMIT */
    rc = sql("BEGIN;");
    CHECK(rc == MYDB_OK, "BEGIN returns MYDB_OK");
    CHECK(strstr(g_res, "BEGIN") != NULL, "BEGIN result says BEGIN");

    rc = sql("COMMIT;");
    CHECK(rc == MYDB_OK, "COMMIT after BEGIN returns MYDB_OK");
    CHECK(strstr(g_res, "COMMIT") != NULL, "COMMIT result says COMMIT");

    /* Normal BEGIN → ROLLBACK */
    rc = sql("BEGIN;");
    CHECK(rc == MYDB_OK, "BEGIN (second time) returns MYDB_OK");

    rc = sql("ROLLBACK;");
    CHECK(rc == MYDB_OK, "ROLLBACK after BEGIN returns MYDB_OK");
    CHECK(strstr(g_res, "ROLLBACK") != NULL, "ROLLBACK result says ROLLBACK");

    /* COMMIT without prior BEGIN → MYDB_ERR_NO_TXN */
    rc = sql("COMMIT;");
    CHECK(rc == MYDB_ERR_NO_TXN, "COMMIT without BEGIN → MYDB_ERR_NO_TXN");
    CHECK(strstr(g_res, "ERROR") != NULL, "result has ERROR text");

    /* ROLLBACK without prior BEGIN → MYDB_ERR_NO_TXN */
    rc = sql("ROLLBACK;");
    CHECK(rc == MYDB_ERR_NO_TXN, "ROLLBACK without BEGIN → MYDB_ERR_NO_TXN");

    /* Nested BEGIN → error */
    sql("BEGIN;");
    rc = sql("BEGIN;");
    CHECK(rc != MYDB_OK, "nested BEGIN returns error");
    CHECK(strstr(g_res, "ERROR") != NULL, "nested BEGIN result has ERROR");
    sql("ROLLBACK;");   /* clean up */

    engine_teardown();
}

/* ======================================================================
 * PHASE 2 — value_cast unit tests
 * ====================================================================== */

static void test_value_cast(void)
{
    printf("\n[test_value_cast]\n");

    ColumnDef col;
    memset(&col, 0, sizeof(col));
    Value v;

    /* --- INT --- */
    col.type = TYPE_INT;
    v = cast_literal("42", col);
    CHECK(v.is_null == 0,   "INT 42 → not null");
    CHECK(v.v.int_val == 42, "INT 42 → int_val=42");

    v = cast_literal("-7", col);
    CHECK(v.v.int_val == -7, "INT -7 → int_val=-7");

    /* --- DECIMAL (scale=2) --- */
    col.type  = TYPE_DECIMAL;
    col.scale = 2;
    v = cast_literal("3.14", col);
    CHECK(v.is_null == 0,           "DECIMAL 3.14 → not null");
    CHECK(v.v.decimal_val == 314,   "DECIMAL 3.14 scale=2 → stored as 314");

    v = cast_literal("100", col);
    CHECK(v.v.decimal_val == 10000, "DECIMAL 100 scale=2 → stored as 10000");

    /* --- VARCHAR --- */
    col.type    = TYPE_VARCHAR;
    col.max_len = 50;
    v = cast_literal("hello", col);
    CHECK(v.is_null == 0,           "VARCHAR hello → not null");
    CHECK(v.v.varchar_val.len == 5, "VARCHAR hello → len=5");
    CHECK(strcmp(v.v.varchar_val.data, "hello") == 0,
          "VARCHAR hello → data correct");

    v = cast_literal("", col);
    CHECK(v.v.varchar_val.len == 0, "VARCHAR empty → len=0");

    /* --- BOOL --- */
    col.type = TYPE_BOOL;
    v = cast_literal("TRUE", col);
    CHECK(v.v.bool_val == 1, "BOOL TRUE → bool_val=1");

    v = cast_literal("FALSE", col);
    CHECK(v.v.bool_val == 0, "BOOL FALSE → bool_val=0");

    v = cast_literal("1", col);
    CHECK(v.v.bool_val == 1, "BOOL 1 → bool_val=1");

    /* --- ENUM --- */
    col.type           = TYPE_ENUM;
    col.num_enum_values = 3;
    strncpy(col.enum_values[0], "active",   MAX_ENUM_STR_LEN - 1);
    strncpy(col.enum_values[1], "inactive", MAX_ENUM_STR_LEN - 1);
    strncpy(col.enum_values[2], "pending",  MAX_ENUM_STR_LEN - 1);

    v = cast_literal("inactive", col);
    CHECK(v.is_null == 0,       "ENUM inactive → not null");
    CHECK(v.v.enum_val == 1,    "ENUM inactive → index=1");

    v = cast_literal("pending", col);
    CHECK(v.v.enum_val == 2,    "ENUM pending → index=2");

    v = cast_literal("unknown", col);
    CHECK(v.is_null == 1,       "ENUM unknown value → is_null=1");

    /* --- DATE (DD-MM-YYYY) --- */
    col.type = TYPE_DATE;
    v = cast_literal("15-06-2024", col);
    CHECK(v.is_null == 0,              "DATE 15-06-2024 → not null");
    CHECK(v.v.date_val == 20240615,    "DATE 15-06-2024 → stored as 20240615");

    /* DATE (YYYY-MM-DD ISO format) */
    v = cast_literal("2024-06-15", col);
    CHECK(v.v.date_val == 20240615,    "DATE 2024-06-15 (ISO) → stored as 20240615");

    /* --- DATETIME --- */
    col.type = TYPE_DATETIME;
    v = cast_literal("2024-01-15 10:30:45", col);
    CHECK(v.is_null == 0,                        "DATETIME → not null");
    CHECK(v.v.datetime_val == 20240115103045LL,  "DATETIME → stored correctly");

    /* --- NULL keyword (any type) --- */
    col.type = TYPE_INT;
    v = cast_literal("NULL", col);
    CHECK(v.is_null == 1, "NULL keyword → is_null=1");

    col.type = TYPE_VARCHAR;
    v = cast_literal("NULL", col);
    CHECK(v.is_null == 1, "NULL keyword on VARCHAR → is_null=1");
}

/* ======================================================================
 * PHASE 2 — expr_eval unit tests
 * ====================================================================== */

/*
 * Build a simple RelationDef with three columns:
 *   col 0: "id"   TYPE_INT
 *   col 1: "age"  TYPE_INT
 *   col 2: "name" TYPE_VARCHAR
 */
static RelationDef make_test_rel(void)
{
    RelationDef rel;
    memset(&rel, 0, sizeof(rel));
    strncpy(rel.relation_name, "test", MAX_TABLE_NAME - 1);
    rel.num_columns = 3;
    rel.pk_col_idx  = 0;

    strncpy(rel.columns[0].name, "id",   MAX_COLUMN_NAME - 1);
    rel.columns[0].type = TYPE_INT;

    strncpy(rel.columns[1].name, "age",  MAX_COLUMN_NAME - 1);
    rel.columns[1].type = TYPE_INT;

    strncpy(rel.columns[2].name, "name", MAX_COLUMN_NAME - 1);
    rel.columns[2].type    = TYPE_VARCHAR;
    rel.columns[2].max_len = 50;

    return rel;
}

/*
 * Build a row: id=1, age=30, name="Alice"
 * pass null_name=true to make name NULL.
 */
static Row make_test_row(int id, int age, const char *name, bool null_name = false)
{
    Row r;
    memset(&r, 0, sizeof(r));
    r.num_cols = 3;

    r.cols[0].type      = TYPE_INT;
    r.cols[0].v.int_val = id;

    r.cols[1].type      = TYPE_INT;
    r.cols[1].v.int_val = age;

    r.cols[2].type    = TYPE_VARCHAR;
    r.cols[2].is_null = null_name ? 1 : 0;
    if (!null_name) {
        size_t len = strlen(name);
        r.cols[2].v.varchar_val.len = (uint16_t)len;
        strncpy(r.cols[2].v.varchar_val.data, name, MAX_VARCHAR_LEN - 1);
    }

    return r;
}

/* Convenience: build "colname OP literal" as a BinaryExpr */
static std::unique_ptr<Expr> make_cmp(const char *col, const char *op,
                                      const char *val, TokenType tt)
{
    auto bin = std::make_unique<BinaryExpr>();
    bin->op = op;

    auto lhs     = std::make_unique<ColumnRefExpr>();
    lhs->column  = col;
    bin->lhs     = std::move(lhs);

    auto rhs  = std::make_unique<LiteralExpr>();
    rhs->type = tt;
    rhs->raw  = val;
    bin->rhs  = std::move(rhs);

    return bin;
}

static void test_expr_eval(void)
{
    printf("\n[test_expr_eval]\n");

    RelationDef rel = make_test_rel();
    Row         row = make_test_row(1, 30, "Alice");

    /* --- resolve_col --- */
    CHECK(resolve_col(&rel, "age")  == 1, "resolve_col age  → 1");
    CHECK(resolve_col(&rel, "name") == 2, "resolve_col name → 2");
    CHECK(resolve_col(&rel, "id")   == 0, "resolve_col id   → 0");
    CHECK(resolve_col(&rel, "xyz")  == -1,"resolve_col unknown → -1");

    /* --- NULL where clause → always passes --- */
    CHECK(where_matches(NULL, &rel, &row) == true,
          "NULL WhereClause → true");

    /* --- Simple equality (=) --- */
    {
        auto e = make_cmp("age", "=", "30", TokenType::NUMBER);
        CHECK(eval_expr(e.get(), &rel, &row) == true,  "age = 30 → true");
        auto e2 = make_cmp("age", "=", "25", TokenType::NUMBER);
        CHECK(eval_expr(e2.get(), &rel, &row) == false, "age = 25 → false");
    }

    /* --- Comparisons < > <= >= != --- */
    {
        auto gt = make_cmp("age", ">",  "25", TokenType::NUMBER);
        CHECK(eval_expr(gt.get(), &rel, &row) == true,  "age > 25 → true");

        auto lt = make_cmp("age", "<",  "25", TokenType::NUMBER);
        CHECK(eval_expr(lt.get(), &rel, &row) == false, "age < 25 → false");

        auto ge = make_cmp("age", ">=", "30", TokenType::NUMBER);
        CHECK(eval_expr(ge.get(), &rel, &row) == true,  "age >= 30 → true");

        auto ne = make_cmp("age", "!=", "30", TokenType::NUMBER);
        CHECK(eval_expr(ne.get(), &rel, &row) == false, "age != 30 → false");

        auto neq= make_cmp("age", "!=", "25", TokenType::NUMBER);
        CHECK(eval_expr(neq.get(), &rel, &row) == true, "age != 25 → true");
    }

    /* --- VARCHAR equality --- */
    {
        auto e = make_cmp("name", "=", "Alice", TokenType::STRING);
        CHECK(eval_expr(e.get(), &rel, &row) == true,  "name = Alice → true");
        auto e2 = make_cmp("name", "=", "Bob",  TokenType::STRING);
        CHECK(eval_expr(e2.get(), &rel, &row) == false, "name = Bob → false");
    }

    /* --- AND (both sides) --- */
    {
        auto bin = std::make_unique<BinaryExpr>();
        bin->op  = "AND";
        bin->lhs = make_cmp("age",  "=", "30",    TokenType::NUMBER);
        bin->rhs = make_cmp("name", "=", "Alice",  TokenType::STRING);
        CHECK(eval_expr(bin.get(), &rel, &row) == true,
              "age=30 AND name=Alice → true");

        auto bin2 = std::make_unique<BinaryExpr>();
        bin2->op  = "AND";
        bin2->lhs = make_cmp("age",  "=", "30",  TokenType::NUMBER);
        bin2->rhs = make_cmp("name", "=", "Bob", TokenType::STRING);
        CHECK(eval_expr(bin2.get(), &rel, &row) == false,
              "age=30 AND name=Bob → false");
    }

    /* --- OR --- */
    {
        auto bin = std::make_unique<BinaryExpr>();
        bin->op  = "OR";
        bin->lhs = make_cmp("age", "=", "99", TokenType::NUMBER);   /* false */
        bin->rhs = make_cmp("age", "=", "30", TokenType::NUMBER);   /* true  */
        CHECK(eval_expr(bin.get(), &rel, &row) == true,
              "age=99 OR age=30 → true");
    }

    /* --- NOT --- */
    {
        auto un    = std::make_unique<UnaryExpr>();
        un->op     = "NOT";
        un->child  = make_cmp("age", "=", "30", TokenType::NUMBER);
        CHECK(eval_expr(un.get(), &rel, &row) == false,
              "NOT age=30 → false");

        auto un2   = std::make_unique<UnaryExpr>();
        un2->op    = "NOT";
        un2->child = make_cmp("age", "=", "99", TokenType::NUMBER);
        CHECK(eval_expr(un2.get(), &rel, &row) == true,
              "NOT age=99 → true");
    }

    /* --- IS NULL / IS NOT NULL (non-null column) --- */
    {
        auto isn         = std::make_unique<IsNullExpr>();
        isn->negated     = false;
        auto col_ref     = std::make_unique<ColumnRefExpr>();
        col_ref->column  = "name";
        isn->child       = std::move(col_ref);
        CHECK(eval_expr(isn.get(), &rel, &row) == false,
              "name IS NULL (not null) → false");

        auto isn2        = std::make_unique<IsNullExpr>();
        isn2->negated    = true;
        auto col_ref2    = std::make_unique<ColumnRefExpr>();
        col_ref2->column = "name";
        isn2->child      = std::move(col_ref2);
        CHECK(eval_expr(isn2.get(), &rel, &row) == true,
              "name IS NOT NULL (not null) → true");
    }

    /* --- IS NULL on a null column --- */
    {
        Row null_row = make_test_row(2, 25, "", true);   /* name is NULL */

        auto isn        = std::make_unique<IsNullExpr>();
        isn->negated    = false;
        auto col_ref    = std::make_unique<ColumnRefExpr>();
        col_ref->column = "name";
        isn->child      = std::move(col_ref);
        CHECK(eval_expr(isn.get(), &rel, &null_row) == true,
              "name IS NULL (null column) → true");
    }

    /* --- BETWEEN (inclusive) --- */
    {
        auto bw = std::make_unique<BetweenExpr>();
        bw->negated = false;

        auto v_ref    = std::make_unique<ColumnRefExpr>();
        v_ref->column = "age";
        bw->v = std::move(v_ref);

        auto lo  = std::make_unique<LiteralExpr>();
        lo->type = TokenType::NUMBER;  lo->raw = "25";
        bw->lo   = std::move(lo);

        auto hi  = std::make_unique<LiteralExpr>();
        hi->type = TokenType::NUMBER;  hi->raw = "35";
        bw->hi   = std::move(hi);

        CHECK(eval_expr(bw.get(), &rel, &row) == true,
              "age BETWEEN 25 AND 35 → true (age=30)");
    }
    {
        auto bw = std::make_unique<BetweenExpr>();
        bw->negated = false;

        auto v_ref    = std::make_unique<ColumnRefExpr>();
        v_ref->column = "age";
        bw->v = std::move(v_ref);

        auto lo  = std::make_unique<LiteralExpr>();
        lo->type = TokenType::NUMBER;  lo->raw = "35";
        bw->lo   = std::move(lo);

        auto hi  = std::make_unique<LiteralExpr>();
        hi->type = TokenType::NUMBER;  hi->raw = "45";
        bw->hi   = std::move(hi);

        CHECK(eval_expr(bw.get(), &rel, &row) == false,
              "age BETWEEN 35 AND 45 → false (age=30)");
    }

    /* --- IN --- */
    {
        auto in_expr = std::make_unique<InExpr>();
        in_expr->negated = false;

        auto v_ref    = std::make_unique<ColumnRefExpr>();
        v_ref->column = "age";
        in_expr->v    = std::move(v_ref);

        for (const char *val : {"25", "30", "35"}) {
            auto lit  = std::make_unique<LiteralExpr>();
            lit->type = TokenType::NUMBER;
            lit->raw  = val;
            in_expr->list.push_back(std::move(lit));
        }

        CHECK(eval_expr(in_expr.get(), &rel, &row) == true,
              "age IN (25, 30, 35) → true");
    }
    {
        auto in_expr = std::make_unique<InExpr>();
        in_expr->negated = false;

        auto v_ref    = std::make_unique<ColumnRefExpr>();
        v_ref->column = "age";
        in_expr->v    = std::move(v_ref);

        for (const char *val : {"10", "20", "40"}) {
            auto lit  = std::make_unique<LiteralExpr>();
            lit->type = TokenType::NUMBER;
            lit->raw  = val;
            in_expr->list.push_back(std::move(lit));
        }

        CHECK(eval_expr(in_expr.get(), &rel, &row) == false,
              "age IN (10, 20, 40) → false");
    }

    /* --- LIKE --- */
    {
        auto lk      = std::make_unique<LikeExpr>();
        lk->negated  = false;
        lk->pattern  = "Ali%";
        auto v_ref   = std::make_unique<ColumnRefExpr>();
        v_ref->column= "name";
        lk->v        = std::move(v_ref);
        CHECK(eval_expr(lk.get(), &rel, &row) == true,
              "name LIKE 'Ali%' → true");
    }
    {
        auto lk      = std::make_unique<LikeExpr>();
        lk->negated  = false;
        lk->pattern  = "A_ice";
        auto v_ref   = std::make_unique<ColumnRefExpr>();
        v_ref->column= "name";
        lk->v        = std::move(v_ref);
        CHECK(eval_expr(lk.get(), &rel, &row) == true,
              "name LIKE 'A_ice' → true");
    }
    {
        auto lk      = std::make_unique<LikeExpr>();
        lk->negated  = false;
        lk->pattern  = "Bob%";
        auto v_ref   = std::make_unique<ColumnRefExpr>();
        v_ref->column= "name";
        lk->v        = std::move(v_ref);
        CHECK(eval_expr(lk.get(), &rel, &row) == false,
              "name LIKE 'Bob%' → false");
    }
    {
        /* NOT LIKE */
        auto lk      = std::make_unique<LikeExpr>();
        lk->negated  = true;
        lk->pattern  = "Bob%";
        auto v_ref   = std::make_unique<ColumnRefExpr>();
        v_ref->column= "name";
        lk->v        = std::move(v_ref);
        CHECK(eval_expr(lk.get(), &rel, &row) == true,
              "name NOT LIKE 'Bob%' → true");
    }
}

/* ======================================================================
 * PHASE 3 — DDL tests via engine_execute_sql
 * ====================================================================== */

static void test_ddl(void)
{
    printf("\n[test_ddl]\n");
    engine_setup();   /* login only — no schema yet */

    int rc;

    /* ---- CREATE DATABASE ---- */
    rc = sql("CREATE DATABASE testdb;");
    CHECK(rc == MYDB_OK, "CREATE DATABASE testdb → MYDB_OK");
    CHECK(strstr(g_res, "Query OK") != NULL,
          "CREATE DATABASE result says Query OK");

    /* duplicate schema → error */
    rc = sql("CREATE DATABASE testdb;");
    CHECK(rc != MYDB_OK, "CREATE DATABASE duplicate → error");

    /* ---- USE ---- */
    rc = sql("USE testdb;");
    CHECK(rc == MYDB_OK, "USE testdb → MYDB_OK");
    CHECK(strstr(g_res, "Database changed") != NULL,
          "USE result says 'Database changed'");

    /* USE non-existent schema → error */
    rc = sql("USE doesnotexist;");
    CHECK(rc != MYDB_OK, "USE nonexistent → error");

    /* ---- SHOW DATABASES ---- */
    rc = sql("SHOW DATABASES;");
    CHECK(rc == MYDB_OK, "SHOW DATABASES → MYDB_OK");
    CHECK(strstr(g_res, "testdb") != NULL,
          "SHOW DATABASES output contains 'testdb'");

    /* re-USE testdb (it was swapped away on the failed USE) */
    sql("USE testdb;");

    /* ---- CREATE TABLE ---- */
    rc = sql("CREATE TABLE users ("
             "  id   INT PRIMARY KEY,"
             "  name VARCHAR(50) NOT NULL"
             ");");
    CHECK(rc == MYDB_OK, "CREATE TABLE users → MYDB_OK");
    CHECK(strstr(g_res, "Query OK") != NULL,
          "CREATE TABLE result says Query OK");

    /* table with no PRIMARY KEY → error */
    rc = sql("CREATE TABLE nopk (id INT, val VARCHAR(10));");
    CHECK(rc != MYDB_OK, "CREATE TABLE without PK → error");
    CHECK(strstr(g_res, "PRIMARY KEY") != NULL,
          "error message mentions PRIMARY KEY");

    /* duplicate table → error */
    rc = sql("CREATE TABLE users (id INT PRIMARY KEY);");
    CHECK(rc != MYDB_OK, "CREATE TABLE duplicate → error");

    /* ---- SHOW TABLES ---- */
    rc = sql("SHOW TABLES;");
    CHECK(rc == MYDB_OK, "SHOW TABLES → MYDB_OK");
    CHECK(strstr(g_res, "users") != NULL,
          "SHOW TABLES output contains 'users'");

    /* ---- CREATE TABLE with all column types ---- */
    rc = sql("CREATE TABLE products ("
             "  id       INT PRIMARY KEY,"
             "  price    DECIMAL,"
             "  label    VARCHAR(100),"
             "  active   BOOL DEFAULT TRUE,"
             "  status   ENUM(new, used, refurb) DEFAULT new,"
             "  created  DATETIME"
             ");");
    CHECK(rc == MYDB_OK, "CREATE TABLE products (all types) → MYDB_OK");

    /* both tables now visible */
    rc = sql("SHOW TABLES;");
    CHECK(strstr(g_res, "users")    != NULL, "SHOW TABLES has users");
    CHECK(strstr(g_res, "products") != NULL, "SHOW TABLES has products");

    /* ---- DROP TABLE ---- */
    rc = sql("DROP TABLE users;");
    CHECK(rc == MYDB_OK, "DROP TABLE users → MYDB_OK");
    CHECK(strstr(g_res, "Query OK") != NULL,
          "DROP TABLE result says Query OK");

    /* users no longer visible */
    sql("SHOW TABLES;");
    CHECK(strstr(g_res, "users") == NULL,
          "SHOW TABLES after DROP: users not present");
    CHECK(strstr(g_res, "products") != NULL,
          "SHOW TABLES after DROP: products still present");

    /* DROP same table again → error */
    rc = sql("DROP TABLE users;");
    CHECK(rc != MYDB_OK, "DROP TABLE non-existent → error");

    /* ---- DROP DATABASE — not supported yet ---- */
    rc = sql("DROP DATABASE testdb;");
    CHECK(rc != MYDB_OK, "DROP DATABASE → not supported (error expected)");
    CHECK(strstr(g_res, "not yet supported") != NULL ||
          strstr(g_res, "ERROR") != NULL,
          "DROP DATABASE result has error text");

    engine_teardown();
}

/* ======================================================================
 * PHASE 4 — INSERT tests via engine_execute_sql
 * ====================================================================== */

static void test_insert(void)
{
    printf("\n[test_insert]\n");
    engine_setup();

    int rc;

    /* set up schema and table via SQL */
    sql("CREATE DATABASE shop;");
    sql("USE shop;");
    sql("CREATE TABLE products ("
        "  id     INT PRIMARY KEY,"
        "  name   VARCHAR(50) NOT NULL,"
        "  price  DECIMAL,"
        "  active BOOL DEFAULT TRUE"
        ");");

    /* ---- basic positional INSERT ---- */
    rc = sql("INSERT INTO products VALUES (1, 'Widget', 9.99, TRUE);");
    CHECK(rc == MYDB_OK, "INSERT positional → MYDB_OK");
    CHECK(strstr(g_res, "Query OK") != NULL, "INSERT result says Query OK");
    CHECK(strstr(g_res, "1 row") != NULL,    "INSERT result says 1 row affected");

    /* ---- named-column INSERT ---- */
    rc = sql("INSERT INTO products (id, name, price) VALUES (2, 'Gadget', 19.99);");
    CHECK(rc == MYDB_OK, "INSERT named columns → MYDB_OK");

    /* ---- INSERT uses DEFAULT for omitted column ---- */
    /* active column not provided → should default to TRUE (bool_val=1) */
    rc = sql("INSERT INTO products (id, name) VALUES (3, 'Doohickey');");
    CHECK(rc == MYDB_OK, "INSERT with omitted DEFAULT column → MYDB_OK");

    /* ---- duplicate PK → error ---- */
    rc = sql("INSERT INTO products VALUES (1, 'Dupe', 0.00, FALSE);");
    CHECK(rc == MYDB_ERR_DUPLICATE, "INSERT duplicate PK → MYDB_ERR_DUPLICATE");

    /* ---- NOT NULL violation ---- */
    rc = sql("INSERT INTO products (id, price) VALUES (10, 5.00);");
    CHECK(rc == MYDB_ERR_NULL_VIOLATION, "INSERT missing NOT NULL column → MYDB_ERR_NULL_VIOLATION");
    CHECK(strstr(g_res, "name") != NULL,
          "NOT NULL error message names the column");

    /* ---- wrong column count ---- */
    rc = sql("INSERT INTO products VALUES (99, 'TooFew');");
    CHECK(rc != MYDB_OK, "INSERT wrong value count → error");

    /* ---- INSERT into non-existent table ---- */
    rc = sql("INSERT INTO nosuchtable VALUES (1, 'x');");
    CHECK(rc == MYDB_ERR_NOT_FOUND, "INSERT unknown table → MYDB_ERR_NOT_FOUND");

    /* ---- AUTO_INCREMENT table ---- */
    sql("CREATE TABLE counters ("
        "  id    INT AUTO_INCREMENT PRIMARY KEY,"
        "  label VARCHAR(20) NOT NULL"
        ");");

    /* named INSERT — omit AUTO_INCREMENT column → MYDB_OK */
    rc = sql("INSERT INTO counters (label) VALUES ('first');");
    CHECK(rc == MYDB_OK, "INSERT AUTO_INCREMENT (omit PK, named) → MYDB_OK");

    rc = sql("INSERT INTO counters (label) VALUES ('second');");
    CHECK(rc == MYDB_OK, "INSERT AUTO_INCREMENT second row → MYDB_OK");

    /* positional INSERT — value count = non-AI columns only (1 value for label) */
    rc = sql("INSERT INTO counters VALUES ('third');");
    CHECK(rc == MYDB_OK, "INSERT AUTO_INCREMENT positional (1 value) → MYDB_OK");

    /* positional INSERT — too many values (includes AI slot) → error */
    rc = sql("INSERT INTO counters VALUES (99, 'fourth');");
    CHECK(rc != MYDB_OK, "INSERT AUTO_INCREMENT positional with AI value → error");

    /* named INSERT — explicitly naming the AUTO_INCREMENT column → error */
    rc = sql("INSERT INTO counters (id, label) VALUES (5, 'fifth');");
    CHECK(rc != MYDB_OK,                      "INSERT AI column by name → error");
    CHECK(strstr(g_res, "AUTO_INCREMENT") != NULL,
          "INSERT AI column by name → error mentions AUTO_INCREMENT");

    /*
     * Multi-row INSERT (VALUES (a),(b),(c)) is not yet supported by the
     * parser — it parses exactly one VALUES row per statement.
     * The execution engine loop is already in place and will handle
     * multiple rows correctly once the parser is updated.
     */

    engine_teardown();
}

/* ======================================================================
 * PHASE 5.1 — SELECT (basic single-table)
 * ====================================================================== */

static void test_select(void)
{
    printf("\n[test_select]\n");
    engine_setup();

    int rc;

    /* ---- REQUIRE_SCHEMA guard: SELECT before USE ---- */
    rc = sql("SELECT * FROM anything;");
    CHECK(rc != MYDB_OK, "SELECT without USE → error");
    CHECK(strstr(g_res, "ERROR") != NULL, "SELECT without USE → error message");

    /* ---- setup: create schema + table + data ---- */
    sql("CREATE DATABASE shop;");
    sql("USE shop;");

    /*
     * Use explicit INT PRIMARY KEY (not AUTO_INCREMENT) so test IDs are
     * predictable regardless of the AUTO_INCREMENT start value.
     */
    sql("CREATE TABLE products ("
        "  id      INT PRIMARY KEY,"
        "  name    VARCHAR(50) NOT NULL,"
        "  code    VARCHAR(20) UNIQUE,"
        "  price   INT,"
        "  active  BOOL DEFAULT TRUE"
        ");");

    /* insert 5 rows with known ids 1..5 */
    sql("INSERT INTO products (id, name, code, price, active)"
        " VALUES (1, 'Alpha',   'A001', 100, TRUE);");
    sql("INSERT INTO products (id, name, code, price, active)"
        " VALUES (2, 'Beta',    'B002', 200, FALSE);");
    sql("INSERT INTO products (id, name, code, price, active)"
        " VALUES (3, 'Gamma',   'G003', 150, TRUE);");
    sql("INSERT INTO products (id, name, code, price, active)"
        " VALUES (4, 'Delta',   'D004', 300, TRUE);");
    sql("INSERT INTO products (id, name, code, price, active)"
        " VALUES (5, 'Epsilon', 'E005', 250, FALSE);");

    /* ---- SELECT * — all rows ---- */
    rc = sql("SELECT * FROM products;");
    CHECK(rc == MYDB_OK,                    "SELECT * → MYDB_OK");
    CHECK(strstr(g_res, "id")   != NULL,    "SELECT * → header has 'id'");
    CHECK(strstr(g_res, "name") != NULL,    "SELECT * → header has 'name'");
    CHECK(strstr(g_res, "Alpha")  != NULL,  "SELECT * → row 'Alpha' present");
    CHECK(strstr(g_res, "Epsilon") != NULL, "SELECT * → row 'Epsilon' present");
    CHECK(strstr(g_res, "(5 rows)") != NULL,"SELECT * → 5 rows reported");

    /* ---- SELECT named columns (projection) ---- */
    rc = sql("SELECT name, price FROM products;");
    CHECK(rc == MYDB_OK,                     "SELECT name,price → MYDB_OK");
    CHECK(strstr(g_res, "name")  != NULL,    "SELECT name,price → header 'name'");
    CHECK(strstr(g_res, "price") != NULL,    "SELECT name,price → header 'price'");
    /* id and code must NOT appear in the header */
    CHECK(strstr(g_res, " id ")  == NULL,    "SELECT name,price → no 'id' column");
    CHECK(strstr(g_res, "(5 rows)") != NULL, "SELECT name,price → 5 rows");

    /* ---- WHERE with full scan (non-indexed column) ---- */
    rc = sql("SELECT * FROM products WHERE price > 150;");
    CHECK(rc == MYDB_OK,                      "WHERE price>150 → MYDB_OK");
    CHECK(strstr(g_res, "Beta")    != NULL,   "WHERE price>150 → Beta(200)");
    CHECK(strstr(g_res, "Delta")   != NULL,   "WHERE price>150 → Delta(300)");
    CHECK(strstr(g_res, "Epsilon") != NULL,   "WHERE price>150 → Epsilon(250)");
    CHECK(strstr(g_res, "Alpha")   == NULL,   "WHERE price>150 → no Alpha(100)");
    CHECK(strstr(g_res, "Gamma")   == NULL,   "WHERE price>150 → no Gamma(150)");
    CHECK(strstr(g_res, "(3 rows)") != NULL,  "WHERE price>150 → 3 rows");

    /* ---- WHERE pk = value → AP_GET_PK ---- */
    rc = sql("SELECT * FROM products WHERE id = 3;");
    CHECK(rc == MYDB_OK,                    "WHERE id=3 (PK) → MYDB_OK");
    CHECK(strstr(g_res, "Gamma") != NULL,   "WHERE id=3 → row is Gamma");
    CHECK(strstr(g_res, "(1 row)") != NULL, "WHERE id=3 → exactly 1 row");

    /* ---- WHERE unique_col = value → AP_GET_INDEX ---- */
    rc = sql("SELECT * FROM products WHERE code = 'D004';");
    CHECK(rc == MYDB_OK,                    "WHERE code='D004' (UNIQUE) → MYDB_OK");
    CHECK(strstr(g_res, "Delta") != NULL,   "WHERE code='D004' → row is Delta");
    CHECK(strstr(g_res, "(1 row)") != NULL, "WHERE code='D004' → exactly 1 row");

    /* ---- WHERE pk >= value → AP_SCAN_FROM ---- */
    rc = sql("SELECT * FROM products WHERE id >= 4;");
    CHECK(rc == MYDB_OK,                     "WHERE id>=4 (SCAN_FROM) → MYDB_OK");
    CHECK(strstr(g_res, "Delta")   != NULL,  "WHERE id>=4 → Delta present");
    CHECK(strstr(g_res, "Epsilon") != NULL,  "WHERE id>=4 → Epsilon present");
    CHECK(strstr(g_res, "Alpha")   == NULL,  "WHERE id>=4 → Alpha absent");
    CHECK(strstr(g_res, "(2 rows)") != NULL, "WHERE id>=4 → 2 rows");

    /* ---- LIMIT (no ORDER BY → cuts stream after N rows) ---- */
    rc = sql("SELECT * FROM products LIMIT 2;");
    CHECK(rc == MYDB_OK,                     "SELECT LIMIT 2 → MYDB_OK");
    CHECK(strstr(g_res, "(2 rows)") != NULL, "SELECT LIMIT 2 → exactly 2 rows");

    /* ---- OFFSET (skip first N rows) ---- */
    rc = sql("SELECT * FROM products LIMIT 10 OFFSET 3;");
    CHECK(rc == MYDB_OK,                     "LIMIT 10 OFFSET 3 → MYDB_OK");
    CHECK(strstr(g_res, "(2 rows)") != NULL, "LIMIT 10 OFFSET 3 → 2 rows (5-3)");

    /* ---- LIMIT + OFFSET combined ---- */
    rc = sql("SELECT * FROM products LIMIT 2 OFFSET 1;");
    CHECK(rc == MYDB_OK,                     "LIMIT 2 OFFSET 1 → MYDB_OK");
    CHECK(strstr(g_res, "(2 rows)") != NULL, "LIMIT 2 OFFSET 1 → 2 rows");
    /* rows 2 and 3 in PK order: Beta, Gamma */
    CHECK(strstr(g_res, "Beta")  != NULL,    "LIMIT 2 OFFSET 1 → Beta (row 2)");
    CHECK(strstr(g_res, "Gamma") != NULL,    "LIMIT 2 OFFSET 1 → Gamma (row 3)");
    CHECK(strstr(g_res, "Alpha") == NULL,    "LIMIT 2 OFFSET 1 → Alpha skipped");

    /* ---- empty result set (no rows match WHERE) ---- */
    rc = sql("SELECT * FROM products WHERE price > 9999;");
    CHECK(rc == MYDB_OK,                     "WHERE no match → MYDB_OK");
    CHECK(strstr(g_res, "(0 rows)") != NULL, "WHERE no match → 0 rows");
    /* header must still be present */
    CHECK(strstr(g_res, "id") != NULL,       "WHERE no match → header still shown");

    /* ---- ORDER BY price ASC (materialise path) ---- */
    rc = sql("SELECT * FROM products ORDER BY price ASC;");
    CHECK(rc == MYDB_OK,                      "ORDER BY price ASC → MYDB_OK");
    CHECK(strstr(g_res, "(5 rows)") != NULL,  "ORDER BY price ASC → 5 rows");
    /* Alpha(100) must appear before Epsilon(250) in output */
    CHECK(strstr(g_res, "Alpha")   != NULL,   "ORDER BY price ASC → Alpha present");
    CHECK(strstr(g_res, "Epsilon") != NULL,   "ORDER BY price ASC → Epsilon present");
    {
        const char *pos_alpha   = strstr(g_res, "Alpha");
        const char *pos_epsilon = strstr(g_res, "Epsilon");
        CHECK(pos_alpha < pos_epsilon,        "ORDER BY price ASC → Alpha before Epsilon");
    }

    /* ---- ORDER BY price DESC ---- */
    rc = sql("SELECT * FROM products ORDER BY price DESC;");
    CHECK(rc == MYDB_OK,                       "ORDER BY price DESC → MYDB_OK");
    {
        const char *pos_delta   = strstr(g_res, "Delta");   /* price=300 */
        const char *pos_alpha   = strstr(g_res, "Alpha");   /* price=100 */
        CHECK(pos_delta < pos_alpha,           "ORDER BY price DESC → Delta(300) before Alpha(100)");
    }

    /* ---- ORDER BY id ASC (PK optimisation — stream path) ---- */
    rc = sql("SELECT * FROM products ORDER BY id ASC;");
    CHECK(rc == MYDB_OK,                       "ORDER BY id ASC (PK opt) → MYDB_OK");
    CHECK(strstr(g_res, "(5 rows)") != NULL,   "ORDER BY id ASC → 5 rows");
    {
        const char *pos_alpha   = strstr(g_res, "Alpha");   /* id=1 */
        const char *pos_epsilon = strstr(g_res, "Epsilon"); /* id=5 */
        CHECK(pos_alpha < pos_epsilon,         "ORDER BY id ASC → id=1 before id=5");
    }

    /* ---- ORDER BY price ASC LIMIT 2 (partial_sort) ---- */
    rc = sql("SELECT name, price FROM products ORDER BY price ASC LIMIT 2;");
    CHECK(rc == MYDB_OK,                       "ORDER BY + LIMIT 2 → MYDB_OK");
    CHECK(strstr(g_res, "(2 rows)") != NULL,   "ORDER BY + LIMIT 2 → 2 rows");
    CHECK(strstr(g_res, "Alpha")  != NULL,     "ORDER BY ASC LIMIT 2 → cheapest Alpha(100)");
    CHECK(strstr(g_res, "Gamma")  != NULL,     "ORDER BY ASC LIMIT 2 → 2nd cheapest Gamma(150)");
    CHECK(strstr(g_res, "Beta")   == NULL,     "ORDER BY ASC LIMIT 2 → Beta(200) not included");

    /* ---- ORDER BY price DESC LIMIT 1 OFFSET 1 ---- */
    rc = sql("SELECT name, price FROM products ORDER BY price DESC LIMIT 1 OFFSET 1;");
    CHECK(rc == MYDB_OK,                       "ORDER BY DESC LIMIT 1 OFFSET 1 → MYDB_OK");
    CHECK(strstr(g_res, "(1 row)") != NULL,    "ORDER BY DESC LIMIT 1 OFFSET 1 → 1 row");
    /* sorted desc: Delta(300), Epsilon(250), Beta(200), Gamma(150), Alpha(100)
     * offset 1 skips Delta → Epsilon is the result */
    CHECK(strstr(g_res, "Epsilon") != NULL,    "ORDER BY DESC LIMIT 1 OFFSET 1 → Epsilon");
    CHECK(strstr(g_res, "Delta")   == NULL,    "ORDER BY DESC LIMIT 1 OFFSET 1 → Delta skipped");

    /* ---- multi-column ORDER BY: price ASC, name DESC ---- */
    rc = sql("SELECT name, price FROM products ORDER BY price ASC, name DESC;");
    CHECK(rc == MYDB_OK,                       "multi-column ORDER BY → MYDB_OK");
    CHECK(strstr(g_res, "(5 rows)") != NULL,   "multi-column ORDER BY → 5 rows");

    /* ---- ORDER BY with WHERE ---- */
    rc = sql("SELECT * FROM products WHERE price > 150 ORDER BY price ASC;");
    CHECK(rc == MYDB_OK,                       "ORDER BY + WHERE → MYDB_OK");
    CHECK(strstr(g_res, "(3 rows)") != NULL,   "ORDER BY + WHERE → 3 rows");
    {
        const char *pos_beta    = strstr(g_res, "Beta");    /* price=200 */
        const char *pos_delta   = strstr(g_res, "Delta");   /* price=300 */
        CHECK(pos_beta < pos_delta,            "ORDER BY + WHERE → Beta(200) before Delta(300)");
    }

    /* ---- ORDER BY bad column → error ---- */
    rc = sql("SELECT * FROM products ORDER BY nosuchcol;");
    CHECK(rc != MYDB_OK,                       "ORDER BY bad column → error");
    CHECK(strstr(g_res, "nosuchcol") != NULL,  "ORDER BY bad column → mentions name");

    /* ---- invalid column in projection ---- */
    rc = sql("SELECT id, nosuchcol FROM products;");
    CHECK(rc != MYDB_OK,                     "SELECT bad column → error");
    CHECK(strstr(g_res, "nosuchcol") != NULL,"SELECT bad column → mentions name");

    /* ---- invalid column in WHERE ---- */
    rc = sql("SELECT * FROM products WHERE nosuchcol = 1;");
    CHECK(rc != MYDB_OK,                     "WHERE bad column → error");
    CHECK(strstr(g_res, "nosuchcol") != NULL,"WHERE bad column → mentions name");

    /* ---- SELECT from non-existent table ---- */
    rc = sql("SELECT * FROM nosuchtable;");
    CHECK(rc == MYDB_ERR_NOT_FOUND,          "SELECT unknown table → MYDB_ERR_NOT_FOUND");

    engine_teardown();
}

/* ======================================================================
 * PHASE 5.3 — Scalar aggregates
 * ====================================================================== */

static void test_aggregates(void)
{
    printf("\n[test_aggregates]\n");
    engine_setup();

    int rc;

    sql("CREATE DATABASE shop;");
    sql("USE shop;");

    /* table: id INT PK, name VARCHAR, price INT, active BOOL */
    sql("CREATE TABLE products ("
        "  id     INT PRIMARY KEY,"
        "  name   VARCHAR(50) NOT NULL,"
        "  price  INT,"
        "  active BOOL DEFAULT TRUE"
        ");");

    /* 5 rows: prices 100, 200, 150, 300, 250 */
    sql("INSERT INTO products (id, name, price, active) VALUES (1, 'Alpha',   100, TRUE);");
    sql("INSERT INTO products (id, name, price, active) VALUES (2, 'Beta',    200, FALSE);");
    sql("INSERT INTO products (id, name, price, active) VALUES (3, 'Gamma',   150, TRUE);");
    sql("INSERT INTO products (id, name, price, active) VALUES (4, 'Delta',   300, TRUE);");
    sql("INSERT INTO products (id, name, price, active) VALUES (5, 'Epsilon', 250, FALSE);");

    /* ---- COUNT(*) — all rows ---- */
    rc = sql("SELECT COUNT(*) FROM products;");
    CHECK(rc == MYDB_OK,                    "COUNT(*) → MYDB_OK");
    CHECK(strstr(g_res, "COUNT(*)") != NULL,"COUNT(*) → header present");
    CHECK(strstr(g_res, "5")        != NULL,"COUNT(*) → value is 5");
    CHECK(strstr(g_res, "(1 row)")  != NULL,"COUNT(*) → exactly 1 row");

    /* ---- COUNT(*) with WHERE ---- */
    rc = sql("SELECT COUNT(*) FROM products WHERE price > 150;");
    CHECK(rc == MYDB_OK,                    "COUNT(*) WHERE → MYDB_OK");
    CHECK(strstr(g_res, "3")        != NULL,"COUNT(*) WHERE price>150 → 3");

    /* ---- SUM ---- */
    rc = sql("SELECT SUM(price) FROM products;");
    CHECK(rc == MYDB_OK,                    "SUM(price) → MYDB_OK");
    /* 100+200+150+300+250 = 1000 */
    CHECK(strstr(g_res, "1000")     != NULL,"SUM(price) → 1000");

    /* ---- AVG ---- */
    rc = sql("SELECT AVG(price) FROM products;");
    CHECK(rc == MYDB_OK,                    "AVG(price) → MYDB_OK");
    /* 1000/5 = 200.00 */
    CHECK(strstr(g_res, "200.00")   != NULL,"AVG(price) → 200.00");

    /* ---- MIN ---- */
    rc = sql("SELECT MIN(price) FROM products;");
    CHECK(rc == MYDB_OK,                    "MIN(price) → MYDB_OK");
    CHECK(strstr(g_res, "100")      != NULL,"MIN(price) → 100");

    /* ---- MAX ---- */
    rc = sql("SELECT MAX(price) FROM products;");
    CHECK(rc == MYDB_OK,                    "MAX(price) → MYDB_OK");
    CHECK(strstr(g_res, "300")      != NULL,"MAX(price) → 300");

    /* ---- multiple aggregates in one query ---- */
    rc = sql("SELECT COUNT(*), SUM(price), MIN(price), MAX(price) FROM products;");
    CHECK(rc == MYDB_OK,                      "multi-agg → MYDB_OK");
    CHECK(strstr(g_res, "(1 row)")  != NULL,  "multi-agg → exactly 1 row");
    CHECK(strstr(g_res, "5")        != NULL,  "multi-agg → COUNT=5");
    CHECK(strstr(g_res, "1000")     != NULL,  "multi-agg → SUM=1000");
    CHECK(strstr(g_res, "100")      != NULL,  "multi-agg → MIN=100");
    CHECK(strstr(g_res, "300")      != NULL,  "multi-agg → MAX=300");

    /* ---- COUNT(*) on empty result (WHERE matches nothing) ---- */
    rc = sql("SELECT COUNT(*) FROM products WHERE price > 9999;");
    CHECK(rc == MYDB_OK,                    "COUNT(*) no match → MYDB_OK");
    CHECK(strstr(g_res, "0")        != NULL,"COUNT(*) no match → 0");
    CHECK(strstr(g_res, "(1 row)")  != NULL,"COUNT(*) no match → still 1 row");

    /* ---- SUM on empty result → NULL ---- */
    rc = sql("SELECT SUM(price) FROM products WHERE price > 9999;");
    CHECK(rc == MYDB_OK,                    "SUM no match → MYDB_OK");
    CHECK(strstr(g_res, "NULL")     != NULL,"SUM no match → NULL");

    /* ---- alias ---- */
    rc = sql("SELECT COUNT(*) AS total FROM products;");
    CHECK(rc == MYDB_OK,                    "COUNT(*) AS total → MYDB_OK");
    CHECK(strstr(g_res, "total")    != NULL,"COUNT(*) AS total → alias in header");

    /* ---- error: mix aggregate and column ---- */
    rc = sql("SELECT name, COUNT(*) FROM products;");
    CHECK(rc != MYDB_OK,                    "mix agg+col → error");
    CHECK(strstr(g_res, "GROUP BY") != NULL,"mix agg+col → mentions GROUP BY");

    /* ---- error: ORDER BY with scalar aggregate ---- */
    rc = sql("SELECT COUNT(*) FROM products ORDER BY price;");
    CHECK(rc != MYDB_OK,                    "agg + ORDER BY → error");

    /* ---- error: bad column in aggregate ---- */
    rc = sql("SELECT SUM(nosuchcol) FROM products;");
    CHECK(rc != MYDB_OK,                     "SUM bad col → error");
    CHECK(strstr(g_res, "nosuchcol") != NULL,"SUM bad col → mentions column");

    engine_teardown();
}

/* ======================================================================
 * main
 * ====================================================================== */

int main(void)
{
    printf("=== test_exec_engine ===\n");

    test_tcl();
    test_value_cast();
    test_expr_eval();
    test_ddl();
    test_insert();
    test_select();
    test_aggregates();

    rm_rf(TEST_ROOT);

    printf("\n%d / %d tests passed\n", tests_passed, tests_run);
    return (tests_passed == tests_run) ? 0 : 1;
}
