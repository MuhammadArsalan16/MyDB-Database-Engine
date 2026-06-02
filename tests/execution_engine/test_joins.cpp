/*
 * test_joins.cpp — functional tests for JOIN execution (v3).
 *
 * Drives the full engine through its single public entry point
 * engine_execute_sql(), which builds the ExecContext internally.  Covers:
 *   - INNER / LEFT / RIGHT / FULL OUTER joins
 *   - implicit comma joins (FROM a, b WHERE a.x = b.y)
 *   - JOIN + WHERE filtering
 *   - chained three-table joins
 *
 * Isolation: bootstraps a fresh engine under TEST_ROOT for the test run.
 */

#include <cstdio>
#include <cstring>
#include <cstdlib>

extern "C" {
#include "engine.h"
#include "common.h"
}

#define TEST_ROOT  "/tmp/mydb_test_joins"
#define TEST_USER  "root"
#define TEST_PASS  "pass"

static int tests_run    = 0;
static int tests_passed = 0;
static EngineState g_eng;
static char g_res[8192];

#define CHECK(cond, msg) do {                                          \
    tests_run++;                                                       \
    if (cond) { tests_passed++; printf("  PASS: %s\n", msg); }         \
    else      { printf("  FAIL: %s  (line %d)\n", msg, __LINE__); }    \
} while (0)

static void rm_rf(const char *path)
{
    char cmd[512];
    snprintf(cmd, sizeof(cmd), "rm -rf '%s'", path);
    (void)!system(cmd);
}

/* Execute one SQL string; result lands in g_res; returns the rc. */
static int sql(const char *query)
{
    memset(g_res, 0, sizeof(g_res));
    return engine_execute_sql(&g_eng, query, g_res, sizeof(g_res));
}

int main(void)
{
    printf("=== test_joins ===\n");

    rm_rf(TEST_ROOT);
    if (engine_bootstrap(TEST_ROOT, TEST_USER, TEST_PASS) != MYDB_OK) {
        printf("  FATAL: engine_bootstrap failed\n");
        return 1;
    }
    if (engine_start(TEST_ROOT, TEST_USER, TEST_PASS, &g_eng) != MYDB_OK) {
        printf("  FATAL: engine_start failed\n");
        return 1;
    }

    /* ---- Schema + data ----
     * customers 101,102,103 ; 103 (Omar) has no orders.
     * orders: order 3 references customer 104 which does not exist. */
    sql("CREATE DATABASE jdb;");
    sql("USE jdb;");
    sql("CREATE TABLE customers (id INT PRIMARY KEY, name VARCHAR(20));");
    sql("CREATE TABLE orders (id INT PRIMARY KEY, customer_id INT, amount INT);");

    sql("INSERT INTO customers (id, name) VALUES (101, 'Ali');");
    sql("INSERT INTO customers (id, name) VALUES (102, 'Sara');");
    sql("INSERT INTO customers (id, name) VALUES (103, 'Omar');");

    sql("INSERT INTO orders (id, customer_id, amount) VALUES (1, 101, 50);");
    sql("INSERT INTO orders (id, customer_id, amount) VALUES (2, 102, 80);");
    sql("INSERT INTO orders (id, customer_id, amount) VALUES (3, 104, 60);");

    int rc;

    /* INNER JOIN — only orders with a matching customer (1,2) */
    printf("\n[inner join]\n");
    rc = sql("SELECT * FROM orders JOIN customers ON orders.customer_id = customers.id;");
    CHECK(rc == MYDB_OK,                     "INNER JOIN → OK");
    CHECK(strstr(g_res, "(2 rows)") != NULL, "INNER JOIN → 2 rows");
    CHECK(strstr(g_res, "Ali")  != NULL,     "INNER JOIN → Ali present");
    CHECK(strstr(g_res, "Omar") == NULL,     "INNER JOIN → Omar absent (no order)");

    /* LEFT JOIN — all orders; order 3 gets NULL customer */
    printf("\n[left join]\n");
    rc = sql("SELECT * FROM orders LEFT JOIN customers ON orders.customer_id = customers.id;");
    CHECK(rc == MYDB_OK,                     "LEFT JOIN → OK");
    CHECK(strstr(g_res, "(3 rows)") != NULL, "LEFT JOIN → 3 rows");
    CHECK(strstr(g_res, "NULL") != NULL,     "LEFT JOIN → NULL-padded right side");

    /* RIGHT JOIN — all customers; Omar gets NULL order */
    printf("\n[right join]\n");
    rc = sql("SELECT * FROM orders RIGHT JOIN customers ON orders.customer_id = customers.id;");
    CHECK(rc == MYDB_OK,                     "RIGHT JOIN → OK");
    CHECK(strstr(g_res, "(3 rows)") != NULL, "RIGHT JOIN → 3 rows");
    CHECK(strstr(g_res, "Omar") != NULL,     "RIGHT JOIN → Omar present (no order)");

    /* FULL JOIN — matched + order 3 (NULL cust) + Omar (NULL order) */
    printf("\n[full join]\n");
    rc = sql("SELECT * FROM orders FULL JOIN customers ON orders.customer_id = customers.id;");
    CHECK(rc == MYDB_OK,                     "FULL JOIN → OK");
    CHECK(strstr(g_res, "(4 rows)") != NULL, "FULL JOIN → 4 rows");
    CHECK(strstr(g_res, "Omar") != NULL,     "FULL JOIN → Omar present");

    /* Implicit comma join (INNER semantics) */
    printf("\n[implicit join]\n");
    rc = sql("SELECT * FROM orders o, customers c WHERE o.customer_id = c.id;");
    CHECK(rc == MYDB_OK,                     "implicit JOIN → OK");
    CHECK(strstr(g_res, "(2 rows)") != NULL, "implicit JOIN → 2 rows");

    /* JOIN + WHERE filter — only the amount>70 matched order (2 → Sara) */
    printf("\n[join + where]\n");
    rc = sql("SELECT * FROM orders JOIN customers ON orders.customer_id = customers.id "
             "WHERE orders.amount > 70;");
    CHECK(rc == MYDB_OK,                     "JOIN + WHERE → OK");
    CHECK(strstr(g_res, "(1 row)") != NULL,  "JOIN + WHERE → 1 row");
    CHECK(strstr(g_res, "Sara") != NULL,     "JOIN + WHERE → Sara present");

    /* Three-table chained join */
    printf("\n[three-table join]\n");
    sql("CREATE TABLE items (id INT PRIMARY KEY, order_id INT, label VARCHAR(20));");
    sql("INSERT INTO items (id, order_id, label) VALUES (1, 1, 'book');");
    sql("INSERT INTO items (id, order_id, label) VALUES (2, 2, 'pen');");

    rc = sql("SELECT * FROM items i "
             "JOIN orders o ON i.order_id = o.id "
             "JOIN customers c ON o.customer_id = c.id;");
    CHECK(rc == MYDB_OK,                     "3-table JOIN → OK");
    CHECK(strstr(g_res, "(2 rows)") != NULL, "3-table JOIN → 2 rows");
    CHECK(strstr(g_res, "book") != NULL,     "3-table JOIN → item present");
    CHECK(strstr(g_res, "Ali")  != NULL,     "3-table JOIN → customer present");

    engine_close(&g_eng);
    rm_rf(TEST_ROOT);

    printf("\n%d / %d tests passed\n", tests_passed, tests_run);
    return (tests_passed == tests_run) ? 0 : 1;
}
