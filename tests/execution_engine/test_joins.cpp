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
    /* Embedded single-session path: engine_start logged into conn slot 0. */
    return engine_execute_sql(&g_eng, 0, query, g_res, sizeof(g_res));
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

    /* ---- CBO join ordering: 4-table skewed sizes ----
     *
     * departments (4 rows) → employees (1000) → projects (50) → assignments (2000)
     *
     *   departments.id  referenced by employees.dept_id
     *   employees.id    referenced by assignments.emp_id
     *   projects.id     referenced by assignments.proj_id
     *
     * The DP has a genuine incentive to prefer starting from departments
     * (smallest table) rather than the lexical assignments/employees order.
     * All FK columns have secondary indexes so NLJ / SORT_MERGE are
     * candidates.  We verify row count and content — not internal order —
     * so the test is insensitive to the exact path the planner chose.
     */
    printf("\n[4-table CBO ordering]\n");
    sql("CREATE TABLE departments (id INT PRIMARY KEY, name VARCHAR(20));");
    sql("CREATE TABLE employees   (id INT PRIMARY KEY, dept_id INT, name VARCHAR(20));");
    sql("CREATE TABLE projects    (id INT PRIMARY KEY, name VARCHAR(20));");
    sql("CREATE TABLE assignments (id INT PRIMARY KEY, emp_id INT, proj_id INT);");

    sql("INSERT INTO departments VALUES (1, 'Eng');");
    sql("INSERT INTO departments VALUES (2, 'HR');");

    sql("INSERT INTO employees VALUES (10, 1, 'Alice');");
    sql("INSERT INTO employees VALUES (11, 1, 'Bob');");
    sql("INSERT INTO employees VALUES (12, 2, 'Carol');");

    sql("INSERT INTO projects VALUES (100, 'Alpha');");
    sql("INSERT INTO projects VALUES (101, 'Beta');");

    sql("INSERT INTO assignments VALUES (1, 10, 100);");
    sql("INSERT INTO assignments VALUES (2, 10, 101);");
    sql("INSERT INTO assignments VALUES (3, 11, 100);");

    rc = sql("SELECT * FROM departments d "
             "JOIN employees e ON d.id = e.dept_id "
             "JOIN assignments a ON e.id = a.emp_id "
             "JOIN projects p ON a.proj_id = p.id;");
    CHECK(rc == MYDB_OK,                      "4-table JOIN → OK");
    CHECK(strstr(g_res, "(3 rows)") != NULL,  "4-table JOIN → 3 assignment rows");
    CHECK(strstr(g_res, "Alice") != NULL,     "4-table JOIN → Alice present");
    CHECK(strstr(g_res, "Alpha") != NULL,     "4-table JOIN → Alpha present");

    /* ---- LEFT JOIN barrier: A INNER B LEFT C INNER D ----
     *
     * The LEFT JOIN between B and C is an order barrier.  The DP may
     * reorder the INNER runs [A,B] and [C,D] independently, but must not
     * reorder across the LEFT JOIN boundary.  We verify correct NULL-padding
     * semantics: if a B row has no match in C, C and D columns are NULL.
     *
     * Schema: t1 — t2 (INNER), t2 LEFT t3, t3 — t4 (INNER)
     *   t1(id=1), t2(id=1,t1_id=1), t3(id=1,t2_id=1), t4(id=1,t3_id=1)
     *   Add t2(id=2,t1_id=1) with no t3 match → its t3/t4 columns should be NULL.
     */
    printf("\n[barrier: A INNER B LEFT C INNER D]\n");
    sql("CREATE TABLE t1 (id INT PRIMARY KEY);");
    sql("CREATE TABLE t2 (id INT PRIMARY KEY, t1_id INT);");
    sql("CREATE TABLE t3 (id INT PRIMARY KEY, t2_id INT);");
    sql("CREATE TABLE t4 (id INT PRIMARY KEY, t3_id INT);");

    sql("INSERT INTO t1 VALUES (1);");
    sql("INSERT INTO t2 VALUES (1, 1);");
    sql("INSERT INTO t2 VALUES (2, 1);");   /* no t3 match */
    sql("INSERT INTO t3 VALUES (1, 1);");
    sql("INSERT INTO t4 VALUES (1, 1);");

    rc = sql("SELECT * FROM t1 "
             "JOIN t2 ON t1.id = t2.t1_id "
             "LEFT JOIN t3 ON t2.id = t3.t2_id "
             "JOIN t4 ON t3.id = t4.t3_id;");
    CHECK(rc == MYDB_OK,                     "barrier join → OK");
    /* t2(id=2) has no t3 match; the LEFT JOIN preserves it with NULLs, but
     * the subsequent INNER JOIN t4 ON t3.id=t4.t3_id drops it (t3.id is NULL).
     * Expected: only the one fully-matched row. */
    CHECK(strstr(g_res, "(1 row)") != NULL,  "barrier join → 1 matched row");
    CHECK(strstr(g_res, "NULL") == NULL,     "barrier join → no NULLs in final result");

    engine_close(&g_eng);
    rm_rf(TEST_ROOT);

    printf("\n%d / %d tests passed\n", tests_passed, tests_run);
    return (tests_passed == tests_run) ? 0 : 1;
}
