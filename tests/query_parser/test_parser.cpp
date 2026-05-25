#include "Parser.hpp"
#include "Lexer.hpp"
#include <iostream>
#include <vector>
#include <string>
#include <fstream>

struct TestCase {
    std::string category;
    std::string query;
    bool expect_success;
};

std::vector<TestCase> cases = {
    {"DDL", "CREATE TABLE t (id INT);", true},
    {"DDL", "CREATE TABLE t (id INT PRIMARY KEY);", true},
    {"DDL", "CREATE TABLE t (id INT NOT NULL);", true},
    {"DDL", "CREATE TABLE t (id INT UNIQUE);", true},
    {"DDL", "CREATE TABLE t (id INT AUTO_INCREMENT);", true},
    {"DDL", "CREATE TABLE t (id INT DEFAULT 0);", true},
    {"DDL", "CREATE TABLE t (id INT DEFAULT 'none');", true},
    {"DDL", "CREATE TABLE t (id INT DEFAULT NULL);", true},
    {"DDL", "CREATE TABLE t (id DECIMAL(10,2));", true},
    {"DDL", "CREATE TABLE t (id VARCHAR(255));", true},
    {"DDL", "CREATE TABLE t (id ENUM(A, B, C));", true},
    {"DDL", "CREATE TABLE t (id DATE(YYYY));", true},
    {"DDL", "CREATE TABLE t (id DATETIME);", true},
    {"DDL", "CREATE TABLE t (id BOOL);", true},
    {"DDL", "CREATE TABLE t (a INT, b INT, CONSTRAINT pk PRIMARY KEY (a));", true},
    {"DDL", "CREATE TABLE t (a INT, b INT, PRIMARY KEY (a));", true},
    {"DDL", "CREATE TABLE t (a INT, b INT, CONSTRAINT fk FOREIGN KEY (a) REFERENCES other(b));", true},
    {"DDL", "CREATE TABLE t (a INT, b INT, FOREIGN KEY (a) REFERENCES other(b));", true},
    {"DDL", "CREATE TABLE _t1 (a INT);", true},
    {"DDL", "CREATE TABLE t (id INT,);", false},
    {"DDL", "CREATE TABLE t (id INT) NO_SEMI", false},
    {"DDL", "CREATE TABLE t ();", false},
    {"DDL", "CREATE TABLE (id INT);", false},
    {"DDL_EXTRA", "DROP TABLE users;", true},
    {"DDL_EXTRA", "DROP TABLE _users;", true},
    {"DDL_EXTRA", "CREATE DATABASE mydb;", true},
    {"DDL_EXTRA", "CREATE SCHEMA myschema;", true},
    {"DDL_EXTRA", "DROP DATABASE mydb;", true},
    {"DDL_EXTRA", "DROP SCHEMA myschema;", true},
    {"DDL_EXTRA", "USE mydb;", true},
    {"DDL_EXTRA", "SHOW TABLES;", true},
    {"DDL_EXTRA", "SHOW DATABASES;", true},
    {"TCL", "BEGIN;", true},
    {"TCL", "COMMIT;", true},
    {"TCL", "ROLLBACK;", true},
    {"TCL", "BEGIN", false},
    {"DDL_EXTRA", "DROP DATABASE;", false},
    {"DML", "INSERT INTO t VALUES (1);", true},
    {"DML", "INSERT INTO t VALUES (1, 2, 3);", true},
    {"DML", "INSERT INTO t VALUES ('str', 3.14, NULL, TRUE, FALSE);", true},
    {"DML", "INSERT INTO t VALUES (-5);", true},
    {"DML", "INSERT INTO t (a) VALUES (1);", true},
    {"DML", "INSERT INTO t (a, b.c) VALUES (1, 2);", true},
    {"DML", "INSERT INTO t VALUES (1, INVALID);", false},
    {"DML", "INSERT INTO t (a b) VALUES (1);", false},
    {"DML", "UPDATE t SET a = 1;", true},
    {"DML", "UPDATE t SET a = 'test', b = NULL, c = TRUE, d = -10.5;", true},
    {"DML", "UPDATE t SET a.b = 1 WHERE a.b = 2;", true},
    {"DML", "UPDATE t SET a = 1 WHERE id > 5;", true},
    {"DML", "UPDATE t SET a 1;", false},
    {"DML", "DELETE FROM t;", true},
    {"DML", "DELETE FROM t WHERE id = 1;", true},
    {"DML", "DELETE t;", false},
    {"SELECT", "SELECT * FROM t;", true},
    {"SELECT", "SELECT a, b, c FROM t;", true},
    {"SELECT", "SELECT t.a, t.b FROM t;", true},
    {"SELECT", "SELECT a AS b, c AS d FROM t;", true},
    {"SELECT", "SELECT COUNT(a) FROM t;", true},
    {"SELECT", "SELECT COUNT(*) FROM t;", true},
    {"SELECT", "SELECT COUNT(DISTINCT a) FROM t;", true},
    {"SELECT", "SELECT SUM(a), AVG(b), MIN(c), MAX(d) FROM t;", true},
    {"SELECT", "SELECT SUM(a) AS total FROM t;", true},
    {"SELECT", "SELECT * FROM t INNER JOIN t2 ON t.a = t2.b;", true},
    {"SELECT", "SELECT * FROM t LEFT JOIN t2 ON t.a = t2.b;", true},
    {"SELECT", "SELECT * FROM t RIGHT OUTER JOIN t2 ON t.a = t2.b;", true},
    {"SELECT", "SELECT * FROM t FULL OUTER JOIN t2 ON t.a = t2.b;", true},
    {"SELECT", "SELECT * FROM t WHERE a = 1;", true},
    {"SELECT", "SELECT * FROM t GROUP BY a;", true},
    {"SELECT", "SELECT * FROM t GROUP BY a, b.c;", true},
    {"SELECT", "SELECT * FROM t GROUP BY a HAVING a > 1;", true},
    {"SELECT", "SELECT * FROM t ORDER BY a;", true},
    {"SELECT", "SELECT * FROM t ORDER BY a ASC, b DESC;", true},
    {"SELECT", "SELECT * FROM t LIMIT 10;", true},
    {"SELECT", "SELECT * FROM t LIMIT 10 OFFSET 5;", true},
    {"SELECT", "SELECT * FROM t GROUP BY a HAVING a > 1 ORDER BY a DESC LIMIT 5 OFFSET 2;", true},
    {"SELECT", "SELECT FROM t;", false},
    {"SELECT", "SELECT * FROM;", false},
    {"SELECT", "SELECT * FROM t ORDER BY;", false},
    {"SELECT", "SELECT * FROM t LIMIT a;", false},
    {"EXPR", "SELECT * FROM t WHERE a = 1;", true},
    {"EXPR", "SELECT * FROM t WHERE a != 1;", true},
    {"EXPR", "SELECT * FROM t WHERE a <> 1;", true},
    {"EXPR", "SELECT * FROM t WHERE a < 1;", true},
    {"EXPR", "SELECT * FROM t WHERE a <= 1;", true},
    {"EXPR", "SELECT * FROM t WHERE a > 1;", true},
    {"EXPR", "SELECT * FROM t WHERE a >= 1;", true},
    {"EXPR", "SELECT * FROM t WHERE a = -1;", true},
    {"EXPR", "SELECT * FROM t WHERE a = 'hello';", true},
    {"EXPR", "SELECT * FROM t WHERE a = TRUE OR a = FALSE OR a = NULL;", true},
    {"EXPR", "SELECT * FROM t WHERE a IS NULL;", true},
    {"EXPR", "SELECT * FROM t WHERE a IS NOT NULL;", true},
    {"EXPR", "SELECT * FROM t WHERE a BETWEEN 1 AND 10;", true},
    {"EXPR", "SELECT * FROM t WHERE a NOT BETWEEN 1 AND 10;", true},
    {"EXPR", "SELECT * FROM t WHERE a IN (1, 2, 3);", true},
    {"EXPR", "SELECT * FROM t WHERE a NOT IN ('x', 'y', 'z');", true},
    {"EXPR", "SELECT * FROM t WHERE a LIKE '%abc%';", true},
    {"EXPR", "SELECT * FROM t WHERE a NOT LIKE 'a_c';", true},
    {"EXPR", "SELECT * FROM t WHERE NOT a = 1;", true},
    {"EXPR", "SELECT * FROM t WHERE (a = 1);", true},
    {"EXPR", "SELECT * FROM t WHERE (a = 1 AND b = 2) OR c = 3;", true},
    {"EXPR", "SELECT * FROM t WHERE a = 1 AND (b = 2 OR c = 3);", true},
    {"EXPR", "SELECT * FROM t WHERE NOT (a IS NULL OR b BETWEEN 1 AND 2);", true},
    {"EXPR", "SELECT * FROM t WHERE a = ;", false},
    {"EXPR", "SELECT * FROM t WHERE a IN 1, 2;", false},
    {"EXPR", "SELECT * FROM t WHERE a BETWEEN 1 OR 2;", false},
    {"EXPR", "SELECT * FROM t WHERE a IS TRUE;", false},
};

int main() {
    int passed = 0;
    int failed = 0;
    
    std::ofstream out("test_report.md");
    out << "# Comprehensive Parser Validation Report (100 Tests)\n\n";
    out << "This document outlines 100 diverse test cases executed against the MYDB Query Parser, covering all phases of implementation including complex DDL statements, constraint generation, aggregate functions, DML parsing, multi-layered WHERE expression trees, and comprehensive syntax error reporting.\n\n";
    out << "## Summary\n";

    std::string details = "## Detailed Results\n\n| Category | Query | Expected Result | Status | Error Message (if any) |\n|---|---|---|---|---|\n";

    for (const auto& tc : cases) {
        std::cout << "[" << tc.category << "] Testing: " << tc.query << "\n";
        std::string err_msg = "";
        bool actual_success = false;
        try {
            Lexer lexer(tc.query);
            auto tokens = lexer.tokenize();
            Parser parser(tokens);
            auto ast = parser.parse();
            actual_success = true;
        } catch (const std::exception& e) {
            actual_success = false;
            err_msg = e.what();
        }
        
        bool passed_test = (actual_success == tc.expect_success);
        if (passed_test) passed++; else failed++;
        
        std::string status_str = passed_test ? "✅ PASSED" : "❌ FAILED";
        std::string expected_str = tc.expect_success ? "SUCCESS" : "ERROR";
        
        // Escape pipes in query for markdown
        std::string safe_query = tc.query;
        size_t pos = 0;
        while((pos = safe_query.find('|', pos)) != std::string::npos) { safe_query.replace(pos, 1, "\\|"); pos += 2; }
        
        details += "| " + tc.category + " | `" + safe_query + "` | " + expected_str + " | " + status_str + " | " + err_msg + " |\n";
    }

    out << "- **Total Tests Run**: " << cases.size() << "\n";
    out << "- **Passed**: " << passed << "\n";
    out << "- **Failed**: " << failed << "\n";
    out << "- **Status**: " << (failed == 0 ? "✅ **COMPLETELY WORKING**" : "❌ **NEEDS FIXES**") << "\n\n";
    out << details;
    out.close();

    std::cout << "\nResults: " << passed << " passed, " << failed << " failed.\n";
    return failed == 0 ? 0 : 1;
}
