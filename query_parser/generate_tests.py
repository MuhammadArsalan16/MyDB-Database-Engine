import json

tests = [
    # --- PHASE 1/2/3: DDL (CREATE TABLE) ---
    ("DDL", "CREATE TABLE t (id INT);", True),
    ("DDL", "CREATE TABLE t (id INT PRIMARY KEY);", True),
    ("DDL", "CREATE TABLE t (id INT NOT NULL);", True),
    ("DDL", "CREATE TABLE t (id INT UNIQUE);", True),
    ("DDL", "CREATE TABLE t (id INT AUTO_INCREMENT);", True),
    ("DDL", "CREATE TABLE t (id INT DEFAULT 0);", True),
    ("DDL", "CREATE TABLE t (id INT DEFAULT 'none');", True),
    ("DDL", "CREATE TABLE t (id INT DEFAULT NULL);", True),
    ("DDL", "CREATE TABLE t (id DECIMAL(10,2));", True),
    ("DDL", "CREATE TABLE t (id VARCHAR(255));", True),
    ("DDL", "CREATE TABLE t (id ENUM(A, B, C));", True),
    ("DDL", "CREATE TABLE t (id DATE(YYYY));", True),
    ("DDL", "CREATE TABLE t (id DATETIME);", True),
    ("DDL", "CREATE TABLE t (id BOOL);", True),
    ("DDL", "CREATE TABLE t (a INT, b INT, CONSTRAINT pk PRIMARY KEY (a));", True),
    ("DDL", "CREATE TABLE t (a INT, b INT, PRIMARY KEY (a));", True),
    ("DDL", "CREATE TABLE t (a INT, b INT, CONSTRAINT fk FOREIGN KEY (a) REFERENCES other(b));", True),
    ("DDL", "CREATE TABLE t (a INT, b INT, FOREIGN KEY (a) REFERENCES other(b));", True),
    ("DDL", "CREATE TABLE _t1 (a INT);", True),
    ("DDL", "CREATE TABLE t (id INT,);", False), # trailing comma
    ("DDL", "CREATE TABLE t (id INT) NO_SEMI", False),
    ("DDL", "CREATE TABLE t ();", False), # empty columns
    ("DDL", "CREATE TABLE (id INT);", False), # missing name
    
    # --- PHASE 6: DDL Extras & TCL ---
    ("DDL_EXTRA", "DROP TABLE users;", True),
    ("DDL_EXTRA", "DROP TABLE _users;", True),
    ("DDL_EXTRA", "CREATE DATABASE mydb;", True),
    ("DDL_EXTRA", "CREATE SCHEMA myschema;", True),
    ("DDL_EXTRA", "DROP DATABASE mydb;", True),
    ("DDL_EXTRA", "DROP SCHEMA myschema;", True),
    ("DDL_EXTRA", "USE mydb;", True),
    ("DDL_EXTRA", "SHOW TABLES;", True),
    ("DDL_EXTRA", "SHOW DATABASES;", True),
    ("TCL", "BEGIN;", True),
    ("TCL", "COMMIT;", True),
    ("TCL", "ROLLBACK;", True),
    ("TCL", "BEGIN", False), # missing semi
    ("DDL_EXTRA", "DROP DATABASE;", False), # missing name

    # --- PHASE 4: DML (INSERT, UPDATE, DELETE) ---
    ("DML", "INSERT INTO t VALUES (1);", True),
    ("DML", "INSERT INTO t VALUES (1, 2, 3);", True),
    ("DML", "INSERT INTO t VALUES ('str', 3.14, NULL, TRUE, FALSE);", True),
    ("DML", "INSERT INTO t VALUES (-5);", True),
    ("DML", "INSERT INTO t (a) VALUES (1);", True),
    ("DML", "INSERT INTO t (a, b.c) VALUES (1, 2);", True),
    ("DML", "INSERT INTO t VALUES (1, INVALID);", False), # invalid literal
    ("DML", "INSERT INTO t (a b) VALUES (1);", False), # missing comma
    ("DML", "UPDATE t SET a = 1;", True),
    ("DML", "UPDATE t SET a = 'test', b = NULL, c = TRUE, d = -10.5;", True),
    ("DML", "UPDATE t SET a.b = 1 WHERE a.b = 2;", True),
    ("DML", "UPDATE t SET a = 1 WHERE id > 5;", True),
    ("DML", "UPDATE t SET a 1;", False), # missing =
    ("DML", "DELETE FROM t;", True),
    ("DML", "DELETE FROM t WHERE id = 1;", True),
    ("DML", "DELETE t;", False), # missing FROM

    # --- PHASE 5 & 7: SELECT ---
    ("SELECT", "SELECT * FROM t;", True),
    ("SELECT", "SELECT a, b, c FROM t;", True),
    ("SELECT", "SELECT t.a, t.b FROM t;", True),
    ("SELECT", "SELECT a AS b, c AS d FROM t;", True),
    ("SELECT", "SELECT COUNT(a) FROM t;", True),
    ("SELECT", "SELECT COUNT(*) FROM t;", True),
    ("SELECT", "SELECT COUNT(DISTINCT a) FROM t;", True),
    ("SELECT", "SELECT SUM(a), AVG(b), MIN(c), MAX(d) FROM t;", True),
    ("SELECT", "SELECT SUM(a) AS total FROM t;", True),
    ("SELECT", "SELECT * FROM t INNER JOIN t2 ON t.a = t2.b;", True),
    ("SELECT", "SELECT * FROM t LEFT JOIN t2 ON t.a = t2.b;", True),
    ("SELECT", "SELECT * FROM t RIGHT OUTER JOIN t2 ON t.a = t2.b;", True),
    ("SELECT", "SELECT * FROM t FULL OUTER JOIN t2 ON t.a = t2.b;", True),
    ("SELECT", "SELECT * FROM t WHERE a = 1;", True),
    ("SELECT", "SELECT * FROM t GROUP BY a;", True),
    ("SELECT", "SELECT * FROM t GROUP BY a, b.c;", True),
    ("SELECT", "SELECT * FROM t GROUP BY a HAVING a > 1;", True),
    ("SELECT", "SELECT * FROM t ORDER BY a;", True),
    ("SELECT", "SELECT * FROM t ORDER BY a ASC, b DESC;", True),
    ("SELECT", "SELECT * FROM t LIMIT 10;", True),
    ("SELECT", "SELECT * FROM t LIMIT 10 OFFSET 5;", True),
    ("SELECT", "SELECT * FROM t GROUP BY a HAVING a > 1 ORDER BY a DESC LIMIT 5 OFFSET 2;", True),
    ("SELECT", "SELECT FROM t;", False), # missing list
    ("SELECT", "SELECT * FROM;", False), # missing table
    ("SELECT", "SELECT * FROM t ORDER BY;", False), # missing col
    ("SELECT", "SELECT * FROM t LIMIT a;", False), # non-number limit

    # --- PHASE 5: EXPRESSIONS (WHERE) ---
    ("EXPR", "SELECT * FROM t WHERE a = 1;", True),
    ("EXPR", "SELECT * FROM t WHERE a != 1;", True),
    ("EXPR", "SELECT * FROM t WHERE a <> 1;", True),
    ("EXPR", "SELECT * FROM t WHERE a < 1;", True),
    ("EXPR", "SELECT * FROM t WHERE a <= 1;", True),
    ("EXPR", "SELECT * FROM t WHERE a > 1;", True),
    ("EXPR", "SELECT * FROM t WHERE a >= 1;", True),
    ("EXPR", "SELECT * FROM t WHERE a = -1;", True),
    ("EXPR", "SELECT * FROM t WHERE a = 'hello';", True),
    ("EXPR", "SELECT * FROM t WHERE a = TRUE OR a = FALSE OR a = NULL;", True),
    ("EXPR", "SELECT * FROM t WHERE a IS NULL;", True),
    ("EXPR", "SELECT * FROM t WHERE a IS NOT NULL;", True),
    ("EXPR", "SELECT * FROM t WHERE a BETWEEN 1 AND 10;", True),
    ("EXPR", "SELECT * FROM t WHERE a NOT BETWEEN 1 AND 10;", True),
    ("EXPR", "SELECT * FROM t WHERE a IN (1, 2, 3);", True),
    ("EXPR", "SELECT * FROM t WHERE a NOT IN ('x', 'y', 'z');", True),
    ("EXPR", "SELECT * FROM t WHERE a LIKE '%abc%';", True),
    ("EXPR", "SELECT * FROM t WHERE a NOT LIKE 'a_c';", True),
    ("EXPR", "SELECT * FROM t WHERE NOT a = 1;", True),
    ("EXPR", "SELECT * FROM t WHERE (a = 1);", True),
    ("EXPR", "SELECT * FROM t WHERE (a = 1 AND b = 2) OR c = 3;", True),
    ("EXPR", "SELECT * FROM t WHERE a = 1 AND (b = 2 OR c = 3);", True),
    ("EXPR", "SELECT * FROM t WHERE NOT (a IS NULL OR b BETWEEN 1 AND 2);", True),
    ("EXPR", "SELECT * FROM t WHERE a = ;", False), # missing RHS
    ("EXPR", "SELECT * FROM t WHERE a IN 1, 2;", False), # missing parens
    ("EXPR", "SELECT * FROM t WHERE a BETWEEN 1 OR 2;", False), # AND not OR
    ("EXPR", "SELECT * FROM t WHERE a IS TRUE;", False), # IS only supports NULL
]

cpp_code = """#include "Parser.hpp"
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
"""

for c, q, s in tests:
    success_str = "true" if s else "false"
    cpp_code += f'    {{"{c}", "{q}", {success_str}}},\n'

cpp_code += """};

int main() {
    int passed = 0;
    int failed = 0;
    
    std::ofstream out("test_report.md");
    out << "# Comprehensive Parser Validation Report (100 Tests)\\n\\n";
    out << "This document outlines 100 diverse test cases executed against the MYDB Query Parser, covering all phases of implementation including complex DDL statements, constraint generation, aggregate functions, DML parsing, multi-layered WHERE expression trees, and comprehensive syntax error reporting.\\n\\n";
    out << "## Summary\\n";

    std::string details = "## Detailed Results\\n\\n| Category | Query | Expected Result | Status | Error Message (if any) |\\n|---|---|---|---|---|\\n";

    for (const auto& tc : cases) {
        std::cout << "[" << tc.category << "] Testing: " << tc.query << "\\n";
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
        while((pos = safe_query.find('|', pos)) != std::string::npos) { safe_query.replace(pos, 1, "\\\\|"); pos += 2; }
        
        details += "| " + tc.category + " | `" + safe_query + "` | " + expected_str + " | " + status_str + " | " + err_msg + " |\\n";
    }

    out << "- **Total Tests Run**: " << cases.size() << "\\n";
    out << "- **Passed**: " << passed << "\\n";
    out << "- **Failed**: " << failed << "\\n";
    out << "- **Status**: " << (failed == 0 ? "✅ **COMPLETELY WORKING**" : "❌ **NEEDS FIXES**") << "\\n\\n";
    out << details;
    out.close();

    std::cout << "\\nResults: " << passed << " passed, " << failed << " failed.\\n";
    return failed == 0 ? 0 : 1;
}
"""

with open("test_parser.cpp", "w") as f:
    f.write(cpp_code)
