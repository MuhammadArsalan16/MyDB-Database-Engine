#pragma once
#include <string>
#include <vector>
#include <memory>
#include <iostream>
#include "Lexer.hpp"

// --- 1. ENUMS ---
enum class StatementType { SELECT, CREATE_TABLE, INSERT, UPDATE, DELETE, TRANSACTION, DROP_TABLE, CREATE_DATABASE, DROP_DATABASE, USE, SHOW_TABLES, SHOW_DATABASES, UNKNOWN };
enum class JoinType { INNER, LEFT, RIGHT, FULL };
enum class TransactionCommand { BEGIN, COMMIT, ROLLBACK };

// --- 2. BASE AST NODE ---
struct ASTNode {
    StatementType type;
    virtual ~ASTNode() = default;
    virtual void print() const = 0; 
};

// --- 3. CLAUSES ---
struct Expr {
    enum class Kind { ColumnRef, Literal, Binary, Unary, IsNull,
                      Between, In, Like };
    Kind kind;
    virtual ~Expr() = default;
};
struct ColumnRefExpr : Expr { std::string table, column; ColumnRefExpr() { kind = Kind::ColumnRef; } };
struct LiteralExpr   : Expr { TokenType type; std::string raw; LiteralExpr() { kind = Kind::Literal; } };
struct BinaryExpr    : Expr { std::string op;
                              std::unique_ptr<Expr> lhs, rhs; 
                              BinaryExpr() { kind = Kind::Binary; } };
struct UnaryExpr     : Expr { std::string op; std::unique_ptr<Expr> child; UnaryExpr() { kind = Kind::Unary; } };
struct IsNullExpr    : Expr { std::unique_ptr<Expr> child; bool negated; IsNullExpr() { kind = Kind::IsNull; } };
struct BetweenExpr   : Expr { std::unique_ptr<Expr> v, lo, hi; bool negated; BetweenExpr() { kind = Kind::Between; } };
struct InExpr        : Expr { std::unique_ptr<Expr> v;
                              std::vector<std::unique_ptr<LiteralExpr>> list;
                              bool negated; 
                              InExpr() { kind = Kind::In; } };
struct LikeExpr      : Expr { std::unique_ptr<Expr> v; std::string pattern;
                              bool negated; 
                              LikeExpr() { kind = Kind::Like; } };

struct WhereClause {
    std::unique_ptr<Expr> root;
};

struct JoinClause {
    JoinType join_type = JoinType::INNER; // Default
    std::string join_table;
    std::string left_condition;
    std::string right_condition;
};

// --- 4. STATEMENTS ---

struct SelectItem {
    enum class Kind { Column, Star, Aggregate };
    Kind        kind;
    std::string table;
    std::string column;
    std::string alias;
    std::string agg_func;
    bool        agg_distinct = false;
};

struct OrderByItem {
    std::string table, column;
    bool        descending = false;
};

// SELECT
struct SelectStatement : public ASTNode {
    std::vector<SelectItem> select_list;
    bool is_select_all = false;
    std::string table_name;
    
    std::unique_ptr<JoinClause> join_clause;
    std::unique_ptr<WhereClause> where_clause;
    std::vector<std::string> group_by;
    std::unique_ptr<Expr> having;
    std::vector<OrderByItem> order_by;
    int64_t limit = -1;
    int64_t offset = 0;

    SelectStatement() { type = StatementType::SELECT; }

    void print() const override {
        std::cout << "\n[AST] Action: SELECT\n";
        std::cout << "      Table : " << table_name << "\n      Cols  : ";
        if (is_select_all) std::cout << "*\n";
        else { 
            for (const auto& item : select_list) {
                if (item.kind == SelectItem::Kind::Aggregate) {
                    std::cout << item.agg_func << "(" << (item.agg_distinct ? "DISTINCT " : "") << item.column << ") ";
                } else {
                    if (!item.table.empty()) std::cout << item.table << ".";
                    std::cout << item.column << " ";
                }
                if (!item.alias.empty()) std::cout << "AS " << item.alias << " ";
            }
            std::cout << "\n"; 
        }
        
        if (join_clause) {
            std::string j_type_str = "INNER";
            if (join_clause->join_type == JoinType::LEFT) j_type_str = "LEFT";
            if (join_clause->join_type == JoinType::RIGHT) j_type_str = "RIGHT";
            if (join_clause->join_type == JoinType::FULL) j_type_str = "FULL";

            std::cout << "      Join  : " << j_type_str << " JOIN " << join_clause->join_table 
                      << " ON " << join_clause->left_condition << " = " << join_clause->right_condition << "\n";
        }

        if (where_clause) {
            std::cout << "      Where : <expr tree>\n";
        }
    }
};

// CREATE TABLE
struct ASTColumnDef {
    std::string name;
    std::string data_type;
    uint16_t    max_len = 0;
    uint8_t     scale = 0;
    std::vector<std::string> enum_values;
    std::string date_format;

    bool is_primary_key = false;
    bool is_not_null = false;
    bool is_unique = false;
    bool is_auto_increment = false;
    bool has_default = false;
    // default value literals
    int default_kind = -1; // -1 for none, or cast to TokenType
    std::string default_text;
};

struct AstForeignKey {
    std::string constraint_name;
    std::string column_name;
    std::string ref_table;
    std::string ref_column;
};

struct CreateTableStatement : public ASTNode {
    std::string table_name;
    std::vector<ASTColumnDef> columns;
    std::vector<AstForeignKey> foreign_keys;

    CreateTableStatement() { type = StatementType::CREATE_TABLE; }

    void print() const override {
        std::cout << "\n[AST] Action: CREATE TABLE\n";
        std::cout << "      Table : " << table_name << "\n      Schema:\n";
        for (const auto& col : columns) {
            std::cout << "        - " << col.name << " (" << col.data_type << ")";
            if (col.is_primary_key) std::cout << " [PRIMARY KEY]";
            std::cout << "\n";
        }
    }
};

// INSERT
struct InsertStatement : public ASTNode {
    std::string table_name;
    std::vector<std::string> target_columns;
    std::vector<std::vector<std::string>> rows; // for phase 6+ multi-row, phase 4 uses rows[0]

    InsertStatement() { type = StatementType::INSERT; }

    void print() const override {
        std::cout << "\n[AST] Action: INSERT INTO\n";
        std::cout << "      Table : " << table_name << "\n";
        if (!target_columns.empty()) {
            std::cout << "      Cols  : ";
            for (const auto& col : target_columns) std::cout << col << " ";
            std::cout << "\n";
        }
        std::cout << "      Values: ";
        if (!rows.empty()) {
            for (const auto& val : rows[0]) std::cout << val << " ";
        }
        std::cout << "\n";
    }
};

// UPDATE
struct UpdateStatement : public ASTNode {
    std::string table_name;
    std::vector<std::pair<std::string, std::string>> assignments;
    std::unique_ptr<WhereClause> where_clause;

    UpdateStatement() { type = StatementType::UPDATE; }

    void print() const override {
        std::cout << "\n[AST] Action: UPDATE\n";
        std::cout << "      Table : " << table_name << "\n";
        for (const auto& assign : assignments) {
            std::cout << "      Set   : " << assign.first << " = " << assign.second << "\n";
        }
        if (where_clause) {
            std::cout << "      Where : <expr tree>\n";
        }
    }
};

// DELETE
struct DeleteStatement : public ASTNode {
    std::string table_name;
    std::unique_ptr<WhereClause> where_clause;

    DeleteStatement() { type = StatementType::DELETE; }

    void print() const override {
        std::cout << "\n[AST] Action: DELETE\n";
        std::cout << "      Table : " << table_name << "\n";
        if (where_clause) {
            std::cout << "      Where : <expr tree>\n";
        }
    }
};

// Phase 6 Statements
struct DropTableStatement : public ASTNode {
    std::string table_name;
    DropTableStatement() { type = StatementType::DROP_TABLE; }
    void print() const override { std::cout << "\n[AST] Action: DROP TABLE " << table_name << "\n"; }
};

struct CreateDatabaseStatement : public ASTNode {
    std::string name;
    CreateDatabaseStatement() { type = StatementType::CREATE_DATABASE; }
    void print() const override { std::cout << "\n[AST] Action: CREATE DATABASE " << name << "\n"; }
};

struct DropDatabaseStatement : public ASTNode {
    std::string name;
    DropDatabaseStatement() { type = StatementType::DROP_DATABASE; }
    void print() const override { std::cout << "\n[AST] Action: DROP DATABASE " << name << "\n"; }
};

struct UseStatement : public ASTNode {
    std::string schema_name;
    UseStatement() { type = StatementType::USE; }
    void print() const override { std::cout << "\n[AST] Action: USE " << schema_name << "\n"; }
};

struct ShowTablesStatement : public ASTNode {
    ShowTablesStatement() { type = StatementType::SHOW_TABLES; }
    void print() const override { std::cout << "\n[AST] Action: SHOW TABLES\n"; }
};

struct ShowDatabasesStatement : public ASTNode {
    ShowDatabasesStatement() { type = StatementType::SHOW_DATABASES; }
    void print() const override { std::cout << "\n[AST] Action: SHOW DATABASES\n"; }
};

// TRANSACTION (TCL)
struct TransactionStatement : public ASTNode {
    TransactionCommand command;

    TransactionStatement(TransactionCommand cmd) : command(cmd) { 
        type = StatementType::TRANSACTION; 
    }

    void print() const override {
        std::cout << "\n[AST] Action: TRANSACTION -> ";
        if (command == TransactionCommand::BEGIN) std::cout << "BEGIN\n";
        else if (command == TransactionCommand::COMMIT) std::cout << "COMMIT\n";
        else if (command == TransactionCommand::ROLLBACK) std::cout << "ROLLBACK\n";
    }
};