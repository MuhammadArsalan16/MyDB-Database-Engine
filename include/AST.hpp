#pragma once
#include <string>
#include <vector>
#include <memory>
#include <iostream>

enum class StatementType { SELECT, CREATE_TABLE, INSERT, UNKNOWN };

// Base class for all AST Nodes
struct ASTNode {
    StatementType type;
    virtual ~ASTNode() = default;
    virtual void print() const = 0; 
};

// --- NEW: WHERE Clause Structure ---
// Represents a condition like: age > 20
struct WhereClause {
    std::string left_operand;  // e.g., "age"
    std::string op;            // e.g., ">", "=", "<="
    std::string right_operand; // e.g., "20"
};

// --- 1. SELECT STATEMENT ---
struct SelectStatement : public ASTNode {
    std::vector<std::string> columns;
    std::string table_name;
    bool is_select_all = false;
    
    // Optional WHERE clause
    std::unique_ptr<WhereClause> where_clause;

    SelectStatement() { type = StatementType::SELECT; }

    void print() const override {
        std::cout << "\n[AST] Action: SELECT\n";
        std::cout << "      Table : " << table_name << "\n      Cols  : ";
        if (is_select_all) std::cout << "*\n";
        else { for (const auto& col : columns) std::cout << col << " "; std::cout << "\n"; }
        
        // Print the WHERE clause if it exists
        if (where_clause) {
            std::cout << "      Where : " << where_clause->left_operand << " " 
                      << where_clause->op << " " << where_clause->right_operand << "\n";
        }
    }
};

// --- 2. CREATE TABLE STATEMENT ---
struct ColumnDef {
    std::string name;
    std::string data_type;
    bool is_primary_key = false;
};

struct CreateTableStatement : public ASTNode {
    std::string table_name;
    std::vector<ColumnDef> columns;

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

// --- 3. INSERT STATEMENT ---
struct InsertStatement : public ASTNode {
    std::string table_name;
    std::vector<std::string> values;

    InsertStatement() { type = StatementType::INSERT; }

    void print() const override {
        std::cout << "\n[AST] Action: INSERT INTO\n";
        std::cout << "      Table : " << table_name << "\n      Values: ";
        for (const auto& val : values) std::cout << val << " ";
        std::cout << "\n";
    }
};
