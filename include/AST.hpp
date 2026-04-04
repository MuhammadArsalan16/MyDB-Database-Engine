#pragma once
#include <string>
#include <vector>
#include <memory>
#include <iostream>

// --- 1. ENUMS ---
enum class StatementType { SELECT, CREATE_TABLE, INSERT, UPDATE, DELETE, TRANSACTION, UNKNOWN };
enum class JoinType { INNER, LEFT, RIGHT, FULL };
enum class TransactionCommand { BEGIN, COMMIT, ROLLBACK };

// --- 2. BASE AST NODE ---
struct ASTNode {
    StatementType type;
    virtual ~ASTNode() = default;
    virtual void print() const = 0; 
};

// --- 3. CLAUSES ---
struct WhereClause {
    std::string left_operand;
    std::string op;
    std::string right_operand;
};

struct JoinClause {
    JoinType join_type = JoinType::INNER; // Default
    std::string join_table;
    std::string left_condition;
    std::string right_condition;
};

// --- 4. STATEMENTS ---

// SELECT
struct SelectStatement : public ASTNode {
    std::vector<std::string> columns;
    std::string table_name;
    bool is_select_all = false;
    
    std::unique_ptr<JoinClause> join_clause;
    std::unique_ptr<WhereClause> where_clause; 

    SelectStatement() { type = StatementType::SELECT; }

    void print() const override {
        std::cout << "\n[AST] Action: SELECT\n";
        std::cout << "      Table : " << table_name << "\n      Cols  : ";
        if (is_select_all) std::cout << "*\n";
        else { for (const auto& col : columns) std::cout << col << " "; std::cout << "\n"; }
        
        if (join_clause) {
            std::string j_type_str = "INNER";
            if (join_clause->join_type == JoinType::LEFT) j_type_str = "LEFT";
            if (join_clause->join_type == JoinType::RIGHT) j_type_str = "RIGHT";
            if (join_clause->join_type == JoinType::FULL) j_type_str = "FULL";

            std::cout << "      Join  : " << j_type_str << " JOIN " << join_clause->join_table 
                      << " ON " << join_clause->left_condition << " = " << join_clause->right_condition << "\n";
        }

        if (where_clause) {
            std::cout << "      Where : " << where_clause->left_operand << " " 
                      << where_clause->op << " " << where_clause->right_operand << "\n";
        }
    }
};

// CREATE TABLE
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

// INSERT
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

// UPDATE
struct UpdateStatement : public ASTNode {
    std::string table_name;
    std::string set_column;
    std::string set_value;
    std::unique_ptr<WhereClause> where_clause;

    UpdateStatement() { type = StatementType::UPDATE; }

    void print() const override {
        std::cout << "\n[AST] Action: UPDATE\n";
        std::cout << "      Table : " << table_name << "\n";
        std::cout << "      Set   : " << set_column << " = " << set_value << "\n";
        if (where_clause) {
            std::cout << "      Where : " << where_clause->left_operand << " " 
                      << where_clause->op << " " << where_clause->right_operand << "\n";
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
            std::cout << "      Where : " << where_clause->left_operand << " " 
                      << where_clause->op << " " << where_clause->right_operand << "\n";
        }
    }
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