#include "../include/Parser.hpp"
#include <iostream>

Parser::Parser(std::vector<Token> tokens) : tokens(std::move(tokens)) {}

// --- Helper Functions ---

Token Parser::peek() {
    if (current_token_idx >= tokens.size()) return tokens.back();
    return tokens[current_token_idx];
}

Token Parser::advance() {
    if (current_token_idx < tokens.size()) current_token_idx++;
    return tokens[current_token_idx - 1];
}

bool Parser::match(TokenType type, const std::string& value) {
    Token current = peek();
    if (current.type == type && (value.empty() || current.value == value)) {
        advance();
        return true;
    }
    return false;
}

void Parser::consume(TokenType type, const std::string& value, const std::string& error_msg) {
    if (!match(type, value)) {
        throw std::runtime_error("Syntax Error: " + error_msg);
    }
}

// --- Main Parsing Dispatcher ---

std::unique_ptr<ASTNode> Parser::parse() {
    if (match(TokenType::KEYWORD, "SELECT")) return parse_select();
    if (match(TokenType::KEYWORD, "CREATE")) return parse_create_table();
    if (match(TokenType::KEYWORD, "INSERT")) return parse_insert();
    if (match(TokenType::KEYWORD, "UPDATE")) return parse_update();
    if (match(TokenType::KEYWORD, "DELETE")) return parse_delete();
    
    // NEW TCL DISPATCHERS
    if (match(TokenType::KEYWORD, "BEGIN")) return parse_transaction("BEGIN");
    if (match(TokenType::KEYWORD, "COMMIT")) return parse_transaction("COMMIT");
    if (match(TokenType::KEYWORD, "ROLLBACK")) return parse_transaction("ROLLBACK");
    
    throw std::runtime_error("Syntax Error: Unsupported or unrecognized statement.");
}

// --- Statement Parsers ---

// 1. SELECT Statement Logic
std::unique_ptr<SelectStatement> Parser::parse_select() {
    auto stmt = std::make_unique<SelectStatement>();

    // Parse columns
    if (match(TokenType::SYMBOL, "*")) {
        stmt->is_select_all = true;
    } else {
        do {
            Token col_token = advance();
            if (col_token.type != TokenType::IDENTIFIER) throw std::runtime_error("Syntax Error: Expected column name.");
            stmt->columns.push_back(col_token.value);
        } while (match(TokenType::SYMBOL, ",")); 
    }

    consume(TokenType::KEYWORD, "FROM", "Expected 'FROM' after column list.");

    // Parse Table Name
    Token table_token = advance();
    if (table_token.type != TokenType::IDENTIFIER) throw std::runtime_error("Syntax Error: Expected table name.");
    stmt->table_name = table_token.value;

    // --- NEW: Parse Optional JOIN Clause ---
// --- FULL JOIN PARSING LOGIC ---
    bool has_join = false;
    JoinType j_type = JoinType::INNER;

    if (match(TokenType::KEYWORD, "LEFT")) {
        match(TokenType::KEYWORD, "OUTER"); // 'OUTER' is optional in SQL, so we just match and ignore
        consume(TokenType::KEYWORD, "JOIN", "Expected 'JOIN' after 'LEFT'.");
        j_type = JoinType::LEFT;
        has_join = true;
    } else if (match(TokenType::KEYWORD, "RIGHT")) {
        match(TokenType::KEYWORD, "OUTER");
        consume(TokenType::KEYWORD, "JOIN", "Expected 'JOIN' after 'RIGHT'.");
        j_type = JoinType::RIGHT;
        has_join = true;
    } else if (match(TokenType::KEYWORD, "FULL")) {
        match(TokenType::KEYWORD, "OUTER");
        consume(TokenType::KEYWORD, "JOIN", "Expected 'JOIN' after 'FULL'.");
        j_type = JoinType::FULL;
        has_join = true;
    } else if (match(TokenType::KEYWORD, "INNER")) {
        consume(TokenType::KEYWORD, "JOIN", "Expected 'JOIN' after 'INNER'.");
        j_type = JoinType::INNER;
        has_join = true;
    } else if (match(TokenType::KEYWORD, "JOIN")) {
        j_type = JoinType::INNER; // Standard JOIN is an INNER JOIN
        has_join = true;
    }

    if (has_join) {
        stmt->join_clause = std::make_unique<JoinClause>();
        stmt->join_clause->join_type = j_type;
        
        Token join_table_token = advance();
        if (join_table_token.type != TokenType::IDENTIFIER) throw std::runtime_error("Syntax Error: Expected table name after JOIN.");
        stmt->join_clause->join_table = join_table_token.value;
consume(TokenType::KEYWORD, "ON", "Expected 'ON' after JOIN table name.");
        
        Token left_cond = advance();
        // Enforce that it is an identifier AND contains a dot '.'
        if (left_cond.type != TokenType::IDENTIFIER || left_cond.value.find('.') == std::string::npos) {
            throw std::runtime_error("Syntax Error: Left side of ON condition must be in 'table.column' format (e.g., users.id).");
        }
        stmt->join_clause->left_condition = left_cond.value;

        consume(TokenType::SYMBOL, "=", "Expected '=' in JOIN condition.");
        
        Token right_cond = advance();
        // Enforce that the right side also contains a dot '.'
        if (right_cond.type != TokenType::IDENTIFIER || right_cond.value.find('.') == std::string::npos) {
            throw std::runtime_error("Syntax Error: Right side of ON condition must be in 'table.column' format (e.g., orders.user_id).");
        }
        stmt->join_clause->right_condition = right_cond.value;
    }

    // Parse Optional WHERE Clause
    if (match(TokenType::KEYWORD, "WHERE")) {
        stmt->where_clause = std::make_unique<WhereClause>();
        stmt->where_clause->left_operand = advance().value;
        stmt->where_clause->op = advance().value; 
        stmt->where_clause->right_operand = advance().value;
    }

    consume(TokenType::SYMBOL, ";", "Expected ';' at the end of the statement.");

    return stmt;
}


// 2. CREATE TABLE Statement Logic
std::unique_ptr<CreateTableStatement> Parser::parse_create_table() {
    auto stmt = std::make_unique<CreateTableStatement>();

    consume(TokenType::KEYWORD, "TABLE", "Expected 'TABLE' after 'CREATE'.");
    
    // Parse Table Name
    Token table_token = advance();
    if (table_token.type != TokenType::IDENTIFIER) throw std::runtime_error("Syntax Error: Expected table name.");
    stmt->table_name = table_token.value;

    consume(TokenType::SYMBOL, "(", "Expected '(' to define columns.");

    // Parse columns loop
    do {
        ColumnDef col;
        
        // Column Name
        Token name_token = advance();
        if (name_token.type != TokenType::IDENTIFIER) throw std::runtime_error("Syntax Error: Expected column name.");
        col.name = name_token.value;

        // Data Type (e.g., INT, VARCHAR)
        Token type_token = advance();
        if (type_token.type != TokenType::IDENTIFIER && type_token.type != TokenType::KEYWORD) {
            throw std::runtime_error("Syntax Error: Expected data type (e.g., INT, VARCHAR).");
        }
        col.data_type = type_token.value;

        // Check for PRIMARY KEY constraint
        if (match(TokenType::KEYWORD, "PRIMARY")) {
            consume(TokenType::KEYWORD, "KEY", "Expected 'KEY' after 'PRIMARY'.");
            col.is_primary_key = true;
        }

        stmt->columns.push_back(col);

    } while (match(TokenType::SYMBOL, ","));

    consume(TokenType::SYMBOL, ")", "Expected ')' to close column definitions.");
    consume(TokenType::SYMBOL, ";", "Expected ';' at the end of the statement.");

    return stmt;
}

// 3. INSERT INTO Statement Logic
std::unique_ptr<InsertStatement> Parser::parse_insert() {
    auto stmt = std::make_unique<InsertStatement>();

    consume(TokenType::KEYWORD, "INTO", "Expected 'INTO' after 'INSERT'.");
    
    // Parse Table Name
    Token table_token = advance();
    if (table_token.type != TokenType::IDENTIFIER) throw std::runtime_error("Syntax Error: Expected table name.");
    stmt->table_name = table_token.value;

    consume(TokenType::KEYWORD, "VALUES", "Expected 'VALUES' keyword.");
    consume(TokenType::SYMBOL, "(", "Expected '(' before values.");

    // Parse values loop
    do {
        Token val_token = advance();
        stmt->values.push_back(val_token.value);
    } while (match(TokenType::SYMBOL, ","));

    consume(TokenType::SYMBOL, ")", "Expected ')' after values.");
    consume(TokenType::SYMBOL, ";", "Expected ';' at the end of the statement.");

    return stmt;
}




// Logic for UPDATE statements
std::unique_ptr<UpdateStatement> Parser::parse_update() {
    auto stmt = std::make_unique<UpdateStatement>();

    Token table_token = advance();
    if (table_token.type != TokenType::IDENTIFIER) throw std::runtime_error("Syntax Error: Expected table name after UPDATE.");
    stmt->table_name = table_token.value;

    consume(TokenType::KEYWORD, "SET", "Expected 'SET' keyword.");

    // Parse the column to update
    Token col_token = advance();
    stmt->set_column = col_token.value;

    consume(TokenType::SYMBOL, "=", "Expected '=' after column name in SET clause.");

    // Parse the new value
    Token val_token = advance();
    stmt->set_value = val_token.value;

    // Optional WHERE clause
    if (match(TokenType::KEYWORD, "WHERE")) {
        stmt->where_clause = std::make_unique<WhereClause>();
        stmt->where_clause->left_operand = advance().value;
        stmt->where_clause->op = advance().value;
        stmt->where_clause->right_operand = advance().value;
    }

    consume(TokenType::SYMBOL, ";", "Expected ';' at the end of the statement.");
    return stmt;
}

// Logic for DELETE statements
std::unique_ptr<DeleteStatement> Parser::parse_delete() {
    auto stmt = std::make_unique<DeleteStatement>();

    consume(TokenType::KEYWORD, "FROM", "Expected 'FROM' after DELETE.");

    Token table_token = advance();
    if (table_token.type != TokenType::IDENTIFIER) throw std::runtime_error("Syntax Error: Expected table name.");
    stmt->table_name = table_token.value;

    // Optional WHERE clause
    if (match(TokenType::KEYWORD, "WHERE")) {
        stmt->where_clause = std::make_unique<WhereClause>();
        stmt->where_clause->left_operand = advance().value;
        stmt->where_clause->op = advance().value;
        stmt->where_clause->right_operand = advance().value;
    }

    consume(TokenType::SYMBOL, ";", "Expected ';' at the end of the statement.");
    return stmt;
}


// Logic for TCL (Transactions)
std::unique_ptr<TransactionStatement> Parser::parse_transaction(const std::string& cmd_str) {
    TransactionCommand cmd;
    
    if (cmd_str == "BEGIN") cmd = TransactionCommand::BEGIN;
    else if (cmd_str == "COMMIT") cmd = TransactionCommand::COMMIT;
    else if (cmd_str == "ROLLBACK") cmd = TransactionCommand::ROLLBACK;

    consume(TokenType::SYMBOL, ";", "Expected ';' after transaction command.");
    
    return std::make_unique<TransactionStatement>(cmd);
}