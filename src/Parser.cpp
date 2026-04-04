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
            if (col_token.type != TokenType::IDENTIFIER) {
                throw std::runtime_error("Syntax Error: Expected column name.");
            }
            stmt->columns.push_back(col_token.value);
        } while (match(TokenType::SYMBOL, ",")); // Loop if there's a comma
    }

    // Parse FROM
    consume(TokenType::KEYWORD, "FROM", "Expected 'FROM' after column list.");

    // Parse Table Name
    Token table_token = advance();
    if (table_token.type != TokenType::IDENTIFIER) {
        throw std::runtime_error("Syntax Error: Expected table name.");
    }
    stmt->table_name = table_token.value;

    // Parse Optional WHERE Clause
    if (match(TokenType::KEYWORD, "WHERE")) {
        stmt->where_clause = std::make_unique<WhereClause>();
        
        // 1. Left Operand (Column)
        Token left = advance();
        if (left.type != TokenType::IDENTIFIER) throw std::runtime_error("Syntax Error: Expected column name in WHERE clause.");
        stmt->where_clause->left_operand = left.value;

        // 2. Operator (=, >, <, etc.)
        Token op = advance();
        stmt->where_clause->op = op.value; 

        // 3. Right Operand (Value/Number)
        Token right = advance();
        stmt->where_clause->right_operand = right.value;
    }

    // Parse ending semicolon
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