#pragma once
#include "Lexer.hpp"
#include "AST.hpp"
#include <memory>
#include <stdexcept>
#include <vector>

class Parser {
public:
    explicit Parser(std::vector<Token> tokens);
    
    // Main entry point
    std::unique_ptr<ASTNode> parse();

private:
    std::vector<Token> tokens;
    size_t current_token_idx = 0;
    std::unique_ptr<UpdateStatement> parse_update();
    std::unique_ptr<DeleteStatement> parse_delete();
    std::unique_ptr<TransactionStatement> parse_transaction(const std::string& cmd); 
   // Helper functions
    Token peek();
    Token advance();
    bool match(TokenType type, const std::string& value = "");
    void consume(TokenType type, const std::string& value, const std::string& error_msg);

    // Specific statement parsers (These were the ones missing!)
    std::unique_ptr<SelectStatement> parse_select();
    std::unique_ptr<CreateTableStatement> parse_create_table();
    std::unique_ptr<InsertStatement> parse_insert();
};
