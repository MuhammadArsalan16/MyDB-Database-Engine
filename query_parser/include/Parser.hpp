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
    [[noreturn]] void throw_error(const std::string& error_msg);
    void consume(TokenType type, const std::string& value, const std::string& error_msg);
    std::string parse_qualified_ident();

    // Specific statement parsers
    std::unique_ptr<SelectStatement> parse_select();
    std::unique_ptr<CreateTableStatement> parse_create_table();
    std::unique_ptr<InsertStatement> parse_insert();
    std::unique_ptr<DropTableStatement> parse_drop_table();
    std::unique_ptr<CreateDatabaseStatement> parse_create_database();
    std::unique_ptr<DropDatabaseStatement> parse_drop_database();
    std::unique_ptr<UseStatement> parse_use();
    std::unique_ptr<ASTNode> parse_show();
    std::unique_ptr<CreateIndexStatement>  parse_create_index();
    std::unique_ptr<AnalyzeTableStatement> parse_analyze();
    std::unique_ptr<CreateUserStatement>   parse_create_user();
    std::unique_ptr<DropUserStatement>     parse_drop_user();
    std::unique_ptr<AlterUserStatement>    parse_alter_user();
    std::unique_ptr<ASTNode>               parse_describe();

    // Expression parsing
    std::unique_ptr<Expr> parse_expr();
    std::unique_ptr<Expr> parse_or_expr();
    std::unique_ptr<Expr> parse_and_expr();
    std::unique_ptr<Expr> parse_not_expr();
    std::unique_ptr<Expr> parse_predicate();
    std::unique_ptr<Expr> parse_term();
};
