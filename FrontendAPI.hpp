#pragma once
#include <string>
#include <memory>
#include <stdexcept>
#include "query_parser/AST.hpp"
// Facade class for the Lexer and Parser
class FrontendAPI {
public:
    /**
     * @brief Takes a raw SQL string, tokenizes it, and parses it into an AST.
     * * @param raw_sql The raw string input by the user (e.g., "SELECT * FROM users;")
     * @return std::unique_ptr<ASTNode> A pointer to the root of the AST tree.
     * @throws std::runtime_error If there is a syntax error in the query.
     */
    static std::unique_ptr<ASTNode> parse_query(const std::string& raw_sql) {
        // NOTE FOR ARSALAN: You will implement this in your .cpp file.
        // It should look something like:
        // Lexer lexer(raw_sql);
        // std::vector<Token> tokens = lexer.tokenize();
        // Parser parser(tokens);
        // return parser.parse();
    }
};
