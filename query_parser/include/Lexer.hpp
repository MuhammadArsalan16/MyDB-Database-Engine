#pragma once
#include <string>
#include <vector>
#include <cstdint>
#include <stdexcept>

enum class TokenType { KEYWORD, IDENTIFIER, NUMBER, STRING, SYMBOL, OPERATOR, END_OF_FILE };

struct Token {
    TokenType type;
    std::string value;
    uint32_t line = 1;
    uint32_t col = 1;
};

class Lexer {
public:
    explicit Lexer(std::string input);
    std::vector<Token> tokenize();

private:
    std::string source;
    size_t current_pos = 0;
    uint32_t line = 1;
    uint32_t col = 1;

    void skip_whitespace();
    bool is_keyword(const std::string& str);
    std::string to_upper(std::string str);
};
