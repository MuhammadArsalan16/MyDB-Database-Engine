#pragma once
#include <string>
#include <vector>

enum class TokenType { KEYWORD, IDENTIFIER, SYMBOL, END_OF_FILE };

struct Token {
    TokenType type;
    std::string value;
};

class Lexer {
public:
    explicit Lexer(std::string input);
    std::vector<Token> tokenize();

private:
    std::string source;
    size_t current_pos = 0;

    void skip_whitespace();
    bool is_keyword(const std::string& str);
    std::string to_upper(std::string str);
};
