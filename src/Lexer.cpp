#include "../include/Lexer.hpp"
#include <cctype>
#include <algorithm>

Lexer::Lexer(std::string input) : source(std::move(input)) {}

void Lexer::skip_whitespace() {
    while (current_pos < source.length() && std::isspace(source[current_pos])) {
        current_pos++;
    }
}

std::string Lexer::to_upper(std::string str) {
    std::transform(str.begin(), str.end(), str.begin(), ::toupper);
    return str;
}

bool Lexer::is_keyword(const std::string& str) {
    // UPDATED: Added WHERE and VALUES to our keyword dictionary!
    std::vector<std::string> keywords = {
        "SELECT", "FROM", "CREATE", "TABLE", 
        "INSERT", "INTO", "VALUES", "WHERE", "PRIMARY", "KEY"
    };
    std::string upper_str = to_upper(str);
    return std::find(keywords.begin(), keywords.end(), upper_str) != keywords.end();
}

std::vector<Token> Lexer::tokenize() {
    std::vector<Token> tokens;
    
    while (current_pos < source.length()) {
        skip_whitespace();
        if (current_pos >= source.length()) break;

        char c = source[current_pos];

        // UPDATED: Added operators =, >, < to the recognized symbols!
        if (c == ',' || c == '*' || c == ';' || c == '=' || c == '>' || c == '<' || c == '(' || c == ')') {
            tokens.push_back({TokenType::SYMBOL, std::string(1, c)});
            current_pos++;
            continue;
        }

        // Match Keywords and Identifiers (including numbers)
        if (std::isalnum(c) || c == '_') {
            std::string word;
            while (current_pos < source.length() && (std::isalnum(source[current_pos]) || source[current_pos] == '_')) {
                word += source[current_pos++];
            }
            
            if (is_keyword(word)) {
                tokens.push_back({TokenType::KEYWORD, to_upper(word)});
            } else {
                tokens.push_back({TokenType::IDENTIFIER, word});
            }
            continue;
        }

        // Skip unrecognized characters
        current_pos++;
    }

    tokens.push_back({TokenType::END_OF_FILE, ""});
    return tokens;
}