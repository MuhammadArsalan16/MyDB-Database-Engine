#include "../include/Lexer.hpp"
#include <cctype>
#include <algorithm>

Lexer::Lexer(std::string input) : source(std::move(input)) {}

void Lexer::skip_whitespace() {
    while (current_pos < source.length() && std::isspace(source[current_pos])) {
        if (source[current_pos] == '\n') {
            line++;
            col = 1;
        } else {
            col++;
        }
        current_pos++;
    }
}

std::string Lexer::to_upper(std::string str) {
    std::transform(str.begin(), str.end(), str.begin(), ::toupper);
    return str;
}

bool Lexer::is_keyword(const std::string& str) {
    std::vector<std::string> keywords = {
        "SELECT", "FROM", "CREATE", "TABLE", 
        "INSERT", "INTO", "VALUES", "WHERE", "PRIMARY", "KEY",
        "UPDATE", "SET", "DELETE",
        "BEGIN", "COMMIT", "ROLLBACK", 
        "JOIN", "ON", "INNER", "LEFT", "RIGHT", "FULL", "OUTER",
        "NULL", "IS", "NOT", "AND", "OR", "BETWEEN", "IN", "LIKE", "AS",
        "DEFAULT", "UNIQUE", "AUTO_INCREMENT", "AUTOINCR", "REFERENCES", "FOREIGN", "CONSTRAINT",
        "INT", "INTEGER", "DECIMAL", "VARCHAR", "ENUM", "BOOL", "BOOLEAN", "DATE", "DATETIME",
        "TRUE", "FALSE",
        "DROP", "DATABASE", "SCHEMA", "USE", "SHOW", "TABLES", "DATABASES",
        "ORDER", "BY", "ASC", "DESC", "LIMIT", "OFFSET",
        "GROUP", "HAVING", "DISTINCT",
        "COUNT", "SUM", "AVG", "MIN", "MAX",
        "INDEX", "INDEXED", "CASCADE", "RESTRICT",
        "ANALYZE",
        "USER", "IDENTIFIED", "PARTITION", "QUOTA", "ALTER",
        "DESCRIBE", "DISCONNECT"
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

        if (c == ',' || c == '*' || c == ';' || c == '(' || c == ')' || c == '.') {
            tokens.push_back({TokenType::SYMBOL, std::string(1, c), line, col});
            current_pos++;
            col++;
            continue;
        }

        if (c == '=' || c == '<' || c == '>' || c == '!' || c == '|') {
            uint32_t start_col = col;
            std::string op(1, c);
            current_pos++;
            col++;
            if (current_pos < source.length()) {
                char next_c = source[current_pos];
                if ((c == '!' && next_c == '=') ||
                    (c == '<' && (next_c == '=' || next_c == '>')) ||
                    (c == '>' && next_c == '=') ||
                    (c == '|' && next_c == '|')) {
                    op += next_c;
                    current_pos++;
                    col++;
                }
            }
            tokens.push_back({TokenType::OPERATOR, op, line, start_col});
            continue;
        }

        
        if (c == '\'' || c == '"') {
            char quote = c;
            current_pos++;
            uint32_t start_col = col;
            uint32_t start_line = line;
            col++;
            std::string buf;
            bool closed = false;
            while (current_pos < source.length()) {
                char ch = source[current_pos];
                if (ch == quote) {
                    if (current_pos + 1 < source.length() && source[current_pos+1] == quote) {
                        buf += quote;
                        current_pos += 2;
                        col += 2;
                        continue;
                    }
                    current_pos++;
                    col++;
                    tokens.push_back({TokenType::STRING, buf, start_line, start_col});
                    closed = true;
                    break;
                }
                if (ch == '\n') {
                    line++;
                    col = 1;
                } else {
                    col++;
                }
                buf += ch;
                current_pos++;
            }
            if (!closed) {
                throw std::runtime_error("unterminated string at " + std::to_string(start_line) + ":" + std::to_string(start_col));
            }
            continue;
        }

        if (std::isdigit(c)) {
            std::string buf;
            uint32_t start_col = col;
            while (current_pos < source.length() && std::isdigit(source[current_pos])) {
                buf += source[current_pos++];
                col++;
            }
            if (current_pos < source.length() && source[current_pos] == '.') {
                buf += source[current_pos++];
                col++;
                while (current_pos < source.length() && std::isdigit(source[current_pos])) {
                    buf += source[current_pos++];
                    col++;
                }
            }
            tokens.push_back({TokenType::NUMBER, buf, line, start_col});
            continue;
        }

        if (std::isalpha(c) || c == '_') {
            std::string word;
            uint32_t start_col = col;
            while (current_pos < source.length() && 
                  (std::isalnum(source[current_pos]) || source[current_pos] == '_')) {
                word += source[current_pos++];
                col++;
            }
            
            if (is_keyword(word)) {
                tokens.push_back({TokenType::KEYWORD, to_upper(word), line, start_col});
            } else {
                tokens.push_back({TokenType::IDENTIFIER, word, line, start_col});
            }
            continue;
        }
        // Skip unrecognized characters
        current_pos++;
        col++;
    }

    tokens.push_back({TokenType::END_OF_FILE, "", line, col});
    return tokens;
}