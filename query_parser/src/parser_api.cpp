/* parser_api.cpp — C-callable bridge over Lexer + Parser.
 *
 * Real implementation, not a stub. Calls into the existing C++ Lexer
 * and Parser, wraps the resulting std::unique_ptr<ASTNode> in a heap-
 * allocated ParserAST, and returns the raw pointer as an opaque handle.
 *
 * Lex/parse exceptions are caught and serialised into err_buf. */

#include "parser_api.hpp"
#include "Lexer.hpp"
#include "Parser.hpp"

#include <cstring>
#include <exception>
#include <string>

extern "C" int parser_parse(const char *sql,
                            ParserAST **out_ast,
                            char *err_buf, size_t err_cap)
{
    if (!sql || !out_ast) return PARSER_ERR;
    *out_ast = nullptr;

    try {
        Lexer  lexer(sql);
        std::vector<Token> tokens = lexer.tokenize();
        Parser parser(tokens);
        std::unique_ptr<ASTNode> node = parser.parse();
        if (!node) {
            if (err_buf && err_cap > 0)
                std::snprintf(err_buf, err_cap, "parser returned null AST");
            return PARSER_ERR;
        }
        *out_ast = new ParserAST{ std::move(node) };
        return PARSER_OK;
    } catch (const std::exception &e) {
        if (err_buf && err_cap > 0) {
            std::strncpy(err_buf, e.what(), err_cap - 1);
            err_buf[err_cap - 1] = '\0';
        }
        return PARSER_ERR;
    } catch (...) {
        if (err_buf && err_cap > 0)
            std::snprintf(err_buf, err_cap, "unknown parser exception");
        return PARSER_ERR;
    }
}

extern "C" void parser_free_ast(ParserAST *ast)
{
    delete ast;
}
