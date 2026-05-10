#pragma once

/* C++ companion to parser_api.h.
 *
 * The execution engine (C++) includes this to reach the underlying
 * ASTNode pointer through an opaque ParserAST handle. The engine
 * module (C) does NOT include this — it only sees ParserAST as an
 * opaque pointer via parser_api.h.
 *
 * Pointer semantics: parser_ast_node() returns a non-owning pointer
 * into the ParserAST. It is valid until parser_free_ast() is called.
 * Do not delete the returned pointer. */

#include "AST.hpp"
#include "parser_api.h"

/* Internal layout — visible to the execution engine and to
 * parser_api.cpp. Other code should treat ParserAST as opaque. */
struct ParserAST {
    std::unique_ptr<ASTNode> node;
};

/* Borrowed pointer to the parsed AST. Returns nullptr if `ast` is
 * nullptr. */
inline const ASTNode *parser_ast_node(const ParserAST *ast)
{
    return ast ? ast->node.get() : nullptr;
}
