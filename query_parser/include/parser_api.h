#ifndef PARSER_API_H
#define PARSER_API_H

/* C-callable bridge over the C++ parser.
 *
 * The engine module (C) calls parser_parse() with a raw SQL string and
 * receives an opaque ParserAST handle. The execution engine (C++) then
 * consumes the handle to walk the AST. The engine never inspects AST
 * internals — it routes the handle from parser to executor and frees
 * it after.
 *
 * Memory model: parser_parse() heap-allocates the AST. The caller must
 * call parser_free_ast() exactly once to release it. Re-using a handle
 * after free or freeing twice is undefined.
 *
 * Thread safety: each call is independent and re-entrant. The caller
 * is responsible for serialising access if multiple threads share a
 * single handle. */

#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Opaque handle. C code treats this as a black box; the C++ definition
 * is exposed in parser_api.hpp for the execution engine. */
typedef struct ParserAST ParserAST;

#define PARSER_OK    0
#define PARSER_ERR  -1

/* Parse `sql` (NUL-terminated). On success, returns PARSER_OK and writes
 * a heap-allocated handle into *out_ast. On failure (lex or parse
 * error), returns PARSER_ERR and copies the error message into err_buf
 * (truncated to err_cap-1 bytes, NUL-terminated). err_buf may be NULL
 * if the caller does not care about the message.
 *
 * On error, *out_ast is set to NULL. */
int parser_parse(const char *sql,
                 ParserAST **out_ast,
                 char *err_buf, size_t err_cap);

/* Release an AST handle. Safe to call on NULL. */
void parser_free_ast(ParserAST *ast);

#ifdef __cplusplus
}
#endif

#endif /* PARSER_API_H */
