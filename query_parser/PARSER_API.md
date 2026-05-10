# MyDB Query Parser — C-Callable API

**Audience:** the engine module (calls into parser) and the execution
engine module (consumes the parsed AST). Bin never touches this directly.

**Header:** `#include "parser_api.h"` from C; `#include "parser_api.hpp"`
from C++ if you need to walk the AST.

---

## What this bridge solves

The parser is C++ (`Lexer`, `Parser`, `ASTNode` hierarchy with
`std::unique_ptr` ownership). The engine module that calls the parser
is C. Without a bridge, the engine cannot:

- own a C++ smart pointer
- catch C++ exceptions
- reach into AST internals

`parser_api.h` exposes a minimal C-callable interface — two functions
plus an opaque handle — that gives the engine everything it needs:
parse a SQL string and free the result. The execution engine, which
*is* C++, includes `parser_api.hpp` to reach the AST node behind the
handle.

---

## API surface (2 functions, 1 opaque type)

### `ParserAST` — opaque handle

```c
typedef struct ParserAST ParserAST;
```

Heap-allocated by `parser_parse()`. The C struct definition is hidden
from C; the C++ definition (`include/parser_api.hpp`) wraps a single
`std::unique_ptr<ASTNode>`.

The execution engine reads it through `parser_ast_node()` (see below).

### `parser_parse`

```c
int parser_parse(const char *sql,
                 ParserAST **out_ast,
                 char *err_buf, size_t err_cap);
```

Tokenises and parses one SQL statement.

| Param | Meaning |
|---|---|
| `sql` | NUL-terminated SQL string. |
| `out_ast` | On success, populated with a fresh heap-allocated handle. On failure, set to `NULL`. |
| `err_buf`, `err_cap` | Optional buffer to receive a parse error message (NUL-terminated, truncated). Pass `NULL` / `0` to ignore. |

**Returns**

| Code | Meaning |
|---|---|
| `PARSER_OK` (0) | Parse succeeded; `*out_ast` is a valid handle. Caller owns it and must `parser_free_ast()` exactly once. |
| `PARSER_ERR` (-1) | Lex error, parse error, or the parser returned a null AST. Error message in `err_buf` if provided. `*out_ast == NULL`. |

**Exception safety**: `Lexer` and `Parser` throw `std::runtime_error`
on bad input. The bridge catches these (and any other `std::exception`)
and serialises `e.what()` into `err_buf`. Unknown C++ exceptions become
the literal string `"unknown parser exception"`.

### `parser_free_ast`

```c
void parser_free_ast(ParserAST *ast);
```

Releases the handle. NULL-safe. Calling it twice on the same handle is
undefined.

### `parser_ast_node` (C++ only)

```cpp
const ASTNode *parser_ast_node(const ParserAST *ast);
```

Defined in `parser_api.hpp`. Returns a non-owning, borrowed pointer to
the wrapped AST root. The pointer is valid until `parser_free_ast()`
is called on the handle.

The execution engine uses this to switch on `ASTNode::type` and reach
the typed subclasses (`SelectStatement`, `CreateTableStatement`, etc.)
defined in `AST.hpp`.

---

## Calling pattern

From C (engine module):

```c
ParserAST *ast = NULL;
char err[256];

int rc = parser_parse(sql, &ast, err, sizeof(err));
if (rc != PARSER_OK) {
    /* err contains the parser's message */
    return MYDB_ERR;
}

rc = exec_engine_execute(eng, ast, result_out, result_cap);
parser_free_ast(ast);
return rc;
```

From C++ (execution engine):

```cpp
const ASTNode *node = parser_ast_node(ast);
switch (node->type) {
    case StatementType::SELECT:
        return execute_select(eng, static_cast<const SelectStatement*>(node), ...);
    case StatementType::INSERT:
        return execute_insert(eng, static_cast<const InsertStatement*>(node), ...);
    // ...
}
```

---

## Memory + lifetime rules

- **One handle per parse**: `parser_parse()` allocates; the caller frees
  via `parser_free_ast()`.
- **No partial ownership**: do not free the underlying `ASTNode` directly.
  The handle owns the `unique_ptr`; freeing the handle deletes the AST.
- **Borrowed pointer from `parser_ast_node`**: the returned `const ASTNode*`
  becomes invalid when the handle is freed. Don't keep it past the
  enclosing call.

---

## What's NOT in this bridge

- **Multi-statement input**: `parser_parse` parses a single statement.
  Bin's REPL terminates on `;`; if multi-statement scripts come later,
  the bridge will need a streaming variant.
- **AST traversal helpers**: deliberately none. Walking is the execution
  engine's job; making the parser expose visitor scaffolding would
  bleed concerns across modules.
- **Validation against schema**: the parser does not consult engine
  state. `engine_find_relation()` is the validation step; the executor
  calls it as it walks each AST node that names a relation.

---

## Module layout

```
query_parser/
├── include/
│   ├── AST.hpp              C++ AST node hierarchy
│   ├── Lexer.hpp            tokeniser
│   ├── Parser.hpp           recursive-descent parser
│   ├── parser_api.h         <-- C-callable bridge (this doc)
│   └── parser_api.hpp       <-- C++ companion (parser_ast_node)
├── src/
│   ├── Lexer.cpp
│   ├── Parser.cpp
│   ├── parser_api.cpp       <-- bridge implementation
│   └── main.cpp             standalone REPL, NOT in the static library
└── CMakeLists.txt           builds libquery_parser.a (without main.cpp)
```

The library is static. Link order: `engine` and `execution_engine`
both depend on `query_parser`.

---

*Maintained by the parser team (Arsalan). Cross-module contract owners:
storage = Hasnat, execution = Rehan.*
