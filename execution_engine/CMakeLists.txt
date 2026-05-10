# Static library — execution engine. Walks ASTs from query_parser and
# dispatches to storage_engine (table DDL/DML/DQL) and engine (auth
# checks, system_schema lookups).
#
# Public API surface is exactly one C-callable function:
#   int exec_engine_execute(EngineState *, ParserAST *, char *, size_t)
# declared in include/exec_engine_api.h.
#
# Currently only the stub is wired up. The pre-redesign files in this
# directory (executor.c, expr.c, node.c, plan.c, ast_executor.cpp,
# main.c) are NOT in the build — they predate the v2 architecture and
# the execution-engine owner integrates them on his own timeline.

add_library(execution_engine STATIC
        src/exec_engine_api.cpp
)

target_include_directories(execution_engine PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}/include
)

# query_parser provides ParserAST + parser_api.hpp (the AST walker
# needs ASTNode definitions).
#
# When the executor starts calling engine_check_access / storage_*, add
# `engine` and `storage_engine` to PRIVATE here. The current stub uses
# only the forward-declared `struct EngineState *` and never dereferences
# it, so no link is required yet.
target_link_libraries(execution_engine PUBLIC query_parser)

target_compile_options(execution_engine PRIVATE -Wall -Wextra -Wpedantic)
