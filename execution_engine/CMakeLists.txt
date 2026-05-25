# execution_engine — AST walker that sits between the parser and storage.
#
# Public API: one C-callable function
#   int exec_engine_execute(EngineState *, ParserAST *, char *, size_t)
# declared in include/exec_engine_api.h.
#
# Link topology note
# ──────────────────
# engine → execution_engine (engine is the sole caller of exec_engine_execute).
# execution_engine → storage_engine  (DML / DQL storage calls).
# execution_engine → query_parser    (AST types, parser_ast_node).
#
# execution_engine does NOT link against `engine` to avoid a circular
# CMake target dependency.  It only needs engine/include for the
# EngineState type and engine_* function signatures; the actual symbols
# are resolved at final-binary link time because every executable that
# uses the executor also links `engine` (which brings both libraries in).
#
# engine/include and system_schema/include are added as PRIVATE include
# paths so implementation files can #include "engine.h" / "system_schema.h"
# without exposing those paths to downstream consumers of this library.

add_library(execution_engine STATIC
    src/exec_engine_api.cpp
    src/dispatch.cpp
    src/tcl.cpp
    src/ddl.cpp
    src/dml.cpp
    src/dql.cpp
    src/value_cast.cpp
    src/expr_eval.cpp
    src/result_fmt.cpp
)

target_include_directories(execution_engine PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/include
)

# engine.h and system_schema.h are needed by implementation files but
# must not be exposed as part of this library's public interface.
target_include_directories(execution_engine PRIVATE
    ${CMAKE_SOURCE_DIR}/engine/include
    ${CMAKE_SOURCE_DIR}/system_schema/include
)

target_link_libraries(execution_engine
    PUBLIC query_parser
    PUBLIC storage_engine
)

target_compile_features(execution_engine PUBLIC cxx_std_17)
target_compile_options(execution_engine PRIVATE -Wall -Wextra -Wpedantic)
