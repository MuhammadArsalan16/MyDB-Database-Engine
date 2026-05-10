# Static library — pure C++ parser plus the C-callable parser_api bridge
# that engine and execution_engine link against.
#
# main.cpp is intentionally NOT in the library. It contains its own main()
# and depends on GNU readline (not always installed in dev environments).
# It can be built standalone for parser-team smoke testing if desired.
add_library(query_parser STATIC
        src/Lexer.cpp
        src/Parser.cpp
        src/parser_api.cpp
)

target_include_directories(query_parser PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}/include
)

target_compile_options(query_parser PRIVATE -Wall -Wextra -Wpedantic)
