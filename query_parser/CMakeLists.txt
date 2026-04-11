add_library(query_parser STATIC
        src/Lexer.cpp
        src/Parser.cpp
        src/main.cpp
)

target_include_directories(query_parser PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}/include
)

target_compile_options(query_parser PRIVATE -Wall -Wextra -Wpedantic)
