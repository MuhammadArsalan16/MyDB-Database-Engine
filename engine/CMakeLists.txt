add_library(engine STATIC
    src/database_file.c
    src/crypto.c
    src/engine.c
)

target_include_directories(engine PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/include
)

# Engine orchestrates storage_engine (page/buffer/btree/catalog) and
# system_schema (users + privileges). PUBLIC so test binaries that
# link `engine` inherit those include paths transitively.
#
# query_parser + execution_engine are needed by engine_execute_sql,
# which routes raw SQL through parse → execute. PUBLIC so bin (and any
# test binary linking engine) gets the parser_api.h / exec_engine_api.h
# include paths transitively.
target_link_libraries(engine PUBLIC
        storage_engine system_schema
        query_parser execution_engine
)

target_compile_options(engine PRIVATE -Wall -Wextra -Wpedantic)
