add_library(system_schema STATIC
    src/system_schema.c
)

target_include_directories(system_schema PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/include
)

# Reuses common.h, file_header, and checksum from storage_engine.
target_link_libraries(system_schema PUBLIC storage_engine)

target_compile_options(system_schema PRIVATE -Wall -Wextra -Wpedantic)
