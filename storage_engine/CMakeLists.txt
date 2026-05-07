add_library(storage_engine STATIC
    src/disk_manager.c
    src/page.c
    src/buffer_pool.c
    src/btree.c
    src/relation_def.c
    src/transaction.c
    src/storage.c
    src/file_header.c
    src/checksum.c
    src/partition.c
    src/schema_file.c
)

target_include_directories(storage_engine PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/include
    # storage.c includes engine.h to read EngineState fields. Header-only
    # dependency: storage_engine does NOT link engine. The engine library
    # still PUBLIC-links storage_engine for object code, so the final
    # binary has each symbol once.
    ${CMAKE_SOURCE_DIR}/engine/include
    ${CMAKE_SOURCE_DIR}/system_schema/include
)

target_compile_options(storage_engine PRIVATE -Wall -Wextra -Wpedantic)
