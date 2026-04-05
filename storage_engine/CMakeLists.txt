add_library(storage_engine STATIC
    src/disk_manager.c
    src/page.c
    src/buffer_pool.c
    src/btree.c
    src/schema.c
    src/transaction.c
    src/storage.c
)

target_include_directories(storage_engine PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/include
)

target_compile_options(storage_engine PRIVATE -Wall -Wextra -Wpedantic)
