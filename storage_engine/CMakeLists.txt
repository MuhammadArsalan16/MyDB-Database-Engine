add_library(storage_engine STATIC
    src/disk_manager.c
    src/page.c
    src/buffer_pool.c
    src/btree.c
    src/relation_def.c
    src/storage.c
    src/file_header.c
    src/checksum.c
)

# v3: partition.c, schema_file.c, transaction.c, stats.c have moved to
# partition_manager. storage_engine is now a truly stateless I/O layer
# (BufferPool, BTree, Page, DiskManager, RelationDef only).
# storage.c needs partition_ctx.h from partition_manager to accept a
# PartitionCtx* in future APIs; for v3 Phase 2 it only needs its own headers.

target_include_directories(storage_engine PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/include
    ${CMAKE_SOURCE_DIR}/system_schema/include
)

target_compile_options(storage_engine PRIVATE -Wall -Wextra -Wpedantic)
