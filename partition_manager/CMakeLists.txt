add_library(partition_manager STATIC
    # Files moved here from storage_engine (v3 redesign):
    # Session-state files belong in partition_manager, not storage_engine.
    src/partition.c       # Catalog: __catalog.mydb, quota tracking
    src/schema_file.c     # SchemaFile: __schema.mydb, RelationDef persistence
    src/transaction.c     # TransactionManager: begin/commit/rollback
    src/stats.c           # StatsFile: __stats.mydb, optimizer statistics

    # partition_manager-native files:
    src/partition_ctx.c    # PartitionCtx lifecycle (init, open_schema, close)
    src/partition_buffer.c # PartitionBuffer: LRU cache of open SchemaFile handles
    src/pm_api.c           # Public pm_* API: session-aware DDL/DML/DQL/TCL wrappers
)

target_include_directories(partition_manager PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/include
    # partition_manager types depend on storage_engine types
    # (BufferPool, BTree, DiskManager, RelationDef, common.h).
    # These are inherited transitively through target_link_libraries below.
)

# partition_manager depends on storage_engine for all low-level I/O types.
# storage_engine does NOT link partition_manager (no circular dependency).
target_link_libraries(partition_manager PUBLIC
    storage_engine
    system_schema
    Threads::Threads
)

target_compile_options(partition_manager PRIVATE -Wall -Wextra -Wpedantic)
