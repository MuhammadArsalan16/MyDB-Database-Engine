add_library(wal STATIC
    src/normal_wal/wal_page.c
    src/normal_wal/wal_segment.c
    src/normal_wal/wal_segment_pool.c
)

# WAL Phase 1: normal_wal's on-disk struct layer only (record/page/segment
# header format + checksum). large_wal/ is added under this same target
# once its own plan lands (MYDB_WAL_IMPLEMENTATION.md §10).
target_include_directories(wal PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/include
)

# common_utils only — not storage_engine. WAL manages its own files and
# doesn't need BufferPool/BTree/DiskManager/RelationDef; pulling in all of
# storage_engine just for common.h/file_header.h/checksum.h would be an
# artificial coupling the common_utils extraction exists to avoid.
target_link_libraries(wal PUBLIC
    common_utils
)

target_compile_options(wal PRIVATE -Wall -Wextra -Wpedantic)
