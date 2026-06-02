add_library(planner STATIC
    src/planner.c
)

target_include_directories(planner PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/include
    # planner.h includes stats.h + schema_file.h, both in partition_manager (v3).
    # The planner is engine-agnostic: it no longer includes or links engine.
    ${CMAKE_SOURCE_DIR}/system_schema/include
    ${CMAKE_SOURCE_DIR}/partition_manager/include
)

target_link_libraries(planner PUBLIC storage_engine partition_manager)

target_compile_options(planner PRIVATE -Wall -Wextra -Wpedantic)
