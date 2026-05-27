add_library(planner STATIC
    src/planner.c
)

target_include_directories(planner PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/include
    # planner.c includes engine.h for EngineState fields.
    # Header-only dependency: planner does NOT link engine directly.
    # Symbols are resolved at final-binary link time via the engine library.
    ${CMAKE_SOURCE_DIR}/engine/include
    ${CMAKE_SOURCE_DIR}/system_schema/include
)

target_link_libraries(planner PUBLIC storage_engine)

target_compile_options(planner PRIVATE -Wall -Wextra -Wpedantic)
