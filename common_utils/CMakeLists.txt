add_library(common_utils STATIC
    src/file_header.c
    src/checksum.c
)

# common.h / file_header.h / checksum.h are genuinely system-wide (every
# module in the repo depends on them) rather than storage-specific, so they
# live here instead of under storage_engine/. Every consumer still reaches
# these headers via a bare #include "common.h" resolved through this
# target's PUBLIC include dir, transitively, exactly as before the move.
target_include_directories(common_utils PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/include
)

target_compile_options(common_utils PRIVATE -Wall -Wextra -Wpedantic)
