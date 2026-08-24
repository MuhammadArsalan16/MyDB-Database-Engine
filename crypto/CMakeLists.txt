# crypto — tiny SHA-256 + salt utilities, no external deps.
#
# Lifted out of engine/ when a second consumer arose: the network client
# (client/) must compute SHA-256(salt || password) with no engine inside it.
# crypto.h itself pulls in only <stddef.h>/<stdint.h>; crypto.c needs common.h
# purely for the MYDB_OK / MYDB_ERR return-code macros. common.h moved to
# common_utils/ in WAL Phase 1 (it's system-wide, not storage-specific), so
# crypto links that directly instead of reaching into storage_engine/include.
add_library(crypto STATIC
    src/crypto.c
)

target_include_directories(crypto PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/include
)
target_link_libraries(crypto PRIVATE
    common_utils   # common.h: MYDB_OK / MYDB_ERR
)

target_compile_options(crypto PRIVATE -Wall -Wextra -Wpedantic)
