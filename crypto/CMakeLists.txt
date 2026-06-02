# crypto — tiny SHA-256 + salt utilities, no external deps.
#
# Lifted out of engine/ when a second consumer arose: the network client
# (client/) must compute SHA-256(salt || password) with no engine inside it.
# crypto.h itself pulls in only <stddef.h>/<stdint.h>; crypto.c needs common.h
# purely for the MYDB_OK / MYDB_ERR return-code macros, so storage_engine's
# include path is PRIVATE (compile-time only, no link dependency on storage).
add_library(crypto STATIC
    src/crypto.c
)

target_include_directories(crypto PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/include
)
target_include_directories(crypto PRIVATE
    ${CMAKE_SOURCE_DIR}/storage_engine/include   # common.h: MYDB_OK / MYDB_ERR
)

target_compile_options(crypto PRIVATE -Wall -Wextra -Wpedantic)
