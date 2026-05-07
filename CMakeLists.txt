cmake_minimum_required(VERSION 3.20)
project(MyDB LANGUAGES C CXX)

set(CMAKE_C_STANDARD 11)
set(CMAKE_C_STANDARD_REQUIRED ON)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Export compile commands so clangd/IDE can find include paths
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# Storage engine — pure C11 static library
add_subdirectory(storage_engine)
# System schema (users + privileges) — engine-level metadata, lives outside
# any partition. Depends on storage_engine for common.h / file_header / checksum.
add_subdirectory(system_schema)
# Engine orchestrator — owns __database.mydb, EngineState, login/use_schema
# flow, and the bootstrap path. Pulls in crypto helpers (SHA-256 + salt).
add_subdirectory(engine)
# `mydb` executable (admin subcommands like `mydb init`).
add_subdirectory(bin)
add_subdirectory(query_parser)

# Tests
enable_testing()
add_subdirectory(tests)
