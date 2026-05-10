cmake_minimum_required(VERSION 3.20)
project(MyDB LANGUAGES C CXX)

if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Debug CACHE STRING "Build type" FORCE)
endif()

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
# Query parser — Lexer/Parser plus parser_api C bridge. Built before
# engine so engine can link it.
add_subdirectory(query_parser)
# Execution engine — AST walker. Built after query_parser, before engine.
add_subdirectory(execution_engine)
# Engine orchestrator — owns __database.mydb, EngineState, login/use_schema
# flow, and the bootstrap path. Pulls in crypto helpers (SHA-256 + salt).
# Engine links query_parser + execution_engine to drive engine_execute_sql.
add_subdirectory(engine)
# `mydb` executable (admin subcommands like `mydb init`).
add_subdirectory(bin)

# Tests
enable_testing()
add_subdirectory(tests)
