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
add_subdirectory(query_parser)

# Tests
enable_testing()
add_subdirectory(tests)
