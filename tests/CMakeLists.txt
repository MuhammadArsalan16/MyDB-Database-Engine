# Storage-engine modules whose tests link only `storage_engine`.
set(STORAGE_TEST_MODULES
    disk_manager page buffer_pool btree transaction
    file_header partition schema_file relation_def)

foreach(mod ${STORAGE_TEST_MODULES})
    add_executable(test_${mod} storage_engine/test_${mod}.c)
    target_link_libraries(test_${mod} PRIVATE storage_engine)
    add_test(NAME test_${mod} COMMAND test_${mod})
endforeach()

# system_schema tests link the system_schema library (transitively
# pulls storage_engine).
add_executable(test_system_schema system_schema/test_system_schema.c)
target_link_libraries(test_system_schema PRIVATE system_schema)
add_test(NAME test_system_schema COMMAND test_system_schema)

# engine tests link `engine` (transitively pulls storage_engine +
# system_schema). test_storage joined this set in phase 9 — it now
# bootstraps an engine to drive the session-aware storage API.
set(ENGINE_TEST_MODULES database_file crypto engine storage)
foreach(mod ${ENGINE_TEST_MODULES})
    add_executable(test_${mod} engine/test_${mod}.c)
    target_link_libraries(test_${mod} PRIVATE engine)
    add_test(NAME test_${mod} COMMAND test_${mod})
endforeach()

# end-to-end lifecycle scenarios — exercises the full stack top-to-bottom
# including cross-session persistence, schema switch flush, analyst access,
# FK lifecycle, transactions, and drop+recreate.
add_executable(test_integration integration/test_integration.c)
target_link_libraries(test_integration PRIVATE engine)
add_test(NAME test_integration COMMAND test_integration)

# query_parser tests — links query_parser (headers exposed via its PUBLIC
# include dirs, so #include "Parser.hpp" / "Lexer.hpp" resolve correctly).
add_executable(test_parser query_parser/test_parser.cpp)
target_link_libraries(test_parser PRIVATE query_parser)
add_test(NAME test_parser COMMAND test_parser)
