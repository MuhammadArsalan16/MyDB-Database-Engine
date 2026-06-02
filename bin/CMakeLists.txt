
# Binary entry points.  These are thin mains over the module libraries:
#   mydbd  — engine + server-side logic (listener/protocol/auth/...)
#   mydb   — socket + REPL only, no engine inside
#
# The legacy embedded `mydb` (init + in-process REPL) has been retired: the
# daemon now owns bootstrap (`mydbd init`) and clients reach the engine only
# over the socket (`mydb connect`).

# mydbd — the daemon.  Links the engine and the server library.
add_executable(mydbd mydbd.c)
target_link_libraries(mydbd PRIVATE engine mydb_server)
target_compile_options(mydbd PRIVATE -Wall -Wextra -Wpedantic)

# mydb — the light client.  Links only the client library (which pulls in the
# shared protocol + crypto) and readline.  NO engine.
add_executable(mydb mydb.c)
target_link_libraries(mydb PRIVATE mydb_client readline)
target_compile_options(mydb PRIVATE -Wall -Wextra -Wpedantic)
