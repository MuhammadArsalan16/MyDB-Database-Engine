# `mydb` — pre-engine CLI subcommand binary. Currently supports only:
#   mydb init -u <username>
# which calls engine_bootstrap() after prompting for a password.
add_executable(mydb mydb.c)
target_link_libraries(mydb PRIVATE engine)
target_compile_options(mydb PRIVATE -Wall -Wextra -Wpedantic)
