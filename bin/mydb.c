/* mydb — the light MyDB client binary.
 *
 *   mydb connect -u <username> [-S|--socket <path>]
 *
 * Connects to a running mydbd daemon over the Unix socket, authenticates
 * with a challenge-response handshake, and runs an interactive SQL REPL.
 * Contains no engine or storage code — it only speaks the wire protocol.
 *
 * The socket path is resolved by proto_socket_path: $MYDB_SOCKET first, then
 * $MYDB_HOME/mydb.sock, then /run/mydb/mydb.sock. The optional --socket flag
 * sets $MYDB_SOCKET for this run, so it overrides the environment — handy for
 * choosing the prod (/run/mydb) vs a dev (~/.mydb) server without juggling
 * env vars. */

#include <stdio.h>
#include <stdlib.h>   /* setenv */
#include <string.h>

#include "client_conn.h"
#include "client_repl.h"

static void print_usage(void)
{
    fprintf(stderr,
        "Usage:\n"
        "  mydb connect -u <username> [-S|--socket <path>]\n"
        "      connect to the MyDB server\n"
        "      -S, --socket <path>  socket to connect to; overrides\n"
        "                           $MYDB_SOCKET / $MYDB_HOME\n"
        "                           prod: /run/mydb/mydb.sock\n"
        "                           dev : ~/.mydb/mydb.sock\n");
}

/* `mydb connect -u <username> [-S|--socket <path>]` — open a session and run
 * the REPL. Flags may appear in any order; -u is required. */
static int run_connect(int argc, char **argv)
{
    /* argv[0] == "connect"; flags follow. */
    const char *username    = NULL;
    const char *socket_path = NULL;

    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "-u") == 0 && i + 1 < argc) {
            username = argv[++i];
        } else if ((strcmp(argv[i], "-S") == 0 ||
                    strcmp(argv[i], "--socket") == 0) && i + 1 < argc) {
            socket_path = argv[++i];
        } else {
            print_usage();
            return 1;
        }
    }

    if (!username || username[0] == '\0') {
        print_usage();
        return 1;
    }

    /* An explicit socket overrides the environment: set $MYDB_SOCKET, which
     * proto_socket_path consults first. Done before opening the connection so
     * client_conn_open resolves the path through the same code as the daemon. */
    if (socket_path && socket_path[0] != '\0') {
        if (setenv("MYDB_SOCKET", socket_path, 1) != 0) {
            fprintf(stderr, "mydb: failed to set socket path\n");
            return 1;
        }
    }

    ClientConn conn;
    if (client_conn_open(&conn, username) != 0)
        return 1;   /* client_conn_open already printed the reason */

    printf("Connected to MyDB as %s.\n", username);
    client_repl_run(&conn);
    client_conn_close(&conn);
    printf("Bye.\n");
    return 0;
}

int main(int argc, char **argv)
{
    if (argc >= 2 && strcmp(argv[1], "connect") == 0)
        return run_connect(argc - 1, argv + 1);
    print_usage();
    return 1;
}
