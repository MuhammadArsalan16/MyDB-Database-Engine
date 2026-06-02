/* mydb — the light MyDB client binary.
 *
 *   mydb connect -u <username>
 *
 * Connects to a running mydbd daemon over the Unix socket, authenticates
 * with a challenge-response handshake, and runs an interactive SQL REPL.
 * Contains no engine or storage code — it only speaks the wire protocol.
 *
 * The socket path resolves from $MYDB_HOME (default /run/mydb), matching
 * the daemon. */

#include <stdio.h>
#include <string.h>

#include "client_conn.h"
#include "client_repl.h"

static void print_usage(void)
{
    fprintf(stderr,
        "Usage:\n"
        "  mydb connect -u <username>   connect to the MyDB server\n");
}

/* `mydb connect -u <username>` — open a session and run the REPL. */
static int run_connect(int argc, char **argv)
{
    /* Expected: argv = { "connect", "-u", "<username>" } */
    if (argc != 3 || strcmp(argv[1], "-u") != 0 || argv[2][0] == '\0') {
        print_usage();
        return 1;
    }
    const char *username = argv[2];

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
