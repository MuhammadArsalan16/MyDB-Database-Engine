/* mydbd — the MyDB daemon binary.
 *
 *   mydbd init -u <username>   first-run engine bootstrap (interactive)
 *   mydbd                      open the engine and serve clients
 *
 * The engine lives entirely inside this binary; clients (mydb) reach it only
 * over the Unix socket.  Engine root resolves from $MYDB_HOME, defaulting to
 * ~/.mydb/.
 *
 * getpass(3) needs _DEFAULT_SOURCE on glibc — define before any include. */
#define _DEFAULT_SOURCE

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "common.h"
#include "engine.h"
#include "server.h"

static const char *resolve_root_dir(char *buf, size_t cap)
{
    const char *env = getenv("MYDB_HOME");
    if (env && env[0] != '\0') {
        if (strlen(env) >= cap) return NULL;
        strncpy(buf, env, cap - 1);
        buf[cap - 1] = '\0';
        return buf;
    }
    const char *home = getenv("HOME");
    if (!home || home[0] == '\0') return NULL;
    int n = snprintf(buf, cap, "%s/.mydb", home);
    if (n < 0 || (size_t)n >= cap) return NULL;
    return buf;
}

static void print_usage(void)
{
    fprintf(stderr,
        "Usage:\n"
        "  mydbd init -u <username>   bootstrap a new MyDB engine\n"
        "  mydbd                      run the MyDB server daemon\n"
        "\n"
        "Engine root resolves from $MYDB_HOME (default ~/.mydb).\n");
}

/* `mydbd init -u <username>` — interactive bootstrap (no parser). */
static int run_init(int argc, char **argv)
{
    /* Expected: argv = { "init", "-u", "<username>" } */
    if (argc != 3 || strcmp(argv[1], "-u") != 0) {
        print_usage();
        return 1;
    }
    const char *username = argv[2];
    if (username[0] == '\0') {
        fprintf(stderr, "mydbd: username may not be empty\n");
        return 1;
    }

    char root_dir[256];
    if (!resolve_root_dir(root_dir, sizeof(root_dir))) {
        fprintf(stderr, "mydbd: cannot resolve engine root "
                        "(set $MYDB_HOME or $HOME)\n");
        return 1;
    }

    char *p1 = getpass("Password: ");
    if (!p1 || p1[0] == '\0') {
        fprintf(stderr, "mydbd: password may not be empty\n");
        return 1;
    }
    char password[128];
    strncpy(password, p1, sizeof(password) - 1);
    password[sizeof(password) - 1] = '\0';

    char *p2 = getpass("Confirm:  ");
    if (!p2 || strcmp(password, p2) != 0) {
        fprintf(stderr, "mydbd: passwords did not match\n");
        return 1;
    }

    int rc = engine_bootstrap(root_dir, username, password);
    memset(password, 0, sizeof(password));
    if (rc != MYDB_OK) {
        fprintf(stderr, "mydbd: bootstrap failed (code=%d). "
                        "Is %s already initialised?\n", rc, root_dir);
        return 1;
    }
    printf("Initialised MyDB engine at %s\n", root_dir);
    printf("Created user '%s' (root partition).\n", username);
    return 0;
}

/* `mydbd` — open the engine and run the daemon. */
static int run_daemon(void)
{
    char root_dir[256];
    if (!resolve_root_dir(root_dir, sizeof(root_dir))) {
        fprintf(stderr, "mydbd: cannot resolve engine root "
                        "(set $MYDB_HOME or $HOME)\n");
        return 1;
    }

    EngineState eng;
    int rc = engine_init(root_dir, &eng);
    if (rc != MYDB_OK) {
        fprintf(stderr, "mydbd: engine_init failed (rc=%d). "
                        "Run `mydbd init` first?\n", rc);
        return 1;
    }

    Server srv;
    if (server_init(&srv, &eng) != 0) {
        fprintf(stderr, "mydbd: failed to bind socket "
                        "(is another server running?)\n");
        engine_close(&eng);
        return 1;
    }

    server_run(&srv);          /* blocks until SIGINT / SIGTERM */

    server_shutdown(&srv);
    engine_close(&eng);
    return 0;
}

int main(int argc, char **argv)
{
    if (argc >= 2 && strcmp(argv[1], "init") == 0)
        return run_init(argc - 1, argv + 1);
    if (argc == 1)
        return run_daemon();
    print_usage();
    return 1;
}
