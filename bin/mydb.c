/* mydb — pre-engine CLI binary.
 *
 *   mydb init -u <username>
 *
 * Prompts for the password interactively (twice, for confirmation)
 * and calls engine_bootstrap(). The password never appears on argv.
 *
 * Engine root resolves from $MYDB_HOME, defaulting to ~/.mydb/. */

/* getpass(3) lives in <unistd.h> but is gated behind _DEFAULT_SOURCE
 * (or the legacy _BSD_SOURCE) on glibc. Define it before any include. */
#define _DEFAULT_SOURCE

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "common.h"
#include "engine.h"

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
        "Usage: mydb init -u <username>\n"
        "\n"
        "Initialises a new MyDB engine at $MYDB_HOME (default ~/.mydb).\n"
        "Prompts for the password.\n");
}

static int run_init(int argc, char **argv)
{
    /* Expected: argv = { "init", "-u", "<username>" } */
    if (argc != 3 || strcmp(argv[1], "-u") != 0) {
        print_usage();
        return 1;
    }
    const char *username = argv[2];
    if (username[0] == '\0') {
        fprintf(stderr, "mydb: username may not be empty\n");
        return 1;
    }

    char root_dir[256];
    if (!resolve_root_dir(root_dir, sizeof(root_dir))) {
        fprintf(stderr, "mydb: cannot resolve engine root "
                        "(set $MYDB_HOME or $HOME)\n");
        return 1;
    }

    /* getpass(3) is deprecated but ubiquitous on Linux and avoids
     * having to disable echo via termios manually. */
    char *p1 = getpass("Password: ");
    if (!p1 || p1[0] == '\0') {
        fprintf(stderr, "mydb: password may not be empty\n");
        return 1;
    }
    /* Copy out before getpass clobbers its static buffer on the next call. */
    char password[128];
    strncpy(password, p1, sizeof(password) - 1);
    password[sizeof(password) - 1] = '\0';

    char *p2 = getpass("Confirm:  ");
    if (!p2 || strcmp(password, p2) != 0) {
        fprintf(stderr, "mydb: passwords did not match\n");
        return 1;
    }

    int rc = engine_bootstrap(root_dir, username, password);
    if (rc != MYDB_OK) {
        fprintf(stderr, "mydb: bootstrap failed (code=%d). "
                        "Is %s already initialised?\n", rc, root_dir);
        return 1;
    }
    printf("Initialised MyDB engine at %s\n", root_dir);
    printf("Created user '%s' (root partition).\n", username);
    return 0;
}

int main(int argc, char **argv)
{
    if (argc < 2) { print_usage(); return 1; }
    if (strcmp(argv[1], "init") == 0) return run_init(argc - 1, argv + 1);
    print_usage();
    return 1;
}
