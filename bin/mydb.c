/* mydb — pre-engine CLI binary.
 *
 *   mydb init  -u <username>     first-run engine bootstrap
 *   mydb start -u <username>     login + interactive REPL
 *
 * Both subcommands prompt for the password interactively; the
 * password never appears on argv.
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
        "Usage:\n"
        "  mydb init  -u <username>   bootstrap a new MYDB engine\n"
        "  mydb start -u <username>   login and open the MYDB shell\n"
        "\n"
        "Engine root resolves from $MYDB_HOME (default ~/.mydb).\n");
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

/* ------------------------------------------------------------------ */
/*  REPL                                                                 */
/*                                                                       */
/*  fgets-based read loop. Buffers lines until ';' is seen, then sends  */
/*  the whole buffer to engine_execute_sql() and prints the result.    */
/*  Engine is the single back-end door — bin/ never touches the parser  */
/*  or execution engine directly.                                       */
/* ------------------------------------------------------------------ */

#define REPL_LINE_CAP   1024
#define REPL_QUERY_CAP  8192
#define REPL_RESULT_CAP 4096

static int is_blank(const char *s)
{
    while (*s) {
        if (*s != ' ' && *s != '\t' && *s != '\n' && *s != '\r') return 0;
        s++;
    }
    return 1;
}

/* Strip trailing whitespace and check for a literal exit command
 * (with or without a terminating semicolon). */
static int is_exit_command(const char *buf)
{
    while (*buf == ' ' || *buf == '\t' || *buf == '\n' || *buf == '\r') buf++;
    if (strncmp(buf, "exit", 4) != 0 && strncmp(buf, ".exit", 5) != 0)
        return 0;
    /* match `exit`, `exit;`, `.exit`, `.exit;` (plus trailing whitespace) */
    const char *p = (buf[0] == '.') ? buf + 5 : buf + 4;
    while (*p == ' ' || *p == '\t' || *p == '\n' || *p == '\r' || *p == ';') p++;
    return *p == '\0';
}

static void run_repl(EngineState *eng)
{
    char query[REPL_QUERY_CAP];
    char line[REPL_LINE_CAP];
    char result[REPL_RESULT_CAP];
    size_t qlen = 0;

    printf("Type `exit` or Ctrl-D to quit.\n");
    query[0] = '\0';

    for (;;) {
        fputs(qlen == 0 ? "mydb> " : "  ... ", stdout);
        fflush(stdout);

        if (!fgets(line, sizeof(line), stdin)) {
            putchar('\n');
            break;   /* EOF — Ctrl-D */
        }

        /* skip empty lines at the start of a fresh query */
        if (qlen == 0 && is_blank(line)) continue;
        if (qlen == 0 && is_exit_command(line)) break;

        size_t llen = strlen(line);
        if (qlen + llen + 1 >= sizeof(query)) {
            fprintf(stderr, "mydb: query too long, discarded\n");
            qlen = 0; query[0] = '\0';
            continue;
        }
        memcpy(query + qlen, line, llen + 1);
        qlen += llen;

        /* submit when the buffer ends with ';' (after stripping whitespace) */
        const char *end = query + qlen;
        while (end > query && (end[-1] == '\n' || end[-1] == '\r' ||
                               end[-1] == ' ' || end[-1] == '\t')) end--;
        if (end == query || end[-1] != ';') continue;

        int rc = engine_execute_sql(eng, query, result, sizeof(result));
        if (rc == MYDB_OK) printf("%s\n", result);
        else               fprintf(stderr, "error: %s (rc=%d)\n", result, rc);

        qlen = 0; query[0] = '\0';
    }
}

static int run_start(int argc, char **argv)
{
    /* Expected: argv = { "start", "-u", "<username>" } */
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

    char *pw = getpass("Password: ");
    if (!pw || pw[0] == '\0') {
        fprintf(stderr, "mydb: password may not be empty\n");
        return 1;
    }
    /* Copy out before getpass clobbers its static buffer, then wipe. */
    char password[128];
    strncpy(password, pw, sizeof(password) - 1);
    password[sizeof(password) - 1] = '\0';
    memset(pw, 0, strlen(pw));

    EngineState eng;
    int rc = engine_start(root_dir, username, password, &eng);
    /* Wipe our copy regardless of outcome. */
    memset(password, 0, sizeof(password));
    if (rc != MYDB_OK) {
        if (rc == MYDB_ERR_NOT_FOUND)
            fprintf(stderr, "mydb: unknown user '%s'\n", username);
        else if (rc == MYDB_ERR_PERM)
            fprintf(stderr, "mydb: authentication failed\n");
        else
            fprintf(stderr, "mydb: engine_start failed (rc=%d). "
                            "Is %s initialised? Run `mydb init` first.\n",
                    rc, root_dir);
        return 1;
    }

    printf("Logged in as %s.\n", username);
    run_repl(&eng);
    engine_close(&eng);
    printf("Goodbye.\n");
    return 0;
}

int main(int argc, char **argv)
{
    if (argc < 2) { print_usage(); return 1; }
    if (strcmp(argv[1], "init")  == 0) return run_init (argc - 1, argv + 1);
    if (strcmp(argv[1], "start") == 0) return run_start(argc - 1, argv + 1);
    print_usage();
    return 1;
}
