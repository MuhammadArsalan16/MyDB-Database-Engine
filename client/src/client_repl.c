#include "client_repl.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <readline/readline.h>
#include <readline/history.h>

#define REPL_QUERY_CAP   8192
#define REPL_RESULT_CAP  65536

static int is_blank(const char *s)
{
    while (*s) {
        if (*s != ' ' && *s != '\t' && *s != '\n' && *s != '\r') return 0;
        s++;
    }
    return 1;
}

/* Match a literal exit command (`exit` / `quit`, optional `.` and `;`). */
static int is_exit_command(const char *buf)
{
    while (*buf == ' ' || *buf == '\t' || *buf == '\n' || *buf == '\r') buf++;
    const char *p = NULL;
    if      (strncmp(buf, ".exit", 5) == 0) p = buf + 5;
    else if (strncmp(buf, "exit",  4) == 0) p = buf + 4;
    else if (strncmp(buf, ".quit", 5) == 0) p = buf + 5;
    else if (strncmp(buf, "quit",  4) == 0) p = buf + 4;
    else return 0;
    while (*p == ' ' || *p == '\t' || *p == '\n' || *p == '\r' || *p == ';') p++;
    return *p == '\0';
}

void client_repl_run(ClientConn *c)
{
    char   query[REPL_QUERY_CAP];
    char   result[REPL_RESULT_CAP];
    size_t qlen = 0;

    printf("Type `exit` or Ctrl-D to quit.\n");
    query[0] = '\0';

    for (;;) {
        const char *prompt = (qlen == 0) ? "mydb> " : "  ... ";
        char *line = readline(prompt);

        if (!line) { putchar('\n'); break; }              /* Ctrl-D */

        if (qlen == 0 && strcmp(line, "clear") == 0) {
            printf("\033[2J\033[H");
            fflush(stdout);
            free(line);
            continue;
        }
        if (qlen == 0 && is_blank(line)) { free(line); continue; }
        if (qlen == 0 && is_exit_command(line)) { free(line); break; }

        size_t llen = strlen(line);
        if (qlen + llen + 2 >= sizeof(query)) {
            fprintf(stderr, "mydb: query too long, discarded\n");
            qlen = 0; query[0] = '\0';
            free(line);
            continue;
        }
        memcpy(query + qlen, line, llen);
        qlen += llen;
        query[qlen++] = '\n';
        query[qlen]   = '\0';
        free(line);

        /* Submit when the buffer ends with ';' (ignoring trailing space). */
        const char *end = query + qlen;
        while (end > query && (end[-1] == '\n' || end[-1] == '\r' ||
                               end[-1] == ' '  || end[-1] == '\t')) end--;
        if (end == query || end[-1] != ';') continue;

        /* Add the completed statement to history (trimmed). */
        char hist[REPL_QUERY_CAP];
        strncpy(hist, query, sizeof(hist) - 1);
        hist[sizeof(hist) - 1] = '\0';
        char *h = hist + strlen(hist) - 1;
        while (h >= hist && (*h == '\n' || *h == '\r' ||
                             *h == ' '  || *h == '\t')) *h-- = '\0';
        if (hist[0] != '\0') add_history(hist);

        if (client_conn_query(c, query, result, sizeof(result)) != 0) {
            fprintf(stderr, "mydb: connection lost\n");
            break;
        }
        printf("%s\n", result);

        qlen = 0; query[0] = '\0';
    }
}
