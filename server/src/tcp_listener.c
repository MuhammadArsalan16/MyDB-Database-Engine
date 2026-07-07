#include "tcp_listener.h"

#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <errno.h>

/* Resolve bind address/port, highest priority first:
 *   1. $MYDB_TCP_BIND_ADDR / $MYDB_TCP_PORT — explicit override.
 *   2. Defaults: 0.0.0.0 : 4442.
 * Mirrors proto_socket_path's env-then-default shape, but stays local to
 * this file since it's TCP-only — mydb_proto (shared with the client) has
 * no AF_INET code. */
static void resolve_tcp_config(char *addr_out, size_t addr_cap, uint16_t *port_out)
{
    const char *addr_env = getenv("MYDB_TCP_BIND_ADDR");
    const char *port_env = getenv("MYDB_TCP_PORT");

    snprintf(addr_out, addr_cap, "%s",
             (addr_env && addr_env[0] != '\0') ? addr_env : "0.0.0.0");

    long port = TCP_LISTENER_DEFAULT_PORT;
    if (port_env && port_env[0] != '\0') {
        char *end;
        long parsed = strtol(port_env, &end, 10);
        if (*end == '\0' && parsed > 0 && parsed <= 65535)
            port = parsed;
    }
    *port_out = (uint16_t)port;
}

int tcp_listener_init(TcpListener *l)
{
    if (!l) return -1;
    l->fd = -1;

    resolve_tcp_config(l->bind_addr, sizeof(l->bind_addr), &l->port);

    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return -1;

    /* Let the daemon rebind the port immediately after a restart during dev,
     * instead of waiting out TIME_WAIT. */
    int one = 1;
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port   = htons(l->port);
    if (inet_pton(AF_INET, l->bind_addr, &addr.sin_addr) != 1) {
        close(fd);
        return -1;
    }

    if (bind(fd, (struct sockaddr *)&addr, sizeof(addr)) != 0) {
        close(fd);
        return -1;
    }
    if (listen(fd, TCP_LISTENER_BACKLOG) != 0) {
        close(fd);
        return -1;
    }

    l->fd = fd;
    return 0;
}

int tcp_listener_accept(TcpListener *l)
{
    if (!l || l->fd < 0) return -1;
    int cfd;
    do {
        cfd = accept(l->fd, NULL, NULL);
    } while (cfd < 0 && errno == EINTR);
    return cfd;
}

void tcp_listener_close(TcpListener *l)
{
    if (!l) return;
    if (l->fd >= 0) {
        close(l->fd);
        l->fd = -1;
    }
}
