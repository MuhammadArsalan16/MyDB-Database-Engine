#include "listener.h"
#include "protocol.h"   /* proto_socket_path */

#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>
#include <string.h>
#include <stdio.h>
#include <errno.h>

int listener_init(Listener *l)
{
    if (!l) return -1;
    l->fd = -1;

    if (proto_socket_path(l->socket_path, sizeof(l->socket_path)) != 0)
        return -1;

    /* sun_path is a fixed 108-byte field — reject paths that won't fit. */
    if (strlen(l->socket_path) >= sizeof(((struct sockaddr_un *)0)->sun_path))
        return -1;

    int fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (fd < 0) return -1;

    /* Drop any stale socket file left by a previous crash before binding. */
    unlink(l->socket_path);

    struct sockaddr_un addr;
    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, l->socket_path, sizeof(addr.sun_path) - 1);

    if (bind(fd, (struct sockaddr *)&addr, sizeof(addr)) != 0) {
        close(fd);
        return -1;
    }
    if (listen(fd, LISTENER_BACKLOG) != 0) {
        close(fd);
        unlink(l->socket_path);
        return -1;
    }

    l->fd = fd;
    return 0;
}

int listener_accept(Listener *l)
{
    if (!l || l->fd < 0) return -1;
    int cfd;
    do {
        cfd = accept(l->fd, NULL, NULL);
    } while (cfd < 0 && errno == EINTR);
    return cfd;
}

void listener_close(Listener *l)
{
    if (!l) return;
    if (l->fd >= 0) {
        close(l->fd);
        l->fd = -1;
    }
    if (l->socket_path[0] != '\0')
        unlink(l->socket_path);
}
