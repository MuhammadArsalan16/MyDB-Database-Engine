#ifndef MYDB_LISTENER_H
#define MYDB_LISTENER_H

#ifdef __cplusplus
extern "C" {
#endif

/*
 * listener.h — Unix domain socket listener (server Layer 1).
 *
 * Creates the socket, binds it to the resolved socket path
 * (proto_socket_path), and accepts client connections.  The listener fd
 * lives for the whole server lifetime; each accept() yields a fresh
 * per-client fd.
 */

#define LISTENER_BACKLOG  8

typedef struct {
    int  fd;                 /* listening socket fd, -1 when closed */
    char socket_path[256];   /* bound path, for unlink on close     */
} Listener;

/* Create + bind + listen.  Removes any stale socket file first.
 * Returns 0 on success, -1 on error. */
int  listener_init(Listener *l);

/* Block until a client connects; returns the new client fd, or -1. */
int  listener_accept(Listener *l);

/* Close the listening fd and unlink the socket file. */
void listener_close(Listener *l);

#ifdef __cplusplus
}
#endif

#endif /* MYDB_LISTENER_H */
