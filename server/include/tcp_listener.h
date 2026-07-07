#ifndef MYDB_TCP_LISTENER_H
#define MYDB_TCP_LISTENER_H

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/*
 * tcp_listener.h — TCP socket listener (server Layer 1, TCP variant).
 *
 * Runs alongside the Unix domain listener (listener.h), not instead of it —
 * mydbd always accepts both. Bind address/port are resolved from
 * MYDB_TCP_BIND_ADDR / MYDB_TCP_PORT (defaults 0.0.0.0:4442). Session,
 * auth, and protocol code are transport-agnostic (they only see a raw fd),
 * so this listener only needs to produce accepted client fds.
 */

#define TCP_LISTENER_BACKLOG   8
#define TCP_LISTENER_DEFAULT_PORT 4442

typedef struct {
    int      fd;             /* listening socket fd, -1 when closed */
    char     bind_addr[64];  /* resolved bind address, for the startup log */
    uint16_t port;
} TcpListener;

/* Resolve bind addr/port from env, then socket() + bind() + listen().
 * Returns 0 on success, -1 on error. */
int  tcp_listener_init(TcpListener *l);

/* Block until a client connects; returns the new client fd, or -1. */
int  tcp_listener_accept(TcpListener *l);

/* Close the listening fd. No filesystem entry to remove (unlike the Unix
 * socket listener). */
void tcp_listener_close(TcpListener *l);

#ifdef __cplusplus
}
#endif

#endif /* MYDB_TCP_LISTENER_H */
