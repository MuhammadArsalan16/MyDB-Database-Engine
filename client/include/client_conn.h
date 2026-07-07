#ifndef MYDB_CLIENT_CONN_H
#define MYDB_CLIENT_CONN_H

#include <stdint.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

/*
 * client_conn.h — client side of the connection (server Layer 7).
 *
 * No engine, no storage: just a socket, the shared protocol, and the
 * shared crypto used to answer the challenge.  ClientConn holds the
 * connection's network state (fd + per-direction sequence counters).
 */

typedef struct {
    int      fd;          /* socket to the server, -1 when closed */
    uint32_t send_seq;    /* outgoing sequence counter            */
    uint32_t recv_seq;    /* incoming sequence counter            */
} ClientConn;

/* Connect to the server's Unix socket and run the challenge-response
 * handshake for `username` (prompts for the password with getpass).
 * Returns 0 on success (authenticated), -1 on connect / auth failure. */
int  client_conn_open(ClientConn *c, const char *username);

/* Same as client_conn_open, but connects over TCP to host:port instead of
 * the Unix socket (e.g. mydbd's default TCP listener on port 4442). */
int  client_conn_open_tcp(ClientConn *c, const char *host, uint16_t port,
                          const char *username);

/* Send one SQL string (PKT_QUERY) and read the server's PKT_RESPONSE into
 * `out` (NUL-terminated, truncated to cap-1).  Returns 0 on success, -1 on
 * I/O / protocol error. */
int  client_conn_query(ClientConn *c, const char *sql, char *out, size_t cap);

/* Send PKT_QUIT (best effort) and close the socket. */
void client_conn_close(ClientConn *c);

#ifdef __cplusplus
}
#endif

#endif /* MYDB_CLIENT_CONN_H */
