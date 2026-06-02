#ifndef MYDB_SESSION_H
#define MYDB_SESSION_H

#include <stdint.h>
#include <stdbool.h>

#include "protocol.h"
#include "crypto.h"     /* MYDB_NONCE_LEN */

#ifdef __cplusplus
extern "C" {
#endif

/*
 * session.h — per-connection network state (server Layer 3).
 *
 * A Session owns ONLY the network side of a connection: its socket fd,
 * sequence counters, lifecycle state, and the in-flight auth nonce.  All
 * database state (user, partition, schema, transaction) lives in the
 * engine, reachable through the single bridge field `conn_id`.
 *
 * Slots are a fixed array — no malloc, no fragmentation.  MAX_SESSIONS
 * should not exceed the engine's MAX_CONNECTIONS; if it does, the extra
 * logins simply fail (pool full) and the session is dropped.
 */

#define MAX_SESSIONS  32

typedef enum {
    SESSION_CONNECTING = 0,   /* accepted; handshake not sent yet      */
    SESSION_AUTHENTICATING,   /* handshake sent; awaiting auth packets */
    SESSION_READY,            /* authenticated; awaiting SQL           */
    SESSION_BUSY,             /* engine executing a query              */
    SESSION_CLOSING           /* QUIT received; cleanup in progress    */
} SessionState;

typedef struct {
    int          client_fd;              /* socket to this client; -1 if free */
    int          conn_id;                /* engine handle; -1 until auth      */
    uint32_t     send_seq;               /* outgoing sequence counter         */
    uint32_t     recv_seq;               /* incoming sequence counter         */
    SessionState state;
    uint8_t      nonce[MYDB_NONCE_LEN];  /* auth challenge; zeroed after use  */
    bool         active;                 /* false = slot is free              */
} Session;

typedef struct {
    Session slots[MAX_SESSIONS];
    int     count;                       /* number of active sessions         */
} SessionManager;

/* Zero all slots. */
void     session_mgr_init(SessionManager *m);

/* Claim a free slot for `client_fd` and initialise it (state CONNECTING,
 * seqs 0, conn_id -1).  Returns the Session*, or NULL if the table is full. */
Session *session_create(SessionManager *m, int client_fd);

/* Close the socket and release the slot.  Does NOT touch the engine — the
 * caller is responsible for engine_logout(conn_id) first if conn_id >= 0. */
void     session_destroy(SessionManager *m, Session *s);

/* Send a packet on this session, advancing its send_seq.  Returns 0/-1. */
int      session_send(Session *s, PacketType type,
                      const void *payload, uint32_t len);

/* Receive a packet on this session, advancing its recv_seq.  Returns 0/-1
 * (a sequence mismatch or EOF yields -1 — the caller drops the session). */
int      session_recv(Session *s, PacketHeader *hdr,
                      void *payload, uint32_t max_len);

#ifdef __cplusplus
}
#endif

#endif /* MYDB_SESSION_H */
