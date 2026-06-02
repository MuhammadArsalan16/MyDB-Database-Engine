#ifndef CONNECTION_H
#define CONNECTION_H

#include <stdint.h>
#include "common.h"

#ifdef __cplusplus
extern "C" {
#endif

/*
 * connection.h — master connection pool, owned by EngineState.
 *
 * The global ConnectionPool holds every active session.  Each Connection
 * carries the per-session auth and schema state that was previously
 * scattered across EngineState fields.
 *
 * Multi-session: the network server (mydb-server) accepts up to
 * MAX_CONNECTIONS clients, each occupying one slot.  Execution is still
 * single-threaded — the server's poll() loop runs one statement at a time
 * (SESSION_BUSY) — so no mutex is needed yet; that arrives with concurrency.
 *
 * PartitionCtx does NOT track connections: it is connection-agnostic and
 * shared by every connection on the same partition.  The engine owns
 * connection→partition routing and the PartitionCtx.n_refs count.
 */

#define MAX_CONNECTIONS  32   /* network sessions; bump with the server */

typedef struct Connection {
    uint32_t  connection_id;
    uint32_t  partition_id;             /* owning PartitionCtx */
    uint32_t  user_id;
    char      username[MAX_USERNAME];
    uint8_t   logged_in;
    uint8_t   partition_open;           /* 0 = analyst (no owned partition) */
    uint8_t   schema_active;
    char      current_schema_name[32];
} Connection;

typedef struct {
    Connection  conns[MAX_CONNECTIONS];
    int         n_active;               /* number of live connections */
} ConnectionPool;

#ifdef __cplusplus
}
#endif

#endif /* CONNECTION_H */
