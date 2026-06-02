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
 * Phase 1: single-session, MAX_CONNECTIONS = 1.
 * Future: multi-session — increase capacity and add a mutex.
 *
 * PartitionCtx does NOT copy these structs; it holds Connection* pointers
 * into this pool (see SubConnPool in partition_ctx.h).
 */

#define MAX_CONNECTIONS  1   /* Phase 1 */

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
