#ifndef MYDB_DISPATCH_H
#define MYDB_DISPATCH_H

#include <stdint.h>
#include "session.h"

#ifdef __cplusplus
extern "C" {
#endif

/*
 * dispatch.h — query routing (server Layer 5).
 *
 * The simplest layer: it never parses SQL.  An authenticated PKT_QUERY is
 * handed verbatim to engine_execute_sql() for the session's conn_id, and
 * whatever string the engine produces (result rows, DML status, or error)
 * is shipped back as a single PKT_RESPONSE.  The client prints it directly.
 */

struct EngineState;

/* Execute the query payload for this session and reply with PKT_RESPONSE.
 * Guards SESSION_READY, marks SESSION_BUSY for the duration, then restores
 * SESSION_READY.  Returns 0 on success, -1 if the session should be dropped
 * (wrong state or send failure). */
int dispatch_query(Session *s, struct EngineState *eng,
                   const void *payload, uint32_t len);

#ifdef __cplusplus
}
#endif

#endif /* MYDB_DISPATCH_H */
