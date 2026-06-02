#include "dispatch.h"
#include "protocol.h"
#include "engine.h"      /* engine_execute_sql */

#include <string.h>

int dispatch_query(Session *s, struct EngineState *eng,
                   const void *payload, uint32_t len)
{
    if (!s || !eng) return -1;
    if (s->state != SESSION_READY) return -1;
    if (len > PROTO_MAX_PAYLOAD)   return -1;

    /* NUL-terminate the SQL (the wire payload is not guaranteed terminated). */
    char sql[PROTO_MAX_PAYLOAD + 1];
    memcpy(sql, payload, len);
    sql[len] = '\0';

    s->state = SESSION_BUSY;

    /* The engine formats both success output and error messages into the
     * result buffer; the client prints whatever comes back verbatim, so we
     * send PKT_RESPONSE regardless of the return code. */
    char result[PROTO_MAX_PAYLOAD];
    result[0] = '\0';
    engine_execute_sql(eng, s->conn_id, sql, result, sizeof(result));

    s->state = SESSION_READY;

    return session_send(s, PKT_RESPONSE, result, (uint32_t)strlen(result));
}
