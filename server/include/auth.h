#ifndef MYDB_AUTH_H
#define MYDB_AUTH_H

#include <stdint.h>
#include "session.h"

#ifdef __cplusplus
extern "C" {
#endif

/*
 * auth.h — challenge-response auth orchestration (server Layer 4).
 *
 * Contains ZERO cryptography: it only moves packets and calls engine_*
 * functions, which own all hashing and verification.  Nonce generation
 * (the one random-bytes call) is the auth handler's job per the design.
 *
 * Wire payloads:
 *   PKT_HANDSHAKE       server→client : version string
 *   PKT_AUTH_INIT       client→server : username bytes (length-delimited)
 *   PKT_AUTH_CHALLENGE  server→client : salt[SALT_LEN] || nonce[MYDB_NONCE_LEN]
 *   PKT_AUTH_RESPONSE   client→server : username '\0' response[SHA256_DIGEST_LEN]
 *   PKT_AUTH_OK / ERR   server→client : empty
 *
 * Each handler returns 0 to keep the session alive, -1 to signal the server
 * to drop it (I/O failure, malformed packet, or failed authentication after
 * PKT_AUTH_ERR has been sent).
 */

struct EngineState;

/* Send PKT_HANDSHAKE and move the session to SESSION_AUTHENTICATING. */
int auth_send_handshake(Session *s);

/* Handle PKT_AUTH_INIT: look up the user's salt (fake salt on unknown user
 * to prevent enumeration), generate a fresh nonce into the session, and
 * reply with PKT_AUTH_CHALLENGE. */
int auth_handle_init(Session *s, struct EngineState *eng,
                     const void *payload, uint32_t len);

/* Handle PKT_AUTH_RESPONSE: verify via engine_login_response, zero the
 * nonce, and reply PKT_AUTH_OK (session → READY, conn_id stored) or
 * PKT_AUTH_ERR (returns -1 to drop the session). */
int auth_handle_response(Session *s, struct EngineState *eng,
                         const void *payload, uint32_t len);

#ifdef __cplusplus
}
#endif

#endif /* MYDB_AUTH_H */
