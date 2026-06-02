#include "auth.h"
#include "protocol.h"
#include "crypto.h"      /* SALT_LEN, MYDB_NONCE_LEN, SHA256_DIGEST_LEN, random */
#include "engine.h"      /* EngineState, engine_get_user_salt, *_login_response */
#include "common.h"      /* MYDB_OK, MAX_USERNAME */

#include <string.h>

#define HANDSHAKE_STR  "MyDB 1.0"

int auth_send_handshake(Session *s)
{
    if (!s) return -1;
    if (session_send(s, PKT_HANDSHAKE,
                     HANDSHAKE_STR, (uint32_t)strlen(HANDSHAKE_STR)) != 0)
        return -1;
    s->state = SESSION_AUTHENTICATING;
    return 0;
}

int auth_handle_init(Session *s, struct EngineState *eng,
                     const void *payload, uint32_t len)
{
    if (!s || !eng) return -1;
    if (s->state != SESSION_AUTHENTICATING) return -1;

    /* Copy the username out, NUL-terminated and bounded. */
    char username[MAX_USERNAME];
    if (len == 0 || len >= sizeof(username)) return -1;
    memcpy(username, payload, len);
    username[len] = '\0';

    /* Look up the salt; on an unknown user, answer with a random salt so
     * the client cannot tell whether the username exists (the response will
     * fail verification either way). */
    uint8_t salt[SALT_LEN];
    if (engine_get_user_salt(eng, username, salt) != MYDB_OK) {
        if (crypto_random_bytes(salt, SALT_LEN) != 0) return -1;
    }

    /* Fresh nonce per challenge → replayed responses can't be reused. */
    if (crypto_random_bytes(s->nonce, MYDB_NONCE_LEN) != 0) return -1;

    uint8_t challenge[SALT_LEN + MYDB_NONCE_LEN];
    memcpy(challenge,            salt,     SALT_LEN);
    memcpy(challenge + SALT_LEN, s->nonce, MYDB_NONCE_LEN);

    return session_send(s, PKT_AUTH_CHALLENGE,
                        challenge, (uint32_t)sizeof(challenge));
}

int auth_handle_response(Session *s, struct EngineState *eng,
                         const void *payload, uint32_t len)
{
    if (!s || !eng) return -1;
    if (s->state != SESSION_AUTHENTICATING) return -1;

    /* Payload = username '\0' response[SHA256_DIGEST_LEN]. */
    const uint8_t *p = (const uint8_t *)payload;
    size_t namelen = 0;
    while (namelen < len && p[namelen] != '\0') namelen++;
    /* Need the NUL plus exactly SHA256_DIGEST_LEN response bytes after it. */
    if (namelen == 0 || namelen >= MAX_USERNAME) return -1;
    if (len != namelen + 1 + SHA256_DIGEST_LEN) return -1;

    char username[MAX_USERNAME];
    memcpy(username, p, namelen);
    username[namelen] = '\0';

    const uint8_t *response = p + namelen + 1;

    int conn_id = -1;
    int rc = engine_login_response(eng, username, response, s->nonce, &conn_id);

    /* The nonce has served its purpose — wipe it immediately, regardless of
     * outcome. */
    memset(s->nonce, 0, MYDB_NONCE_LEN);

    if (rc != MYDB_OK || conn_id < 0) {
        /* Identical failure for wrong password and unknown user. */
        session_send(s, PKT_AUTH_ERR, NULL, 0);
        return -1;   /* drop the session */
    }

    s->conn_id = conn_id;
    s->state   = SESSION_READY;
    return session_send(s, PKT_AUTH_OK, NULL, 0);
}
