#include "session.h"

#include <unistd.h>
#include <string.h>

void session_mgr_init(SessionManager *m)
{
    if (!m) return;
    memset(m, 0, sizeof(*m));
    for (int i = 0; i < MAX_SESSIONS; i++) {
        m->slots[i].client_fd = -1;
        m->slots[i].conn_id   = -1;
        m->slots[i].active    = false;
    }
}

Session *session_create(SessionManager *m, int client_fd)
{
    if (!m) return NULL;
    for (int i = 0; i < MAX_SESSIONS; i++) {
        Session *s = &m->slots[i];
        if (!s->active) {
            memset(s, 0, sizeof(*s));
            s->client_fd = client_fd;
            s->conn_id   = -1;
            s->send_seq  = 0;
            s->recv_seq  = 0;
            s->state     = SESSION_CONNECTING;
            s->active    = true;
            m->count++;
            return s;
        }
    }
    return NULL;   /* session table full */
}

void session_destroy(SessionManager *m, Session *s)
{
    if (!m || !s || !s->active) return;
    if (s->client_fd >= 0) close(s->client_fd);
    /* Wipe the nonce on the way out — it must never linger in memory. */
    memset(s, 0, sizeof(*s));
    s->client_fd = -1;
    s->conn_id   = -1;
    s->active    = false;
    if (m->count > 0) m->count--;
}

int session_send(Session *s, PacketType type, const void *payload, uint32_t len)
{
    if (!s || s->client_fd < 0) return -1;
    return proto_send(s->client_fd, type, payload, len, &s->send_seq);
}

int session_recv(Session *s, PacketHeader *hdr, void *payload, uint32_t max_len)
{
    if (!s || s->client_fd < 0) return -1;
    return proto_recv(s->client_fd, hdr, payload, max_len, &s->recv_seq);
}
