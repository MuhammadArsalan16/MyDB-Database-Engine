#include "server.h"
#include "protocol.h"
#include "auth.h"
#include "dispatch.h"
#include "engine.h"      /* engine_logout */

#include <poll.h>
#include <signal.h>
#include <string.h>
#include <stdio.h>
#include <unistd.h>
#include <errno.h>

/* The poll() loop is single-threaded, so a file-static pointer is enough for
 * the signal handler to ask the loop to stop. */
static Server *g_server = NULL;

static void signal_handler(int sig)
{
    (void)sig;
    if (g_server) g_server->running = 0;
}

int server_init(Server *srv, struct EngineState *eng)
{
    if (!srv || !eng) return -1;
    memset(srv, 0, sizeof(*srv));
    srv->eng     = eng;
    srv->running = 0;
    session_mgr_init(&srv->sessions);
    if (listener_init(&srv->listener) != 0) return -1;
    if (tcp_listener_init(&srv->tcp_listener) != 0) {
        listener_close(&srv->listener);
        return -1;
    }
    return 0;
}

/* Detach a session from the engine and free its slot. */
static void drop_session(Server *srv, Session *s)
{
    if (s->conn_id >= 0) {
        engine_logout(srv->eng, s->conn_id);
        s->conn_id = -1;
    }
    session_destroy(&srv->sessions, s);
}

/* Read and act on one ready session.  Drops the session on any failure. */
static void service_session(Server *srv, Session *s)
{
    PacketHeader hdr;
    uint8_t payload[PROTO_MAX_PAYLOAD];

    if (session_recv(s, &hdr, payload, sizeof(payload)) != 0) {
        /* Dead socket, EOF, or bad sequence number → client gone / hostile. */
        drop_session(srv, s);
        return;
    }

    int rc = 0;
    switch (hdr.type) {
        case PKT_AUTH_INIT:
            rc = auth_handle_init(s, srv->eng, payload, hdr.length);
            break;
        case PKT_AUTH_RESPONSE:
            rc = auth_handle_response(s, srv->eng, payload, hdr.length);
            break;
        case PKT_QUERY:
            rc = dispatch_query(s, srv->eng, payload, hdr.length);
            break;
        case PKT_QUIT:
            rc = -1;                 /* graceful close */
            break;
        default:
            rc = -1;                 /* unexpected packet type */
            break;
    }
    if (rc != 0) drop_session(srv, s);
}

int server_run(Server *srv)
{
    if (!srv) return -1;

    g_server = srv;
    signal(SIGINT,  signal_handler);
    signal(SIGTERM, signal_handler);
    signal(SIGPIPE, SIG_IGN);        /* a dead client must not kill us */

    srv->running = 1;
    printf("mydb-server: listening on %s\n", srv->listener.socket_path);
    printf("mydb-server: listening on TCP %s:%u\n",
           srv->tcp_listener.bind_addr, srv->tcp_listener.port);
    fflush(stdout);

    while (srv->running) {
        struct pollfd fds[2 + MAX_SESSIONS];
        Session      *owner[2 + MAX_SESSIONS];
        nfds_t        nfds = 0;

        /* fds[0] is the Unix listener, fds[1] the TCP listener — both
         * always active. */
        fds[nfds].fd      = srv->listener.fd;
        fds[nfds].events  = POLLIN;
        fds[nfds].revents = 0;
        owner[nfds]       = NULL;
        nfds++;

        fds[nfds].fd      = srv->tcp_listener.fd;
        fds[nfds].events  = POLLIN;
        fds[nfds].revents = 0;
        owner[nfds]       = NULL;
        nfds++;

        /* One entry per active session not currently executing. */
        for (int i = 0; i < MAX_SESSIONS; i++) {
            Session *s = &srv->sessions.slots[i];
            if (s->active && s->state != SESSION_BUSY && s->client_fd >= 0) {
                fds[nfds].fd      = s->client_fd;
                fds[nfds].events  = POLLIN;
                fds[nfds].revents = 0;
                owner[nfds]       = s;
                nfds++;
            }
        }

        int n = poll(fds, nfds, 5000);
        if (n < 0) {
            if (errno == EINTR) continue;   /* interrupted by a signal */
            break;
        }
        if (n == 0) continue;               /* timeout — re-check running */

        /* New connection on either listener. */
        if (fds[0].revents & POLLIN) {
            int cfd = listener_accept(&srv->listener);
            if (cfd >= 0) {
                Session *s = session_create(&srv->sessions, cfd);
                if (!s) {
                    close(cfd);             /* session table full */
                } else if (auth_send_handshake(s) != 0) {
                    drop_session(srv, s);
                }
            }
        }
        if (fds[1].revents & POLLIN) {
            int cfd = tcp_listener_accept(&srv->tcp_listener);
            if (cfd >= 0) {
                Session *s = session_create(&srv->sessions, cfd);
                if (!s) {
                    close(cfd);             /* session table full */
                } else if (auth_send_handshake(s) != 0) {
                    drop_session(srv, s);
                }
            }
        }

        /* Existing sessions with data (or hangup) waiting. */
        for (nfds_t k = 2; k < nfds; k++) {
            if (!owner[k]) continue;
            if (fds[k].revents & (POLLIN | POLLHUP | POLLERR))
                service_session(srv, owner[k]);
        }
    }

    printf("mydb-server: shutting down\n");
    fflush(stdout);
    return 0;
}

void server_shutdown(Server *srv)
{
    if (!srv) return;
    for (int i = 0; i < MAX_SESSIONS; i++) {
        Session *s = &srv->sessions.slots[i];
        if (s->active) drop_session(srv, s);
    }
    listener_close(&srv->listener);
    tcp_listener_close(&srv->tcp_listener);
    g_server = NULL;
}
