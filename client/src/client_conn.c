/* getpass(3) needs _DEFAULT_SOURCE on glibc — define before any include. */
#define _DEFAULT_SOURCE

#include "client_conn.h"
#include "protocol.h"
#include "crypto.h"      /* crypto_hash_password, sha256, SALT_LEN, ...     */

#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>
#include <string.h>
#include <stdio.h>

/* Connect a fresh AF_UNIX stream socket to the resolved server path. */
static int connect_socket(void)
{
    char path[256];
    if (proto_socket_path(path, sizeof(path)) != 0) return -1;
    if (strlen(path) >= sizeof(((struct sockaddr_un *)0)->sun_path)) return -1;

    int fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (fd < 0) return -1;

    struct sockaddr_un addr;
    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, path, sizeof(addr.sun_path) - 1);

    if (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) != 0) {
        close(fd);
        return -1;
    }
    return fd;
}

int client_conn_open(ClientConn *c, const char *username)
{
    if (!c || !username || username[0] == '\0') return -1;
    c->fd = -1;
    c->send_seq = 0;
    c->recv_seq = 0;

    int fd = connect_socket();
    if (fd < 0) {
        fprintf(stderr, "ERROR: cannot connect to MyDB server\n"
                        "       is mydb-server running?\n");
        return -1;
    }
    c->fd = fd;

    PacketHeader hdr;
    uint8_t payload[PROTO_MAX_PAYLOAD];

    /* 1. HANDSHAKE (server version) — read and ignore the contents. */
    if (proto_recv(fd, &hdr, payload, sizeof(payload), &c->recv_seq) != 0 ||
        hdr.type != PKT_HANDSHAKE) {
        close(fd); c->fd = -1; return -1;
    }

    /* 2. AUTH_INIT (username). */
    if (proto_send(fd, PKT_AUTH_INIT, username,
                   (uint32_t)strlen(username), &c->send_seq) != 0) {
        close(fd); c->fd = -1; return -1;
    }

    /* 3. AUTH_CHALLENGE (salt || nonce). */
    if (proto_recv(fd, &hdr, payload, sizeof(payload), &c->recv_seq) != 0 ||
        hdr.type != PKT_AUTH_CHALLENGE ||
        hdr.length != SALT_LEN + MYDB_NONCE_LEN) {
        close(fd); c->fd = -1; return -1;
    }
    const uint8_t *salt  = payload;
    const uint8_t *nonce = payload + SALT_LEN;

    /* 4. Compute response = SHA-256(nonce || SHA-256(salt || password)). */
    char *pw = getpass("Password: ");
    if (!pw) { close(fd); c->fd = -1; return -1; }

    uint8_t h1[SHA256_DIGEST_LEN];
    crypto_hash_password(pw, salt, h1);          /* SHA-256(salt || password) */
    memset(pw, 0, strlen(pw));                   /* wipe the plaintext        */

    uint8_t buf[MYDB_NONCE_LEN + SHA256_DIGEST_LEN];
    memcpy(buf, nonce, MYDB_NONCE_LEN);
    memcpy(buf + MYDB_NONCE_LEN, h1, SHA256_DIGEST_LEN);
    uint8_t response[SHA256_DIGEST_LEN];
    sha256(buf, sizeof(buf), response);

    /* 5. AUTH_RESPONSE = username '\0' response[32]. */
    size_t namelen = strlen(username);
    uint8_t out[PROTO_MAX_PAYLOAD];
    if (namelen + 1 + SHA256_DIGEST_LEN > sizeof(out)) {
        close(fd); c->fd = -1; return -1;
    }
    memcpy(out, username, namelen);
    out[namelen] = '\0';
    memcpy(out + namelen + 1, response, SHA256_DIGEST_LEN);
    if (proto_send(fd, PKT_AUTH_RESPONSE, out,
                   (uint32_t)(namelen + 1 + SHA256_DIGEST_LEN),
                   &c->send_seq) != 0) {
        close(fd); c->fd = -1; return -1;
    }

    /* 6. AUTH_OK / AUTH_ERR. */
    if (proto_recv(fd, &hdr, payload, sizeof(payload), &c->recv_seq) != 0) {
        close(fd); c->fd = -1; return -1;
    }
    if (hdr.type != PKT_AUTH_OK) {
        fprintf(stderr, "ERROR: authentication failed\n");
        close(fd); c->fd = -1; return -1;
    }
    return 0;
}

int client_conn_query(ClientConn *c, const char *sql, char *out, size_t cap)
{
    if (!c || c->fd < 0 || !sql || !out || cap == 0) return -1;

    if (proto_send(c->fd, PKT_QUERY, sql, (uint32_t)strlen(sql),
                   &c->send_seq) != 0)
        return -1;

    PacketHeader hdr;
    uint8_t payload[PROTO_MAX_PAYLOAD];
    if (proto_recv(c->fd, &hdr, payload, sizeof(payload), &c->recv_seq) != 0 ||
        hdr.type != PKT_RESPONSE)
        return -1;

    uint32_t n = hdr.length;
    if (n > cap - 1) n = (uint32_t)(cap - 1);
    memcpy(out, payload, n);
    out[n] = '\0';
    return 0;
}

void client_conn_close(ClientConn *c)
{
    if (!c || c->fd < 0) return;
    proto_send(c->fd, PKT_QUIT, NULL, 0, &c->send_seq);   /* best effort */
    close(c->fd);
    c->fd = -1;
}
