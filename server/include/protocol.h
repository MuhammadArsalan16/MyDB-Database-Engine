#ifndef MYDB_PROTOCOL_H
#define MYDB_PROTOCOL_H

#include <stdint.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

/*
 * protocol.h — MyDB wire protocol (shared by mydb-server and mydb-client).
 *
 * This is the ONLY source file compiled into both binaries; client and
 * server must agree on it byte-for-byte.  Nothing above this layer calls
 * read()/write() directly.
 *
 * Packet = 9-byte header + `length` payload bytes:
 *
 *   +----------------+--------+----------------+
 *   | length (4 LE→N)| type(1)| seq_no (4 LE→N)|   payload[length]
 *   +----------------+--------+----------------+
 *
 * `length` and `seq_no` travel in network byte order (htonl/ntohl) so the
 * protocol is correct across architectures.  Each direction keeps its own
 * sequence counter starting at 0 and incrementing by 1 per packet; a
 * receiver that sees an unexpected seq_no drops the connection (guards
 * against dropped, duplicated, or replayed packets — including replayed
 * auth packets, whose captured seq_no won't match a fresh session).
 */

#define PROTO_HEADER_SIZE     9u
#define PROTO_MAX_PAYLOAD     65536u   /* 64 KB — caps queries and results */

typedef enum {
    PKT_HANDSHAKE      = 1,   /* server → client: server version string     */
    PKT_AUTH_INIT      = 2,   /* client → server: username                  */
    PKT_AUTH_CHALLENGE = 3,   /* server → client: salt(16) + nonce(32)      */
    PKT_AUTH_RESPONSE  = 4,   /* client → server: username\0 + response(32) */
    PKT_AUTH_OK        = 5,   /* server → client: login success             */
    PKT_AUTH_ERR       = 6,   /* server → client: login failed              */
    PKT_QUERY          = 7,   /* client → server: SQL string                */
    PKT_RESPONSE       = 8,   /* server → client: formatted result / error  */
    PKT_QUIT           = 9    /* client → server: disconnect                */
} PacketType;

/* Parsed header (host byte order). */
typedef struct {
    uint32_t length;          /* payload size in bytes (excludes header)    */
    uint8_t  type;            /* PacketType                                 */
    uint32_t seq_no;          /* sender's sequence number for this packet   */
} PacketHeader;

/* Send one packet on `fd`: frames the header, writes header + payload in
 * full (looping over short writes), and post-increments *seq.  `payload`
 * may be NULL when `len` is 0.  Returns 0 on success, -1 on I/O error or if
 * len > PROTO_MAX_PAYLOAD. */
int proto_send(int fd, PacketType type, const void *payload, uint32_t len,
               uint32_t *seq);

/* Receive one packet from `fd`: reads the full header, then up to
 * `max_len` payload bytes into `payload`.  Validates header.seq_no against
 * *expected_seq (mismatch → -1) and length against max_len, then
 * post-increments *expected_seq.  Returns 0 on success, -1 on I/O error,
 * EOF, sequence mismatch, or oversized payload. */
int proto_recv(int fd, PacketHeader *hdr, void *payload, uint32_t max_len,
               uint32_t *expected_seq);

/* Resolve the Unix socket path the same way on both ends:
 *   $MYDB_HOME/mydb.sock   if MYDB_HOME is set, else
 *   /run/mydb/mydb.sock    (systemd RuntimeDirectory default).
 * Writes a NUL-terminated path into `buf`.  Returns 0 on success, -1 on
 * overflow. */
int proto_socket_path(char *buf, size_t cap);

#ifdef __cplusplus
}
#endif

#endif /* MYDB_PROTOCOL_H */
