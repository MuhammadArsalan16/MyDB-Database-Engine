#include "protocol.h"

#include <arpa/inet.h>   /* htonl / ntohl */
#include <unistd.h>      /* read / write  */
#include <errno.h>
#include <string.h>
#include <stdlib.h>      /* getenv        */
#include <stdio.h>       /* snprintf      */

/* Loop until exactly `n` bytes have been written, tolerating short writes
 * and EINTR.  Returns 0 on success, -1 on error or closed socket. */
static int write_exact(int fd, const void *buf, size_t n)
{
    const uint8_t *p = (const uint8_t *)buf;
    size_t off = 0;
    while (off < n) {
        ssize_t w = write(fd, p + off, n - off);
        if (w < 0) {
            if (errno == EINTR) continue;
            return -1;
        }
        if (w == 0) return -1;          /* peer closed */
        off += (size_t)w;
    }
    return 0;
}

/* Loop until exactly `n` bytes have been read.  Returns 0 on success,
 * -1 on error or EOF before `n` bytes arrived. */
static int read_exact(int fd, void *buf, size_t n)
{
    uint8_t *p = (uint8_t *)buf;
    size_t off = 0;
    while (off < n) {
        ssize_t r = read(fd, p + off, n - off);
        if (r < 0) {
            if (errno == EINTR) continue;
            return -1;
        }
        if (r == 0) return -1;          /* EOF / peer closed */
        off += (size_t)r;
    }
    return 0;
}

int proto_send(int fd, PacketType type, const void *payload, uint32_t len,
               uint32_t *seq)
{
    if (len > PROTO_MAX_PAYLOAD) return -1;
    if (len > 0 && !payload)     return -1;

    uint8_t  hdr[PROTO_HEADER_SIZE];
    uint32_t nlen = htonl(len);
    uint32_t nseq = htonl(seq ? *seq : 0);
    memcpy(hdr,     &nlen, 4);
    hdr[4] = (uint8_t)type;
    memcpy(hdr + 5, &nseq, 4);

    if (write_exact(fd, hdr, PROTO_HEADER_SIZE) != 0) return -1;
    if (len > 0 && write_exact(fd, payload, len) != 0) return -1;

    if (seq) (*seq)++;
    return 0;
}

int proto_recv(int fd, PacketHeader *hdr, void *payload, uint32_t max_len,
               uint32_t *expected_seq)
{
    if (!hdr) return -1;

    uint8_t raw[PROTO_HEADER_SIZE];
    if (read_exact(fd, raw, PROTO_HEADER_SIZE) != 0) return -1;

    uint32_t nlen, nseq;
    memcpy(&nlen, raw,     4);
    memcpy(&nseq, raw + 5, 4);
    hdr->length = ntohl(nlen);
    hdr->type   = raw[4];
    hdr->seq_no = ntohl(nseq);

    /* Sequence check — a mismatch means a missing / duplicate / replayed
     * packet; drop the connection. */
    if (expected_seq && hdr->seq_no != *expected_seq) return -1;

    if (hdr->length > max_len) return -1;
    if (hdr->length > 0) {
        if (!payload) return -1;
        if (read_exact(fd, payload, hdr->length) != 0) return -1;
    }

    if (expected_seq) (*expected_seq)++;
    return 0;
}

int proto_socket_path(char *buf, size_t cap)
{
    if (!buf || cap == 0) return -1;
    const char *home = getenv("MYDB_HOME");
    int n = (home && home[0] != '\0')
            ? snprintf(buf, cap, "%s/mydb.sock", home)
            : snprintf(buf, cap, "/run/mydb/mydb.sock");
    return (n < 0 || (size_t)n >= cap) ? -1 : 0;
}
