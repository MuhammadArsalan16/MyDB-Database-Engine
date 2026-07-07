"""Wire protocol framing — mirrors server/include/protocol.h exactly.

Every packet is a 9-byte header (length, type, seq_no; length and seq_no in
network byte order) followed by `length` payload bytes. Each direction
(send/recv) keeps its own sequence counter starting at 0.
"""

import struct

from mydb_client._compat import to_bytes

HEADER_SIZE = 9
MAX_PAYLOAD = 65536

PKT_HANDSHAKE = 1
PKT_AUTH_INIT = 2
PKT_AUTH_CHALLENGE = 3
PKT_AUTH_RESPONSE = 4
PKT_AUTH_OK = 5
PKT_AUTH_ERR = 6
PKT_QUERY = 7
PKT_RESPONSE = 8
PKT_QUIT = 9

_HEADER_FMT = "!IBI"  # network-order: uint32 length, uint8 type, uint32 seq_no


class ProtocolError(Exception):
    pass


def _recv_exact(sock, n):
    chunks = []
    remaining = n
    while remaining > 0:
        chunk = sock.recv(remaining)
        if not chunk:
            raise ProtocolError("connection closed")
        chunks.append(chunk)
        remaining -= len(chunk)
    return b"".join(chunks)


def send_packet(sock, ptype, payload, seq):
    """Send one packet. `seq` is the current send counter; returns the next
    value (mirrors proto_send's seq post-increment)."""
    payload = to_bytes(payload) if payload else b""
    if len(payload) > MAX_PAYLOAD:
        raise ProtocolError("payload too large")
    header = struct.pack(_HEADER_FMT, len(payload), ptype, seq)
    sock.sendall(header + payload)
    return seq + 1


def recv_packet(sock, expected_seq):
    """Receive one packet, validating the sequence number. Returns
    (ptype, payload, next_expected_seq)."""
    header = _recv_exact(sock, HEADER_SIZE)
    length, ptype, seq_no = struct.unpack(_HEADER_FMT, header)

    if seq_no != expected_seq:
        raise ProtocolError("sequence mismatch (connection dropped)")
    if length > MAX_PAYLOAD:
        raise ProtocolError("oversized packet")

    payload = _recv_exact(sock, length) if length > 0 else b""
    return ptype, payload, expected_seq + 1
