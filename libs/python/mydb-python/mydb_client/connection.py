"""MyDBConnection — Python counterpart of client/src/client_conn.c.

Speaks the same wire protocol (server/include/protocol.h) and the same
challenge-response auth (crypto/include/crypto.h) as the C `mydb` client, so
it can talk to mydbd over either the Unix socket or TCP.
"""

import getpass
import socket as _socket

from mydb_client import crypto
from mydb_client import protocol
from mydb_client._compat import to_bytes, to_text

DEFAULT_PORT = 4442


class MyDBError(Exception):
    """Raised for connect / auth / protocol failures."""
    pass


class MyDBConnection(object):
    def __init__(self, host=None, port=DEFAULT_PORT, unix_path=None,
                 user=None, password=None):
        if not user:
            raise MyDBError("user is required")
        if host and unix_path:
            raise MyDBError("host and unix_path are mutually exclusive")
        if not host and not unix_path:
            raise MyDBError("one of host or unix_path is required")

        self._sock = None
        self._send_seq = 0
        self._recv_seq = 0

        try:
            if unix_path:
                sock = _socket.socket(_socket.AF_UNIX, _socket.SOCK_STREAM)
                sock.connect(unix_path)
            else:
                sock = _socket.create_connection((host, port))
        except _socket.error as e:
            raise MyDBError("cannot connect to MyDB server: {0}".format(e))

        self._sock = sock
        self._handshake(user, password)

    def _send(self, ptype, payload):
        try:
            self._send_seq = protocol.send_packet(
                self._sock, ptype, payload, self._send_seq)
        except (protocol.ProtocolError, _socket.error) as e:
            self.close()
            raise MyDBError(str(e))

    def _recv(self):
        try:
            ptype, payload, self._recv_seq = protocol.recv_packet(
                self._sock, self._recv_seq)
            return ptype, payload
        except (protocol.ProtocolError, _socket.error) as e:
            self.close()
            raise MyDBError(str(e))

    def _handshake(self, user, password):
        user_bytes = to_bytes(user)

        # 1. HANDSHAKE (server version) — read and ignore.
        ptype, _payload = self._recv()
        if ptype != protocol.PKT_HANDSHAKE:
            self.close()
            raise MyDBError("unexpected packet during handshake")

        # 2. AUTH_INIT (username).
        self._send(protocol.PKT_AUTH_INIT, user_bytes)

        # 3. AUTH_CHALLENGE (salt || nonce).
        ptype, payload = self._recv()
        if (ptype != protocol.PKT_AUTH_CHALLENGE or
                len(payload) != crypto.SALT_LEN + crypto.NONCE_LEN):
            self.close()
            raise MyDBError("authentication failed")
        salt = payload[:crypto.SALT_LEN]
        nonce = payload[crypto.SALT_LEN:]

        # 4. response = SHA-256(nonce || SHA-256(salt || password)).
        if password is None:
            password = getpass.getpass("Password: ")
        h1 = crypto.hash_password(password, salt)
        response = crypto.compute_response(nonce, h1)

        # 5. AUTH_RESPONSE = username '\0' response.
        self._send(protocol.PKT_AUTH_RESPONSE, user_bytes + b"\x00" + response)

        # 6. AUTH_OK / AUTH_ERR.
        ptype, _payload = self._recv()
        if ptype != protocol.PKT_AUTH_OK:
            self.close()
            raise MyDBError("authentication failed")

    def query(self, sql):
        """Send one SQL statement, return the server's formatted result
        text (same opaque string the C REPL prints — there is no
        structured row/column protocol to parse)."""
        if self._sock is None:
            raise MyDBError("connection is closed")
        self._send(protocol.PKT_QUERY, sql)
        ptype, payload = self._recv()
        if ptype != protocol.PKT_RESPONSE:
            raise MyDBError("unexpected response packet")
        return to_text(payload)

    def close(self):
        if self._sock is None:
            return
        try:
            self._send_seq = protocol.send_packet(
                self._sock, protocol.PKT_QUIT, b"", self._send_seq)
        except Exception:
            pass  # best effort, matches client_conn_close
        try:
            self._sock.close()
        finally:
            self._sock = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False
