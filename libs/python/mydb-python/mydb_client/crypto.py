"""Challenge-response auth hashing — mirrors crypto/include/crypto.h.

response = SHA-256(nonce || SHA-256(salt || password))
"""

import hashlib

from mydb_client._compat import to_bytes

SALT_LEN = 16
NONCE_LEN = 32
SHA256_DIGEST_LEN = 32


def hash_password(password, salt):
    """SHA-256(salt || password). `password` is treated as raw text (no NUL
    terminator, matching crypto_hash_password)."""
    return hashlib.sha256(salt + to_bytes(password)).digest()


def compute_response(nonce, h1):
    """SHA-256(nonce || h1)."""
    return hashlib.sha256(nonce + h1).digest()
