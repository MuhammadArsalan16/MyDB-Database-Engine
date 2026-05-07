#ifndef CRYPTO_H
#define CRYPTO_H

#include <stddef.h>
#include <stdint.h>

/* ------------------------------------------------------------------ */
/*  Tiny crypto utilities used by the engine bootstrap + login flow.  */
/*                                                                    */
/*  No external dependencies (no libcrypto / openssl). Currently      */
/*  consumed only by engine.c — if a second consumer arises the       */
/*  whole {crypto.h, crypto.c} pair lifts to a top-level module       */
/*  (same lift policy used for fnv1a in checksum.{h,c}).              */
/* ------------------------------------------------------------------ */

#define SHA256_DIGEST_LEN  32   /* matches USER_PASSWORD_HASH_LEN */
#define SALT_LEN           16   /* matches USER_PASSWORD_SALT_LEN */

/* Compute SHA-256 over `len` bytes at `msg` and write the 32-byte
 * digest to `out`. Implementation follows FIPS-180-4 verbatim. */
void sha256(const void *msg, size_t len, uint8_t out[SHA256_DIGEST_LEN]);

/* Hash a password with a per-user salt:
 *
 *   out = SHA-256(salt || password)
 *
 * `password` is treated as a NUL-terminated C string (its NUL is
 * NOT included in the hash). Bit-wise stable so it can be persisted
 * and compared verbatim across runs. */
void crypto_hash_password(const char *password,
                          const uint8_t salt[SALT_LEN],
                          uint8_t out[SHA256_DIGEST_LEN]);

/* Read SALT_LEN bytes from /dev/urandom into `salt`. Returns 0 on
 * success, -1 (MYDB_ERR) on failure (e.g. /dev/urandom unavailable
 * or short read). */
int  crypto_random_salt(uint8_t salt[SALT_LEN]);

#endif /* CRYPTO_H */
