#ifndef SYSTEM_SCHEMA_H
#define SYSTEM_SCHEMA_H

#include "common.h"

/* ------------------------------------------------------------------ */
/*  system_schema — engine-level user + privilege catalog             */
/*                                                                    */
/*  Two flat files live under <engine_root>/system_schema/:           */
/*                                                                    */
/*    users.mydb       — one slot per user account (USERS_MAX_SLOTS)  */
/*    privileges.mydb  — one slot per cross-partition SELECT grant    */
/*                       (PRIVILEGES_MAX_SLOTS)                       */
/*                                                                    */
/*  Both are loaded into RAM at engine startup with direct            */
/*  pread/pwrite I/O — they bypass the buffer pool entirely. Lookups  */
/*  go through open-addressing hash maps maintained alongside the     */
/*  slot arrays:                                                      */
/*                                                                    */
/*    users:      hash by username        (login lookup)              */
/*                linear scan by user_id  (FK resolution from priv)   */
/*    privileges: hash by                                             */
/*                (grantee_id, partition_id, schema_name)             */
/*                                                                    */
/*  Phase 7 stores raw bytes only. Hash computation (SHA-256), salt   */
/*  generation, login flow, and FK / authorization checks live in    */
/*  later phases.                                                     */
/* ------------------------------------------------------------------ */


/* ================================================================== */
/*  Slot record types                                                 */
/* ================================================================== */

/* One row of users.mydb — design doc §3.1.
 *
 * password_hash and password_salt are raw bytes, NOT C strings.
 * hash_algorithm: 1 = SHA-256, 2 reserved for Argon2id. */
typedef struct {
    uint32_t user_id;
    char     username[MAX_USERNAME];
    uint8_t  password_hash[USER_PASSWORD_HASH_LEN];
    uint8_t  password_salt[USER_PASSWORD_SALT_LEN];
    uint8_t  hash_algorithm;
    uint8_t  is_active;
    uint64_t created_at;            /* YYYYMMDDHHmmSS */
    uint64_t last_login;            /* 0 if never logged in */
    uint8_t  is_valid;              /* 1 = occupied slot, 0 = empty */
} UserSlot;

/* One row of privileges.mydb — design doc §3.2.
 *
 * grantee_id and granted_by reference users.user_id. FK validity is
 * enforced by the authorization layer (phase 10), not here. */
typedef struct {
    uint32_t privilege_id;
    uint32_t grantee_id;            /* FK -> users.user_id */
    uint32_t partition_id;          /* which partition the grant applies to */
    uint32_t granted_by;            /* FK -> users.user_id */
    uint64_t granted_at;            /* YYYYMMDDHHmmSS */
    char     schema_name[32];
    uint8_t  is_valid;              /* 1 = occupied slot, 0 = empty */
} PrivilegeSlot;


/* ================================================================== */
/*  Hash maps                                                         */
/*                                                                    */
/*  Open-addressing with linear probing. Capacity is a power of two   */
/*  and >= 2 x slot count, so load factor never exceeds 0.5. Each     */
/*  bucket stores either -1 (empty) or a slot index into the array.   */
/*  Keys are compared against the slot record itself.                 */
/* ================================================================== */

#define USERS_HASH_CAPACITY        64    /* >= 2 x USERS_MAX_SLOTS */
#define PRIVILEGES_HASH_CAPACITY  512    /* >= 2 x PRIVILEGES_MAX_SLOTS */

typedef struct {
    int16_t slot;   /* -1 = empty, else index into UsersFile.slots */
} UserHashBucket;

typedef struct {
    int16_t slot;   /* -1 = empty, else index into PrivilegesFile.slots */
} PrivHashBucket;


/* ================================================================== */
/*  In-memory file mirrors                                            */
/* ================================================================== */

typedef struct {
    UserSlot       slots[USERS_MAX_SLOTS];
    UserHashBucket name_idx[USERS_HASH_CAPACITY];   /* username -> slot */
    uint32_t       next_user_id;                     /* monotonic counter */
    uint8_t        num_users;                        /* count of valid slots */
    int            fd;                               /* -1 when closed */
    char           path[256];
} UsersFile;

typedef struct {
    PrivilegeSlot  slots[PRIVILEGES_MAX_SLOTS];
    PrivHashBucket key_idx[PRIVILEGES_HASH_CAPACITY]; /* (gid,pid,name) -> slot */
    uint32_t       next_privilege_id;                 /* monotonic counter */
    uint16_t       num_privileges;                    /* count of valid slots */
    int            fd;                                /* -1 when closed */
    char           path[256];
} PrivilegesFile;

/* The engine holds one of these. Both files are opened together at
 * startup before any login is accepted (design doc §5.1). */
typedef struct {
    UsersFile      users;
    PrivilegesFile privileges;
} SystemSchema;


/* ================================================================== */
/*  Public API                                                        */
/* ================================================================== */

/* ---- aggregate lifecycle ---------------------------------------- */

/* Create both files under <root_dir>/system_schema/. Fails if either
 * already exists. The directory itself must exist; this function does
 * not mkdir (caller's responsibility — engine init in phase 8). */
int system_schema_create(const char *root_dir, SystemSchema *out);

/* Open both existing files. Verifies magic / version / file_type /
 * checksum on each, builds the hash maps. */
int system_schema_open(const char *root_dir, SystemSchema *out);

/* Close both file descriptors. Does not flush — every public mutator
 * already saves. */
int system_schema_close(SystemSchema *ss);


/* ---- users -------------------------------------------------------- */

/* Insert a new user. user_id is allocated from the in-memory counter;
 * the caller's slot.user_id is ignored. password_hash / password_salt
 * are stored verbatim (caller pre-hashes — phase 8 responsibility).
 *
 * Rejects duplicate usernames with MYDB_ERR_DUPLICATE.
 * Rejects when the file is full with MYDB_ERR_FULL.
 * On success, *out_user_id receives the assigned id. */
int users_insert(UsersFile *uf, const UserSlot *slot, uint32_t *out_user_id);

/* Copy the matching slot into *out. Returns MYDB_ERR_NOT_FOUND on miss. */
int users_find_by_name(const UsersFile *uf, const char *username, UserSlot *out);
int users_find_by_id  (const UsersFile *uf, uint32_t user_id,    UserSlot *out);

/* Replace the slot with user_id == new_slot->user_id. Username may
 * change (hash map is updated). Persists. */
int users_update(UsersFile *uf, const UserSlot *new_slot);

/* Mark the slot empty and remove from the hash map. Persists. */
int users_delete(UsersFile *uf, uint32_t user_id);


/* ---- privileges --------------------------------------------------- */

/* Insert a new privilege. privilege_id is allocated; caller's value
 * is ignored. The composite key (grantee_id, partition_id,
 * schema_name) must be unique — duplicates → MYDB_ERR_DUPLICATE. */
int privileges_insert(PrivilegesFile *pf, const PrivilegeSlot *slot,
                      uint32_t *out_privilege_id);

/* Look up a grant by composite key. */
int privileges_find(const PrivilegesFile *pf,
                    uint32_t grantee_id, uint32_t partition_id,
                    const char *schema_name, PrivilegeSlot *out);

/* Look up a grant by primary key. Linear scan (rare path). */
int privileges_find_by_id(const PrivilegesFile *pf, uint32_t privilege_id,
                          PrivilegeSlot *out);

/* Remove a grant by composite key. */
int privileges_delete(PrivilegesFile *pf,
                      uint32_t grantee_id, uint32_t partition_id,
                      const char *schema_name);

#endif /* SYSTEM_SCHEMA_H */
