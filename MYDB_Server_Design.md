# MyDB — Server Design

This document covers the full server architecture for MyDB. No code has been
written yet. The goal is to add a proper client-server model on top of the
existing engine, with separate binaries, a custom protocol, Unix socket
transport, and systemd integration.

---

## Motivation

The current `bin/mydb` embeds the engine directly — the user, client, and
engine are one process. This works for single-user local use but has structural
problems:

1. **No network boundary.** Every user must be on the same machine with direct
   filesystem access to `$MYDB_HOME`.
2. **No concurrent sessions.** One process means one user at a time.
3. **No isolation.** The client binary contains all engine internals.

The server layer adds a clean network boundary. The engine becomes a daemon.
Clients connect over a Unix socket using a custom protocol.

---

## Binary Split

```
mydb-server    heavy binary: listener + sessions + protocol + auth +
                             dispatch + engine (all server-side logic)

mydb           light binary: client only — no engine, no storage,
                             just socket + protocol + terminal UI
```

Separate binaries for three reasons:

- **Size** — the client has no need to carry engine code
- **Security** — engine internals are not exposed in the distributed binary
- **Versioning** — client and server can be updated independently

`protocol.c` is the only source file shared between both binaries. Client and
server must speak the same protocol.

---

## Transport — Unix Domain Socket

MyDB uses a Unix domain socket for client-server communication.

```
Socket file:  $MYDB_HOME/mydb.sock
              development:  ~/.mydb/mydb.sock
              production:   /run/mydb/mydb.sock  (systemd RuntimeDirectory)
```

Unix socket chosen over TCP because:

| | TCP | Unix Socket |
|---|---|---|
| Remote access | Yes | No — same machine only |
| Speed | Slower (network stack) | Faster (kernel copy) |
| Security | Firewall rules | File permissions |
| Overhead | 3-way handshake, checksums | None |

TCP can be added later for remote access without changing any layer above the
listener.

Both `mydb-server` and `mydb` resolve the socket path the same way:

```c
const char *home = getenv("MYDB_HOME");
if (home) snprintf(path, len, "%s/mydb.sock", home);
else      snprintf(path, len, "/run/mydb/mydb.sock");
```

---

## Architecture — Eight Layers

```
Layer 8   systemd             service file, system user, data directory
Layer 7   Client Binary       mydb — connect, auth, REPL, print
Layer 6   Server Main Loop    poll() event loop — ties all layers together
Layer 5   Dispatch            SQL → engine → PKT_RESPONSE back to client
Layer 4   Auth Handler        handshake, challenge-response, engine_login
Layer 3   Session Manager     one Session per connection, network state only
Layer 2   Protocol            packet format, send/recv, sequence numbers
Layer 1   Listener            Unix socket, bind, listen, accept
```

---

## Layer 1 — Listener

Creates the Unix socket, binds it to `$MYDB_HOME/mydb.sock`, and accepts
incoming connections.

```
socket()   create socket fd
bind()     attach to $MYDB_HOME/mydb.sock  (creates socket file on disk)
listen()   mark as passive, backlog = 8
accept()   block until client connects, return client_fd
```

Key points:

- `listener_fd` is permanent — lives for the lifetime of the server
- Each `accept()` returns a new `client_fd` dedicated to one client
- Before `bind()`, the server calls `unlink()` to remove any stale socket file
  left from a previous crash
- `listener_close()` calls `unlink()` again on clean shutdown

```
server/
  include/listener.h
  src/listener.c
```

```c
typedef struct {
    int  fd;
    char socket_path[256];
} Listener;

int  listener_init(Listener *l);
int  listener_accept(Listener *l);   // returns client_fd
void listener_close(Listener *l);    // closes fd + unlinks socket file
```

---

## Layer 2 — Protocol

Defines how bytes on the socket become structured messages. Nothing above this
layer ever calls `read()` or `write()` directly.

### Packet Structure

```
+-------------------+------------------+---------------------+
|   length (4 bytes)|  type  (1 byte)  |  seq_no  (4 bytes)  |
+-------------------+------------------+---------------------+
         9 bytes header, followed by `length` bytes of payload
```

- **length** — payload size in bytes (not including header)
- **type** — packet type (see below)
- **seq_no** — monotonically increasing sequence number per direction

### Packet Types

```c
PKT_HANDSHAKE       = 1   // server → client: server version
PKT_AUTH_INIT       = 2   // client → server: username
PKT_AUTH_CHALLENGE  = 3   // server → client: salt + nonce
PKT_AUTH_RESPONSE   = 4   // client → server: hashed response
PKT_AUTH_OK         = 5   // server → client: login success
PKT_AUTH_ERR        = 6   // server → client: login failed
PKT_QUERY           = 7   // client → server: SQL string
PKT_RESPONSE        = 8   // server → client: everything (engine formatted)
PKT_QUIT            = 9   // client → server: disconnect
```

`PKT_RESPONSE` covers all query results — SELECT rows, DML status, errors.
The engine formats the output string; the client prints it directly. No result
type interpretation needed on the client side.

### Sequence Numbers

Each side maintains its own counter starting at 0, incrementing by 1 per
packet sent. The receiver checks the incoming `seq_no` against the expected
value. A mismatch means a missing, duplicate, or replayed packet — the
connection is dropped.

Sequence numbers also make replayed authentication packets invalid: the
`seq_no` from a captured session will not match the counter in a new session.

### read_exact / write_exact

A single `read()` on a socket does not guarantee all bytes arrive at once.
`read_exact()` and `write_exact()` loop until all bytes are transferred.
This is mandatory — omitting it is the most common socket programming bug.

### Network Byte Order

All multi-byte integers in the header are converted to network byte order
(`htonl`) before sending and back to host byte order (`ntohl`) on receipt.
This ensures correctness across any CPU architecture.

```
server/
  include/protocol.h
  src/protocol.c
```

```c
int proto_send(int fd, PacketType type, const void *payload,
               uint32_t len, uint32_t *seq);
int proto_recv(int fd, PacketHeader *header, void *payload,
               uint32_t max_len, uint32_t *expected_seq);
```

---

## Layer 3 — Session Manager

Owns the network state of every active connection. One `Session` per connected
client. Nothing about database state — no username, no schema, no transaction.
Those belong to the engine.

### Session Struct

```c
typedef struct {
    int           client_fd;      // socket to this client
    int           conn_id;        // engine handle, -1 until auth
    uint32_t      send_seq;       // outgoing sequence counter
    uint32_t      recv_seq;       // incoming sequence counter
    SessionState  state;          // lifecycle state
    uint8_t       nonce[32];      // auth challenge, zeroed after use
    bool          active;         // false = slot is free
} Session;
```

### Session States

```
SESSION_CONNECTING      just accepted, handshake not sent yet
SESSION_AUTHENTICATING  handshake sent, waiting for auth packets
SESSION_READY           authenticated, waiting for SQL
SESSION_BUSY            engine executing a query
SESSION_CLOSING         QUIT received, cleanup in progress
```

### Session Manager

```c
typedef struct {
    Session  slots[MAX_SESSIONS];   // fixed array, no malloc
    int      count;
} SessionManager;
```

Fixed-size slot array — no dynamic allocation, no fragmentation. A session is
created by marking a slot `active = true`, destroyed by marking it
`active = false`.

### Boundary

The session manager owns only the network layer:

| Owned by Session | Owned by Engine |
|---|---|
| client_fd | username |
| sequence numbers | partition |
| lifecycle state | schema |
| auth nonce | transaction state |

`conn_id` is the only bridge — server holds it, engine owns everything behind it.

### On Client Crash

If `session_recv()` returns `-1` (dead socket), the server calls
`engine_logout(conn_id)` and `session_destroy()`. The engine handles all
cleanup internally — rollback, partition release, connection pool slot.

```
server/
  include/session.h
  src/session.c
```

---

## Layer 4 — Auth Handler

Orchestrates the challenge-response exchange. Does zero credential
verification. All crypto lives in the engine.

### Auth Sequence

```
Server                              Client

send PKT_HANDSHAKE "MyDB 1.0"
state = SESSION_AUTHENTICATING
                                    receive HANDSHAKE
                                    send PKT_AUTH_INIT
                                      username = "root"

receive PKT_AUTH_INIT
engine_get_user_salt(username)
generate nonce → store in session
send PKT_AUTH_CHALLENGE
  salt  = a7f3c9...
  nonce = 8f3a9c2d...
                                    receive PKT_AUTH_CHALLENGE
                                    prompt: Enter password:
                                    h1       = SHA256(salt + password)
                                    response = SHA256(nonce + h1)
                                    send PKT_AUTH_RESPONSE
                                      username + response

receive PKT_AUTH_RESPONSE
engine_login(username, response, nonce)
  engine verifies internally
  returns conn_id
store conn_id in session
zero nonce in session
state = SESSION_READY
send PKT_AUTH_OK
                                    receive PKT_AUTH_OK
                                    show prompt: mydb>
```

### Design Decisions

**Fake challenge on unknown username.** If `engine_get_user_salt()` returns
`-1` (user does not exist), the server generates a random salt and sends a
challenge anyway. The response will fail verification. The client never learns
whether the username was wrong or the password was wrong — prevents username
enumeration.

**Nonce zeroed immediately after use.** `memset(s->nonce, 0, 32)` is called
the moment `engine_login()` returns, regardless of success or failure. A nonce
that stays in memory past its use is a security liability.

**Zero crypto in auth.c.** `auth.c` contains no SHA-256, no hash comparison,
no cryptographic operations. It only orchestrates packet exchange and calls
engine functions. If the hash algorithm changes, `auth.c` is untouched.

### New Engine Functions Required

```c
int engine_get_user_salt(EngineState *eng,
                         const char *username,
                         uint8_t *salt_out);

int engine_login(EngineState *eng,
                 const char *username,
                 const uint8_t *response,  // SHA256(nonce + stored_hash)
                 const uint8_t *nonce);    // returns conn_id or -1
```

```
server/
  include/auth.h
  src/auth.c
```

---

## Layer 5 — Dispatch

Routes an authenticated query to the engine and sends the response back.
The simplest layer — no SQL parsing, no result interpretation, no transaction
awareness.

```c
int dispatch_query(Session *s, EngineState *eng,
                   const void *payload, uint32_t len) {

    if (s->state != SESSION_READY) return -1;

    s->state = SESSION_BUSY;

    char result[RESULT_BUF_SIZE];
    engine_execute_sql(eng, s->conn_id, q->sql,
                       result, sizeof(result));

    s->state = SESSION_READY;
    return session_send(s, PKT_RESPONSE, result, strlen(result));
}
```

### Design Decisions

**Dispatch never parses SQL.** The raw string goes straight to
`engine_execute_sql()`. `BEGIN`, `COMMIT`, `ROLLBACK`, `USE`, DDL, DML,
DQL — all handled by the engine. Dispatch does not look at the SQL.

**SESSION_BUSY guards concurrent queries.** While the engine is running,
the session is marked `SESSION_BUSY` and excluded from the `poll()` watch
list. This prevents a second query arriving before the first result is sent.

**On QUIT, server calls `engine_logout(conn_id)` only.** The server does not
call `ROLLBACK` or manage transactions. `engine_logout()` tells the engine the
connection is closing. The engine walks the chain internally:

```
engine_logout(conn_id)
  → ConnectionPool finds Connection
  → gets partition_id
  → tells PartitionCtx: connection closing
  → PartitionCtx removes from SubConnPool
  → TxnManager: open transaction? → rollback
  → Connection slot freed
```

The server knows none of this happened.

```
server/
  include/dispatch.h
  src/dispatch.c
```

---

## Layer 6 — Server Main Loop

The `poll()` event loop that ties all layers together. One thread. Watches
every active file descriptor simultaneously.

### poll() Watch List

```
fds[0]    = listener_fd          new connections
fds[1..n] = session client_fds   existing client data
```

Sessions in `SESSION_BUSY` are excluded from the watch list — engine is
running for them, no new data expected.

### Loop Body

```
poll(fds, nfds, 5000)   wait up to 5 seconds

fds[0] ready (listener):
  client_fd = listener_accept()
  session   = session_create(client_fd)
  auth_send_handshake(session)

fds[i] ready (session):
  session_recv() → header + payload
  if recv fails → engine_logout + session_destroy (client crashed)
  else route by header.type:
    PKT_AUTH_INIT      → auth_handle_init()
    PKT_AUTH_RESPONSE  → auth_handle_response()
    PKT_QUERY          → dispatch_query()
    PKT_QUIT           → engine_logout + session_destroy
```

### Signal Handling

```c
signal(SIGINT,  signal_handler);   // Ctrl+C
signal(SIGTERM, signal_handler);   // systemctl stop

static void signal_handler(int sig) {
    server_stop(g_server);   // sets running = 0
}
```

When `running = 0`, the poll loop exits. `server_shutdown()` closes the
listener and calls `engine_shutdown()`. systemd sees a clean exit and does not
restart the service.

### Server Struct

```c
typedef struct {
    Listener        listener;
    SessionManager  sessions;
    EngineState    *eng;
    int             running;
} Server;
```

`engine_shutdown()` is called once — only on full server shutdown, never on
individual connection close.

```
server/
  include/server.h
  src/server.c
  src/main_server.c
```

---

## Layer 7 — Client Binary

`mydb` is a standalone binary with no engine inside. It knows how to connect
to the server, authenticate, read SQL from the terminal, and print responses.

### Auth Flow (Client Side)

```
receive PKT_HANDSHAKE          server version
send    PKT_AUTH_INIT          username
receive PKT_AUTH_CHALLENGE     salt + nonce
  compute h1       = SHA256(salt + password)
  compute response = SHA256(nonce + h1)
send    PKT_AUTH_RESPONSE      username + response
receive PKT_AUTH_OK / ERR
```

Password is read with `getpass()` — terminal echo disabled during input.
Password is zeroed from memory immediately after `PKT_AUTH_RESPONSE` is sent.

### REPL

```
mydb> SELECT * FROM users;
+----+-------+
| id | name  |
+----+-------+
|  1 | Alice |
+----+-------+

mydb> quit
Bye.
```

Client sends `PKT_QUERY`. Receives `PKT_RESPONSE`. Prints the string directly.
No formatting on the client side — the engine already formatted the output.

### Connection Resolution

```c
const char *home = getenv("MYDB_HOME");
if (home) snprintf(path, len, "%s/mydb.sock", home);
else      snprintf(path, len, "/run/mydb/mydb.sock");
```

If the server is not running, the client prints a clear error:

```
ERROR: cannot connect to MyDB server
       is mydb-server running?
```

```
client/
  include/client_conn.h
  include/client_repl.h
  src/client_conn.c
  src/client_repl.c
  src/main_client.c
```

---

## Layer 8 — systemd Integration

No code changes required. systemd integration is purely a service file and
OS configuration.

### Service File

```ini
[Unit]
Description=MyDB Database Engine
After=local-fs.target
Wants=local-fs.target

[Service]
Type=simple
ExecStart=/usr/local/bin/mydb-server
ExecStop=/bin/kill -TERM $MAINPID
Restart=on-failure
RestartSec=5

User=mydb
Group=mydb

Environment=MYDB_HOME=/var/lib/mydb

RuntimeDirectory=mydb
RuntimeDirectoryMode=0750

StandardOutput=journal
StandardError=journal
SyslogIdentifier=mydb-server

[Install]
WantedBy=multi-user.target
```

### Key Fields

| Field | Purpose |
|---|---|
| `Type=simple` | Process runs in foreground; systemd watches PID directly |
| `Restart=on-failure` | Auto-restart on crash, not on clean stop |
| `User=mydb` | Dedicated system user — not root |
| `Environment=MYDB_HOME` | Production data directory |
| `RuntimeDirectory=mydb` | systemd creates `/run/mydb/` before start, removes after stop |
| `StandardOutput=journal` | All server logs go to journald |

### Production Socket Path

Under systemd, `RuntimeDirectory=mydb` creates `/run/mydb/`. The socket lives
at `/run/mydb/mydb.sock` — the standard location for Unix socket files on
Linux. `MYDB_HOME` is set to `/var/lib/mydb` for data files.

### Installation

```bash
# build
cmake -B build && cmake --build build

# install binaries
install -m 755 build/mydb-server /usr/local/bin/mydb-server
install -m 755 build/mydb        /usr/local/bin/mydb

# create system user
useradd --system --no-create-home --shell /sbin/nologin mydb

# create data directory
mkdir -p /var/lib/mydb
chown mydb:mydb /var/lib/mydb
chmod 750 /var/lib/mydb

# install service file
install -m 644 packaging/mydb.service /etc/systemd/system/mydb.service

# reload systemd
systemctl daemon-reload

# initialise engine (one time only)
MYDB_HOME=/var/lib/mydb mydb-server init -u root

# enable and start
systemctl enable mydb
systemctl start mydb
```

### Operations

```bash
systemctl status mydb        check running
systemctl stop mydb          clean shutdown (SIGTERM)
systemctl restart mydb       restart
journalctl -u mydb -f        watch live logs
```

---

## Responsibility Boundaries

| Concern | Owner |
|---|---|
| Unix socket lifecycle | Listener |
| Packet framing, sequence numbers | Protocol |
| Network state per connection | Session Manager |
| Auth protocol exchange | Auth Handler |
| Nonce generation | Auth Handler |
| Credential verification | Engine (engine_login) |
| Partition loading | Engine (PartitionCtx) |
| Connection pool | Engine (ConnectionPool) |
| Transaction rollback on disconnect | Engine (TxnManager via engine_logout) |
| SQL routing | Dispatch |
| SQL parsing and execution | Engine |
| Result formatting | Engine |
| Service lifecycle | systemd |

---

## Distributed Database Note (Future)

The current design naturally extends to a distributed model. The server routes
connections by `partition_id`. Today, each `PartitionCtx` is local — a direct
function call. The abstraction is one step away:

```c
typedef struct {
    EngineHandleType  type;      // ENGINE_LOCAL or ENGINE_REMOTE
    uint32_t          partition_id;
    union {
        EngineState  *local;     // direct function call
        int           remote_fd; // TCP socket to remote engine node
    };
} EngineHandle;
```

With this, a single `mydb-server` can route to multiple engine instances on
different machines — the coordinator pattern used by Vitess, TiDB, and Citus.
The session, protocol, auth, and dispatch layers touch nothing. Only the
routing table and `EngineHandle` resolution change.

---

## Final Project Structure

```
MyDB/
├── server/
│   ├── include/
│   │   ├── listener.h
│   │   ├── protocol.h
│   │   ├── session.h
│   │   ├── auth.h
│   │   ├── dispatch.h
│   │   └── server.h
│   └── src/
│       ├── listener.c
│       ├── protocol.c
│       ├── session.c
│       ├── auth.c
│       ├── dispatch.c
│       ├── server.c
│       └── main_server.c
├── client/
│   ├── include/
│   │   ├── client_conn.h
│   │   └── client_repl.h
│   └── src/
│       ├── client_conn.c
│       ├── client_repl.c
│       └── main_client.c
└── packaging/
    └── mydb.service
```