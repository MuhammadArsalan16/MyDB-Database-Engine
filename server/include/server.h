#ifndef MYDB_SERVER_H
#define MYDB_SERVER_H

#include <signal.h>

#include "listener.h"
#include "session.h"

#ifdef __cplusplus
extern "C" {
#endif

/*
 * server.h — the poll() event loop (server Layer 6).
 *
 * Single-threaded: one loop watches the listener fd and every active
 * session fd.  Exactly one statement executes at a time (a session is
 * marked SESSION_BUSY only for the synchronous duration of dispatch), so
 * the engine needs no locking yet — concurrency is a later layer.
 */

struct EngineState;

typedef struct {
    Listener            listener;
    SessionManager      sessions;
    struct EngineState *eng;       /* borrowed; owned by the caller */
    volatile sig_atomic_t running;
} Server;

/* Initialise the listener + session table.  `eng` must already be opened
 * (engine_init).  Returns 0 on success, -1 on error. */
int  server_init(Server *srv, struct EngineState *eng);

/* Install signal handlers and run the event loop until SIGINT/SIGTERM.
 * Returns 0 on clean shutdown. */
int  server_run(Server *srv);

/* Close the listener and destroy every session (engine_logout each first).
 * Does NOT close the engine — the caller owns that. */
void server_shutdown(Server *srv);

#ifdef __cplusplus
}
#endif

#endif /* MYDB_SERVER_H */
