#ifndef MYDB_CLIENT_REPL_H
#define MYDB_CLIENT_REPL_H

#include "client_conn.h"

#ifdef __cplusplus
extern "C" {
#endif

/*
 * client_repl.h — terminal REPL (server Layer 7).
 *
 * readline loop: buffers input lines until a ';' terminator, ships the
 * whole statement over the connection, and prints the server's response
 * string verbatim (the engine already formatted it).  Returns when the
 * user types `exit` / `quit`, on Ctrl-D, or when the connection drops.
 */
void client_repl_run(ClientConn *c);

#ifdef __cplusplus
}
#endif

#endif /* MYDB_CLIENT_REPL_H */
