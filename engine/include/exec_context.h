#ifndef EXEC_CONTEXT_H
#define EXEC_CONTEXT_H

#include "stats.h"

#ifdef __cplusplus
extern "C" {
#endif

/*
 * exec_context.h — per-query execution context (v3).
 *
 * ExecContext bundles the three resources the execution engine needs for
 * every statement:
 *
 *   engine    — global state (DatabaseFile, SystemSchema, StatsBuffer,
 *                ConnectionPool).  Required for DDL that touches global
 *                metadata (CREATE/DROP USER, SHOW DATABASES, etc.).
 *
 *   partition — per-partition state (Catalog, PartitionBuffer / Cache 2,
 *                TransactionManager, StorageEngine).  Required for all
 *                DML / DQL and schema-level DDL.
 *
 *   conn      — per-session state (login status, active schema name).
 *                Required for auth checks (REQUIRE_LOGIN, REQUIRE_SCHEMA).
 *
 *   stats     — StatsFile handle from the engine's StatsBuffer, resolved
 *                for the current (partition_id, schema_name) before each
 *                exec_engine_execute call.  NULL if ANALYZE TABLE has
 *                never been run for this schema; the planner treats NULL
 *                as "no stats available" and falls back to full-scan.
 *
 * The engine builds this struct inside engine_execute_sql() and passes
 * it to exec_engine_execute().  The execution engine never constructs
 * one itself.
 *
 * All pointers are borrowed for the duration of exec_engine_execute().
 * The engine owns the lifetime of every pointed-to object.
 */

/* Forward declarations — full definitions in their respective headers.
 * Implementation files that need the full types include those headers
 * directly. */
struct EngineState;
struct PartitionCtx;
struct Connection;

typedef struct {
    struct EngineState   *engine;    /* global: DatabaseFile, SystemSchema, StatsBuffer */
    struct PartitionCtx  *partition; /* per-partition: Catalog, Cache 2, TxnMgr, Storage */
    struct Connection    *conn;      /* per-session: auth state, active schema name */
    StatsFile            *stats;     /* planner input; NULL = no stats for this schema */
} ExecContext;

#ifdef __cplusplus
}
#endif

#endif /* EXEC_CONTEXT_H */
