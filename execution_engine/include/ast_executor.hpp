#pragma once
/*
 * ast_executor.hpp — internal per-statement handler declarations.
 *
 * Every SQL statement type has one handler function.  All handlers share
 * the same signature:
 *
 *   int exec_<stmt>(ExecContext *ectx, const <StmtType> *s,
 *                   char *out, size_t cap);
 *
 * Responsibilities of each handler:
 *   - Write a NUL-terminated result string into out (truncated to cap-1).
 *   - Return a MYDB_* code (MYDB_OK on success).
 *   - Own its own auto-commit bracket for DML (using g_in_explicit_txn).
 *
 * The dispatcher in dispatch.cpp calls these; exec_engine_api.cpp calls
 * the dispatcher.  Nothing outside execution_engine touches these headers.
 */

extern "C" {
#include "common.h"
#include "relation_def.h"
#include "engine.h"
#include "pm_api.h"        /* PartitionCtx, pm_* wrappers, Row/Cursor (storage.h) */
#include "exec_context.h"  /* ExecContext — the handler's first parameter */
}
#include "AST.hpp"
#include "parser_api.hpp"

/* ------------------------------------------------------------------
 * Auto-commit state
 * Defined in tcl.cpp.  DML handlers read it to decide whether to
 * wrap their operation in their own begin/commit pair.
 * ------------------------------------------------------------------ */
extern bool g_in_explicit_txn;

/* ------------------------------------------------------------------
 * Dispatcher
 * ------------------------------------------------------------------ */
int exec_dispatch(ExecContext *ectx, const ASTNode *node,
                  char *out, size_t cap);

/* ------------------------------------------------------------------
 * TCL  (src/tcl.cpp)
 * ------------------------------------------------------------------ */
int exec_tcl(ExecContext *ectx, const TransactionStatement *s,
             char *out, size_t cap);

/* ------------------------------------------------------------------
 * DDL  (src/ddl.cpp)
 * ------------------------------------------------------------------ */
int exec_create_table   (ExecContext *ectx, const CreateTableStatement    *s,
                         char *out, size_t cap);
int exec_create_index   (ExecContext *ectx, const CreateIndexStatement    *s,
                         char *out, size_t cap);
int exec_drop_table     (ExecContext *ectx, const DropTableStatement      *s,
                         char *out, size_t cap);
int exec_create_database(ExecContext *ectx, const CreateDatabaseStatement *s,
                         char *out, size_t cap);
int exec_drop_database  (ExecContext *ectx, const DropDatabaseStatement   *s,
                         char *out, size_t cap);
int exec_use            (ExecContext *ectx, const UseStatement            *s,
                         char *out, size_t cap);
int exec_show_tables    (ExecContext *ectx, const ShowTablesStatement     *s,
                         char *out, size_t cap);
int exec_show_databases (ExecContext *ectx, const ShowDatabasesStatement  *s,
                         char *out, size_t cap);
int exec_show_users     (ExecContext *ectx, const ShowUsersStatement      *s,
                         char *out, size_t cap);
int exec_database       (ExecContext *ectx, const DatabaseStatement       *s,
                         char *out, size_t cap);
int exec_show_grants    (ExecContext *ectx, const ShowGrantsStatement     *s,
                         char *out, size_t cap);
int exec_describe_table     (ExecContext *ectx, const DescribeTableStatement     *s,
                             char *out, size_t cap);
int exec_describe_schema    (ExecContext *ectx, const DescribeSchemaStatement    *s,
                             char *out, size_t cap);
int exec_describe_partition (ExecContext *ectx, const DescribePartitionStatement *s,
                             char *out, size_t cap);
int exec_disconnect         (ExecContext *ectx, const DisconnectStatement        *s,
                             char *out, size_t cap);

/* ------------------------------------------------------------------
 * DML  (src/dml.cpp)
 * ------------------------------------------------------------------ */
int exec_insert(ExecContext *ectx, const InsertStatement *s,
                char *out, size_t cap);
int exec_update(ExecContext *ectx, const UpdateStatement *s,
                char *out, size_t cap);
int exec_delete(ExecContext *ectx, const DeleteStatement *s,
                char *out, size_t cap);

/* ------------------------------------------------------------------
 * DQL  (src/dql.cpp)
 * ------------------------------------------------------------------ */
int exec_select(ExecContext *ectx, const SelectStatement *s,
                char *out, size_t cap);

/* ------------------------------------------------------------------
 * Utility  (src/ddl.cpp)
 * ------------------------------------------------------------------ */
int exec_analyze_table(ExecContext *ectx, const AnalyzeTableStatement *s,
                       char *out, size_t cap);

/* ------------------------------------------------------------------
 * User management  (src/ddl.cpp)
 * ------------------------------------------------------------------ */
int exec_create_user(ExecContext *ectx, const CreateUserStatement *s,
                     char *out, size_t cap);
int exec_drop_user  (ExecContext *ectx, const DropUserStatement   *s,
                     char *out, size_t cap);
int exec_alter_user (ExecContext *ectx, const AlterUserStatement  *s,
                     char *out, size_t cap);
