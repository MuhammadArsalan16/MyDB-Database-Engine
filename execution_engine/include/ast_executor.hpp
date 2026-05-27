#pragma once
/*
 * ast_executor.hpp — internal per-statement handler declarations.
 *
 * Every SQL statement type has one handler function.  All handlers share
 * the same signature:
 *
 *   int exec_<stmt>(EngineState *eng, const <StmtType> *s,
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
#include "storage.h"
#include "engine.h"
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
int exec_dispatch(EngineState *eng, const ASTNode *node,
                  char *out, size_t cap);

/* ------------------------------------------------------------------
 * TCL  (src/tcl.cpp)
 * ------------------------------------------------------------------ */
int exec_tcl(EngineState *eng, const TransactionStatement *s,
             char *out, size_t cap);

/* ------------------------------------------------------------------
 * DDL  (src/ddl.cpp)
 * ------------------------------------------------------------------ */
int exec_create_table   (EngineState *eng, const CreateTableStatement    *s,
                         char *out, size_t cap);
int exec_create_index   (EngineState *eng, const CreateIndexStatement    *s,
                         char *out, size_t cap);
int exec_drop_table     (EngineState *eng, const DropTableStatement      *s,
                         char *out, size_t cap);
int exec_create_database(EngineState *eng, const CreateDatabaseStatement *s,
                         char *out, size_t cap);
int exec_drop_database  (EngineState *eng, const DropDatabaseStatement   *s,
                         char *out, size_t cap);
int exec_use            (EngineState *eng, const UseStatement            *s,
                         char *out, size_t cap);
int exec_show_tables    (EngineState *eng, const ShowTablesStatement     *s,
                         char *out, size_t cap);
int exec_show_databases (EngineState *eng, const ShowDatabasesStatement  *s,
                         char *out, size_t cap);
int exec_describe_table     (EngineState *eng, const DescribeTableStatement     *s,
                             char *out, size_t cap);
int exec_describe_schema    (EngineState *eng, const DescribeSchemaStatement    *s,
                             char *out, size_t cap);
int exec_describe_partition (EngineState *eng, const DescribePartitionStatement *s,
                             char *out, size_t cap);
int exec_disconnect         (EngineState *eng, const DisconnectStatement        *s,
                             char *out, size_t cap);

/* ------------------------------------------------------------------
 * DML  (src/dml.cpp)
 * ------------------------------------------------------------------ */
int exec_insert(EngineState *eng, const InsertStatement *s,
                char *out, size_t cap);
int exec_update(EngineState *eng, const UpdateStatement *s,
                char *out, size_t cap);
int exec_delete(EngineState *eng, const DeleteStatement *s,
                char *out, size_t cap);

/* ------------------------------------------------------------------
 * DQL  (src/dql.cpp)
 * ------------------------------------------------------------------ */
int exec_select(EngineState *eng, const SelectStatement *s,
                char *out, size_t cap);

/* ------------------------------------------------------------------
 * Utility  (src/ddl.cpp)
 * ------------------------------------------------------------------ */
int exec_analyze_table(EngineState *eng, const AnalyzeTableStatement *s,
                       char *out, size_t cap);

/* ------------------------------------------------------------------
 * User management  (src/ddl.cpp)
 * ------------------------------------------------------------------ */
int exec_create_user(EngineState *eng, const CreateUserStatement *s,
                     char *out, size_t cap);
int exec_drop_user  (EngineState *eng, const DropUserStatement   *s,
                     char *out, size_t cap);
int exec_alter_user (EngineState *eng, const AlterUserStatement  *s,
                     char *out, size_t cap);
