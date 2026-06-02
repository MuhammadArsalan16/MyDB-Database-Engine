/*
 * dispatch.cpp — route a parsed ASTNode to the correct statement handler.
 *
 * This is the only file that knows about all statement types.  Every
 * handler it calls owns its own logic; this switch stays thin.
 */

#include "ast_executor.hpp"

#include <cstdio>

int exec_dispatch(ExecContext *ectx, const ASTNode *node,
                  char *out, size_t cap)
{
    switch (node->type) {

    /* TCL */
    case StatementType::TRANSACTION:
        return exec_tcl(ectx,
                        static_cast<const TransactionStatement *>(node),
                        out, cap);

    /* DDL */
    case StatementType::CREATE_TABLE:
        return exec_create_table(ectx,
                                 static_cast<const CreateTableStatement *>(node),
                                 out, cap);
    case StatementType::CREATE_INDEX:
        return exec_create_index(ectx,
                                 static_cast<const CreateIndexStatement *>(node),
                                 out, cap);
    case StatementType::DROP_TABLE:
        return exec_drop_table(ectx,
                               static_cast<const DropTableStatement *>(node),
                               out, cap);
    case StatementType::CREATE_DATABASE:
        return exec_create_database(ectx,
                                    static_cast<const CreateDatabaseStatement *>(node),
                                    out, cap);
    case StatementType::DROP_DATABASE:
        return exec_drop_database(ectx,
                                  static_cast<const DropDatabaseStatement *>(node),
                                  out, cap);
    case StatementType::USE:
        return exec_use(ectx,
                        static_cast<const UseStatement *>(node),
                        out, cap);
    case StatementType::SHOW_TABLES:
        return exec_show_tables(ectx,
                                static_cast<const ShowTablesStatement *>(node),
                                out, cap);
    case StatementType::SHOW_DATABASES:
        return exec_show_databases(ectx,
                                   static_cast<const ShowDatabasesStatement *>(node),
                                   out, cap);
    case StatementType::SHOW_CURRENT_DB:
        return exec_database(ectx,
                             static_cast<const DatabaseStatement *>(node),
                             out, cap);
    case StatementType::SHOW_USERS:
        return exec_show_users(ectx,
                               static_cast<const ShowUsersStatement *>(node),
                               out, cap);
    case StatementType::SHOW_GRANTS:
        return exec_show_grants(ectx,
                                static_cast<const ShowGrantsStatement *>(node),
                                out, cap);
    case StatementType::DESCRIBE_TABLE:
        return exec_describe_table(ectx,
                                   static_cast<const DescribeTableStatement *>(node),
                                   out, cap);
    case StatementType::DESCRIBE_SCHEMA:
        return exec_describe_schema(ectx,
                                    static_cast<const DescribeSchemaStatement *>(node),
                                    out, cap);
    case StatementType::DESCRIBE_PARTITION:
        return exec_describe_partition(ectx,
                                       static_cast<const DescribePartitionStatement *>(node),
                                       out, cap);
    case StatementType::DISCONNECT:
        return exec_disconnect(ectx,
                               static_cast<const DisconnectStatement *>(node),
                               out, cap);

    /* DML */
    case StatementType::INSERT:
        return exec_insert(ectx,
                           static_cast<const InsertStatement *>(node),
                           out, cap);
    case StatementType::UPDATE:
        return exec_update(ectx,
                           static_cast<const UpdateStatement *>(node),
                           out, cap);
    case StatementType::DELETE:
        return exec_delete(ectx,
                           static_cast<const DeleteStatement *>(node),
                           out, cap);

    /* DQL */
    case StatementType::SELECT:
        return exec_select(ectx,
                           static_cast<const SelectStatement *>(node),
                           out, cap);

    /* Utility */
    case StatementType::ANALYZE_TABLE:
        return exec_analyze_table(ectx,
                                  static_cast<const AnalyzeTableStatement *>(node),
                                  out, cap);

    /* User management */
    case StatementType::CREATE_USER:
        return exec_create_user(ectx,
                                static_cast<const CreateUserStatement *>(node),
                                out, cap);
    case StatementType::DROP_USER:
        return exec_drop_user(ectx,
                              static_cast<const DropUserStatement *>(node),
                              out, cap);
    case StatementType::ALTER_USER:
        return exec_alter_user(ectx,
                               static_cast<const AlterUserStatement *>(node),
                               out, cap);

    default:
        std::snprintf(out, cap, "unsupported statement type");
        return MYDB_ERR;
    }
}
