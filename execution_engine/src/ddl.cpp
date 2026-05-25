/*
 * ddl.cpp — CREATE TABLE, DROP TABLE, CREATE DATABASE, DROP DATABASE,
 *            USE, SHOW TABLES, SHOW DATABASES handlers.
 *
 * Phase 3 implements this file.
 */

#include "ast_executor.hpp"
#include "result_fmt.hpp"

#include <cstdio>

int exec_create_table(EngineState * /*eng*/,
                      const CreateTableStatement * /*s*/,
                      char *out, size_t cap)
{
    std::snprintf(out, cap, "not implemented: CREATE TABLE");
    return MYDB_ERR;
}

int exec_drop_table(EngineState * /*eng*/,
                    const DropTableStatement * /*s*/,
                    char *out, size_t cap)
{
    std::snprintf(out, cap, "not implemented: DROP TABLE");
    return MYDB_ERR;
}

int exec_create_database(EngineState * /*eng*/,
                         const CreateDatabaseStatement * /*s*/,
                         char *out, size_t cap)
{
    std::snprintf(out, cap, "not implemented: CREATE DATABASE");
    return MYDB_ERR;
}

int exec_drop_database(EngineState * /*eng*/,
                       const DropDatabaseStatement * /*s*/,
                       char *out, size_t cap)
{
    std::snprintf(out, cap, "not implemented: DROP DATABASE");
    return MYDB_ERR;
}

int exec_use(EngineState * /*eng*/,
             const UseStatement * /*s*/,
             char *out, size_t cap)
{
    std::snprintf(out, cap, "not implemented: USE");
    return MYDB_ERR;
}

int exec_show_tables(EngineState * /*eng*/,
                     const ShowTablesStatement * /*s*/,
                     char *out, size_t cap)
{
    std::snprintf(out, cap, "not implemented: SHOW TABLES");
    return MYDB_ERR;
}

int exec_show_databases(EngineState * /*eng*/,
                        const ShowDatabasesStatement * /*s*/,
                        char *out, size_t cap)
{
    std::snprintf(out, cap, "not implemented: SHOW DATABASES");
    return MYDB_ERR;
}
