/*
 * ddl.cpp — CREATE TABLE, DROP TABLE, CREATE DATABASE, DROP DATABASE,
 *            USE, SHOW TABLES, SHOW DATABASES handlers.
 *
 * Guard macros used in most handlers:
 *
 *   REQUIRE_LOGIN     — returns error if user is not logged in.
 *   REQUIRE_SCHEMA    — returns error if no schema is active (USE not called).
 *   REQUIRE_PARTITION — returns error if user owns no partition.
 */

#include "ast_executor.hpp"
#include "exec_internal.h"
#include "result_fmt.hpp"
#include "value_cast.hpp"

#include <cstring>
#include <cstdio>

/*
 * Map the parser's data_type string to a DataType enum value.
 * Returns -1 for unrecognised type names.
 */
static int str_to_datatype(const char *s)
{
    if (strcmp(s, "INT")      == 0 ||
        strcmp(s, "INTEGER")  == 0) return TYPE_INT;
    if (strcmp(s, "DECIMAL")  == 0 ||
        strcmp(s, "NUMERIC")  == 0) return TYPE_DECIMAL;
    if (strcmp(s, "VARCHAR")  == 0) return TYPE_VARCHAR;
    if (strcmp(s, "ENUM")     == 0) return TYPE_ENUM;
    if (strcmp(s, "BOOL")     == 0 ||
        strcmp(s, "BOOLEAN")  == 0) return TYPE_BOOL;
    if (strcmp(s, "DATE")     == 0) return TYPE_DATE;
    if (strcmp(s, "DATETIME") == 0) return TYPE_DATETIME;
    return -1;
}

/* ======================================================================
 * CREATE DATABASE
 * ====================================================================== */

int exec_create_database(EngineState *eng,
                         const CreateDatabaseStatement *s,
                         char *out, size_t cap)
{
    REQUIRE_LOGIN(eng);
    REQUIRE_PARTITION(eng);

    int rc = storage_create_schema(s->name.c_str());
    if (rc != MYDB_OK) {
        format_error(rc, out, cap, s->name.c_str());
        return rc;
    }

    snprintf(out, cap, "Query OK, 1 row affected");
    return MYDB_OK;
}

/* ======================================================================
 * DROP DATABASE
 * ====================================================================== */

int exec_drop_database(EngineState * /*eng*/,
                       const DropDatabaseStatement * /*s*/,
                       char *out, size_t cap)
{
    /*
     * storage_drop_schema() does not exist in the storage API yet.
     * Dropping a schema requires removing all its relation files,
     * rmdir-ing the schema directory, and updating __catalog.mydb —
     * none of which are exposed through a single storage call today.
     * This will be supported once storage_drop_schema is added to
     * the storage engine.
     */
    snprintf(out, cap, "ERROR: DROP DATABASE not yet supported");
    return MYDB_ERR;
}

/* ======================================================================
 * USE
 * ====================================================================== */

int exec_use(EngineState *eng, const UseStatement *s,
             char *out, size_t cap)
{
    REQUIRE_LOGIN(eng);

    /*
     * If the user is inside an explicit transaction, USE does an
     * implicit COMMIT first (per design doc: USE flushes + swaps).
     * This matches MySQL behaviour.
     */
    if (g_in_explicit_txn) {
        storage_commit();
        g_in_explicit_txn = false;
    }

    /*
     * Flush any dirty pages from the old schema to disk before
     * swapping Cache 2.  In auto-commit mode this is a no-op (every
     * DML already committed).  With WAL this becomes critical — WAL
     * commits leave dirty pages in the buffer pool intentionally, so
     * we must force them out before losing the schema reference.
     */
    if (eng->schema_active)
        storage_flush_all_dirty();

    int rc = engine_use_schema(eng, s->schema_name.c_str());
    if (rc != MYDB_OK) {
        format_error(rc, out, cap, s->schema_name.c_str());
        return rc;
    }

    snprintf(out, cap, "Database changed");
    return MYDB_OK;
}

/* ======================================================================
 * CREATE TABLE
 * ====================================================================== */

int exec_create_table(EngineState *eng, const CreateTableStatement *s,
                      char *out, size_t cap)
{
    REQUIRE_LOGIN(eng);
    REQUIRE_SCHEMA(eng);

    if (s->columns.empty()) {
        snprintf(out, cap, "ERROR: CREATE TABLE with no columns");
        return MYDB_ERR;
    }
    if ((int)s->columns.size() > MAX_COLUMNS) {
        snprintf(out, cap, "ERROR: too many columns (max %d)", MAX_COLUMNS);
        return MYDB_ERR;
    }

    RelationDef rel;
    memset(&rel, 0, sizeof(rel));
    strncpy(rel.relation_name, s->table_name.c_str(), MAX_TABLE_NAME - 1);

    rel.num_columns = (uint8_t)s->columns.size();
    int pk_idx = -1;

    for (int i = 0; i < (int)s->columns.size(); i++) {
        const ASTColumnDef &ac = s->columns[(size_t)i];
        ColumnDef          &cd = rel.columns[i];

        /* --- name --- */
        strncpy(cd.name, ac.name.c_str(), MAX_COLUMN_NAME - 1);

        /* --- data type --- */
        int dt = str_to_datatype(ac.data_type.c_str());
        if (dt < 0) {
            snprintf(out, cap, "ERROR: unknown type '%s' for column '%s'",
                     ac.data_type.c_str(), ac.name.c_str());
            return MYDB_ERR;
        }
        cd.type = (DataType)dt;

        /* --- size / precision --- */
        cd.max_len = ac.max_len;
        cd.scale   = ac.scale;
        /* apply defaults when the user omitted them */
        if (cd.type == TYPE_VARCHAR && cd.max_len == 0) cd.max_len = MAX_VARCHAR_LEN;
        if (cd.type == TYPE_DECIMAL && cd.scale   == 0) cd.scale   = 2;

        /* --- constraints --- */
        /* PRIMARY KEY implies NOT NULL and UNIQUE */
        cd.is_primary_key    = ac.is_primary_key    ? 1 : 0;
        cd.is_not_null       = (ac.is_not_null || ac.is_primary_key) ? 1 : 0;
        cd.is_unique         = (ac.is_unique   || ac.is_primary_key) ? 1 : 0;
        cd.is_auto_increment = ac.is_auto_increment ? 1 : 0;

        /* track which column is the primary key */
        if (ac.is_primary_key) {
            if (pk_idx >= 0) {
                snprintf(out, cap,
                         "ERROR: table '%s' has more than one PRIMARY KEY",
                         s->table_name.c_str());
                return MYDB_ERR;
            }
            pk_idx = i;
        }

        /* --- ENUM values --- */
        if (cd.type == TYPE_ENUM) {
            cd.num_enum_values = (uint8_t)ac.enum_values.size();
            for (int j = 0; j < (int)ac.enum_values.size() && j < MAX_ENUM_VALUES; j++) {
                strncpy(cd.enum_values[j], ac.enum_values[(size_t)j].c_str(),
                        MAX_ENUM_STR_LEN - 1);
            }
        }

        /* --- DEFAULT value --- */
        if (ac.has_default) {
            cd.has_default = 1;

            /*
             * Special case: DEFAULT NOW / DEFAULT NOW()
             * We cannot encode "current timestamp" in a fixed Value, so we
             * store a sentinel: is_null=0, datetime_val=0 (year 0 is never
             * a real date).  The INSERT handler detects this sentinel and
             * substitutes the actual current timestamp at insert time.
             */
            if (ac.default_text == "NOW" || ac.default_text == "NOW()") {
                cd.default_value.type          = cd.type;
                cd.default_value.is_null       = 0;
                cd.default_value.v.datetime_val = 0;   /* sentinel = "use NOW" */
            } else {
                cd.default_value = cast_literal(ac.default_text, cd);
            }
        }
    }

    /* every table must have exactly one primary key */
    if (pk_idx < 0) {
        snprintf(out, cap,
                 "ERROR: table '%s' has no PRIMARY KEY — "
                 "every table must declare a PRIMARY KEY column",
                 s->table_name.c_str());
        return MYDB_ERR;
    }
    rel.pk_col_idx = (uint8_t)pk_idx;

    /*
     * Register secondary B+ tree indexes.
     *
     * Two sources:
     *   1. UNIQUE non-PK columns  → unique secondary index (is_secondary=1 in BTree)
     *   2. INDEXED non-PK columns → non-unique secondary index (is_secondary=2 in BTree)
     *
     * The BTree type is derived at open_table time from column.is_unique, so
     * the secondary_col_idx[] array does not need a separate "is_unique" field.
     * Here we just collect which columns need secondary indexes.
     */
    rel.num_secondary_indexes = 0;
    for (int i = 0; i < rel.num_columns; i++) {
        const ASTColumnDef &ac = s->columns[(size_t)i];
        /* Skip the PK column — it IS the clustered index */
        if (rel.columns[i].is_primary_key) continue;
        /* Include if UNIQUE or explicitly INDEXED */
        if (!rel.columns[i].is_unique && !ac.is_indexed) continue;
        if (rel.num_secondary_indexes >= MAX_SECONDARY_IDX) {
            snprintf(out, cap,
                     "ERROR: too many indexed columns (max %d secondary indexes)",
                     MAX_SECONDARY_IDX);
            return MYDB_ERR;
        }
        rel.secondary_col_idx[rel.num_secondary_indexes++] = (uint8_t)i;
    }

    /* --- foreign keys --- */
    rel.num_foreign_keys = 0;
    for (const auto &fk : s->foreign_keys) {
        if (rel.num_foreign_keys >= MAX_FOREIGN_KEYS) {
            snprintf(out, cap,
                     "ERROR: too many FOREIGN KEY constraints (max %d)",
                     MAX_FOREIGN_KEYS);
            return MYDB_ERR;
        }
        ForeignKey &rfk = rel.foreign_keys[rel.num_foreign_keys++];
        strncpy(rfk.constraint_name,   fk.constraint_name.c_str(), MAX_COLUMN_NAME - 1);
        strncpy(rfk.column_name,       fk.column_name.c_str(),     MAX_COLUMN_NAME - 1);
        strncpy(rfk.ref_relation_name, fk.ref_table.c_str(),       MAX_TABLE_NAME  - 1);
        strncpy(rfk.ref_column_name,   fk.ref_column.c_str(),      MAX_COLUMN_NAME - 1);

        /* Map parser's on_delete string to the storage constant */
        if (fk.on_delete == "CASCADE")
            rfk.on_delete_action = FK_ON_DELETE_CASCADE;
        else if (fk.on_delete == "SET_NULL")
            rfk.on_delete_action = FK_ON_DELETE_SET_NULL;
        else
            rfk.on_delete_action = FK_ON_DELETE_RESTRICT;   /* default */
    }

    int rc = storage_create_table(&rel);
    if (rc != MYDB_OK) {
        format_error(rc, out, cap, s->table_name.c_str());
        return rc;
    }

    snprintf(out, cap, "Query OK, 0 rows affected");
    return MYDB_OK;
}

/* ======================================================================
 * CREATE INDEX
 * ====================================================================== */

int exec_create_index(EngineState *eng, const CreateIndexStatement *s,
                      char *out, size_t cap)
{
    REQUIRE_LOGIN(eng);
    REQUIRE_SCHEMA(eng);

    /* Resolve the table */
    const RelationDef *rel = engine_find_relation(eng, s->table_name.c_str());
    if (!rel) {
        snprintf(out, cap, "ERROR: table '%s' does not exist",
                 s->table_name.c_str());
        return MYDB_ERR_NOT_FOUND;
    }

    /* Find the column */
    int col_idx = -1;
    for (int i = 0; i < rel->num_columns; i++) {
        if (s->column_name == rel->columns[i].name) {
            col_idx = i;
            break;
        }
    }
    if (col_idx < 0) {
        snprintf(out, cap, "ERROR: column '%s' not found in table '%s'",
                 s->column_name.c_str(), s->table_name.c_str());
        return MYDB_ERR_NOT_FOUND;
    }

    /* Cannot index the primary key column — it is already the clustered index */
    if (col_idx == rel->pk_col_idx) {
        snprintf(out, cap,
                 "ERROR: column '%s' is the PRIMARY KEY — already indexed",
                 s->column_name.c_str());
        return MYDB_ERR;
    }

    int rc = storage_add_index((RelationDef *)rel, col_idx);
    if (rc != MYDB_OK) {
        if (rc == MYDB_ERR_DUPLICATE)
            snprintf(out, cap,
                     "ERROR: column '%s' already has a secondary index",
                     s->column_name.c_str());
        else if (rc == MYDB_ERR_FULL)
            snprintf(out, cap,
                     "ERROR: too many indexes on table '%s' (max %d)",
                     s->table_name.c_str(), MAX_SECONDARY_IDX);
        else
            format_error(rc, out, cap, s->table_name.c_str());
        return rc;
    }

    snprintf(out, cap, "Query OK, 0 rows affected");
    return MYDB_OK;
}

/* ======================================================================
 * DROP TABLE
 * ====================================================================== */

int exec_drop_table(EngineState *eng, const DropTableStatement *s,
                    char *out, size_t cap)
{
    REQUIRE_LOGIN(eng);
    REQUIRE_SCHEMA(eng);

    const RelationDef *rel = engine_find_relation(eng, s->table_name.c_str());
    if (!rel) {
        snprintf(out, cap, "ERROR: table '%s' does not exist",
                 s->table_name.c_str());
        return MYDB_ERR_NOT_FOUND;
    }

    /*
     * storage_drop_table takes RelationDef * (non-const) because it
     * zeroes the definition after removing the file.  The const here
     * is the engine's read-only contract; storage is the single writer.
     */
    int rc = storage_drop_table((RelationDef *)rel);
    if (rc != MYDB_OK) {
        format_error(rc, out, cap, s->table_name.c_str());
        return rc;
    }

    snprintf(out, cap, "Query OK, 0 rows affected");
    return MYDB_OK;
}

/* ======================================================================
 * SHOW TABLES
 * ====================================================================== */

int exec_show_tables(EngineState *eng,
                     const ShowTablesStatement * /*s*/,
                     char *out, size_t cap)
{
    REQUIRE_LOGIN(eng);
    REQUIRE_SCHEMA(eng);

    ResultBuf rb(out, cap);

    /* header */
    char header[64];
    snprintf(header, sizeof(header),
             "Tables_in_%s", eng->current_schema_name);
    rb.append(header);
    rb.append("\n");

    size_t nrows = 0;
    int    limit = MAX_RELATIONS_PER_SCHEMA;

    for (int i = 0; i < limit; i++) {
        if (!eng->active_schema.relations[i].is_valid) continue;
        rb.append(eng->active_schema.relations[i].relation_name);
        rb.append("\n");
        nrows++;
    }

    rb.finalize(nrows);
    return MYDB_OK;
}

/* ======================================================================
 * SHOW DATABASES
 * ====================================================================== */

int exec_show_databases(EngineState *eng,
                        const ShowDatabasesStatement * /*s*/,
                        char *out, size_t cap)
{
    REQUIRE_LOGIN(eng);
    REQUIRE_PARTITION(eng);

    ResultBuf rb(out, cap);

    rb.append("Database\n");

    size_t nrows = 0;
    int    limit = MAX_SCHEMAS_PER_PARTITION;

    for (int i = 0; i < limit; i++) {
        if (!eng->active_catalog.schemas[i].is_valid) continue;
        rb.append(eng->active_catalog.schemas[i].schema_name);
        rb.append("\n");
        nrows++;
    }

    rb.finalize(nrows);
    return MYDB_OK;
}
