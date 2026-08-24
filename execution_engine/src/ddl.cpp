/*
 * ddl.cpp — CREATE TABLE, DROP TABLE, CREATE DATABASE, DROP DATABASE,
 *            USE, SHOW TABLES, SHOW DATABASES,
 *            CREATE USER, DROP USER, ALTER USER handlers.
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
#include "stats.h"
#include "relation_guard.hpp"

#include <cstring>
#include <cstdio>
#include <cctype>
#include <string>
#include <stdexcept>

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

int exec_create_database(ExecContext *ectx,
                         const CreateDatabaseStatement *s,
                         char *out, size_t cap)
{
    REQUIRE_LOGIN(ectx);
    REQUIRE_PARTITION(ectx);

    int rc = pm_create_schema(ectx->partition, s->name.c_str());
    if (rc != MYDB_OK) {
        if (rc == MYDB_ERR_DUPLICATE)
            snprintf(out, cap, "  Error: database '%s' already exists", s->name.c_str());
        else
            format_error(rc, out, cap, s->name.c_str());
        return rc;
    }

    snprintf(out, cap, "OK  Database '%s' created", s->name.c_str());
    return MYDB_OK;
}

/* ======================================================================
 * DROP DATABASE
 * ====================================================================== */

int exec_drop_database(ExecContext *ectx,
                       const DropDatabaseStatement *s,
                       char *out, size_t cap)
{
    REQUIRE_LOGIN(ectx);
    REQUIRE_PARTITION(ectx);

    /* The user must not be inside the schema they are dropping.
     * pm_drop_schema rejects that with MYDB_ERR — surface a
     * clearer message here. */
    if (ectx->conn->schema_active &&
        s->name == ectx->conn->current_schema_name) {
        snprintf(out, cap,
                 "  Error: cannot drop the currently active database '%s' "
                 "— run USE <another_db> or disconnect first",
                 s->name.c_str());
        return MYDB_ERR;
    }

    int rc = pm_drop_schema(ectx->partition, s->name.c_str());
    if (rc == MYDB_ERR_NOT_FOUND) {
        snprintf(out, cap, "  Error: database '%s' does not exist", s->name.c_str());
        return rc;
    }
    if (rc != MYDB_OK) {
        format_error(rc, out, cap, s->name.c_str());
        return rc;
    }

    /* The schema's stats file lives in the engine-owned pool
     * (system_schema/stats/), outside the partition — remove it here since
     * partition_manager has no visibility into the StatsBuffer. */
    sb_remove(&ectx->engine->stats_buf, ectx->conn->partition_id,
              s->name.c_str());

    snprintf(out, cap, "OK  Database '%s' dropped", s->name.c_str());
    return MYDB_OK;
}

/* ======================================================================
 * DISCONNECT
 * ====================================================================== */

int exec_disconnect(ExecContext *ectx,
                    const DisconnectStatement * /*s*/,
                    char *out, size_t cap)
{
    REQUIRE_LOGIN(ectx);

    if (!ectx->conn->schema_active) {
        snprintf(out, cap, "  Error: no active database — nothing to disconnect from");
        return MYDB_ERR;
    }

    /* Commit any open transaction before closing the schema (mirrors USE). */
    if (g_in_explicit_txn) {
        pm_commit(ectx->partition);
        g_in_explicit_txn = false;
    }

    char prev[64];
    strncpy(prev, ectx->conn->current_schema_name, sizeof(prev) - 1);
    prev[sizeof(prev) - 1] = '\0';

    int rc = engine_deactivate_schema(ectx->engine);
    if (rc != MYDB_OK) {
        snprintf(out, cap, "  Error: failed to disconnect from '%s'", prev);
        return rc;
    }

    snprintf(out, cap,
             "OK  Disconnected from '%s'  (no active database)", prev);
    return MYDB_OK;
}

/* ======================================================================
 * USE
 * ====================================================================== */

int exec_use(ExecContext *ectx, const UseStatement *s,
             char *out, size_t cap)
{
    REQUIRE_LOGIN(ectx);

    /*
     * If the user is inside an explicit transaction, USE does an
     * implicit COMMIT first (per design doc: USE flushes + swaps).
     * This matches MySQL behaviour.
     */
    if (g_in_explicit_txn) {
        pm_commit(ectx->partition);
        g_in_explicit_txn = false;
    }

    /*
     * Flushing dirty pages from the old schema before swapping Cache 2 is
     * now the engine's responsibility: engine_use_schema deactivates the
     * current schema via pctx_deactivate_schema, which flushes through the
     * partition's StorageEngine.  In auto-commit mode this is a no-op
     * (every DML already committed); with WAL it becomes critical.  The
     * execution engine no longer touches storage directly.
     */
    int rc = engine_use_schema(ectx->engine, s->schema_name.c_str());
    if (rc != MYDB_OK) {
        format_error(rc, out, cap, s->schema_name.c_str());
        return rc;
    }

    snprintf(out, cap, "OK  Database changed to '%s'", s->schema_name.c_str());
    return MYDB_OK;
}

/* ======================================================================
 * CREATE TABLE
 * ====================================================================== */

int exec_create_table(ExecContext *ectx, const CreateTableStatement *s,
                      char *out, size_t cap)
{
    REQUIRE_LOGIN(ectx);
    REQUIRE_SCHEMA(ectx);

    if (s->columns.empty()) {
        snprintf(out, cap, "  Error: CREATE TABLE with no columns");
        return MYDB_ERR;
    }
    if ((int)s->columns.size() > MAX_COLUMNS) {
        snprintf(out, cap, "  Error: too many columns (max %d)", MAX_COLUMNS);
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
            snprintf(out, cap, "  Error: unknown type '%s' for column '%s'",
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
                         "  Error: table '%s' has more than one PRIMARY KEY",
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
                if (cd.type != TYPE_DATETIME) {
                    snprintf(out, cap,
                             "  Error: DEFAULT NOW is only valid for DATETIME columns"
                             " (column '%s')", ac.name.c_str());
                    return MYDB_ERR;
                }
                cd.default_value.type           = cd.type;
                cd.default_value.is_null        = 0;
                cd.default_value.v.datetime_val = 0;   /* sentinel = "use NOW" */
            } else {
                /* Reject literals whose text cannot be parsed as the column's type. */
                if (!validate_literal(ac.default_text, cd)) {
                    if (cd.type == TYPE_INT)
                        snprintf(out, cap,
                                 "  Error: DEFAULT value '%s' is out of range for column '%s'"
                                 " (INT is 32-bit signed: -2147483648 to 2147483647)",
                                 ac.default_text.c_str(), ac.name.c_str());
                    else
                        snprintf(out, cap,
                                 "  Error: DEFAULT value '%s' is not valid for column '%s' (%s)",
                                 ac.default_text.c_str(), ac.name.c_str(),
                                 ac.data_type.c_str());
                    return MYDB_ERR;
                }
                cd.default_value = cast_literal(ac.default_text, cd);
            }
        }
    }

    /* every table must have exactly one primary key */
    if (pk_idx < 0) {
        snprintf(out, cap,
                 "  Error: table '%s' has no PRIMARY KEY — "
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
                     "  Error: too many indexed columns (max %d secondary indexes)",
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
                     "  Error: too many FOREIGN KEY constraints (max %d)",
                     MAX_FOREIGN_KEYS);
            return MYDB_ERR;
        }

        /* (1) FK column must exist in the table being created. */
        int fk_col_idx = -1;
        for (int i = 0; i < (int)rel.num_columns; i++) {
            if (fk.column_name == rel.columns[i].name) {
                fk_col_idx = i;
                break;
            }
        }
        if (fk_col_idx < 0) {
            snprintf(out, cap,
                     "  Error: FOREIGN KEY column '%s' does not exist in table '%s'",
                     fk.column_name.c_str(), s->table_name.c_str());
            return MYDB_ERR_NOT_FOUND;
        }

        /* (2) Referenced table must exist in the active schema. */
        RelationGuard ref_rel_guard(ectx->partition,
                                     pm_find_relation_const(ectx->partition, fk.ref_table.c_str()));
        const RelationDef *ref_rel = ref_rel_guard.get();
        if (!ref_rel) {
            snprintf(out, cap,
                     "  Error: FOREIGN KEY references unknown table '%s'",
                     fk.ref_table.c_str());
            return MYDB_ERR_NOT_FOUND;
        }

        /* (3) Referenced column must exist in the referenced table. */
        int ref_col_idx = -1;
        for (int i = 0; i < ref_rel->num_columns; i++) {
            if (fk.ref_column == ref_rel->columns[i].name) {
                ref_col_idx = i;
                break;
            }
        }
        if (ref_col_idx < 0) {
            snprintf(out, cap,
                     "  Error: FOREIGN KEY references unknown column '%s' in table '%s'",
                     fk.ref_column.c_str(), fk.ref_table.c_str());
            return MYDB_ERR_NOT_FOUND;
        }

        /* (4) Data types must be compatible (same base type). */
        if (rel.columns[fk_col_idx].type != ref_rel->columns[ref_col_idx].type) {
            snprintf(out, cap,
                     "  Error: FOREIGN KEY type mismatch — '%s.%s' and '%s.%s' have different types",
                     s->table_name.c_str(), fk.column_name.c_str(),
                     fk.ref_table.c_str(),  fk.ref_column.c_str());
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

    int rc = pm_create_table(ectx->partition, &rel);
    if (rc != MYDB_OK) {
        if (rc == MYDB_ERR_DUPLICATE)
            snprintf(out, cap, "  Error: table '%s' already exists",
                     s->table_name.c_str());
        else
            format_error(rc, out, cap, s->table_name.c_str());
        return rc;
    }

    snprintf(out, cap, "OK  Table '%s' created", s->table_name.c_str());
    return MYDB_OK;
}

/* ======================================================================
 * CREATE INDEX
 * ====================================================================== */

int exec_create_index(ExecContext *ectx, const CreateIndexStatement *s,
                      char *out, size_t cap)
{
    REQUIRE_LOGIN(ectx);
    REQUIRE_SCHEMA(ectx);

    /* Resolve the table */
    RelationGuard rel_guard(ectx->partition,
                             pm_find_relation_const(ectx->partition, s->table_name.c_str()));
    const RelationDef *rel = rel_guard.get();
    if (!rel) {
        snprintf(out, cap, "  Error: table '%s' does not exist",
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
        snprintf(out, cap, "  Error: column '%s' not found in table '%s'",
                 s->column_name.c_str(), s->table_name.c_str());
        return MYDB_ERR_NOT_FOUND;
    }

    /* Cannot index the primary key column — it is already the clustered index */
    if (col_idx == rel->pk_col_idx) {
        snprintf(out, cap,
                 "  Error: column '%s' is the PRIMARY KEY — already indexed",
                 s->column_name.c_str());
        return MYDB_ERR;
    }

    int rc = pm_add_index(ectx->partition, (RelationDef *)rel, col_idx);
    if (rc != MYDB_OK) {
        if (rc == MYDB_ERR_DUPLICATE)
            snprintf(out, cap,
                     "  Error: column '%s' already has a secondary index",
                     s->column_name.c_str());
        else if (rc == MYDB_ERR_FULL)
            snprintf(out, cap,
                     "  Error: too many indexes on table '%s' (max %d)",
                     s->table_name.c_str(), MAX_SECONDARY_IDX);
        else
            format_error(rc, out, cap, s->table_name.c_str());
        return rc;
    }

    snprintf(out, cap, "OK  Index created on '%s'('%s')",
             s->table_name.c_str(), s->column_name.c_str());
    return MYDB_OK;
}

/* ======================================================================
 * DROP TABLE
 * ====================================================================== */

int exec_drop_table(ExecContext *ectx, const DropTableStatement *s,
                    char *out, size_t cap)
{
    REQUIRE_LOGIN(ectx);
    REQUIRE_SCHEMA(ectx);

    RelationGuard rel_guard(ectx->partition,
                             pm_find_relation_const(ectx->partition, s->table_name.c_str()));
    const RelationDef *rel = rel_guard.get();
    if (!rel) {
        snprintf(out, cap, "  Error: table '%s' does not exist",
                 s->table_name.c_str());
        return MYDB_ERR_NOT_FOUND;
    }

    /*
     * pm_drop_table takes RelationDef * (non-const) because it
     * zeroes the definition after removing the file.  The const here
     * is the engine's read-only contract; storage is the single writer.
     */
    int rc = pm_drop_table(ectx->partition, (RelationDef *)rel);
    if (rc != MYDB_OK) {
        format_error(rc, out, cap, s->table_name.c_str());
        return rc;
    }

    snprintf(out, cap, "OK  Table '%s' dropped", s->table_name.c_str());
    return MYDB_OK;
}

/* ======================================================================
 * SHOW TABLES
 * ====================================================================== */

int exec_show_tables(ExecContext *ectx,
                     const ShowTablesStatement * /*s*/,
                     char *out, size_t cap)
{
    REQUIRE_LOGIN(ectx);
    REQUIRE_SCHEMA(ectx);

    char hdr[64];
    snprintf(hdr, sizeof(hdr), "Tables_in_%s", ectx->conn->current_schema_name);

    ResultBuf    rb(out, cap);
    TableBuilder tb;
    tb.set_headers({hdr});

    for (int i = 0; i < MAX_RELATIONS_PER_SCHEMA; i++) {
        if (!pctx_active_schema(ectx->partition)->relations[i].is_valid) continue;
        tb.add_row({pctx_active_schema(ectx->partition)->relations[i].relation_name});
    }

    tb.render(rb);
    return MYDB_OK;
}

/* ======================================================================
 * SHOW DATABASES
 * ====================================================================== */

int exec_show_databases(ExecContext *ectx,
                        const ShowDatabasesStatement * /*s*/,
                        char *out, size_t cap)
{
    REQUIRE_LOGIN(ectx);
    REQUIRE_PARTITION(ectx);

    ResultBuf    rb(out, cap);
    TableBuilder tb;
    tb.set_headers({"Database"});

    for (int i = 0; i < MAX_SCHEMAS_PER_PARTITION; i++) {
        if (!ectx->partition->catalog.schemas[i].is_valid) continue;
        tb.add_row({ectx->partition->catalog.schemas[i].schema_name});
    }

    tb.render(rb);
    return MYDB_OK;
}

static void fmt_datetime_ts(uint64_t dt, char *buf, size_t cap);

/* ======================================================================
 * DATABASE
 * ====================================================================== */

int exec_database(ExecContext *ectx,
                  const DatabaseStatement * /*s*/,
                  char *out, size_t cap)
{
    REQUIRE_LOGIN(ectx);

    if (!ectx->conn->schema_active)
        snprintf(out, cap, "No database selected");
    else
        snprintf(out, cap, "%s", ectx->conn->current_schema_name);

    return MYDB_OK;
}

/* ======================================================================
 * SHOW USERS  (root only)
 * ====================================================================== */

int exec_show_users(ExecContext *ectx,
                    const ShowUsersStatement * /*s*/,
                    char *out, size_t cap)
{
    REQUIRE_LOGIN(ectx);

    if (ectx->conn->user_id != 1) {
        snprintf(out, cap, "  Error: SHOW USERS requires root privileges");
        return MYDB_ERR_PERM;
    }

    ResultBuf    rb(out, cap);
    TableBuilder tb;
    tb.set_headers({"user_id", "username", "is_active", "created_at", "last_login"});

    const UsersFile *uf = &ectx->engine->system_schema.users;
    for (int i = 0; i < USERS_MAX_SLOTS; i++) {
        const UserSlot *u = &uf->slots[i];
        if (!u->is_valid) continue;

        char created[24], last_login[24];
        fmt_datetime_ts(u->created_at, created,    sizeof(created));
        fmt_datetime_ts(u->last_login, last_login, sizeof(last_login));

        tb.add_row({
            std::to_string(u->user_id),
            u->username,
            u->is_active ? "active" : "inactive",
            created,
            last_login
        });
    }

    tb.render(rb);
    return MYDB_OK;
}

/* ======================================================================
 * SHOW GRANTS [user_id]
 * ====================================================================== */

int exec_show_grants(ExecContext *ectx,
                     const ShowGrantsStatement *s,
                     char *out, size_t cap)
{
    REQUIRE_LOGIN(ectx);

    uint32_t target_id = (s->user_id == 0) ? ectx->conn->user_id : s->user_id;

    if (target_id != ectx->conn->user_id && ectx->conn->user_id != 1) {
        snprintf(out, cap,
                 "  Error: SHOW GRANTS for another user requires root privileges");
        return MYDB_ERR_PERM;
    }

    /* Resolve target username for display */
    UserSlot target_slot;
    if (users_find_by_id(&ectx->engine->system_schema.users, target_id, &target_slot)
            != MYDB_OK) {
        snprintf(out, cap, "  Error: user_id %u does not exist", target_id);
        return MYDB_ERR_NOT_FOUND;
    }

    ResultBuf    rb(out, cap);
    TableBuilder tb;
    tb.set_headers({"privilege_id", "grantee", "schema_name",
                    "partition_id", "granted_by", "granted_at"});

    const PrivilegesFile *pf = &ectx->engine->system_schema.privileges;
    for (int i = 0; i < PRIVILEGES_MAX_SLOTS; i++) {
        const PrivilegeSlot *p = &pf->slots[i];
        if (!p->is_valid) continue;
        if (p->grantee_id != target_id) continue;

        /* Resolve granter username */
        UserSlot granter;
        char granter_name[MAX_USERNAME + 16];
        if (users_find_by_id(&ectx->engine->system_schema.users,
                              p->granted_by, &granter) == MYDB_OK)
            snprintf(granter_name, sizeof(granter_name), "%s", granter.username);
        else
            snprintf(granter_name, sizeof(granter_name), "#%u", p->granted_by);

        char granted_at[24];
        fmt_datetime_ts(p->granted_at, granted_at, sizeof(granted_at));

        tb.add_row({
            std::to_string(p->privilege_id),
            target_slot.username,
            p->schema_name,
            std::to_string(p->partition_id),
            granter_name,
            granted_at
        });
    }

    tb.render(rb);
    return MYDB_OK;
}

/* ======================================================================
 * ANALYZE TABLE
 * ====================================================================== */

int exec_analyze_table(ExecContext *ectx, const AnalyzeTableStatement *s,
                       char *out, size_t cap)
{
    REQUIRE_LOGIN(ectx);
    REQUIRE_SCHEMA(ectx);

    RelationGuard rel_guard(ectx->partition,
                             pm_find_relation_const(ectx->partition, s->table_name.c_str()));
    const RelationDef *rel = rel_guard.get();
    if (!rel) {
        snprintf(out, cap, "  Error: table '%s' does not exist",
                 s->table_name.c_str());
        return MYDB_ERR_NOT_FOUND;
    }

    /*
     * Resolve the relation's slot in the active SchemaFile.  pm_analyze_table
     * writes the computed stats into sf->pages[slot_idx]; the planner later
     * reads the same slot.  pm_find_relation_const returns &defs[i], so the
     * pointer difference gives i directly (parallel arrays).
     */
    SchemaFile *as       = pctx_active_schema(ectx->partition);
    int         slot_idx = (int)(rel - as->defs);
    if (slot_idx < 0 || slot_idx >= MAX_RELATIONS_PER_SCHEMA) {
        snprintf(out, cap, "  Error: cannot locate '%s' in schema",
                 s->table_name.c_str());
        return MYDB_ERR;
    }

    /*
     * The engine resolved this schema's StatsFile* (opened/created under
     * system_schema/stats/) into ectx->stats before dispatch.  ANALYZE
     * writes through that pooled handle; the planner reads it later.  The
     * StatsBuffer owns the handle's lifecycle — we neither open nor close.
     */
    if (!ectx->stats) {
        snprintf(out, cap,
                 "  Error: statistics storage unavailable for this schema");
        return MYDB_ERR;
    }

    int rc = pm_analyze_table(ectx->partition, (RelationDef *)rel,
                              ectx->stats, slot_idx);
    if (rc != MYDB_OK) {
        format_error(rc, out, cap, s->table_name.c_str());
        return rc;
    }

    snprintf(out, cap, "OK  Statistics updated for '%s'",
             s->table_name.c_str());
    return MYDB_OK;
}

/* ======================================================================
 * DESCRIBE [TABLE] t
 * ====================================================================== */

/* Format the data type of a column into buf (e.g. "VARCHAR(50)", "DECIMAL(10,2)"). */
static void fmt_col_type(const ColumnDef *col, char *buf, size_t cap)
{
    switch (col->type) {
    case TYPE_INT:
        snprintf(buf, cap, "INT");
        break;
    case TYPE_DECIMAL:
        snprintf(buf, cap, "DECIMAL(%u,%u)", col->max_len, col->scale);
        break;
    case TYPE_VARCHAR:
        snprintf(buf, cap, "VARCHAR(%u)", col->max_len);
        break;
    case TYPE_BOOL:
        snprintf(buf, cap, "BOOL");
        break;
    case TYPE_DATE:
        snprintf(buf, cap, "DATE");
        break;
    case TYPE_DATETIME:
        snprintf(buf, cap, "DATETIME");
        break;
    case TYPE_ENUM: {
        int off = snprintf(buf, cap, "ENUM(");
        for (int i = 0; i < col->num_enum_values && off < (int)cap - 2; i++) {
            if (i) off += snprintf(buf + off, cap - (size_t)off, ",");
            off += snprintf(buf + off, cap - (size_t)off, "%s", col->enum_values[i]);
        }
        snprintf(buf + off, cap - (size_t)off, ")");
        break;
    }
    default:
        snprintf(buf, cap, "UNKNOWN");
    }
}

/* Format the default value of a column into buf.
 * Returns "NULL" when no default is set. */
static void fmt_default(const ColumnDef *col, char *buf, size_t cap)
{
    if (!col->has_default) {
        snprintf(buf, cap, "NULL");
        return;
    }
    const Value *v = &col->default_value;
    switch (col->type) {
    case TYPE_INT:
        snprintf(buf, cap, "%d", v->v.int_val);
        break;
    case TYPE_DECIMAL: {
        int64_t dv     = v->v.decimal_val;
        int64_t factor = 1;
        for (int i = 0; i < col->scale; i++) factor *= 10;
        int64_t whole  =  dv / factor;
        int64_t frac   = (dv % factor < 0) ? -(dv % factor) : (dv % factor);
        snprintf(buf, cap, "%lld.%0*lld", (long long)whole, col->scale, (long long)frac);
        break;
    }
    case TYPE_VARCHAR:
        snprintf(buf, cap, "'%.*s'", (int)v->v.varchar_val.len, v->v.varchar_val.data);
        break;
    case TYPE_BOOL:
        snprintf(buf, cap, "%s", v->v.bool_val ? "TRUE" : "FALSE");
        break;
    case TYPE_DATE:
        snprintf(buf, cap, "%08d", v->v.date_val);
        break;
    case TYPE_DATETIME:
        snprintf(buf, cap, "%lld", (long long)v->v.datetime_val);
        break;
    case TYPE_ENUM:
        snprintf(buf, cap, "%s", col->enum_values[v->v.enum_val]);
        break;
    default:
        snprintf(buf, cap, "NULL");
    }
}

/* Format a YYYYMMDDHHmmSS timestamp (as stored in SchemaHeader / CatalogHeader)
 * into "YYYY-MM-DD HH:MM:SS". Writes "N/A" for a zero value. */
static void fmt_datetime_ts(uint64_t dt, char *buf, size_t cap)
{
    if (dt == 0) { snprintf(buf, cap, "N/A"); return; }
    int sec  = (int)(dt % 100);  dt /= 100;
    int min  = (int)(dt % 100);  dt /= 100;
    int hour = (int)(dt % 100);  dt /= 100;
    int day  = (int)(dt % 100);  dt /= 100;
    int mon  = (int)(dt % 100);  dt /= 100;
    int year = (int)dt;
    snprintf(buf, cap, "%04d-%02d-%02d %02d:%02d:%02d",
             year, mon, day, hour, min, sec);
}

/* Format a byte count into a human-readable string (B / KB / MB / GB). */
static void fmt_bytes(uint64_t bytes, char *buf, size_t cap)
{
    if      (bytes >= 1024ULL * 1024ULL * 1024ULL)
        snprintf(buf, cap, "%.2f GB", (double)bytes / (1024.0 * 1024.0 * 1024.0));
    else if (bytes >= 1024ULL * 1024ULL)
        snprintf(buf, cap, "%.2f MB", (double)bytes / (1024.0 * 1024.0));
    else if (bytes >= 1024ULL)
        snprintf(buf, cap, "%.2f KB", (double)bytes / 1024.0);
    else
        snprintf(buf, cap, "%llu B",  (unsigned long long)bytes);
}

/* Format a stats min/max numeric (int64 encoded) into a human-readable string.
 * Encoding matches the planner's int64 convention:
 *   INT      → widened int32
 *   DECIMAL  → value * 10^scale
 *   DATE     → YYYYMMDD
 *   DATETIME → YYYYMMDDHHmmSS
 *   BOOL/ENUM→ raw index
 *   VARCHAR  → not applicable (writes "N/A") */
static void fmt_stat_numeric(int64_t v, const ColumnDef *col, char *buf, size_t cap)
{
    switch (col->type) {
    case TYPE_INT:
        snprintf(buf, cap, "%d", (int32_t)v);
        break;
    case TYPE_DECIMAL: {
        int64_t factor = 1;
        for (int i = 0; i < col->scale; i++) factor *= 10;
        int64_t whole = v / factor;
        int64_t frac  = (v % factor < 0) ? -(v % factor) : (v % factor);
        snprintf(buf, cap, "%lld.%0*lld", (long long)whole, col->scale, (long long)frac);
        break;
    }
    case TYPE_DATE: {
        int y = (int)(v / 10000);
        int m = (int)((v / 100) % 100);
        int d = (int)(v % 100);
        snprintf(buf, cap, "%04d-%02d-%02d", y, m, d);
        break;
    }
    case TYPE_DATETIME: {
        int64_t dt = v;
        int sec  = (int)(dt % 100); dt /= 100;
        int min  = (int)(dt % 100); dt /= 100;
        int hour = (int)(dt % 100); dt /= 100;
        int day  = (int)(dt % 100); dt /= 100;
        int mon  = (int)(dt % 100); dt /= 100;
        int year = (int)dt;
        snprintf(buf, cap, "%04d-%02d-%02d %02d:%02d:%02d",
                 year, mon, day, hour, min, sec);
        break;
    }
    case TYPE_BOOL:
        snprintf(buf, cap, "%s", v ? "TRUE" : "FALSE");
        break;
    case TYPE_ENUM:
        /* show the string label if in range, else raw index */
        if (v >= 0 && v < col->num_enum_values)
            snprintf(buf, cap, "%s", col->enum_values[(int)v]);
        else
            snprintf(buf, cap, "%lld", (long long)v);
        break;
    case TYPE_VARCHAR:
    default:
        snprintf(buf, cap, "N/A");
        break;
    }
}

int exec_describe_table(ExecContext *ectx,
                        const DescribeTableStatement *s,
                        char *out, size_t cap)
{
    REQUIRE_LOGIN(ectx);
    REQUIRE_SCHEMA(ectx);

    RelationGuard rel_guard(ectx->partition,
                             pm_find_relation_const(ectx->partition, s->table_name.c_str()));
    const RelationDef *rel = rel_guard.get();
    if (!rel) {
        snprintf(out, cap, "  Error: Table '%s' not found in schema '%s'",
                 s->table_name.c_str(), ectx->conn->current_schema_name);
        return MYDB_ERR;
    }

    /*
     * FULL mode — load this relation's stats page from the engine-pooled
     * StatsFile* (ectx->stats, under system_schema/stats/).  stats_load_relation
     * returns MYDB_ERR_NOT_FOUND when ANALYZE has never run for this relation;
     * we fall back gracefully: stats columns show "N/A".  The StatsBuffer owns
     * the handle — we do not open or close it here.
     *
     * slot_idx: pm_find_relation_const returns &active_schema.defs[i], so
     * pointer arithmetic gives us i directly (parallel arrays).
     */
    StatsFile *sf       = ectx->stats;
    bool       stats_ok = false;   /* true once the page is loaded */
    int        slot_idx = -1;

    if (s->full && sf) {
        slot_idx = (int)(rel - pctx_active_schema(ectx->partition)->defs);
        if (slot_idx >= 0 && slot_idx < MAX_RELATIONS_PER_SCHEMA &&
            stats_load_relation(sf, slot_idx) == MYDB_OK) {
            stats_ok = true;
        } else {
            slot_idx = -1;
        }
    }

    ResultBuf    rb(out, cap);
    TableBuilder tb;

    /* Set column headers — 8 base columns. */
    tb.set_headers({"Field", "Type", "Null", "Key",
                    "Default", "Indexed", "References", "Extra"});

    /* Per-column rows. */
    for (int i = 0; i < (int)rel->num_columns; i++) {
        const ColumnDef *col = &rel->columns[i];

        char type_buf[128];
        fmt_col_type(col, type_buf, sizeof(type_buf));

        const char *nullable = (col->is_not_null || col->is_primary_key) ? "NO" : "YES";

        const char *key = "";
        if (col->is_primary_key) {
            key = "PRI";
        } else if (col->is_unique) {
            key = "UNI";
        } else {
            for (int j = 0; j < rel->num_secondary_indexes; j++)
                if (rel->secondary_col_idx[j] == (uint8_t)i) { key = "MUL"; break; }
        }

        char def_buf[128];
        fmt_default(col, def_buf, sizeof(def_buf));

        const char *indexed = "";
        if (col->is_primary_key) {
            indexed = "B-Tree";
        } else {
            for (int j = 0; j < rel->num_secondary_indexes; j++)
                if (rel->secondary_col_idx[j] == (uint8_t)i) { indexed = "B-Tree"; break; }
        }

        char ref_buf[192] = "";
        for (int j = 0; j < rel->num_foreign_keys; j++) {
            if (strcmp(rel->foreign_keys[j].column_name, col->name) == 0) {
                snprintf(ref_buf, sizeof(ref_buf), "%s(%s)",
                         rel->foreign_keys[j].ref_relation_name,
                         rel->foreign_keys[j].ref_column_name);
                break;
            }
        }

        const char *extra = col->is_auto_increment ? "AUTO_INCREMENT" : "";

        tb.add_row({col->name, type_buf, nullable, key,
                    def_buf, indexed, ref_buf, extra});
    }

    tb.render(rb);

    /* FULL mode — second section: stats table. */
    if (s->full) {
        /* Build stats ANALYZE timestamp header. */
        char stats_hdr[96] = "Statistics (run ANALYZE TABLE to populate)";
        if (stats_ok) {
            /* Use the schema's last_modified as a proxy (stats are refreshed then). */
            char ts[32];
            fmt_datetime_ts(pctx_active_schema(ectx->partition)->header.last_modified, ts, sizeof(ts));
            snprintf(stats_hdr, sizeof(stats_hdr), "Statistics (Analyzed: %s)", ts);
        }

        /* Emit blank line + stats section header. */
        char sec_line[128];
        snprintf(sec_line, sizeof(sec_line), "\n-- %s --\n", stats_hdr);
        rb.append(sec_line);

        TableBuilder stb;
        stb.set_headers({"Field", "Stats", "Distinct", "Nulls", "Rows", "Min", "Max"});

        for (int i = 0; i < (int)rel->num_columns; i++) {
            const ColumnDef *col = &rel->columns[i];
            const char *stype    = "N/A";
            char distinct_buf[32] = "N/A";
            char nulls_buf[32]    = "N/A";
            char rows_buf[32]     = "N/A";
            char min_buf[64]      = "N/A";
            char max_buf[64]      = "N/A";

            if (stats_ok) {
                ColumnStats *cs = stats_get_column(sf, slot_idx, i);
                if (cs && cs->has_stats) {
                    switch (cs->stats_type) {
                    case STATS_TYPE_MCV:       stype = "MCV";       break;
                    case STATS_TYPE_HISTOGRAM: stype = "Histogram"; break;
                    default:                   stype = "None";      break;
                    }
                    snprintf(distinct_buf, sizeof(distinct_buf), "%u", cs->num_distinct);
                    snprintf(nulls_buf,    sizeof(nulls_buf),    "%u", cs->num_nulls);
                    snprintf(rows_buf,     sizeof(rows_buf),     "%u", cs->total_rows);
                    fmt_stat_numeric(cs->min_numeric, col, min_buf, sizeof(min_buf));
                    fmt_stat_numeric(cs->max_numeric, col, max_buf, sizeof(max_buf));
                } else if (cs) {
                    stype = "None";
                }
            }

            stb.add_row({col->name, stype, distinct_buf, nulls_buf,
                         rows_buf, min_buf, max_buf});
        }

        stb.render(rb);
        /* sf is the engine-pooled handle — do not close it here. */
    }

    return MYDB_OK;
}

/* ======================================================================
 * DESCRIBE SCHEMA
 *
 * Describes the currently active schema (loaded by USE).
 * Requires partition ownership — analysts are not allowed.
 *
 * Output (two sections, tab-separated):
 *
 *   Section 1 — header properties
 *     Property      Value
 *     Schema        myapp
 *     Partition ID  3
 *     Created       2026-05-01 12:00:00
 *     Last Modified 2026-05-27 09:41:03
 *     Tables        5
 *
 *   Section 2 — per-table summary (from RelationEntry slot directory)
 *     Table    Columns  Rows    Pages  Height  Size
 *     users    7        10240   84     3       1.31 MB
 *     ...
 * ====================================================================== */

int exec_describe_schema(ExecContext *ectx,
                         const DescribeSchemaStatement * /*s*/,
                         char *out, size_t cap)
{
    REQUIRE_LOGIN(ectx);
    REQUIRE_PARTITION(ectx);   /* analysts may not inspect schema metadata */
    REQUIRE_SCHEMA(ectx);

    const SchemaFile   *sf  = pctx_active_schema(ectx->partition);
    const SchemaHeader *hdr = &sf->header;

    ResultBuf rb(out, cap);

    /* ---- Section 1: header properties ---- */
    char ts1[32], ts2[32];
    fmt_datetime_ts(hdr->created_at,    ts1, sizeof(ts1));
    fmt_datetime_ts(hdr->last_modified, ts2, sizeof(ts2));

    {
        TableBuilder tb;
        tb.set_headers({"Property", "Value"});

        char pid[16], nrel[16];
        snprintf(pid,  sizeof(pid),  "%u", hdr->partition_id);
        snprintf(nrel, sizeof(nrel), "%u", hdr->num_relations);

        tb.add_row({"Schema",        hdr->schema_name});
        tb.add_row({"Partition ID",  pid});
        tb.add_row({"Created",       ts1});
        tb.add_row({"Last Modified", ts2});
        tb.add_row({"Tables",        nrel});
        tb.render(rb);
    }

    rb.append("\n");

    /* ---- Section 2: per-table summary ---- */
    {
        TableBuilder tb;
        tb.set_headers({"Table", "Columns", "Rows", "Pages", "Height", "Size"});

        for (int i = 0; i < MAX_RELATIONS_PER_SCHEMA; i++) {
            const RelationEntry *re = &sf->relations[i];
            if (!re->is_valid) continue;

            uint64_t size_bytes = (uint64_t)re->num_pages * PAGE_SIZE;
            char size_buf[32];
            fmt_bytes(size_bytes, size_buf, sizeof(size_buf));

            char ncols[8], nrows[16], npages[16], height[8];
            snprintf(ncols,  sizeof(ncols),  "%u", (unsigned)re->num_columns);
            snprintf(nrows,  sizeof(nrows),  "%u", (unsigned)re->num_rows);
            snprintf(npages, sizeof(npages), "%u", (unsigned)re->num_pages);
            snprintf(height, sizeof(height), "%u", (unsigned)re->tree_height);

            tb.add_row({re->relation_name, ncols, nrows, npages, height, size_buf});
        }
        tb.render(rb);
    }

    return MYDB_OK;
}

/* ======================================================================
 * DESCRIBE PARTITION
 *
 * Describes the current user's partition: quota/usage, timestamps, and
 * the list of schemas (databases) it contains.
 * Requires partition ownership — analysts are not allowed.
 * Does NOT require an active schema (can be called before USE).
 *
 * Output (two sections, tab-separated):
 *
 *   Section 1 — partition properties
 *     Property       Value
 *     Partition ID   3
 *     Owner          alice
 *     Path           /home/alice/.mydb/alice/
 *     Quota          2.00 GB
 *     Used           340.25 MB
 *     Free           1.67 GB
 *     Created        2026-05-01 12:00:00
 *     Last Modified  2026-05-27 09:41:03
 *     Databases      3
 *
 *   Section 2 — schema list (from SchemaEntry[] in __catalog.mydb)
 *     Database       Tables
 *     myapp          5
 *     logs           2
 *     staging        0
 * ====================================================================== */

int exec_describe_partition(ExecContext *ectx,
                            const DescribePartitionStatement * /*s*/,
                            char *out, size_t cap)
{
    REQUIRE_LOGIN(ectx);
    REQUIRE_PARTITION(ectx);   /* analysts may not inspect partition metadata */

    const Catalog       *cat = &ectx->partition->catalog;
    const CatalogHeader *hdr = &cat->header;

    ResultBuf rb(out, cap);

    /* ---- Section 1: partition properties ---- */
    char ts1[32], ts2[32];
    fmt_datetime_ts(hdr->created_at,    ts1, sizeof(ts1));
    fmt_datetime_ts(hdr->last_modified, ts2, sizeof(ts2));

    uint64_t free_bytes = (hdr->quota_bytes > hdr->used_bytes)
                          ? (hdr->quota_bytes - hdr->used_bytes) : 0;
    char quota_buf[32], used_buf[32], free_buf[32];
    fmt_bytes(hdr->quota_bytes, quota_buf, sizeof(quota_buf));
    fmt_bytes(hdr->used_bytes,  used_buf,  sizeof(used_buf));
    fmt_bytes(free_bytes,       free_buf,  sizeof(free_buf));

    {
        TableBuilder tb;
        tb.set_headers({"Property", "Value"});

        char pid[16], ndb[16];
        snprintf(pid, sizeof(pid), "%u", hdr->partition_id);
        snprintf(ndb, sizeof(ndb), "%u", (unsigned)hdr->num_schemas);

        tb.add_row({"Partition ID",  pid});
        tb.add_row({"Owner",         ectx->conn->username});
        tb.add_row({"Path",          ectx->partition->partition_path});
        tb.add_row({"Quota",         quota_buf});
        tb.add_row({"Used",          used_buf});
        tb.add_row({"Free",          free_buf});
        tb.add_row({"Created",       ts1});
        tb.add_row({"Last Modified", ts2});
        tb.add_row({"Databases",     ndb});
        tb.render(rb);
    }

    rb.append("\n");

    /* ---- Section 2: schema list ---- */
    {
        TableBuilder tb;
        tb.set_headers({"Database", "Tables"});

        for (int i = 0; i < MAX_SCHEMAS_PER_PARTITION; i++) {
            const SchemaEntry *se = &cat->schemas[i];
            if (!se->is_valid) continue;
            char nrel[8];
            snprintf(nrel, sizeof(nrel), "%u", (unsigned)se->num_relations);
            tb.add_row({se->schema_name, nrel});
        }
        tb.render(rb);
    }

    return MYDB_OK;
}

/* ======================================================================
 * parse_quota_str — shared by CREATE USER and ALTER USER SET QUOTA
 *
 * Accepts "nM" or "nG" (case-insensitive, n must be a positive integer).
 * An empty string maps to ENGINE_DEFAULT_QUOTA_BYTES.
 * Returns MYDB_OK on success (writes *out_bytes), MYDB_ERR on bad format,
 * range violation, or out-of-bounds quota.
 * ====================================================================== */
static int parse_quota_str(const std::string &s, uint64_t *out_bytes)
{
    if (s.empty()) {
        *out_bytes = ENGINE_DEFAULT_QUOTA_BYTES;
        return MYDB_OK;
    }

    if (s.size() < 2) return MYDB_ERR;   /* need at least "1M" */

    char suffix = (char)std::toupper((unsigned char)s.back());
    if (suffix != 'M' && suffix != 'G') return MYDB_ERR;

    /* Parse the numeric prefix. */
    std::string num_part = s.substr(0, s.size() - 1);
    for (char c : num_part)
        if (!std::isdigit((unsigned char)c)) return MYDB_ERR;

    uint64_t n = (uint64_t)std::stoull(num_part);
    if (n == 0) return MYDB_ERR;

    uint64_t bytes = (suffix == 'G')
                     ? n * 1024ULL * 1024ULL * 1024ULL
                     : n * 1024ULL * 1024ULL;

    if (bytes < ENGINE_MIN_QUOTA_BYTES || bytes > ENGINE_MAX_QUOTA_BYTES)
        return MYDB_ERR;

    *out_bytes = bytes;
    return MYDB_OK;
}

/* ======================================================================
 * CREATE USER
 * ====================================================================== */

int exec_create_user(ExecContext *ectx,
                     const CreateUserStatement *s,
                     char *out, size_t cap)
{
    REQUIRE_LOGIN(ectx);

    /* Parse quota (empty → default). */
    uint64_t quota = 0;
    if (!s->quota_str.empty()) {
        if (parse_quota_str(s->quota_str, &quota) != MYDB_OK) {
            snprintf(out, cap,
                     "  Error: invalid quota '%s' — use format nM or nG "
                     "(e.g. 500M, 2G), range 100M–5G",
                     s->quota_str.c_str());
            return MYDB_ERR;
        }
    }

    const char *part_name = s->partition_name.empty()
                            ? nullptr
                            : s->partition_name.c_str();

    int rc = engine_create_user(ectx->engine,
                                s->username.c_str(),
                                s->password.c_str(),
                                part_name,
                                quota);
    if (rc == MYDB_ERR_PERM) {
        snprintf(out, cap, "  Error: only root may create users");
        return rc;
    }
    if (rc == MYDB_ERR_DUPLICATE) {
        snprintf(out, cap,
                 "  Error: user or partition name '%s' already exists",
                 s->username.c_str());
        return rc;
    }
    if (rc == MYDB_ERR_FULL) {
        snprintf(out, cap, "  Error: partition limit reached");
        return rc;
    }
    if (rc != MYDB_OK) {
        format_error(rc, out, cap, s->username.c_str());
        return rc;
    }

    snprintf(out, cap, "OK  User '%s' created", s->username.c_str());
    return MYDB_OK;
}

/* ======================================================================
 * DROP USER
 * ====================================================================== */

int exec_drop_user(ExecContext *ectx,
                   const DropUserStatement *s,
                   char *out, size_t cap)
{
    REQUIRE_LOGIN(ectx);

    int rc = engine_drop_user(ectx->engine, s->username.c_str());
    if (rc == MYDB_ERR_PERM) {
        snprintf(out, cap,
                 "  Error: only root may drop users, and root cannot be dropped");
        return rc;
    }
    if (rc == MYDB_ERR_NOT_FOUND) {
        snprintf(out, cap, "  Error: user '%s' does not exist",
                 s->username.c_str());
        return rc;
    }
    if (rc != MYDB_OK) {
        format_error(rc, out, cap, s->username.c_str());
        return rc;
    }

    snprintf(out, cap, "OK  User '%s' dropped", s->username.c_str());
    return MYDB_OK;
}

/* ======================================================================
 * ALTER USER
 * ====================================================================== */

int exec_alter_user(ExecContext *ectx,
                    const AlterUserStatement *s,
                    char *out, size_t cap)
{
    REQUIRE_LOGIN(ectx);

    if (s->action == AlterUserStatement::Action::SET_PASSWORD) {

        int rc = engine_alter_user_password(ectx->engine,
                                            s->username.c_str(),
                                            s->new_password.c_str());
        if (rc == MYDB_ERR_PERM) {
            snprintf(out, cap, "  Error: only root may change passwords");
            return rc;
        }
        if (rc == MYDB_ERR_NOT_FOUND) {
            snprintf(out, cap, "  Error: user '%s' does not exist",
                     s->username.c_str());
            return rc;
        }
        if (rc != MYDB_OK) { format_error(rc, out, cap, s->username.c_str()); return rc; }

        snprintf(out, cap, "OK  Password updated for '%s'", s->username.c_str());
        return MYDB_OK;

    } else {  /* SET_QUOTA */

        uint64_t quota = 0;
        if (parse_quota_str(s->quota_str, &quota) != MYDB_OK) {
            snprintf(out, cap,
                     "  Error: invalid quota '%s' — use nM or nG, range 100M–5G",
                     s->quota_str.c_str());
            return MYDB_ERR;
        }

        int rc = engine_alter_user_quota(ectx->engine, s->username.c_str(), quota);
        if (rc == MYDB_ERR_PERM) {
            snprintf(out, cap, "  Error: only root may alter quotas");
            return rc;
        }
        if (rc == MYDB_ERR_NOT_FOUND) {
            snprintf(out, cap, "  Error: user '%s' does not exist",
                     s->username.c_str());
            return rc;
        }
        if (rc == MYDB_ERR_FULL) {
            snprintf(out, cap,
                     "  Error: new quota is below current usage for user '%s'",
                     s->username.c_str());
            return rc;
        }
        if (rc != MYDB_OK) { format_error(rc, out, cap, s->username.c_str()); return rc; }

        snprintf(out, cap, "OK  Quota updated for user '%s' to %s",
                 s->username.c_str(), s->quota_str.c_str());
        return MYDB_OK;
    }
}
