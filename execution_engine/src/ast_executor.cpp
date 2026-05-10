#include <algorithm>
#include <cctype>
#include <cstring>
#include <iostream>
#include <string>
#include <unordered_map>
#include <vector>

#define ColumnDef ASTColumnDef
#include "../../query_parser/include/AST.hpp"
#undef ColumnDef

#include "../../storage_engine/include/storage.h"
#include "../include/ast_executor.hpp"
#include "../include/plan.h"
#include "../include/executor.h"

#ifdef DELETE
#undef DELETE
#endif

static std::unordered_map<std::string, Schema> schema_registry;

static std::string strip_quotes(const std::string &value) {
    if (value.size() >= 2 && ((value.front() == '"' && value.back() == '"') ||
                               (value.front() == '\'' && value.back() == '\''))) {
        return value.substr(1, value.size() - 2);
    }
    return value;
}

static bool is_integer_literal(const std::string &value) {
    if (value.empty()) return false;
    size_t start = (value[0] == '-' ? 1 : 0);
    for (size_t i = start; i < value.size(); ++i) {
        if (!std::isdigit(static_cast<unsigned char>(value[i]))) return false;
    }
    return (start < value.size());
}

static DataType map_data_type(const std::string &type_name) {
    if (type_name == "INT" || type_name == "INTEGER") return TYPE_INT;
    if (type_name == "VARCHAR") return TYPE_VARCHAR;
    if (type_name == "BOOL" || type_name == "BOOLEAN") return TYPE_BOOL;
    if (type_name == "DECIMAL") return TYPE_DECIMAL;
    if (type_name == "DATE") return TYPE_DATE;
    if (type_name == "DATETIME") return TYPE_DATETIME;
    return TYPE_VARCHAR;
}

static Value build_value(const std::string &token, DataType type) {
    Value value{};
    value.type = type;
    value.is_null = 0;

    if (token == "NULL") {
        value.is_null = 1;
        return value;
    }

    switch (type) {
        case TYPE_INT:
            if (is_integer_literal(token)) {
                value.v.int_val = std::stoi(token);
            } else {
                value.is_null = 1;
            }
            break;

        case TYPE_BOOL: {
            std::string lower;
            lower.resize(token.size());
            std::transform(token.begin(), token.end(), lower.begin(), ::tolower);
            value.v.bool_val = (lower == "true" || lower == "1") ? 1 : 0;
            break;
        }

        case TYPE_VARCHAR: {
            std::string text = strip_quotes(token);
            value.v.varchar_val.len = static_cast<uint16_t>(std::min<size_t>(text.size(), sizeof(value.v.varchar_val.data)));
            memcpy(value.v.varchar_val.data, text.data(), value.v.varchar_val.len);
            break;
        }

        case TYPE_DECIMAL:
        case TYPE_DATE:
        case TYPE_DATETIME:
        case TYPE_ENUM:
        default: {
            std::string text = strip_quotes(token);
            value.v.varchar_val.len = static_cast<uint16_t>(std::min<size_t>(text.size(), sizeof(value.v.varchar_val.data)));
            memcpy(value.v.varchar_val.data, text.data(), value.v.varchar_val.len);
            break;
        }
    }

    return value;
}

static const Schema *get_schema(const std::string &table_name) {
    auto it = schema_registry.find(table_name);
    if (it == schema_registry.end()) return nullptr;
    return &it->second;
}

static int find_column_index(const Schema *schema, const std::string &column_name) {
    if (!schema) return -1;
    for (int i = 0; i < schema->num_columns; ++i) {
        if (column_name == schema->columns[i].name) return i;
    }
    return -1;
}

static std::string normalize_column(const std::string &expr) {
    size_t dot = expr.find('.');
    if (dot != std::string::npos) {
        return expr.substr(dot + 1);
    }
    return expr;
}

static void fill_row(Row *row, const std::vector<std::string> &values, const Schema *schema) {
    memset(row, 0, sizeof(Row));
    if (!schema) return;

    row->num_cols = schema->num_columns;
    for (int i = 0; i < schema->num_columns && i < (int)values.size(); ++i) {
        row->cols[i] = build_value(values[i], schema->columns[i].type);
    }
    for (int i = values.size(); i < schema->num_columns; ++i) {
        row->cols[i].type = schema->columns[i].type;
        row->cols[i].is_null = 1;
    }
}

static PlanNode *build_select_plan_from_ast(const SelectStatement *stmt) {
    PlanNode *scan = create_plan_node(NODE_SEQ_SCAN);
    strncpy(scan->table, stmt->table_name.c_str(), sizeof(scan->table) - 1);

    if (!stmt->where_clause) {
        return scan;
    }

    const Schema *schema = get_schema(stmt->table_name);
    if (!schema) {
        std::cerr << "Query Processor: schema not found for table " << stmt->table_name << "\n";
        free_plan(scan);
        return nullptr;
    }

    PlanNode *filter = create_plan_node(NODE_FILTER);
    filter->left = scan;
    filter->col_idx = find_column_index(schema, normalize_column(stmt->where_clause->left_operand));
    filter->value = build_value(stmt->where_clause->right_operand, schema->columns[filter->col_idx].type);

    return filter;
}

static PlanNode *build_insert_plan_from_ast(const InsertStatement *stmt) {
    const Schema *schema = get_schema(stmt->table_name);
    if (!schema) {
        std::cerr << "Query Processor: schema not found for table " << stmt->table_name << "\n";
        return nullptr;
    }

    PlanNode *plan = create_plan_node(NODE_INSERT);
    strncpy(plan->table, stmt->table_name.c_str(), sizeof(plan->table) - 1);
    fill_row(&plan->insert_row, stmt->values, schema);
    return plan;
}

static PlanNode *build_delete_plan_from_ast(const DeleteStatement *stmt) {
    const Schema *schema = get_schema(stmt->table_name);
    if (!schema) {
        std::cerr << "Query Processor: schema not found for table " << stmt->table_name << "\n";
        return nullptr;
    }

    PlanNode *plan = create_plan_node(NODE_DELETE);
    strncpy(plan->table, stmt->table_name.c_str(), sizeof(plan->table) - 1);
    if (stmt->where_clause) {
        plan->col_idx = find_column_index(schema, normalize_column(stmt->where_clause->left_operand));
        plan->value = build_value(stmt->where_clause->right_operand, schema->columns[plan->col_idx].type);
    }

    return plan;
}

static PlanNode *build_update_plan_from_ast(const UpdateStatement *stmt) {
    const Schema *schema = get_schema(stmt->table_name);
    if (!schema) {
        std::cerr << "Query Processor: schema not found for table " << stmt->table_name << "\n";
        return nullptr;
    }

    PlanNode *plan = create_plan_node(NODE_UPDATE);
    strncpy(plan->table, stmt->table_name.c_str(), sizeof(plan->table) - 1);

    if (stmt->where_clause) {
        plan->col_idx = find_column_index(schema, normalize_column(stmt->where_clause->left_operand));
        if (plan->col_idx >= 0) {
            plan->value = build_value(stmt->where_clause->right_operand, schema->columns[plan->col_idx].type);
        }
    }

    plan->set_col_idx = find_column_index(schema, stmt->set_column);
    if (plan->set_col_idx >= 0) {
        plan->set_value = build_value(stmt->set_value, schema->columns[plan->set_col_idx].type);
    }

    return plan;
}

static int execute_create_table_ast(const CreateTableStatement *stmt) {
    Schema schema{};
    memset(&schema, 0, sizeof(Schema));
    strncpy(schema.table_name, stmt->table_name.c_str(), sizeof(schema.table_name) - 1);
    schema.num_columns = static_cast<uint8_t>(stmt->columns.size());
    schema.num_foreign_keys = 0;
    schema.num_secondary_indexes = 0;
    schema.auto_incr_counter = 1;
    schema.pk_col_idx = 0;

    for (int i = 0; i < (int)stmt->columns.size(); ++i) {
        const auto &col = stmt->columns[i];
        strncpy(schema.columns[i].name, col.name.c_str(), sizeof(schema.columns[i].name) - 1);
        schema.columns[i].type = map_data_type(col.data_type);
        schema.columns[i].is_primary_key = col.is_primary_key ? 1 : 0;
        schema.columns[i].is_not_null = col.is_primary_key ? 1 : 0;
        if (col.is_primary_key) {
            schema.pk_col_idx = static_cast<uint8_t>(i);
        }
        if (schema.columns[i].type == TYPE_VARCHAR) {
            schema.columns[i].max_len = 150;
        }
    }

    int rc = storage_create_table(stmt->table_name.c_str(), &schema);
    if (rc == 0) {
        schema_registry[stmt->table_name] = schema;
    }
    return rc;
}

static int execute_transaction_ast(const TransactionStatement *stmt) {
    switch (stmt->command) {
        case TransactionCommand::BEGIN:
            return storage_begin();
        case TransactionCommand::COMMIT:
            return storage_commit();
        case TransactionCommand::ROLLBACK:
            return storage_rollback();
        default:
            return -1;
    }
}

int execute_ast(const void *ast_ptr) {
    if (!ast_ptr) return -1;
    const ASTNode *ast = static_cast<const ASTNode*>(ast_ptr);

    if (ast->type == StatementType::SELECT) {
        const SelectStatement *stmt = static_cast<const SelectStatement*>(ast);
        PlanNode *plan = build_select_plan_from_ast(stmt);
        if (!plan) return -1;
        plan = optimize_plan(plan);
        int rc = execute_plan(plan);
        free_plan(plan);
        return rc;
    } else if (ast->type == StatementType::INSERT) {
        const InsertStatement *stmt = static_cast<const InsertStatement*>(ast);
        PlanNode *plan = build_insert_plan_from_ast(stmt);
        if (!plan) return -1;
        int rc = execute_plan(plan);
        free_plan(plan);
        return rc;
    } else if (ast->type == StatementType::UPDATE) {
        const UpdateStatement *stmt = static_cast<const UpdateStatement*>(ast);
        PlanNode *plan = build_update_plan_from_ast(stmt);
        if (!plan) return -1;
        int rc = execute_plan(plan);
        free_plan(plan);
        return rc;
    } else if (ast->type == StatementType::DELETE) {
        const DeleteStatement *stmt = static_cast<const DeleteStatement*>(ast);
        PlanNode *plan = build_delete_plan_from_ast(stmt);
        if (!plan) return -1;
        int rc = execute_plan(plan);
        free_plan(plan);
        return rc;
    } else if (ast->type == StatementType::CREATE_TABLE) {
        const CreateTableStatement *stmt = static_cast<const CreateTableStatement*>(ast);
        return execute_create_table_ast(stmt);
    } else if (ast->type == StatementType::TRANSACTION) {
        const TransactionStatement *stmt = static_cast<const TransactionStatement*>(ast);
        return execute_transaction_ast(stmt);
    } else {
        std::cerr << "Query Processor: unsupported AST statement type\n";
        return -1;
    }
}
