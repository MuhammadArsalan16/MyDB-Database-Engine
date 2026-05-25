#pragma once
/*
 * value_cast.hpp — convert AST string literals to typed storage Values.
 *
 * The parser stores every literal as a raw string (e.g. "42", "hello",
 * "TRUE", "2023-12-25").  Before storage can use them, they must be
 * converted to a typed Value struct that matches the column's DataType.
 *
 * Implemented in src/value_cast.cpp (Phase 2).
 */

#include <string>
extern "C" {
#include "common.h"
#include "relation_def.h"
}

/*
 * Convert a raw AST token string to a Value of the given column's type.
 *
 * token  — the raw string from the AST (no surrounding quotes; the lexer
 *           already strips them).  Examples: "42", "-7", "hello", "TRUE",
 *           "NULL", "2023-12-25".
 * col    — the target column definition (provides type, scale, max_len,
 *           enum_values, date_format).
 *
 * Returns a Value with is_null=1 if token is "NULL" or cannot be parsed
 * for the given type.
 */
Value cast_literal(const std::string &token, const ColumnDef &col);
