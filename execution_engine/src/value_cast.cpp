/*
 * value_cast.cpp — convert AST string literals to typed storage Values.
 *
 * Phase 2 implements this file.
 */

#include "value_cast.hpp"

Value cast_literal(const std::string & /*token*/, const ColumnDef & /*col*/)
{
    Value v{};
    v.is_null = 1;   /* stub — always null until Phase 2 */
    return v;
}
