/*
 * value_cast.cpp — convert AST string literals to typed storage Values.
 *
 * The lexer already strips surrounding quotes from STRING tokens, so
 * 'hello' arrives here as just: hello
 * Numbers and keywords arrive as-is: 42, 3.14, TRUE, NULL, 2024-01-15
 *
 * If the token is the SQL keyword NULL, or cannot be parsed for the
 * requested type, is_null is set to 1 and the union is zeroed.
 */

#include "value_cast.hpp"

#include <cstring>
#include <cstdlib>
#include <cstdio>
#include <cctype>

bool validate_literal(const std::string &token, const ColumnDef &col)
{
    /* NULL is valid for any type (DEFAULT NULL is always legal). */
    if (token == "NULL") return true;

    const char *s = token.c_str();
    char       *end = nullptr;

    switch (col.type) {

    case TYPE_INT:
        /*
         * Must be an optional leading '-' or '+' followed by at least one
         * digit, with nothing left over.  strtol sets end to the first
         * character it could not consume.
         */
        strtol(s, &end, 10);
        return (end != s) && (*end == '\0');

    case TYPE_DECIMAL:
        /*
         * Must be a valid floating-point literal with nothing left over.
         * Rejects pure alphabetic strings like "hello" which strtod silently
         * converts to 0.0.
         */
        strtod(s, &end);
        return (end != s) && (*end == '\0');

    case TYPE_BOOL:
        /*
         * Only the six canonical boolean literals are accepted.
         * Rejects numeric values like 42 — use 1 or 0 explicitly.
         */
        return (token == "TRUE"  || token == "true"  ||
                token == "FALSE" || token == "false" ||
                token == "1"     || token == "0");

    case TYPE_ENUM:
        /* Token must match one of the column's declared enum labels. */
        for (int i = 0; i < col.num_enum_values; i++)
            if (strcmp(col.enum_values[i], s) == 0) return true;
        return false;

    case TYPE_VARCHAR:
        /* Any string is valid; length is clamped elsewhere. */
        return true;

    case TYPE_DATE: {
        /* Must parse as DD-MM-YYYY, DD/MM/YYYY, or YYYY-MM-DD. */
        int a = 0, b = 0, c = 0;
        int parsed = sscanf(s, "%d-%d-%d", &a, &b, &c);
        if (parsed != 3) parsed = sscanf(s, "%d/%d/%d", &a, &b, &c);
        return parsed == 3;
    }

    case TYPE_DATETIME: {
        /* Must parse as YYYY-MM-DD HH:MM:SS. */
        int Y = 0, Mo = 0, D = 0, h = 0, mi = 0, sec = 0;
        return sscanf(s, "%d-%d-%d %d:%d:%d", &Y, &Mo, &D, &h, &mi, &sec) == 6;
    }

    default:
        return false;
    }
}

Value cast_literal(const std::string &token, const ColumnDef &col)
{
    Value v;
    memset(&v, 0, sizeof(v));
    v.type = col.type;

    /* SQL NULL keyword — valid for any type */
    if (token == "NULL") {
        v.is_null = 1;
        return v;
    }

    const char *s = token.c_str();

    switch (col.type) {

    /* ------------------------------------------------------------------ */
    case TYPE_INT:
        v.v.int_val = (int32_t)strtol(s, NULL, 10);
        break;

    /* ------------------------------------------------------------------ */
    case TYPE_DECIMAL: {
        /*
         * Stored as integer: real_value * 10^scale
         * Example: 3.14 with scale=2 → stored as 314
         */
        int scale = (col.scale > 0) ? col.scale : 2;
        double d  = strtod(s, NULL);

        double factor = 1.0;
        for (int i = 0; i < scale; i++) factor *= 10.0;

        /* round half-away-from-zero */
        v.v.decimal_val = (int64_t)(d >= 0.0 ? d * factor + 0.5
                                              : d * factor - 0.5);
        break;
    }

    /* ------------------------------------------------------------------ */
    case TYPE_VARCHAR: {
        size_t len = token.size();
        if (len > MAX_VARCHAR_LEN) len = MAX_VARCHAR_LEN;
        v.v.varchar_val.len = (uint16_t)len;
        memcpy(v.v.varchar_val.data, s, len);
        v.v.varchar_val.data[len] = '\0';
        break;
    }

    /* ------------------------------------------------------------------ */
    case TYPE_BOOL:
        v.v.bool_val = (token == "TRUE"  || token == "true"  ||
                        token == "1") ? 1 : 0;
        break;

    /* ------------------------------------------------------------------ */
    case TYPE_ENUM: {
        /* find the string in the column's enum list → store its index */
        for (int i = 0; i < col.num_enum_values; i++) {
            if (strcmp(col.enum_values[i], s) == 0) {
                v.v.enum_val = (uint8_t)i;
                return v;
            }
        }
        /* not found */
        v.is_null = 1;
        break;
    }

    /* ------------------------------------------------------------------ */
    case TYPE_DATE: {
        /*
         * Accepted formats:
         *   DD-MM-YYYY  (default)
         *   DD/MM/YYYY
         *   YYYY-MM-DD  (ISO — detected when the first number > 31)
         *
         * Stored as YYYYMMDD integer.
         */
        int a = 0, b = 0, c = 0;
        int y = 0, m = 0, d = 0;
        int parsed = sscanf(s, "%d-%d-%d", &a, &b, &c);
        if (parsed != 3)
            parsed = sscanf(s, "%d/%d/%d", &a, &b, &c);

        if (parsed == 3) {
            if (a > 31) { y = a; m = b; d = c; }   /* YYYY-MM-DD */
            else        { d = a; m = b; y = c; }   /* DD-MM-YYYY */
            v.v.date_val = y * 10000 + m * 100 + d;
        } else {
            v.is_null = 1;
        }
        break;
    }

    /* ------------------------------------------------------------------ */
    case TYPE_DATETIME: {
        /*
         * Accepted format: YYYY-MM-DD HH:MM:SS
         * Stored as YYYYMMDDHHmmSS integer.
         */
        int Y = 0, M = 0, D = 0, h = 0, mi = 0, sec = 0;
        if (sscanf(s, "%d-%d-%d %d:%d:%d", &Y, &M, &D, &h, &mi, &sec) == 6) {
            v.v.datetime_val = (int64_t)Y  * 10000000000LL
                             + (int64_t)M  *   100000000LL
                             + (int64_t)D  *     1000000LL
                             + (int64_t)h  *       10000LL
                             + (int64_t)mi *         100LL
                             + (int64_t)sec;
        } else {
            v.is_null = 1;
        }
        break;
    }

    /* ------------------------------------------------------------------ */
    default:
        v.is_null = 1;
        break;
    }

    return v;
}
