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
#include <cerrno>
#include <climits>

bool validate_literal(const std::string &token, const ColumnDef &col)
{
    /* NULL is valid for any type (DEFAULT NULL is always legal). */
    if (token == "NULL") return true;

    const char *s = token.c_str();
    char       *end = nullptr;

    switch (col.type) {

    case TYPE_INT: {
        /*
         * Must parse as a valid integer with nothing left over, and the
         * value must fit in a signed 32-bit integer.  strtol returns a
         * long (64-bit on Linux); we check ERANGE and the INT32 bounds
         * explicitly so values like 99999999999 are rejected here rather
         * than silently truncated in cast_literal.
         */
        errno = 0;
        long val = strtol(s, &end, 10);
        return (end != s) && (*end == '\0')
            && (errno != ERANGE)
            && (val >= INT32_MIN) && (val <= INT32_MAX);
    }

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
    case TYPE_INT: {
        errno = 0;
        char *end = nullptr;
        long val = strtol(s, &end, 10);
        if (end == s || *end != '\0' || errno == ERANGE || val < INT32_MIN || val > INT32_MAX)
            v.is_null = 1;
        else
            v.v.int_val = (int32_t)val;
        break;
    }

    /* ------------------------------------------------------------------ */
    case TYPE_DECIMAL: {
        /*
         * Stored as a fixed-point integer: real_value * 10^scale.
         * Example: 3.14 with scale=2 → 314.
         *
         * We parse integer and fractional parts as strings separately —
         * no double intermediate — so there is no floating-point precision
         * loss for large values like DECIMAL(18,4).
         *
         * Algorithm:
         *   1. Handle optional leading '-'.
         *   2. Split at '.': integer_str and frac_str.
         *   3. Parse integer_str with strtoll.
         *   4. Pad frac_str to exactly `scale` digits (truncate if longer,
         *      right-pad with '0' if shorter).
         *   5. result = integer_part * 10^scale + frac_part.
         *      Apply sign to the whole result.
         */
        int scale = (col.scale > 0) ? col.scale : 2;

        const char *p   = s;
        int         neg = 0;
        if (*p == '-') { neg = 1; p++; }
        else if (*p == '+') { p++; }

        /* Split at decimal point */
        const char *dot = strchr(p, '.');
        char int_str[32]  = {0};
        char frac_str[32] = {0};

        if (dot) {
            size_t int_len = (size_t)(dot - p);
            if (int_len >= sizeof(int_str)) int_len = sizeof(int_str) - 1;
            memcpy(int_str, p, int_len);

            const char *frac_start = dot + 1;
            size_t frac_len = strlen(frac_start);
            if (frac_len >= sizeof(frac_str)) frac_len = sizeof(frac_str) - 1;
            memcpy(frac_str, frac_start, frac_len);
        } else {
            size_t int_len = strlen(p);
            if (int_len >= sizeof(int_str)) int_len = sizeof(int_str) - 1;
            memcpy(int_str, p, int_len);
            /* frac_str stays all zeros */
        }

        /* Parse integer part */
        int64_t int_part = (int_str[0] != '\0') ? (int64_t)strtoll(int_str, NULL, 10) : 0;

        /* Normalise fractional string to exactly `scale` digits:
         * truncate if longer, right-pad with '0' if shorter. */
        int frac_len = (int)strlen(frac_str);
        if (frac_len > scale) {
            frac_str[scale] = '\0';    /* truncate */
        } else {
            for (int i = frac_len; i < scale; i++)
                frac_str[i] = '0';     /* right-pad */
            frac_str[scale] = '\0';
        }

        int64_t frac_part = (frac_str[0] != '\0') ? (int64_t)strtoll(frac_str, NULL, 10) : 0;

        /* Compute scale factor as integer (exact, no floating point) */
        int64_t factor = 1;
        for (int i = 0; i < scale; i++) factor *= 10;

        int64_t result = int_part * factor + frac_part;
        v.v.decimal_val = neg ? -result : result;
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
