/*
 * result_fmt.cpp — format storage rows and error codes into result strings.
 *
 * format_error  — already implemented (maps MYDB_* codes to text).
 * ResultBuf     — already implemented (safe append-only buffer).
 *
 * Phase 2 adds:
 *   append_value  — type-aware Value → text formatting.
 *   emit_header   — column names line followed by a separator.
 *   emit_row      — one pipe-separated data row.
 *
 * Output format example:
 *   id | name | age
 *   ---+------+----
 *   1 | Alice | 30
 *   2 | Bob | 25
 *
 *   (2 rows)
 */

#include "result_fmt.hpp"

#include <cstring>
#include <cstdio>

/* ======================================================================
 * format_error
 * ====================================================================== */

void format_error(int rc, char *out, size_t cap, const char *ctx)
{
    const char *msg;
    switch (rc) {
    case MYDB_OK:                 msg = "OK";                              break;
    case MYDB_ERR_NOT_FOUND:      msg = "not found";                       break;
    case MYDB_ERR_DUPLICATE:      msg = "duplicate key value";             break;
    case MYDB_ERR_FULL:           msg = "storage full";                    break;
    case MYDB_ERR_FK_VIOLATION:   msg = "foreign key violation";           break;
    case MYDB_ERR_NULL_VIOLATION: msg = "NULL not allowed";                break;
    case MYDB_ERR_NO_TXN:         msg = "no active transaction";           break;
    case MYDB_ERR_PERM:           msg = "permission denied";               break;
    case MYDB_ERR_CROSS_SCHEMA:   msg = "cross-schema query not supported";break;
    case MYDB_ERR_BAD_MAGIC:
    case MYDB_ERR_BAD_FILE_TYPE:
    case MYDB_ERR_BAD_VERSION:
    case MYDB_ERR_BAD_CHECKSUM:   msg = "database corruption";             break;
    default:                      msg = "internal error";                  break;
    }

    if (ctx) snprintf(out, cap, "%s: %s", msg, ctx);
    else     snprintf(out, cap, "%s",     msg);
}

/* ======================================================================
 * ResultBuf
 * ====================================================================== */

void ResultBuf::append(const char *s)
{
    if (truncated || !s) return;
    size_t len = strlen(s);

    /* Reserve 17 bytes for "... (truncated)" + NUL */
    if (pos + len >= cap - 17) {
        const char *marker = "... (truncated)";
        size_t mlen  = strlen(marker);
        size_t space = (cap > mlen + 1) ? (cap - mlen - 1) : 0;
        if (pos < space) {
            size_t copy = space - pos;
            if (copy > len) copy = len;
            memcpy(buf + pos, s, copy);
            pos += copy;
        }
        memcpy(buf + pos, marker, mlen);
        pos += mlen;
        buf[pos] = '\0';
        truncated = true;
        return;
    }

    memcpy(buf + pos, s, len);
    pos += len;
    buf[pos] = '\0';
}

void ResultBuf::append_char(char c)
{
    char tmp[2] = {c, '\0'};
    append(tmp);
}

/* ======================================================================
 * append_value — format one Value as human-readable text
 * ====================================================================== */

void ResultBuf::append_value(const Value &v, const ColumnDef &col)
{
    char tmp[64];

    if (v.is_null) { append("NULL"); return; }

    switch (col.type) {

    case TYPE_INT:
        snprintf(tmp, sizeof(tmp), "%d", v.v.int_val);
        append(tmp);
        break;

    case TYPE_DECIMAL: {
        /*
         * Stored as integer * 10^scale.
         * Reconstruct whole and fractional parts for display.
         * Example: stored=314, scale=2 → "3.14"
         */
        int scale = (col.scale > 0) ? col.scale : 2;
        int64_t divisor = 1;
        for (int i = 0; i < scale; i++) divisor *= 10;

        int64_t whole = v.v.decimal_val / divisor;
        int64_t frac  = v.v.decimal_val % divisor;
        if (frac < 0) frac = -frac;

        /* build format string: e.g. "%lld.%02lld" for scale=2 */
        char fmt[24];
        snprintf(fmt, sizeof(fmt), "%%lld.%%0%dlld", scale);
        snprintf(tmp, sizeof(tmp), fmt, (long long)whole, (long long)frac);
        append(tmp);
        break;
    }

    case TYPE_VARCHAR:
        /* data is always NUL-terminated (cast_literal ensures this) */
        append(v.v.varchar_val.data);
        break;

    case TYPE_BOOL:
        append(v.v.bool_val ? "true" : "false");
        break;

    case TYPE_ENUM:
        if (v.v.enum_val < col.num_enum_values)
            append(col.enum_values[v.v.enum_val]);
        else
            append("?");
        break;

    case TYPE_DATE: {
        /* stored as YYYYMMDD → display as DD-MM-YYYY */
        int y = v.v.date_val / 10000;
        int m = (v.v.date_val / 100) % 100;
        int d = v.v.date_val % 100;
        snprintf(tmp, sizeof(tmp), "%02d-%02d-%04d", d, m, y);
        append(tmp);
        break;
    }

    case TYPE_DATETIME: {
        /* stored as YYYYMMDDHHmmSS → display as YYYY-MM-DD HH:MM:SS */
        int64_t dt  = v.v.datetime_val;
        int sec  = (int)(dt % 100);  dt /= 100;
        int min  = (int)(dt % 100);  dt /= 100;
        int hour = (int)(dt % 100);  dt /= 100;
        int day  = (int)(dt % 100);  dt /= 100;
        int mon  = (int)(dt % 100);  dt /= 100;
        int year = (int)dt;
        snprintf(tmp, sizeof(tmp), "%04d-%02d-%02d %02d:%02d:%02d",
                 year, mon, day, hour, min, sec);
        append(tmp);
        break;
    }

    default:
        append("?");
        break;
    }
}

/* ======================================================================
 * finalize
 * ====================================================================== */

void ResultBuf::finalize(size_t nrows)
{
    char footer[64];
    snprintf(footer, sizeof(footer), "\n(%zu row%s)\n",
             nrows, nrows == 1 ? "" : "s");
    if (!truncated) append(footer);
}

/* ======================================================================
 * emit_header — column names + separator line
 * ====================================================================== */

void emit_header(ResultBuf &rb, const RelationDef *rel,
                 const SelectStatement *stmt)
{
    bool first = true;

    if (stmt->is_select_all) {
        /* SELECT * — print every column */
        for (int i = 0; i < (int)rel->num_columns; i++) {
            if (!first) rb.append(" | ");
            rb.append(rel->columns[i].name);
            first = false;
        }
    } else {
        /* Specific column list */
        for (const auto &item : stmt->select_list) {
            if (!first) rb.append(" | ");

            if (item.kind == SelectItem::Kind::Aggregate) {
                /* e.g. COUNT(id) or SUM(price) AS total */
                char tmp[128];
                if (!item.alias.empty())
                    snprintf(tmp, sizeof(tmp), "%s", item.alias.c_str());
                else
                    snprintf(tmp, sizeof(tmp), "%s(%s)",
                             item.agg_func.c_str(), item.column.c_str());
                rb.append(tmp);
            } else {
                /* Kind::Column — use alias if provided, else column name */
                const char *label = item.alias.empty()
                                    ? item.column.c_str()
                                    : item.alias.c_str();
                rb.append(label);
            }
            first = false;
        }
    }

    rb.append("\n");

    /*
     * Separator line: replace every non-separator character with '-'
     * and every ' | ' with '-+-'.
     * Simple approach: just emit a fixed-width dashes line.
     * We use the last-written position as an approximation of width.
     */
    size_t header_len = rb.pos;   /* length up to and including \n */
    /* emit dashes equal to the header width (minus the newline) */
    for (size_t i = 0; i + 1 < header_len; i++) {
        char c = rb.buf[i];
        if (c == '|')      rb.append_char('+');
        else if (c == ' ') rb.append_char('-');
        else               rb.append_char('-');
    }
    rb.append("\n");
}

/* ======================================================================
 * emit_row — one pipe-separated data row
 * ====================================================================== */

void emit_row(ResultBuf &rb, const RelationDef *rel,
              const Row *row, const SelectStatement *stmt)
{
    bool first = true;

    if (stmt->is_select_all) {
        for (int i = 0; i < (int)rel->num_columns; i++) {
            if (!first) rb.append(" | ");
            rb.append_value(row->cols[i], rel->columns[i]);
            first = false;
        }
    } else {
        for (const auto &item : stmt->select_list) {
            if (!first) rb.append(" | ");

            if (item.kind == SelectItem::Kind::Aggregate) {
                /* aggregates are resolved before emit_row is called —
                 * Phase 5 (dql.cpp) computes them and passes a pre-built
                 * row; for now just print NULL as a placeholder */
                rb.append("NULL");
            } else {
                /* find the column index by name */
                int idx = -1;
                for (int i = 0; i < (int)rel->num_columns; i++) {
                    if (strcmp(rel->columns[i].name, item.column.c_str()) == 0) {
                        idx = i;
                        break;
                    }
                }
                if (idx >= 0)
                    rb.append_value(row->cols[idx], rel->columns[idx]);
                else
                    rb.append("NULL");
            }
            first = false;
        }
    }

    rb.append("\n");
}
