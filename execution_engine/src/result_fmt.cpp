/*
 * result_fmt.cpp — Design 3 output formatting implementation.
 *
 * format_error  — maps MYDB_* codes to "  Error: ..." strings.
 * value_to_str  — Value → std::string for any column type.
 * ResultBuf     — safe append-only buffer (unchanged interface).
 * TableBuilder  — two-pass column-aligned table renderer.
 *
 * Design 3 table format:
 *   id    name        age
 *   ---   ----------  ---
 *   1     Alice        30
 *   2     Bob          25
 *
 *   (2 rows)
 *
 * Column separator: two spaces ("  ").
 * All columns are left-aligned.
 * Footer produced by finalize(): "\n(N rows)".
 */

#include "result_fmt.hpp"

#include <algorithm>
#include <cstring>
#include <cstdio>

/* ======================================================================
 * format_error
 * ====================================================================== */

void format_error(int rc, char *out, size_t cap, const char *ctx)
{
    const char *msg;
    switch (rc) {
    case MYDB_OK:                 msg = "OK";                               break;
    case MYDB_ERR_NOT_FOUND:      msg = "not found";                        break;
    case MYDB_ERR_DUPLICATE:      msg = "duplicate key value";              break;
    case MYDB_ERR_FULL:           msg = "storage full";                     break;
    case MYDB_ERR_FK_VIOLATION:   msg = "foreign key violation";            break;
    case MYDB_ERR_NULL_VIOLATION: msg = "NULL not allowed";                 break;
    case MYDB_ERR_NO_TXN:         msg = "no active transaction";            break;
    case MYDB_ERR_PERM:           msg = "permission denied";                break;
    case MYDB_ERR_CROSS_SCHEMA:   msg = "cross-schema query not supported"; break;
    case MYDB_ERR_BAD_MAGIC:
    case MYDB_ERR_BAD_FILE_TYPE:
    case MYDB_ERR_BAD_VERSION:
    case MYDB_ERR_BAD_CHECKSUM:   msg = "database corruption";              break;
    default:                      msg = "internal error";                   break;
    }

    if (ctx) snprintf(out, cap, "  Error: %s: %s", msg, ctx);
    else     snprintf(out, cap, "  Error: %s",     msg);
}

/* ======================================================================
 * value_to_str
 * ====================================================================== */

std::string value_to_str(const Value &v, const ColumnDef &col)
{
    if (v.is_null) return "NULL";

    char tmp[128];

    switch (col.type) {

    case TYPE_INT:
        snprintf(tmp, sizeof(tmp), "%d", v.v.int_val);
        return tmp;

    case TYPE_DECIMAL: {
        int scale   = (col.scale > 0) ? col.scale : 2;
        int64_t div = 1;
        for (int i = 0; i < scale; i++) div *= 10;
        int64_t whole = v.v.decimal_val / div;
        int64_t frac  = v.v.decimal_val % div;
        if (frac < 0) frac = -frac;
        char fmt[24];
        snprintf(fmt, sizeof(fmt), "%%lld.%%0%dlld", scale);
        snprintf(tmp, sizeof(tmp), fmt, (long long)whole, (long long)frac);
        return tmp;
    }

    case TYPE_VARCHAR:
        return std::string(v.v.varchar_val.data);

    case TYPE_BOOL:
        return v.v.bool_val ? "true" : "false";

    case TYPE_ENUM:
        if (v.v.enum_val < col.num_enum_values)
            return col.enum_values[v.v.enum_val];
        return "?";

    case TYPE_DATE: {
        int y = v.v.date_val / 10000;
        int m = (v.v.date_val / 100) % 100;
        int d = v.v.date_val % 100;
        snprintf(tmp, sizeof(tmp), "%02d-%02d-%04d", d, m, y);
        return tmp;
    }

    case TYPE_DATETIME: {
        int64_t dt  = v.v.datetime_val;
        int sec  = (int)(dt % 100); dt /= 100;
        int min  = (int)(dt % 100); dt /= 100;
        int hour = (int)(dt % 100); dt /= 100;
        int day  = (int)(dt % 100); dt /= 100;
        int mon  = (int)(dt % 100); dt /= 100;
        int year = (int)dt;
        snprintf(tmp, sizeof(tmp), "%04d-%02d-%02d %02d:%02d:%02d",
                 year, mon, day, hour, min, sec);
        return tmp;
    }

    default:
        return "?";
    }
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

void ResultBuf::append_value(const Value &v, const ColumnDef &col)
{
    std::string s = value_to_str(v, col);
    append(s.c_str());
}

void ResultBuf::finalize(size_t nrows)
{
    /* Blank line + footer.  Engine timing ("  (0.01s)") is appended
     * by engine_execute_sql() directly after this footer. */
    char footer[64];
    snprintf(footer, sizeof(footer), "\n(%zu row%s)",
             nrows, nrows == 1 ? "" : "s");
    if (!truncated) append(footer);
}

/* ======================================================================
 * TableBuilder
 * ====================================================================== */

void TableBuilder::set_headers(const std::vector<std::string> &h)
{
    headers_ = h;
}

void TableBuilder::add_row(const std::vector<std::string> &r)
{
    rows_.push_back(r);
}

/*
 * render() — two-pass aligned table.
 *
 * Pass 1: compute max width per column (max of header and all cell widths).
 * Pass 2: emit header, separator, data rows, and "(N rows)" footer.
 *
 * Column separator: two spaces.
 * All columns left-aligned (trailing spaces on all but the last column).
 */
void TableBuilder::render(ResultBuf &rb) const
{
    if (headers_.empty()) {
        rb.finalize(0);
        return;
    }

    int ncols = (int)headers_.size();

    /* Pass 1 — compute per-column widths. */
    std::vector<size_t> widths(ncols, 0);
    for (int i = 0; i < ncols; i++)
        widths[i] = headers_[i].size();
    for (const auto &row : rows_)
        for (int i = 0; i < ncols && i < (int)row.size(); i++)
            widths[i] = std::max(widths[i], row[i].size());

    /* Pass 2a — header row. */
    for (int i = 0; i < ncols; i++) {
        if (i > 0) rb.append("  ");
        const std::string &h = headers_[i];
        rb.append(h.c_str());
        /* Pad to column width (skip trailing pad on last column). */
        if (i + 1 < ncols)
            for (size_t p = h.size(); p < widths[i]; p++) rb.append_char(' ');
    }
    rb.append("\n");

    /* Pass 2b — separator row (dashes). */
    for (int i = 0; i < ncols; i++) {
        if (i > 0) rb.append("  ");
        for (size_t p = 0; p < widths[i]; p++) rb.append_char('-');
    }
    rb.append("\n");

    /* Pass 2c — data rows. */
    for (const auto &row : rows_) {
        for (int i = 0; i < ncols; i++) {
            if (i > 0) rb.append("  ");
            const std::string &cell = (i < (int)row.size()) ? row[i] : "";
            rb.append(cell.c_str());
            if (i + 1 < ncols)
                for (size_t p = cell.size(); p < widths[i]; p++) rb.append_char(' ');
        }
        rb.append("\n");
    }

    /* Footer. */
    rb.finalize(rows_.size());
}
