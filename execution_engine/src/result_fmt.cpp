/*
 * result_fmt.cpp — format storage rows and error codes into result strings.
 *
 * Phase 2 implements the full versions.  The ResultBuf skeleton is
 * provided here already so it can be used by the stubs.
 */

#include "result_fmt.hpp"

#include <cstring>
#include <cstdio>

/* ------------------------------------------------------------------ */
/*  format_error                                                       */
/* ------------------------------------------------------------------ */

void format_error(int rc, char *out, size_t cap, const char *ctx)
{
    const char *msg;
    switch (rc) {
    case MYDB_OK:                 msg = "OK";                         break;
    case MYDB_ERR_NOT_FOUND:      msg = "not found";                  break;
    case MYDB_ERR_DUPLICATE:      msg = "duplicate key value";        break;
    case MYDB_ERR_FULL:           msg = "storage full";               break;
    case MYDB_ERR_FK_VIOLATION:   msg = "foreign key violation";      break;
    case MYDB_ERR_NULL_VIOLATION: msg = "NULL not allowed";           break;
    case MYDB_ERR_NO_TXN:         msg = "no active transaction";      break;
    case MYDB_ERR_PERM:           msg = "permission denied";          break;
    case MYDB_ERR_CROSS_SCHEMA:   msg = "cross-schema query not supported"; break;
    case MYDB_ERR_BAD_MAGIC:
    case MYDB_ERR_BAD_FILE_TYPE:
    case MYDB_ERR_BAD_VERSION:
    case MYDB_ERR_BAD_CHECKSUM:   msg = "database corruption";        break;
    default:                      msg = "internal error";             break;
    }

    if (ctx)
        std::snprintf(out, cap, "%s: %s", msg, ctx);
    else
        std::snprintf(out, cap, "%s", msg);
}

/* ------------------------------------------------------------------ */
/*  ResultBuf                                                          */
/* ------------------------------------------------------------------ */

void ResultBuf::append(const char *s)
{
    if (truncated || !s) return;
    size_t len = std::strlen(s);
    /* Reserve room for the truncation marker (16 bytes) + NUL. */
    if (pos + len >= cap - 17) {
        const char *marker = "... (truncated)";
        size_t mlen = std::strlen(marker);
        size_t space = (cap > mlen + 1) ? (cap - mlen - 1) : 0;
        if (pos < space) {
            size_t copy = space - pos;
            if (copy > len) copy = len;
            std::memcpy(buf + pos, s, copy);
            pos += copy;
        }
        std::memcpy(buf + pos, marker, mlen);
        pos += mlen;
        buf[pos] = '\0';
        truncated = true;
        return;
    }
    std::memcpy(buf + pos, s, len);
    pos += len;
    buf[pos] = '\0';
}

void ResultBuf::append_char(char c)
{
    char tmp[2] = {c, '\0'};
    append(tmp);
}

void ResultBuf::append_value(const Value &v, const ColumnDef & /*col*/)
{
    /* Phase 2 will add type-aware formatting.
     * For now just emit NULL or a placeholder. */
    if (v.is_null) { append("NULL"); return; }
    append("?");
}

void ResultBuf::finalize(size_t nrows)
{
    char footer[64];
    std::snprintf(footer, sizeof(footer), "\n(%zu rows)\n", nrows);
    if (!truncated) append(footer);
}

/* ------------------------------------------------------------------ */
/*  SELECT output helpers  (Phase 2 fills in full implementations)    */
/* ------------------------------------------------------------------ */

void emit_header(ResultBuf &rb, const RelationDef * /*rel*/,
                 const SelectStatement * /*stmt*/)
{
    rb.append("(header not yet implemented)\n");
}

void emit_row(ResultBuf &rb, const RelationDef * /*rel*/,
              const Row * /*row*/, const SelectStatement * /*stmt*/)
{
    rb.append("(row not yet implemented)\n");
}
