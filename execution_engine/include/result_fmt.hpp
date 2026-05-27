#pragma once
/*
 * result_fmt.hpp — Design 3 output formatting.
 *
 * ResultBuf     : safe append-only output buffer.
 * TableBuilder  : two-pass column-width-aligned table renderer.
 * format_error  : writes "  Error: ..." into a caller-supplied out buffer.
 * value_to_str  : Value → std::string (shared by dql.cpp / ddl.cpp helpers).
 *
 * Design 3 output conventions:
 *   - DDL/DML/TCL success: "OK  <action>"
 *   - Errors:              "  Error: <message>"
 *   - Tabular output:      space-aligned columns, dash separator under header,
 *                          "(N rows)" footer.
 *   - Timing:              appended by engine.c as "  (%.2fs)" on last line.
 */

#include <cstddef>
#include <cstdio>
#include <string>
#include <vector>
extern "C" {
#include "common.h"
#include "relation_def.h"
#include "storage.h"
}
#include "AST.hpp"

/* ------------------------------------------------------------------
 * format_error — map a MYDB_* return code + optional context to a
 * Design 3 error string:  "  Error: <message>" or
 *                         "  Error: <message>: <ctx>"
 *
 * Always writes a NUL-terminated string within cap bytes.
 * ------------------------------------------------------------------ */
void format_error(int rc, char *out, size_t cap, const char *ctx);

/* ------------------------------------------------------------------
 * value_to_str — format a Value as a human-readable std::string.
 * Returns "NULL" when v.is_null.
 * ------------------------------------------------------------------ */
std::string value_to_str(const Value &v, const ColumnDef &col);

/* ------------------------------------------------------------------
 * ResultBuf — safe append-only buffer for building result strings.
 * ------------------------------------------------------------------ */
struct ResultBuf {
    char   *buf;
    size_t  cap;
    size_t  pos;
    bool    truncated;

    ResultBuf(char *b, size_t c)
        : buf(b), cap(c), pos(0), truncated(false) {}

    /* Append a NUL-terminated C string. */
    void append(const char *s);

    /* Append a single character. */
    void append_char(char c);

    /* Append a Value formatted as human-readable text (kept for compatibility). */
    void append_value(const Value &v, const ColumnDef &col);

    /* Append "\n(N row[s])" footer.  Engine adds "  (Xs)" timing after this. */
    void finalize(size_t nrows);
};

/* ------------------------------------------------------------------
 * TableBuilder — two-pass aligned table renderer (Design 3).
 *
 * Collects column headers and row data as strings, then on render()
 * computes per-column max widths and emits a clean aligned table:
 *
 *   id    name        age
 *   ---   ----------  ---
 *   1     Alice        30
 *   2     Bob          25
 *
 *   (2 rows)
 *
 * Usage:
 *   TableBuilder tb;
 *   tb.set_headers({"id", "name", "age"});
 *   tb.add_row({"1", "Alice", "30"});
 *   tb.render(rb);
 * ------------------------------------------------------------------ */
class TableBuilder {
public:
    void set_headers(const std::vector<std::string> &h);
    void add_row(const std::vector<std::string> &r);
    void render(ResultBuf &rb) const;   /* writes table + (N rows) footer */
    size_t row_count() const { return rows_.size(); }
    bool   empty()     const { return headers_.empty(); }

private:
    std::vector<std::string>              headers_;
    std::vector<std::vector<std::string>> rows_;
};
