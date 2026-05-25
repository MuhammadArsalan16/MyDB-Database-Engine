#pragma once
/*
 * result_fmt.hpp — format storage rows and error codes into result strings.
 *
 * The execution engine writes its output into a fixed-size char buffer
 * (result_out / result_cap from exec_engine_execute).  This module
 * provides safe, append-only writing into that buffer and helpers for
 * emitting tabular SELECT output and error messages.
 *
 * Implemented in src/result_fmt.cpp (Phase 2).
 */

#include <cstddef>
#include <cstdio>
extern "C" {
#include "common.h"
#include "relation_def.h"
#include "storage.h"
}
#include "AST.hpp"

/* ------------------------------------------------------------------
 * format_error — map a MYDB_* return code to a human-readable message.
 *
 * ctx is an optional context string appended after the message
 * (e.g. a table name or column name).  Pass nullptr to omit it.
 * Always writes a NUL-terminated string within cap bytes.
 * ------------------------------------------------------------------ */
void format_error(int rc, char *out, size_t cap, const char *ctx);

/* ------------------------------------------------------------------
 * ResultBuf — safe append-only buffer for building result strings.
 *
 * Wraps the caller-supplied (char *buf, size_t cap) pair and tracks
 * the current write position.  On overflow it sets truncated=true and
 * writes "... (truncated)" at the end instead of crashing.
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

    /* Append a Value formatted as a human-readable string. */
    void append_value(const Value &v, const ColumnDef &col);

    /* Append the footer line "\n(N rows)\n" and NUL-terminate.
     * If truncated, appends "... (truncated)" before the footer. */
    void finalize(size_t nrows);
};

/* ------------------------------------------------------------------
 * SELECT output helpers
 *
 * emit_header — write a tab-separated line of column names based on
 *               the SELECT list (or all column names for SELECT *).
 *
 * emit_row    — write one tab-separated data row, projecting the
 *               columns requested by the SELECT list.
 * ------------------------------------------------------------------ */
void emit_header(ResultBuf &rb, const RelationDef *rel,
                 const SelectStatement *stmt);

void emit_row(ResultBuf &rb, const RelationDef *rel,
              const Row *row, const SelectStatement *stmt);
