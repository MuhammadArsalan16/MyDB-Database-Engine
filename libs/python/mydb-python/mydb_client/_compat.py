"""Python 2.7 / 3.x compatibility shims. No external dependency (no `six`)."""

import sys

PY2 = sys.version_info[0] == 2


def to_bytes(s):
    """Return `s` as bytes, encoding str->utf-8 on py3. On py2, str is
    already bytes, so only unicode gets encoded."""
    if isinstance(s, bytes):
        return s
    return s.encode("utf-8")


def to_text(b):
    """Return `b` as a text string, decoding utf-8 on both py2 and py3."""
    if PY2:
        if isinstance(b, unicode):  # noqa: F821 (py2-only builtin)
            return b
        return b.decode("utf-8", "replace")
    if isinstance(b, str):
        return b
    return b.decode("utf-8", "replace")
