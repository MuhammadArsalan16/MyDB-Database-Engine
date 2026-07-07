"""mydb_client — pure-Python client for the MyDB database engine's mydbd
daemon. Speaks the same wire protocol as the C `mydb` CLI; no third-party
dependencies. Compatible with Python 2.7 and Python 3.
"""

from mydb_client.connection import MyDBConnection, MyDBError, DEFAULT_PORT

__version__ = "0.1.0"

__all__ = ["connect", "MyDBConnection", "MyDBError", "DEFAULT_PORT"]


def connect(host=None, port=DEFAULT_PORT, unix_path=None,
            user=None, password=None):
    """Connect and authenticate to mydbd.

    Either `host` (+ optional `port`, TCP) or `unix_path` (Unix socket) must
    be given, but not both. `password` is prompted for interactively
    (getpass) if omitted.

    Returns an open, authenticated MyDBConnection. Raises MyDBError on
    connect/auth failure.
    """
    return MyDBConnection(host=host, port=port, unix_path=unix_path,
                          user=user, password=password)
