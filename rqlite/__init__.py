"""rqlite - Python DB-API 2.0 client for rqlite distributed SQLite clusters.

This package provides:
- DB-API 2.0 compliant client for rqlite (sync and async)
- SQLAlchemy dialect integration (sync and async)
- Read consistency levels (LINEARIZABLE, WEAK, NONE, STRONG, AUTO)
- Transaction support with optional locking mechanism

Note on Transaction Warnings:
    When using this library **without a lock**, you will receive a `UserWarning`:

        UserWarning: Explicit BEGIN/COMMIT/ROLLBACK/SAVEPOINT SQL is not supported.

    This warning is **expected behavior** and is fine if you understand rqlite's
    transaction model (queue-based, atomic batch). To suppress this warning and
    indicate intentional handling of transaction limitations, provide a lock:

        >>> from rqlite import ThreadLock
        >>> conn = rqlite.connect(lock=ThreadLock())

    For true ACID compliance with proper isolation guarantees, it is recommended
    to use a lock (e.g., `ThreadLock()` for sync, `AioLock()` for async).

DB-API 2.0 Example (Sync):
    >>> import rqlite
    >>> from rqlite import ReadConsistency, ThreadLock
    >>> # Basic connection
    >>> conn = rqlite.connect(host="localhost", port=4001)
    >>> cursor = conn.cursor()
    >>> cursor.execute("SELECT * FROM users")
    >>> rows = cursor.fetchall()
    >>> conn.close()

DB-API 2.0 Example (Async):
    >>> import asyncio
    >>> import rqlite
    >>> from rqlite import AioLock
    >>> async def main():
    ...     conn = rqlite.async_connect(host="localhost", port=4001, lock=AioLock())
    ...     cursor = await conn.cursor()
    ...     await cursor.execute("SELECT * FROM users")
    ...     rows = cursor.fetchall()
    ...     await conn.close()
    >>> asyncio.run(main())

SQLAlchemy Usage (Sync):
    >>> from sqlalchemy import create_engine
    >>> engine = create_engine("rqlite://localhost:4001")

SQLAlchemy Usage (Async):
    >>> from sqlalchemy.ext.asyncio import create_async_engine
    >>> engine = create_async_engine("sqlite+aiorqlite://localhost:4001")
"""


from typing import Any

from .async_connection import AsyncConnection, async_connect
from .async_cursor import AsyncCursor
from .async_types import AioLock, AsyncLock, AsyncLockProtocol
from .connection import Connection, ReadConsistencyStr, connect
from .cursor import Cursor
from .exceptions import (
    DatabaseError,
    DataError,
    Error,
    IntegrityError,
    InterfaceError,
    InternalError,
    NotSupportedError,
    OperationalError,
    ProgrammingError,
)
from .types import (
    BINARY,
    DATETIME,
    NUMBER,
    ROWID,
    STRING,
    Lock,
    LockProtocol,
    ReadConsistency,
    ThreadLock,
    Time,
    Timestamp,
    adapt_value,
)

# DB-API 2.0 required attributes
paramstyle = "qmark"  # Use ? for positional parameters (also support :name)

# SQLite compatibility (required by SQLAlchemy SQLite dialect base)
sqlite_version_info = (3, 45, 0)  # Mock version for compatibility

__all__ = [
    # Sync DB-API 2.0
    "connect",
    "Connection",
    "Cursor",
    "ReadConsistencyStr",
    # Async DB-API 2.0
    "async_connect",
    "AsyncConnection",
    "AsyncCursor",
    # Exceptions (DB-API 2.0)
    "Error",
    "InterfaceError",
    "DatabaseError",
    "DataError",
    "OperationalError",
    "IntegrityError",
    "InternalError",
    "ProgrammingError",
    "NotSupportedError",
    # Types (DB-API 2.0)
    "STRING",
    "BINARY",
    "NUMBER",
    "DATETIME",
    "ROWID",
    "Time",
    "Timestamp",
    "adapt_value",
    # Read Consistency
    "ReadConsistency",
    # Sync Locking
    "LockProtocol",
    "Lock",
    "ThreadLock",
    # Async Locking
    "AsyncLockProtocol",
    "AsyncLock",
    "AioLock",
    # Redis distributed locks (lazy import)
    "RedisLock",
    "AioRedisLock",
    # Valkey distributed locks (lazy import)
    "ValkeyLock",
    "AioValkeyLock",
]


def __getattr__(name: str) -> Any:
    """Lazy import for optional redis/valkey-dependent classes."""
    if name == "RedisLock":
        from .redis_lock import RedisLock as _RedisLock

        return _RedisLock
    if name == "AioRedisLock":
        from .async_redis_lock import AioRedisLock as _AioRedisLock

        return _AioRedisLock
    if name == "ValkeyLock":
        from .valkey_lock import ValkeyLock as _ValkeyLock

        return _ValkeyLock
    if name == "AioValkeyLock":
        from .async_valkey_lock import AioValkeyLock as _AioValkeyLock

        return _AioValkeyLock
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

__version__ = "0.1.0"
