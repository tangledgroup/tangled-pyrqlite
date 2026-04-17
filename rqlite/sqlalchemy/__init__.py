"""SQLAlchemy dialect for rqlite.

This module provides SQLAlchemy integration for rqlite databases.

Features:
    - Full SQLAlchemy Core and ORM support via SQLite dialect extension
    - Synchronous dialect (rqlite://)
    - Asynchronous dialect (rqlite+aiorqlite://)
    - Read consistency levels (LINEARIZABLE, WEAK, NONE, STRONG, AUTO)
    - Transaction support with optional locking mechanism

Note on Transaction Warnings:
    When using this dialect **without a lock**, you will receive a `UserWarning`:
    
        UserWarning: Explicit BEGIN/COMMIT/ROLLBACK/SAVEPOINT SQL is not supported.
    
    This warning indicates that explicit transaction SQL commands are not supported
    in rqlite's traditional sense. This is **expected behavior** and is fine if you
    understand rqlite's queue-based transaction model.
    
    To suppress this warning and indicate intentional handling of transaction
    limitations, provide a lock via connect_args:
    
        >>> from sqlalchemy import create_engine
        >>> from rqlite import ThreadLock
        >>> engine = create_engine(
        ...     "rqlite://localhost:4001",
        ...     connect_args={"lock": ThreadLock()}
        ... )
    
    For true ACID compliance with proper isolation guarantees, it is recommended
    to use a lock.

Sync Usage:
    Basic engine:
        >>> from sqlalchemy import create_engine
        >>> engine = create_engine("rqlite://localhost:4001")

    With authentication:
        >>> engine = create_engine("rqlite://user:pass@localhost:4001")

    With read consistency and lock via connect_args:
        >>> from rqlite import ReadConsistency, ThreadLock
        >>> engine = create_engine(
        ...     "rqlite://localhost:4001",
        ...     connect_args={
        ...         "read_consistency": ReadConsistency.WEAK,
        ...         "lock": ThreadLock()
        ...     }
        ... )

Async Usage:
    Basic async engine:
        >>> from sqlalchemy.ext.asyncio import create_async_engine
        >>> engine = create_async_engine("rqlite+aiorqlite://localhost:4001")

    With read consistency and lock via connect_args:
        >>> from rqlite import ReadConsistency, AioLock
        >>> engine = create_async_engine(
        ...     "rqlite+aiorqlite://localhost:4001",
        ...     connect_args={
        ...         "read_consistency": ReadConsistency.WEAK,
        ...         "lock": AioLock()
        ...     }
        ... )
"""

from .async_dialect import AioRQLiteDialect, AioRQLiteDialect_pyrlite
from .dialect import RQLiteDialect, RQLiteDialect_pyrlite

__all__ = [
    "RQLiteDialect",
    "RQLiteDialect_pyrlite",
    "AioRQLiteDialect",
    "AioRQLiteDialect_pyrlite",
]
