"""Type definitions and helpers for rqlite DB-API 2.0 compliance."""
from __future__ import annotations

import datetime
import threading
from enum import Enum
from types import TracebackType
from typing import Any, Protocol, TypeVar, runtime_checkable


class ReadConsistency(Enum):
    """Read consistency levels for rqlite queries.

    See: https://rqlite.io/docs/db_api/#read-consistency-levels

    Attributes:
        WEAK: Fast reads, usually current (sub-second staleness possible).
              Node checks local leadership state.
        LINEARIZABLE: Guaranteed fresh reads. Leader contacts quorum before
                      serving read. Recommended for critical reads.
        NONE: Fastest reads, no consistency guarantees. No leadership check.
              Use with read-only nodes or when staleness is acceptable.
        STRONG: Testing only. Goes through full Raft consensus. Slow.
        AUTO: Node automatically chooses (NONE for read-only, WEAK for voting).
    """

    WEAK = "weak"
    LINEARIZABLE = "linearizable"
    NONE = "none"
    STRONG = "strong"
    AUTO = "auto"

    def to_query_param(self) -> str:
        """Return the query parameter value for this consistency level."""
        return self.value


def Date(year: int, month: int, day: int) -> datetime.date:  # noqa: N802
    """Construct a date object from year, month, day (DB-API 2.0)."""
    return datetime.date(year, month, day)


def Time(hour: int, minute: int, second: int) -> datetime.time:  # noqa: N802
    """Construct a time object from hour, minute, second (DB-API 2.0)."""
    return datetime.time(hour, minute, second)


def Timestamp(  # noqa: N802
    year: int,
    month: int,
    day: int,
    hour: int,
    minute: int,
    second: int,
) -> datetime.datetime:
    """Construct a datetime object (DB-API 2.0)."""
    return datetime.datetime(year, month, day, hour, minute, second)


def DateFromTicks(ticks: float) -> datetime.date:  # noqa: N802
    """Construct a date object from timestamp ticks (DB-API 2.0)."""
    return datetime.date.fromtimestamp(ticks)


def TimeFromTicks(ticks: float) -> datetime.time:  # noqa: N802
    """Construct a time object from timestamp ticks (DB-API 2.0)."""
    return datetime.datetime.fromtimestamp(ticks).time()


def TimestampFromTicks(ticks: float) -> datetime.datetime:  # noqa: N802
    """Construct a datetime object from timestamp ticks (DB-API 2.0)."""
    return datetime.datetime.fromtimestamp(ticks)


# Type objects for DB-API 2.0 compliance
STRING = str
BINARY = bytes
NUMBER = (int, float)
DATETIME = (datetime.date, datetime.time, datetime.datetime)
ROWID = int


# Lock protocol based on threading.Lock interface
@runtime_checkable
class LockProtocol(Protocol):
    """Protocol for lock implementations compatible with rqlite transactions.

    Any class implementing these methods satisfies this protocol,
    including threading.Lock, ThreadLock, or custom locks.

    Example:
        >>> import threading
        >>> lock = threading.Lock()  # Satisfies LockProtocol
        >>> from rqlite import ThreadLock
        >>> thread_lock = ThreadLock()  # Also satisfies LockProtocol
    """

    def __init__(self) -> None: ...
    def acquire(self, blocking: bool = ..., timeout: float = ...) -> bool: ...
    def release(self) -> None: ...
    def __enter__(self) -> LockProtocol: ...
    def __exit__(
        self,
        exc_type: type[Exception] | None,
        exc_val: Exception | None,
        exc_tb: Any,
    ) -> Any: ...


# Type variable for lock implementations
LockT = TypeVar("LockT", bound=LockProtocol)


class Lock:
    """Abstract base class for lock implementations satisfying LockProtocol.

    This is an abstract class that should NOT be used directly. Users should
    either:
    1. Use ThreadLock for thread-safe locking based on threading.Lock
    2. Use threading.Lock directly (it satisfies LockProtocol)
    3. Subclass this to implement custom locking behavior

    Example:
        >>> from rqlite import ThreadLock, connect
        >>> conn = connect(lock=ThreadLock())  # Thread-safe locking
        >>> # Or use threading.Lock directly
        >>> import threading
        >>> conn = connect(lock=threading.Lock())
    """

    def __init__(self) -> None:
        """Initialize the lock.

        Raises:
            NotImplementedError: This abstract class should not be instantiated directly.
        """
        raise NotImplementedError(
            "Lock is an abstract class. Use ThreadLock or threading.Lock instead."
        )

    def acquire(self, blocking: bool = True, timeout: float = -1) -> bool:
        """Acquire the lock.

        Args:
            blocking: Whether to block waiting for lock.
            timeout: Maximum time to wait.

        Returns:
            True if lock acquired, False otherwise.

        Raises:
            NotImplementedError: This abstract class should not be used directly.
        """
        raise NotImplementedError(
            "Lock is an abstract class. Use ThreadLock or threading.Lock instead."
        )

    def release(self) -> None:
        """Release the lock.

        Raises:
            NotImplementedError: This abstract class should not be used directly.
        """
        raise NotImplementedError(
            "Lock is an abstract class. Use ThreadLock or threading.Lock instead."
        )

    def __enter__(self) -> Lock:
        """Enter context manager.

        Returns:
            Self for use in with statement.

        Raises:
            NotImplementedError: This abstract class should not be used directly.
        """
        raise NotImplementedError(
            "Lock is an abstract class. Use ThreadLock or threading.Lock instead."
        )

    def __exit__(
        self,
        exc_type: type[Exception] | None,
        exc_val: Exception | None,
        exc_tb: object,
    ) -> None:
        """Exit context manager.

        Args:
            exc_type: Exception type if an exception occurred.
            exc_val: Exception instance if an exception occurred.
            exc_tb: Exception traceback if an exception occurred.

        Raises:
            NotImplementedError: This abstract class should not be used directly.
        """
        raise NotImplementedError(
            "Lock is an abstract class. Use ThreadLock or threading.Lock instead."
        )


class ThreadLock(Lock):
    """Thread-safe lock wrapper based on threading.Lock.

    This class wraps threading.Lock to provide a rqlite-compatible lock
    implementation for transaction support.

    Example:
        >>> from rqlite import ThreadLock, connect
        >>> conn = connect(lock=ThreadLock())
        >>> cursor = conn.cursor()
        >>> cursor.execute("BEGIN")  # No warning, thread-safe
    """

    def __init__(self) -> None:
        """Initialize the thread lock."""
        self._lock = threading.Lock()

    def acquire(self, blocking: bool = True, timeout: float = -1) -> bool:
        """Acquire the lock.

        Args:
            blocking: Whether to block waiting for lock (default: True).
            timeout: Maximum time to wait in seconds (default: -1, wait forever).

        Returns:
            True if lock was acquired, False otherwise.
        """
        return self._lock.acquire(blocking=blocking, timeout=timeout)

    def release(self) -> None:
        """Release the lock."""
        self._lock.release()

    def __enter__(self) -> ThreadLock:
        """Enter context manager.

        Returns:
            Self for use in with statement.
        """
        self._lock.__enter__()
        return self

    def __exit__(
        self,
        exc_type: type[Exception] | None,
        exc_val: Exception | None,
        exc_tb: TracebackType | None,
    ) -> None:
        """Exit context manager.

        Args:
            exc_type: Exception type if an exception occurred.
            exc_val: Exception instance if an exception occurred.
            exc_tb: Exception traceback if an exception occurred.
        """
        self._lock.__exit__(exc_type, exc_val, exc_tb)  # type: ignore[arg-type]


def adapt_value(value: Any) -> Any:
    """Adapt Python values to rqlite-compatible JSON values.

    Args:
        value: Python value to adapt.

    Returns:
        JSON-serializable value.
    """
    if value is None:
        return None
    if isinstance(value, (int, float, str)):
        return value
    if isinstance(value, bytes):
        # Encode bytes as hex string for BLOB support
        return value.hex()
    if isinstance(value, datetime.datetime):
        return value.isoformat()
    if isinstance(value, datetime.date):
        return value.isoformat()
    if isinstance(value, datetime.time):
        return value.isoformat()
    # For other types, try string conversion
    return str(value)
