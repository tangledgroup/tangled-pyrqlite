"""Async type definitions and helpers for rqlite DB-API 2.0 compliance."""
from __future__ import annotations

import asyncio
from typing import Protocol, runtime_checkable


@runtime_checkable
class AsyncLockProtocol(Protocol):
    """Async-compatible lock protocol with async acquire/release methods.

    Any class implementing these methods satisfies this protocol,
    including AioLock or custom async locks.

    Example:
        >>> from rqlite import AioLock
        >>> lock = AioLock()  # Satisfies AsyncLockProtocol
    """

    def __init__(self) -> None: ...

    async def acquire(self, blocking: bool = ..., timeout: float = ...) -> bool: ...

    async def release(self) -> None: ...

    async def __aenter__(self) -> AsyncLockProtocol: ...

    async def __aexit__(
        self,
        exc_type: type[Exception] | None,
        exc_val: Exception | None,
        exc_tb: object,
    ) -> None: ...


class AsyncLock:
    """Abstract base class for async lock implementations satisfying AsyncLockProtocol.

    This is an abstract class that should NOT be used directly. Users should
    either:
    1. Use AioLock for async-safe locking based on asyncio.Lock
    2. Subclass this to implement custom async locking behavior

    Example:
        >>> from rqlite import AioLock, async_connect
        >>> conn = await async_connect(lock=AioLock())  # Async-safe locking
    """

    def __init__(self) -> None:
        """Initialize the lock.

        Raises:
            NotImplementedError: This abstract class should not be instantiated directly.
        """
        raise NotImplementedError(
            "AsyncLock is an abstract class. Use AioLock instead."
        )

    async def acquire(self, blocking: bool = True, timeout: float = -1) -> bool:
        """Acquire the lock asynchronously.

        Args:
            blocking: Whether to block waiting for lock.
            timeout: Maximum time to wait.

        Returns:
            True if lock acquired, False otherwise.

        Raises:
            NotImplementedError: This abstract class should not be used directly.
        """
        raise NotImplementedError(
            "AsyncLock is an abstract class. Use AioLock instead."
        )

    async def release(self) -> None:
        """Release the lock asynchronously.

        Raises:
            NotImplementedError: This abstract class should not be used directly.
        """
        raise NotImplementedError(
            "AsyncLock is an abstract class. Use AioLock instead."
        )

    async def __aenter__(self) -> AsyncLock:
        """Enter async context manager.

        Returns:
            Self for use in async with statement.

        Raises:
            NotImplementedError: This abstract class should not be instantiated directly.
        """
        raise NotImplementedError(
            "AsyncLock is an abstract class. Use AioLock instead."
        )

    async def __aexit__(
        self,
        exc_type: type[Exception] | None,
        exc_val: Exception | None,
        exc_tb: object,
    ) -> None:
        """Exit async context manager.

        Args:
            exc_type: Exception type if an exception occurred.
            exc_val: Exception instance if an exception occurred.
            exc_tb: Exception traceback if an exception occurred.

        Raises:
            NotImplementedError: This abstract class should not be instantiated directly.
        """
        raise NotImplementedError(
            "AsyncLock is an abstract class. Use AioLock instead."
        )


class AioLock(AsyncLock):
    """Async lock implementation using asyncio.Lock internally.

    This class wraps asyncio.Lock to provide a rqlite-compatible async lock
    implementation for transaction support.

    Note: asyncio.Lock is used internally but never exposed directly.
    All public methods (acquire, release) are async coroutines.

    Example:
        >>> from rqlite import AioLock, async_connect
        >>> conn = await async_connect(lock=AioLock())
        >>> cursor = await conn.cursor()
        >>> await cursor.execute("BEGIN")  # No warning, async-safe
    """

    def __init__(self) -> None:
        """Initialize the async lock."""
        self._lock = asyncio.Lock()

    async def acquire(self, blocking: bool = True, timeout: float = -1) -> bool:
        """Acquire the lock asynchronously.

        Args:
            blocking: Whether to wait for the lock (default: True).
            timeout: Maximum time to wait in seconds (default: -1, wait forever).

        Returns:
            True if lock was acquired, False if timeout occurred.
        """
        if timeout >= 0:
            try:
                await asyncio.wait_for(self._lock.acquire(), timeout=timeout)
                return True
            except asyncio.TimeoutError:
                return False
        else:
            await self._lock.acquire()
            return True

    async def release(self) -> None:
        """Release the lock asynchronously."""
        self._lock.release()

    async def __aenter__(self) -> AioLock:
        """Enter async context manager.

        Returns:
            Self for use in async with statement.
        """
        await self.acquire()
        return self

    async def __aexit__(
        self,
        exc_type: type[Exception] | None,
        exc_val: Exception | None,
        exc_tb: object,
    ) -> None:
        """Exit async context manager.

        Args:
            exc_type: Exception type if an exception occurred.
            exc_val: Exception instance if an exception occurred.
            exc_tb: Exception traceback if an exception occurred.
        """
        await self.release()
