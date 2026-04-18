"""Asynchronous Valkey distributed lock for rqlite.

Wraps valkey-py's ``valkey.asyncio.lock.Lock`` to provide an async Valkey-backed
distributed lock that serializes transactions across multiple tasks, coroutines,
and processes, enabling true ACID isolation for rqlite's queue-based transaction
model.

Usage:
    >>> import asyncio
    >>> from rqlite import AioValkeyLock, async_connect
    >>>
    >>> async def main():
    ...     lock = AioValkeyLock(name="transfer", host="localhost", port=6379)
    ...     conn = await async_connect(lock=lock)
    ...     cursor = await conn.cursor()
    ...     async with lock:
    ...         await cursor.execute("SELECT balance FROM accounts WHERE id=?", (1,))
    ...         balance = cursor.fetchone()[0]
    ...         await cursor.execute(
    ...             "UPDATE accounts SET balance=? WHERE id=?",
    ...             (balance - 100, 1),
    ...         )
    ...         await conn.commit()
    >>> asyncio.run(main())

Prerequisites:
    Install optional dependency:
        uv add tangled-pyrqlite[valkey]

    Start Valkey server:
        podman rm -f valkey-test
        podman run -d --name valkey-test -p 6379:6379 docker.io/valkey/valkey

Note:
    The valkey package is optional. This module raises ImportError
    at import time if valkey is not installed, since it's a top-level
    import from rqlite.__init__. Use try/except around the import
    or install with [valkey] extra.
"""
from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    pass

# Import valkey at module level — requires [valkey] extra
try:
    import valkey  # noqa: PLC0417
    from valkey.asyncio.lock import Lock as _AsyncValkeyLock  # noqa: PLC0417
except ImportError as exc:
    raise ImportError(
        "valkey package is required for AioValkeyLock. "
        "Install it with: uv add tangled-pyrqlite[valkey]"
    ) from exc


class AioValkeyLock:
    """Async distributed lock backed by valkey-py's ``valkey.asyncio.lock.Lock``.

    Wraps valkey-py's async ``Lock`` class to provide cross-process and
    cross-coroutine mutual exclusion using Valkey. Uses token-based ownership
    (handled internally) to prevent releasing another holder's lock, and
    TTL-based auto-expiry to prevent deadlocks on crash.

    The lock key prefix is ``pyrqlite:lock:`` to avoid collisions with
    other Valkey keys in the same database.

    Example:
        >>> import asyncio
        >>> from rqlite import AioValkeyLock, async_connect
        >>>
        >>> async def main():
        ...     lock = AioValkeyLock(name="transfer", timeout=10.0)
        ...     conn = await async_connect(lock=lock)
        ...     cursor = await conn.cursor()
        ...     async with lock:  # Acquire distributed lock
        ...         await cursor.execute(
        ...             "SELECT balance FROM accounts WHERE id=?", (1,)
        ...         )
        ...         balance = cursor.fetchone()[0]
        ...         await cursor.execute(
        ...             "UPDATE accounts SET balance=? WHERE id=?",
        ...             (balance - 50, 1),
        ...         )
        ...         await conn.commit()
        >>> asyncio.run(main())
    """

    PREFIX = "pyrqlite:lock:"

    def __init__(
        self,
        name: str = "default",
        host: str = "localhost",
        port: int = 6379,
        password: str | None = None,
        db: int = 0,
        timeout: float = 10.0,
        lock_timeout: float = -1.0,
        retry_interval: float = 0.05,
    ) -> None:
        """Initialize the async Valkey distributed lock.

        Args:
            name: Unique lock identifier (key suffix). Must be unique across
                  all processes/coroutines that compete for this lock.
            host: Valkey server hostname (default: localhost).
            port: Valkey server port (default: 6379).
            password: Valkey authentication password (optional).
            db: Valkey database number (default: 0).
            timeout: Time in seconds before the lock auto-expires. Prevents
                     deadlocks if the holder crashes. Must be > 0.
            lock_timeout: Maximum time to wait for the lock (-1 = wait forever).
                          When reached, acquire() raises TimeoutError.
            retry_interval: Base interval in seconds between acquisition retries.

        Raises:
            ValueError: If timeout <= 0 or name is empty.
        """
        if not name:
            raise ValueError("Lock name must not be empty")
        if timeout <= 0:
            raise ValueError("timeout must be > 0")

        self.name = name
        self.host = host
        self.port = port
        self.password = password
        self.db = db
        self.timeout = timeout
        self.lock_timeout = lock_timeout
        self.retry_interval = retry_interval

        # Full Valkey key for this lock
        self._key: str = f"{self.PREFIX}{name}"

        # Internal state
        self._acquired = False
        self._lock: _AsyncValkeyLock | None = None

    async def _get_client(self) -> valkey.asyncio.Valkey[Any]:
        """Create a fresh async Valkey client.

        Creates a new client each time to avoid event loop binding issues
        when acquire/release are called in different asyncio.run() contexts.

        Returns:
            A valkey.asyncio.Valkey client instance.
        """
        return valkey.asyncio.from_url(
            f"redis://{self.host}:{self.port}/{self.db}",
            password=self.password,
            decode_responses=True,
            socket_connect_timeout=5.0,
            socket_timeout=30.0,
        )

    async def _get_valkey_lock(self) -> _AsyncValkeyLock:
        """Get or create the wrapped valkey-py async Lock.

        Creates a fresh lock per event loop to avoid binding issues.

        Returns:
            A valkey.asyncio.lock.Lock instance.
        """
        if self._lock is None:
            client = await self._get_client()
            self._lock = _AsyncValkeyLock(
                client,
                name=self._key,
                timeout=self.timeout,
            )
        return self._lock

    async def acquire(self, blocking: bool = True, timeout: float = -1) -> bool:
        """Acquire the distributed lock asynchronously via valkey-py's ``Lock.acquire()``.

        Args:
            blocking: If False and lock is unavailable, return immediately
                      with False. If True (default), wait up to ``timeout``.
            timeout: Maximum time in seconds to wait (-1 = use instance default).

        Returns:
            True if the lock was acquired, False otherwise (non-blocking only).

        Raises:
            TimeoutError: If blocking=True and lock_timeout reached.
            valkey.ValkeyError: If Valkey is unreachable or returns an error.
        """
        if self._acquired:
            return True

        effective_timeout = timeout if timeout >= 0 else self.lock_timeout

        try:
            valkey_lock = await self._get_valkey_lock()
            acquired = await valkey_lock.acquire(
                blocking=blocking,
                blocking_timeout=None if effective_timeout < 0 else effective_timeout,
            )
            if acquired:
                self._acquired = True
                return True
            # valkey-py Lock returns False on timeout when blocking=True
            if not blocking:
                return False
            raise TimeoutError(
                f"Could not acquire Valkey lock '{self.name}' "
                f"within {effective_timeout}s"
            )
        except valkey.exceptions.TimeoutError as exc:
            raise TimeoutError(
                f"Could not acquire Valkey lock '{self.name}' "
                f"within {effective_timeout}s"
            ) from exc

    async def release(self) -> None:
        """Release the lock if we still own it.

        Delegates to valkey-py's ``Lock.release()`` which uses a Lua script
        for atomic check-and-delete. Safe to call multiple times or from
        non-holder.
        """
        if not self._acquired:
            return

        try:
            valkey_lock = await self._get_valkey_lock()
            await valkey_lock.release()
        except valkey.ValkeyError:
            # Best-effort release — don't raise on release errors
            pass
        finally:
            self._acquired = False

    async def __aenter__(self) -> AioValkeyLock:
        """Enter async context manager — acquire the lock."""
        await self.acquire()
        return self

    async def __aexit__(
        self,
        exc_type: type[Exception] | None,
        exc_val: Exception | None,
        exc_tb: object,
    ) -> None:
        """Exit async context manager — release the lock."""
        await self.release()
