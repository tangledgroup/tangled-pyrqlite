"""Asynchronous Redis distributed lock for rqlite.

Wraps redis-py's ``redis.asyncio.lock.Lock`` to provide an async Redis-backed
distributed lock that serializes transactions across multiple tasks, coroutines,
and processes, enabling true ACID isolation for rqlite's queue-based transaction
model.

Usage:
    >>> import asyncio
    >>> from rqlite import AioRedisLock, async_connect
    >>>
    >>> async def main():
    ...     lock = AioRedisLock(name="transfer", host="localhost", port=6379)
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
        uv add tangled-pyrqlite[redis]

    Start Redis server:
        podman rm -f redis-test
        podman run -d --name redis-test -p 6379:6379 docker.io/redis

Note:
    The redis package is optional. This module raises ImportError
    at import time if redis is not installed, since it's a top-level
    import from rqlite.__init__. Use try/except around the import
    or install with [redis] extra.
"""
from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    pass

# Import redis at module level — requires [redis] extra
try:
    import redis  # noqa: PLC0417
    from redis.asyncio.lock import Lock as _AsyncRedisLock  # noqa: PLC0417
except ImportError as exc:
    raise ImportError(
        "redis package is required for AioRedisLock. "
        "Install it with: uv add tangled-pyrqlite[redis]"
    ) from exc


class AioRedisLock:
    """Async distributed lock backed by redis-py's ``redis.asyncio.lock.Lock``.

    Wraps redis-py's async ``Lock`` class to provide cross-process and
    cross-coroutine mutual exclusion using Redis. Uses token-based ownership
    (handled internally) to prevent releasing another holder's lock, and
    TTL-based auto-expiry to prevent deadlocks on crash.

    The lock key prefix is ``pyrqlite:lock:`` to avoid collisions with
    other Redis keys in the same database.

    Example:
        >>> import asyncio
        >>> from rqlite import AioRedisLock, async_connect
        >>>
        >>> async def main():
        ...     lock = AioRedisLock(name="transfer", timeout=10.0)
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
        """Initialize the async Redis distributed lock.

        Args:
            name: Unique lock identifier (key suffix). Must be unique across
                  all processes/coroutines that compete for this lock.
            host: Redis server hostname (default: localhost).
            port: Redis server port (default: 6379).
            password: Redis authentication password (optional).
            db: Redis database number (default: 0).
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

        # Full Redis key for this lock
        self._key: str = f"{self.PREFIX}{name}"

        # Internal state
        self._acquired = False
        self._lock: _AsyncRedisLock | None = None

    async def _get_client(self) -> redis.asyncio.Redis[Any]:
        """Create a fresh async Redis client.

        Creates a new client each time to avoid event loop binding issues
        when acquire/release are called in different asyncio.run() contexts.

        Returns:
            A redis.asyncio.Redis client instance.
        """
        return redis.asyncio.from_url(
            f"redis://{self.host}:{self.port}/{self.db}",
            password=self.password,
            decode_responses=True,
            socket_connect_timeout=5.0,
            socket_timeout=30.0,
        )

    async def _get_redis_lock(self) -> _AsyncRedisLock:
        """Get or create the wrapped redis-py async Lock.

        Creates a fresh lock per event loop to avoid binding issues.

        Returns:
            A redis.asyncio.lock.Lock instance.
        """
        if self._lock is None:
            client = await self._get_client()
            self._lock = _AsyncRedisLock(
                client,
                name=self._key,
                timeout=self.timeout,
            )
        return self._lock

    async def acquire(self, blocking: bool = True, timeout: float = -1) -> bool:
        """Acquire the distributed lock asynchronously via redis-py's ``Lock.acquire()``.

        Args:
            blocking: If False and lock is unavailable, return immediately
                      with False. If True (default), wait up to ``timeout``.
            timeout: Maximum time in seconds to wait (-1 = use instance default).

        Returns:
            True if the lock was acquired, False otherwise (non-blocking only).

        Raises:
            TimeoutError: If blocking=True and lock_timeout reached.
            redis.RedisError: If Redis is unreachable or returns an error.
        """
        if self._acquired:
            return True

        effective_timeout = timeout if timeout >= 0 else self.lock_timeout

        try:
            redis_lock = await self._get_redis_lock()
            acquired = await redis_lock.acquire(
                blocking=blocking,
                blocking_timeout=None if effective_timeout < 0 else effective_timeout,
            )
            if acquired:
                self._acquired = True
                return True
            # redis-py Lock returns False on timeout when blocking=True
            if not blocking:
                return False
            raise TimeoutError(
                f"Could not acquire Redis lock '{self.name}' "
                f"within {effective_timeout}s"
            )
        except redis.exceptions.TimeoutError as exc:
            raise TimeoutError(
                f"Could not acquire Redis lock '{self.name}' "
                f"within {effective_timeout}s"
            ) from exc

    async def release(self) -> None:
        """Release the lock if we still own it.

        Delegates to redis-py's ``Lock.release()`` which uses a Lua script
        for atomic check-and-delete. Safe to call multiple times or from
        non-holder.
        """
        if not self._acquired:
            return

        try:
            redis_lock = await self._get_redis_lock()
            await redis_lock.release()
        except redis.RedisError:
            # Best-effort release — don't raise on release errors
            pass
        finally:
            self._acquired = False

    async def __aenter__(self) -> AioRedisLock:
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
