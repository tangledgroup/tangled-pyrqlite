"""Synchronous Redis distributed lock for rqlite.

Wraps redis-py's ``redis.lock.Lock`` to provide a Redis-backed distributed
lock that serializes transactions across multiple processes and threads,
enabling true ACID isolation for rqlite's queue-based transaction model.

Usage:
    >>> from rqlite import RedisLock, connect
    >>> lock = RedisLock(name="transfer", host="localhost", port=6379)
    >>> conn = connect(lock=lock)
    >>> cursor = conn.cursor()
    >>> with lock:
    ...     cursor.execute("SELECT balance FROM accounts WHERE id=?", (1,))
    ...     balance = cursor.fetchone()[0]
    ...     cursor.execute("UPDATE accounts SET balance=? WHERE id=?", (balance - 100, 1))
    ...     conn.commit()

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

import threading
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    pass

# Import redis at module level — requires [redis] extra
try:
    import redis  # noqa: PLC0417
    from redis.lock import Lock as _RedisLock  # noqa: PLC0417
except ImportError as exc:
    raise ImportError(
        "redis package is required for RedisLock. "
        "Install it with: uv add tangled-pyrqlite[redis]"
    ) from exc


class RedisLock:
    """Distributed lock backed by redis-py's ``redis.lock.Lock``.

    Wraps redis-py's ``Lock`` class to provide cross-process and cross-thread
    mutual exclusion using Redis. Uses token-based ownership (handled internally)
    to prevent releasing another holder's lock, and TTL-based auto-expiry to
    prevent deadlocks on crash.

    The lock key prefix is ``pyrqlite:lock:`` to avoid collisions with
    other Redis keys in the same database.

    Example:
        >>> from rqlite import RedisLock, connect
        >>> lock = RedisLock(name="transfer", timeout=10.0)
        >>> conn = connect(lock=lock)
        >>> cursor = conn.cursor()
        >>> with lock:  # Acquire distributed lock
        ...     cursor.execute("SELECT balance FROM accounts WHERE id=?", (1,))
        ...     balance = cursor.fetchone()[0]
        ...     cursor.execute("UPDATE accounts SET balance=? WHERE id=?",
        ...                    (balance - 50, 1))
        ...     conn.commit()  # Released automatically
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
        """Initialize the Redis distributed lock.

        Args:
            name: Unique lock identifier (key suffix). Must be unique across
                  all processes/threads that compete for this lock.
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
        self._client: redis.Redis[Any] | None = None
        self._client_lock = threading.Lock()
        self._lock: _RedisLock | None = None

    def _get_client(self) -> redis.Redis[Any]:
        """Get or create a Redis client (thread-safe).

        Returns:
            A redis.Redis client instance.
        """
        if self._client is None:
            with self._client_lock:
                if self._client is None:
                    self._client = redis.Redis(
                        host=self.host,
                        port=self.port,
                        password=self.password,
                        db=self.db,
                        decode_responses=True,
                        socket_connect_timeout=5.0,
                        socket_timeout=30.0,
                    )
        return self._client

    def _get_redis_lock(self) -> _RedisLock:
        """Get or create the wrapped redis-py Lock (thread-safe).

        Returns:
            A redis.lock.Lock instance.
        """
        if self._lock is None:
            with self._client_lock:
                if self._lock is None:
                    # Create client here to avoid deadlock —
                    # _get_client() also acquires _client_lock
                    client = redis.Redis(
                        host=self.host,
                        port=self.port,
                        password=self.password,
                        db=self.db,
                        decode_responses=True,
                        socket_connect_timeout=5.0,
                        socket_timeout=30.0,
                    )
                    self._client = client
                    self._lock = _RedisLock(
                        client,
                        name=self._key,
                        timeout=self.timeout,
                        thread_local=False,
                    )
        return self._lock

    def acquire(self, blocking: bool = True, timeout: float = -1) -> bool:
        """Acquire the distributed lock via redis-py's ``Lock.acquire()``.

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
            acquired = self._get_redis_lock().acquire(
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

    def release(self) -> None:
        """Release the lock if we still own it.

        Delegates to redis-py's ``Lock.release()`` which uses a Lua script
        for atomic check-and-delete. Safe to call multiple times or from
        non-holder.
        """
        if not self._acquired:
            return

        try:
            self._get_redis_lock().release()
        except redis.RedisError:
            # Best-effort release — don't raise on release errors
            pass
        finally:
            self._acquired = False

    def __enter__(self) -> RedisLock:
        """Enter context manager — acquire the lock."""
        self.acquire()
        return self

    def __exit__(
        self,
        exc_type: type[Exception] | None,
        exc_val: Exception | None,
        exc_tb: object,
    ) -> None:
        """Exit context manager — release the lock."""
        self.release()
