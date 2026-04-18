# ty: ignore[unresolved-attribute]
"""Async Redis distributed lock examples for rqlite.

This example demonstrates how to use AioRedisLock with the async rqlite
DB-API 2.0 client to achieve cross-process transaction serialization.

Prerequisites:
    Install optional dependency:
        uv add tangled-pyrqlite[redis]

    Start Redis server:
        podman rm -f redis-test
        podman run -d --name redis-test -p 6379:6379 docker.io/redis

    Start rqlite server:
        podman rm -f rqlite-test
        podman run -d --name rqlite-test -p 4001:4001 docker.io/rqlite/rqlite

Usage:
    uv run python -B examples/async_redis_lock_basic_usage.py
"""

from __future__ import annotations

import argparse
import asyncio
import functools
from collections.abc import Callable
from typing import Any

import rqlite
from rqlite import AioRedisLock


def print_docstring(func: Callable) -> Callable:
    """Decorator that prints the function's docstring when called."""

    @functools.wraps(func)
    async def wrapper(*args: Any, **kwargs: Any) -> Any:
        if func.__doc__:
            print(f"\n{'─' * 60}")
            print(f"📝 {func.__name__}: {func.__doc__.strip()}")
            print("─" * 60)
        return await func(*args, **kwargs)

    return wrapper


@print_docstring
async def basic_lock_usage() -> None:
    """Basic AioRedisLock usage with async rqlite connection."""
    lock = AioRedisLock(name="async_basic_demo", timeout=10.0)

    conn = rqlite.async_connect(host="localhost", port=4001, lock=lock)
    cursor = await conn.cursor()

    try:
        # Create table
        await cursor.execute("DROP TABLE IF EXISTS redis_async_basic")
        await cursor.execute("""
            CREATE TABLE redis_async_basic (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                balance REAL
            )
        """)
        await conn.commit()
        print("✓ Table 'redis_async_basic' created")

        # Insert with lock — serialized across processes
        await cursor.execute(
            "INSERT INTO redis_async_basic (name, balance) VALUES (?, ?)",
            ("Alice", 1000.0),
        )
        await conn.commit()
        print("✓ Inserted Alice with $1000.00")

    finally:
        await cursor.close()
        await conn.close()


@print_docstring
async def context_manager_pattern() -> None:
    """Using AioRedisLock as async context manager."""
    lock = AioRedisLock(name="async_context_demo", timeout=10.0)

    conn = rqlite.async_connect(host="localhost", port=4001, lock=lock)
    async with conn:
        cursor = await conn.cursor()

        try:
            # Drop and create table
            await cursor.execute("DROP TABLE IF EXISTS redis_async_context")
            await cursor.execute("""
                CREATE TABLE redis_async_context (
                    id INTEGER PRIMARY KEY,
                    account TEXT NOT NULL UNIQUE,
                    balance REAL
                )
            """)
            await conn.commit()

            # Insert initial data
            await cursor.execute(
                "INSERT INTO redis_async_context (account, balance) VALUES (?, ?)",
                ("ACC001", 5000.0),
            )
            await conn.commit()
            print("✓ Table 'redis_async_context' created with $5000.00")

            # Use async lock as context manager — auto-release on exit
            amount = 200.0
            async with lock:
                await cursor.execute(
                    "SELECT balance FROM redis_async_context WHERE account=?",
                    ("ACC001",),
                )
                row = cursor.fetchone()
                assert row is not None
                old_balance = row[0]

                await cursor.execute(
                    "UPDATE redis_async_context SET balance=? WHERE account=?",
                    (old_balance - amount, "ACC001"),
                )
                await conn.commit()

                await cursor.execute(
                    "SELECT balance FROM redis_async_context WHERE account=?",
                    ("ACC001",),
                )
                _row = cursor.fetchone()

                assert _row is not None

                new_balance = _row[0]

            print(
                f"✓ With async lock: ${old_balance:.2f} → ${new_balance:.2f} (transferred ${amount:.2f})"
            )

        finally:
            await cursor.close()


@print_docstring
async def transfer_workflow() -> None:
    """Complete bank transfer workflow with AioRedisLock."""
    lock = AioRedisLock(name="async_transfer", timeout=10.0)

    conn = rqlite.async_connect(host="localhost", port=4001, lock=lock)

    try:
        cursor = await conn.cursor()

        # Setup accounts table
        await cursor.execute("DROP TABLE IF EXISTS redis_async_accounts")
        await cursor.execute("""
            CREATE TABLE redis_async_accounts (
                id INTEGER PRIMARY KEY,
                account TEXT NOT NULL UNIQUE,
                balance REAL NOT NULL
            )
        """)
        await conn.commit()

        # Seed accounts
        for acct, bal in [("ACC001", 1000.0), ("ACC002", 2000.0)]:
            await cursor.execute(
                "INSERT INTO redis_async_accounts (account, balance) VALUES (?, ?)",
                (acct, bal),
            )
        await conn.commit()

        # Show initial state
        await cursor.execute("SELECT account, balance FROM redis_async_accounts ORDER BY account")
        print("\n  Initial balances:")
        for acct, bal in cursor.fetchall():
            print(f"    {acct}: ${bal:.2f}")

        # Perform transfer with lock — serialized across processes
        amount = 150.0
        async with lock:
            await cursor.execute(
                "SELECT balance FROM redis_async_accounts WHERE account=?",
                ("ACC001",),
            )
            _row = cursor.fetchone()

            assert _row is not None

            sender_bal = _row[0]

            if sender_bal < amount:
                print(f"  ✗ Insufficient funds: ${sender_bal:.2f} < ${amount:.2f}")
                return

            await cursor.execute(
                "UPDATE redis_async_accounts SET balance=? WHERE account=?",
                (sender_bal - amount, "ACC001"),
            )

            await cursor.execute(
                "SELECT balance FROM redis_async_accounts WHERE account=?",
                ("ACC002",),
            )
            _row = cursor.fetchone()

            assert _row is not None

            receiver_bal = _row[0]

            await cursor.execute(
                "UPDATE redis_async_accounts SET balance=? WHERE account=?",
                (receiver_bal + amount, "ACC002"),
            )

            await conn.commit()

        # Show final state
        await cursor.execute("SELECT account, balance FROM redis_async_accounts ORDER BY account")
        print(f"\n  After transfer (${amount:.2f}):")
        for acct, bal in cursor.fetchall():
            print(f"    {acct}: ${bal:.2f}")

        print("✓ Async transfer completed safely under distributed lock")

        await cursor.close()

    finally:
        await conn.close()


@print_docstring
async def concurrent_tasks_demo() -> None:
    """Demonstrate lock serializing concurrent async tasks.

    Uses asyncio.gather to run multiple coroutines that compete for the
    same RedisLock — proving cross-task serialization.
    """
    lock = AioRedisLock(name="async_concurrent_demo", timeout=10.0)
    results: list[str] = []

    async def worker(task_id: int) -> None:
        """Worker that performs a locked operation."""
        conn = rqlite.async_connect(host="localhost", port=4001, lock=lock)
        cursor = await conn.cursor()

        try:
            # Create per-task table for isolation
            table = f"redis_async_worker_{task_id}"
            await cursor.execute(f"DROP TABLE IF EXISTS {table}")
            await cursor.execute(f"CREATE TABLE {table} (id INTEGER PRIMARY KEY, value TEXT)")
            await conn.commit()

            # Each task acquires the SAME lock — serialized access
            async with lock:
                await cursor.execute(f"INSERT INTO {table} (value) VALUES (?)", ("locked",))
                await conn.commit()

                await cursor.execute(f"SELECT COUNT(*) FROM {table}")
                _row = cursor.fetchone()

                assert _row is not None

                count = _row[0]
                results.append(f"  Task {task_id}: acquired lock, count={count}")

        finally:
            await cursor.close()
            await conn.close()

    # Run multiple tasks concurrently — they serialize via AioRedisLock
    tasks = [worker(i) for i in range(3)]
    await asyncio.gather(*tasks)

    print("\n  Task execution order (serialized by AioRedisLock):")
    for r in results:
        print(r)
    print("✓ All tasks serialized through distributed lock")


async def main() -> None:
    """Run all async Redis lock examples."""
    parser = argparse.ArgumentParser(
        description="rqlite async Redis lock examples",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  uv run python -B examples/async_redis_lock_basic_usage.py

Prerequisites:
  - Start Redis: podman run -d --name redis-test -p 6379:6379 docker.io/redis
  - Start rqlite: podman run -d --name rqlite-test -p 4001:4001 docker.io/rqlite/rqlite
        """,
    )
    _ = parser.parse_args()

    print("=" * 60)
    print("rqlite Async Redis Lock Basic Usage Examples")
    print("=" * 60)

    try:
        await basic_lock_usage()
        await context_manager_pattern()
        await transfer_workflow()
        await concurrent_tasks_demo()

        print("\n" + "=" * 60)
        print("All async Redis lock examples completed successfully!")
        print("=" * 60)

    except rqlite.OperationalError as e:
        print(f"\n✗ Database error: {e}")
        print("Make sure rqlite is running on localhost:4001")
    except ImportError as e:
        print(f"\n✗ Missing dependency: {e}")
        print("Install with: uv add tangled-pyrqlite[redis]")
    except Exception as e:
        print(f"\n✗ Error: {e}")


if __name__ == "__main__":
    asyncio.run(main())
