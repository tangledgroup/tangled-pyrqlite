"""Sync Redis distributed lock examples for rqlite.

This example demonstrates how to use RedisLock with the rqlite DB-API 2.0
client to achieve cross-process transaction serialization.

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
    uv run python -B examples/sync_redis_lock_basic_usage.py
"""

from __future__ import annotations

import argparse
import functools
from collections.abc import Callable
from typing import Any

import rqlite
from rqlite import RedisLock


def print_docstring(func: Callable) -> Callable:
    """Decorator that prints the function's docstring when called."""
    @functools.wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        if func.__doc__:
            print(f"\n{'─' * 60}")
            print(f"📝 {func.__name__}: {func.__doc__.strip()}")
            print("─" * 60)
        return func(*args, **kwargs)
    return wrapper


@print_docstring
def basic_lock_usage():
    """Basic RedisLock usage with rqlite connection."""
    lock = RedisLock(name="basic_demo", timeout=10.0)

    # Connect with RedisLock — suppresses transaction warnings
    conn = rqlite.connect(host="localhost", port=4001, lock=lock)
    cursor = conn.cursor()

    try:
        # Create table
        cursor.execute("DROP TABLE IF EXISTS redis_basic")
        cursor.execute("""
            CREATE TABLE redis_basic (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                balance REAL
            )
        """)
        conn.commit()
        print("✓ Table 'redis_basic' created")

        # Insert with lock — serialized across processes
        cursor.execute(
            "INSERT INTO redis_basic (name, balance) VALUES (?, ?)",
            ("Alice", 1000.0),
        )
        conn.commit()
        print("✓ Inserted Alice with $1000.00")

    finally:
        cursor.close()
        conn.close()


@print_docstring
def context_manager_pattern():
    """Using RedisLock as context manager for automatic acquire/release."""
    lock = RedisLock(name="context_demo", timeout=10.0)

    with rqlite.connect(host="localhost", port=4001, lock=lock) as conn:
        cursor = conn.cursor()

        try:
            # Drop and create table
            cursor.execute("DROP TABLE IF EXISTS redis_context")
            cursor.execute("""
                CREATE TABLE redis_context (
                    id INTEGER PRIMARY KEY,
                    account TEXT NOT NULL UNIQUE,
                    balance REAL
                )
            """)
            conn.commit()

            # Insert initial data
            cursor.execute(
                "INSERT INTO redis_context (account, balance) VALUES (?, ?)",
                ("ACC001", 5000.0),
            )
            conn.commit()
            print("✓ Table 'redis_context' created with $5000.00")

            # Use lock as context manager — auto-release on exit
            amount = 200.0
            with lock:
                cursor.execute(
                    "SELECT balance FROM redis_context WHERE account=?",
                    ("ACC001",),
                )
                row = cursor.fetchone()
                old_balance = row[0]

                cursor.execute(
                    "UPDATE redis_context SET balance=? WHERE account=?",
                    (old_balance - amount, "ACC001"),
                )
                conn.commit()

                cursor.execute(
                    "SELECT balance FROM redis_context WHERE account=?",
                    ("ACC001",),
                )
                new_balance = cursor.fetchone()[0]

            print(f"✓ With lock: ${old_balance:.2f} → ${new_balance:.2f} (transferred ${amount:.2f})")

        finally:
            cursor.close()


@print_docstring
def transfer_workflow():
    """Complete bank transfer workflow with RedisLock."""
    lock = RedisLock(name="transfer", timeout=10.0)

    with rqlite.connect(host="localhost", port=4001, lock=lock) as conn:
        cursor = conn.cursor()

        try:
            # Setup accounts table
            cursor.execute("DROP TABLE IF EXISTS redis_accounts")
            cursor.execute("""
                CREATE TABLE redis_accounts (
                    id INTEGER PRIMARY KEY,
                    account TEXT NOT NULL UNIQUE,
                    balance REAL NOT NULL
                )
            """)
            conn.commit()

            # Seed accounts
            for acct, bal in [("ACC001", 1000.0), ("ACC002", 2000.0)]:
                cursor.execute(
                    "INSERT INTO redis_accounts (account, balance) VALUES (?, ?)",
                    (acct, bal),
                )
            conn.commit()

            # Show initial state
            cursor.execute("SELECT account, balance FROM redis_accounts ORDER BY account")
            print("\n  Initial balances:")
            for acct, bal in cursor.fetchall():
                print(f"    {acct}: ${bal:.2f}")

            # Perform transfer with lock — serialized across processes
            amount = 150.0
            with lock:
                # Read sender balance
                cursor.execute(
                    "SELECT balance FROM redis_accounts WHERE account=?",
                    ("ACC001",),
                )
                sender_bal = cursor.fetchone()[0]

                if sender_bal < amount:
                    print(f"  ✗ Insufficient funds: ${sender_bal:.2f} < ${amount:.2f}")
                    return

                # Deduct from sender
                cursor.execute(
                    "UPDATE redis_accounts SET balance=? WHERE account=?",
                    (sender_bal - amount, "ACC001"),
                )

                # Read receiver balance
                cursor.execute(
                    "SELECT balance FROM redis_accounts WHERE account=?",
                    ("ACC002",),
                )
                receiver_bal = cursor.fetchone()[0]

                # Credit receiver
                cursor.execute(
                    "UPDATE redis_accounts SET balance=? WHERE account=?",
                    (receiver_bal + amount, "ACC002"),
                )

                conn.commit()

            # Show final state
            cursor.execute("SELECT account, balance FROM redis_accounts ORDER BY account")
            print(f"\n  After transfer (${amount:.2f}):")
            for acct, bal in cursor.fetchall():
                print(f"    {acct}: ${bal:.2f}")

            print("✓ Transfer completed safely under distributed lock")

        finally:
            cursor.close()


@print_docstring
def concurrent_operations_demo():
    """Demonstrate lock serializing concurrent operations.

    Uses threading to simulate concurrent access — each thread acquires
    the same RedisLock, proving cross-thread serialization.
    """
    import threading

    lock = RedisLock(name="concurrent_demo", timeout=10.0)
    results: list[str] = []
    results_lock = threading.Lock()

    def worker(thread_id: int) -> None:
        """Worker that performs a locked operation."""
        with rqlite.connect(host="localhost", port=4001, lock=lock) as conn:
            cursor = conn.cursor()
            try:
                # Create per-thread table for isolation
                table = f"redis_worker_{thread_id}"
                cursor.execute(f"DROP TABLE IF EXISTS {table}")
                cursor.execute(
                    f"CREATE TABLE {table} (id INTEGER PRIMARY KEY, value TEXT)"
                )
                conn.commit()

                # Each thread acquires the SAME lock — serialized access
                with lock:
                    cursor.execute(f"INSERT INTO {table} (value) VALUES (?)", ("locked",))
                    conn.commit()

                    cursor.execute(f"SELECT COUNT(*) FROM {table}")
                    count = cursor.fetchone()[0]
                    with results_lock:
                        results.append(f"  Thread {thread_id}: acquired lock, count={count}")

            finally:
                cursor.close()

    # Create threads — they will serialize via RedisLock
    threads = [threading.Thread(target=worker, args=(i,)) for i in range(3)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    print("\n  Thread execution order (serialized by RedisLock):")
    for r in results:
        print(r)
    print("✓ All threads serialized through distributed lock")


def main() -> None:
    """Run all sync Redis lock examples."""
    parser = argparse.ArgumentParser(
        description="rqlite sync Redis lock examples",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  uv run python -B examples/sync_redis_lock_basic_usage.py

Prerequisites:
  - Start Redis: podman run -d --name redis-test -p 6379:6379 docker.io/redis
  - Start rqlite: podman run -d --name rqlite-test -p 4001:4001 docker.io/rqlite/rqlite
        """,
    )
    _ = parser.parse_args()

    print("=" * 60)
    print("rqlite Sync Redis Lock Basic Usage Examples")
    print("=" * 60)

    try:
        basic_lock_usage()
        context_manager_pattern()
        transfer_workflow()
        concurrent_operations_demo()

        print("\n" + "=" * 60)
        print("All sync Redis lock examples completed successfully!")
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
    main()
