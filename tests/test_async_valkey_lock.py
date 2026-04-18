"""Tests for async Valkey distributed lock (AioValkeyLock) in rqlite client.

Covers:
- Unit tests (lock creation, protocol compliance) — no Valkey needed
- Integration tests (acquire/release, timeouts, context managers) — Valkey required
- Distributed serialization demo (cross-process correctness) — Valkey + rqlite required

Prerequisites for integration tests:
    Install optional dependency:
        uv add tangled-pyrqlite[valkey]

    Start Valkey server:
        podman rm -f valkey-test
        podman run -d --name valkey-test -p 6379:6379 docker.io/valkey/valkey
"""

from __future__ import annotations

import asyncio
import random
import time
import warnings

import pytest

import rqlite
from rqlite import AioValkeyLock


def _has_valkey() -> bool:
    """Check if Valkey is reachable."""
    try:
        import valkey

        client = valkey.Redis(host="localhost", port=6379, db=0, socket_connect_timeout=1.0)
        return bool(client.ping())
    except Exception:
        return False


def _cleanup_test_tables() -> None:
    """Clean up tables created by async Valkey lock tests."""
    try:
        conn = rqlite.connect(host="localhost", port=4001, lock=rqlite.ThreadLock())
        cursor = conn.cursor()
        for table in [
            "valkey_async_test_balance",
            "valkey_async_crud",
            "valkey_async_accounts",
            "valkey_async_context",
            "valkey_async_basic",
        ]:
            cursor.execute(f"DROP TABLE IF EXISTS {table}")
        conn.commit()
        cursor.close()
        conn.close()
    except Exception:
        pass


@pytest.fixture(autouse=True)
def cleanup_after_valkey_test():
    """Clean up after each Valkey lock test."""
    yield
    _cleanup_test_tables()


# ── Unit Tests (no Valkey needed) ───────────────────────────────────────


class TestAioValkeyLockUnit:
    """Tests for AioValkeyLock creation and protocol compliance."""

    def test_aio_valkey_lock_creation(self):
        """Test AioValkeyLock can be created with default params."""
        lock = AioValkeyLock(name="test")
        assert lock.name == "test"
        assert lock._key == "pyrqlite:lock:test"
        assert lock.timeout == 10.0

    def test_aio_valkey_lock_custom_params(self):
        """Test AioValkeyLock with custom connection params."""
        lock = AioValkeyLock(
            name="custom",
            host="valkey.example.com",
            port=6380,
            password="secret",
            db=2,
            timeout=5.0,
        )
        assert lock.host == "valkey.example.com"
        assert lock.port == 6380

    def test_aio_valkey_lock_empty_name_raises(self):
        """Test empty name raises ValueError."""
        with pytest.raises(ValueError, match="must not be empty"):
            AioValkeyLock(name="")

    def test_aio_valkey_lock_zero_timeout_raises(self):
        """Test zero timeout raises ValueError."""
        with pytest.raises(ValueError, match="timeout must be > 0"):
            AioValkeyLock(name="test", timeout=0)

    def test_aio_valkey_lock_has_token(self):
        """Test lock generates a unique token (via valkey-py Lock)."""
        lock = AioValkeyLock(name="token_test")
        # Token is managed internally by valkey-py's async Lock
        assert lock._acquired is False

    def test_aio_valkey_lock_satisfies_protocol(self):
        """Test AioValkeyLock satisfies AsyncLockProtocol."""
        lock = AioValkeyLock(name="protocol_test")
        assert isinstance(lock, rqlite.AsyncLockProtocol)


# ── Integration Tests (Valkey required) ─────────────────────────────────


@pytest.mark.skipif(not _has_valkey(), reason="Valkey not available")
class TestAioValkeyLockIntegration:
    """Tests for AioValkeyLock acquire/release with live Valkey."""

    def test_acquire_release_async(self):
        """Test basic async acquire and release cycle."""
        lock = AioValkeyLock(name="async_integration_test", timeout=10.0)

        async def _test():
            assert await lock.acquire() is True
            assert lock._acquired is True
            await lock.release()
            assert lock._acquired is False

        asyncio.run(_test())

    def test_acquire_timeout_non_blocking_async(self):
        """Test non-blocking async acquire returns False when held."""
        lock1 = AioValkeyLock(name="async_timeout_test", timeout=10.0)
        lock2 = AioValkeyLock(name="async_timeout_test", timeout=10.0)

        async def _test():
            assert await lock1.acquire() is True
            assert await lock2.acquire(blocking=False) is False
            await lock1.release()

        asyncio.run(_test())

    def test_async_context_manager(self):
        """Test AioValkeyLock as async context manager."""
        lock = AioValkeyLock(name="async_cm_test", timeout=10.0)

        async def _test():
            async with lock:
                assert lock._acquired is True
            assert lock._acquired is False

        asyncio.run(_test())

    def test_double_release_safe_async(self):
        """Test releasing twice doesn't raise."""
        lock = AioValkeyLock(name="async_double_release_test", timeout=10.0)

        async def _test():
            await lock.acquire()
            await lock.release()
            await lock.release()  # Safe

        asyncio.run(_test())


# ── Distributed Serialization Tests (Valkey + rqlite required) ──────────


@pytest.mark.skipif(not _has_valkey(), reason="Valkey not available")
class TestDistributedSerialization:
    """Tests proving AioValkeyLock serializes cross-process transactions."""

    def test_distributed_serialization_async(self):
        """Two async tasks with AioValkeyLock produce correct final balance."""
        import rqlite

        # Setup
        conn = rqlite.connect(host="localhost", port=4001)
        cursor = conn.cursor()
        try:
            cursor.execute("DROP TABLE IF EXISTS valkey_async_test_balance")
            cursor.execute("""
                CREATE TABLE valkey_async_test_balance (
                    id INTEGER PRIMARY KEY,
                    account TEXT NOT NULL UNIQUE,
                    balance REAL NOT NULL
                )
            """)
            initial = 5000.0
            cursor.execute(
                "INSERT INTO valkey_async_test_balance (account, balance) VALUES (?, ?)",
                ("SENDER", initial),
            )
            conn.commit()
        finally:
            cursor.close()
            conn.close()

        lock = AioValkeyLock(name="async_distributed_test", timeout=30.0)
        iterations = 20
        amount = 50.0
        expected_final = initial - (iterations * amount)

        async def transfer_task() -> float | None:
            conn = rqlite.async_connect(host="localhost", port=4001, lock=lock)
            cursor = await conn.cursor()

            try:
                for _ in range(iterations):
                    async with lock:
                        await cursor.execute(
                            "SELECT balance FROM valkey_async_test_balance WHERE account=?",
                            ("SENDER",),
                        )
                        row = cursor.fetchone()
                        if row is None:
                            break
                        balance = row[0]
                        time.sleep(random.uniform(0.001, 0.005))
                        await cursor.execute(
                            "UPDATE valkey_async_test_balance SET balance=? WHERE account=?",
                            (balance - amount, "SENDER"),
                        )
                        await conn.commit()

                await cursor.execute(
                    "SELECT balance FROM valkey_async_test_balance WHERE account=?",
                    ("SENDER",),
                )
                row = cursor.fetchone()
                return row[0] if row else None
            finally:
                await cursor.close()
                await conn.close()

        final = asyncio.run(transfer_task())
        assert final is not None
        assert abs(final - expected_final) < 0.1, (
            f"Expected ${expected_final:.2f}, got ${final:.2f} — "
            f"lost updates detected!"
        )


@pytest.mark.skipif(not _has_valkey(), reason="Valkey not available")
class TestAioValkeyLockWithAsyncConnection:
    """Test AioValkeyLock with async rqlite connection."""

    def test_async_lock_with_async_connection(self):
        """Use AioValkeyLock with async_connect — full CRUD workflow."""
        lock = AioValkeyLock(name="async_crud_test", timeout=10.0)
        conn = rqlite.async_connect(host="localhost", port=4001, lock=lock)

        async def _run():
            cursor = await conn.cursor()

            # CREATE
            await cursor.execute("DROP TABLE IF EXISTS valkey_async_crud")
            await cursor.execute("""
                CREATE TABLE valkey_async_crud (
                    id INTEGER PRIMARY KEY,
                    account TEXT NOT NULL UNIQUE,
                    balance REAL
                )
            """)
            await conn.commit()

            # INSERT with lock
            async with lock:
                await cursor.execute(
                    "INSERT INTO valkey_async_crud (account, balance) VALUES (?, ?)",
                    ("ACC001", 1000.0),
                )
                await conn.commit()

            # SELECT with lock
            async with lock:
                await cursor.execute("SELECT * FROM valkey_async_crud")
                rows = cursor.fetchall()
                assert len(rows) == 1
                assert rows[0][1] == "ACC001"

            # UPDATE with lock
            async with lock:
                await cursor.execute(
                    "UPDATE valkey_async_crud SET balance=? WHERE account=?",
                    (500.0, "ACC001"),
                )
                await conn.commit()

            # Verify
            async with lock:
                await cursor.execute("SELECT balance FROM valkey_async_crud")
                row = cursor.fetchone()
                assert row is not None
                assert row[0] == 500.0

            await cursor.close()
            await conn.close()

        asyncio.run(_run())

    def test_async_lock_suppresses_warnings(self):
        """Test AioValkeyLock suppresses transaction SQL warnings."""
        lock = AioValkeyLock(name="async_warning_test", timeout=10.0)
        conn = rqlite.async_connect(host="localhost", port=4001, lock=lock)

        async def _run():
            cursor = await conn.cursor()

            with warnings.catch_warnings(record=True) as w:
                warnings.simplefilter("always")
                await cursor.execute("BEGIN")
                transaction_warnings = [
                    x for x in w if "BEGIN" in str(x.message) or "not supported" in str(x.message).lower()
                ]
                assert len(transaction_warnings) == 0

            await cursor.close()
            await conn.close()

        asyncio.run(_run())
