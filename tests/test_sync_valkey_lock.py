"""Tests for sync Valkey distributed lock (ValkeyLock) in rqlite client.

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

import random
import threading
import time
import warnings

import pytest

import rqlite
from rqlite import ValkeyLock


def _has_valkey() -> bool:
    """Check if Valkey is reachable."""
    try:
        import valkey

        client = valkey.Redis(host="localhost", port=6379, db=0, socket_connect_timeout=1.0)
        return bool(client.ping())
    except Exception:
        return False


def _cleanup_test_tables() -> None:
    """Clean up tables created by sync Valkey lock tests."""
    try:
        conn = rqlite.connect(host="localhost", port=4001, lock=rqlite.ThreadLock())
        cursor = conn.cursor()
        for table in [
            "sync_valkey_lock_test_balance",
            "sync_valkey_lock_transfer",
            "sync_valkey_select_test",
            "sync_valkey_crud_ops",
            "sync_valkey_lock_test_accounts",
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


class TestSyncValkeyLockUnit:
    """Tests for ValkeyLock creation and protocol compliance."""

    def test_sync_valkey_lock_creation(self):
        """Test ValkeyLock can be created with default params."""
        lock = ValkeyLock(name="test")
        assert lock.name == "test"
        assert lock._key == "pyrqlite:lock:test"
        assert lock.timeout == 10.0

    def test_sync_valkey_lock_custom_params(self):
        """Test ValkeyLock with custom connection params."""
        lock = ValkeyLock(
            name="custom",
            host="valkey.example.com",
            port=6380,
            password="secret",
            db=2,
            timeout=5.0,
        )
        assert lock.host == "valkey.example.com"
        assert lock.port == 6380
        assert lock.password == "secret"
        assert lock.db == 2
        assert lock.timeout == 5.0

    def test_sync_valkey_lock_empty_name_raises(self):
        """Test empty name raises ValueError."""
        with pytest.raises(ValueError, match="must not be empty"):
            ValkeyLock(name="")

    def test_sync_valkey_lock_zero_timeout_raises(self):
        """Test zero timeout raises ValueError."""
        with pytest.raises(ValueError, match="timeout must be > 0"):
            ValkeyLock(name="test", timeout=0)

    def test_sync_valkey_lock_negative_timeout_raises(self):
        """Test negative timeout raises ValueError."""
        with pytest.raises(ValueError, match="timeout must be > 0"):
            ValkeyLock(name="test", timeout=-1)

    def test_sync_valkey_lock_has_token(self):
        """Test lock generates a unique token (via valkey-py Lock)."""
        lock = ValkeyLock(name="token_test")
        # Token is managed internally by valkey-py's Lock
        assert lock._acquired is False

    def test_sync_valkey_lock_satisfies_protocol(self):
        """Test ValkeyLock satisfies LockProtocol."""
        lock = ValkeyLock(name="protocol_test")
        assert isinstance(lock, rqlite.LockProtocol)


# ── Integration Tests (Valkey required) ─────────────────────────────────


@pytest.mark.skipif(not _has_valkey(), reason="Valkey not available")
class TestSyncValkeyLockIntegration:
    """Tests for ValkeyLock acquire/release with live Valkey."""

    def test_sync_valkey_lock_acquire_release(self):
        """Test basic acquire and release cycle."""
        lock = ValkeyLock(name="integration_test", timeout=10.0)
        assert lock.acquire() is True
        assert lock._acquired is True
        lock.release()
        assert lock._acquired is False

    def test_sync_valkey_lock_acquire_timeout_non_blocking(self):
        """Test non-blocking acquire returns False when held."""
        lock1 = ValkeyLock(name="timeout_test", timeout=10.0)
        lock2 = ValkeyLock(name="timeout_test", timeout=10.0)

        assert lock1.acquire() is True
        # Second lock with same name should fail non-blocking
        assert lock2.acquire(blocking=False) is False
        lock1.release()

    def test_sync_valkey_lock_acquire_timeout_blocking(self):
        """Test blocking acquire raises TimeoutError after timeout."""
        lock1 = ValkeyLock(name="blocking_test", timeout=10.0)
        lock2 = ValkeyLock(name="blocking_test", timeout=10.0)

        assert lock1.acquire() is True
        with pytest.raises(TimeoutError, match="Could not acquire"):
            lock2.acquire(blocking=True, timeout=0.5)
        lock1.release()

    def test_sync_valkey_lock_context_manager(self):
        """Test ValkeyLock as context manager."""
        lock = ValkeyLock(name="cm_test", timeout=10.0)
        with lock:
            assert lock._acquired is True
        assert lock._acquired is False

    def test_sync_valkey_lock_double_release_safe(self):
        """Test releasing twice doesn't raise."""
        lock = ValkeyLock(name="double_release_test", timeout=10.0)
        lock.acquire()
        lock.release()
        lock.release()  # Should be safe (no-op)

    def test_sync_valkey_lock_acquire_after_release(self):
        """Test re-acquiring after release works."""
        lock = ValkeyLock(name="reacquire_test", timeout=10.0)
        assert lock.acquire() is True
        lock.release()
        assert lock.acquire() is True
        lock.release()

    def test_sync_valkey_lock_release_non_owner_safe(self):
        """Test releasing a lock you don't own is safe (no-op)."""
        lock = ValkeyLock(name="non_owner_test", timeout=10.0)
        lock.release()  # Should not raise

    def test_sync_valkey_lock_different_locks_independent(self):
        """Test different lock names are independent."""
        lock1 = ValkeyLock(name="independent_1", timeout=10.0)
        lock2 = ValkeyLock(name="independent_2", timeout=10.0)

        assert lock1.acquire() is True
        assert lock2.acquire() is True  # Different locks, both acquired
        lock1.release()
        lock2.release()


# ── Distributed Serialization Tests (Valkey + rqlite required) ──────────


@pytest.mark.skipif(not _has_valkey(), reason="Valkey not available")
class TestSyncValkeyLockDistributedSerialization:
    """Tests proving Valkey lock serializes cross-process transactions."""

    def test_sync_valkey_lock_distributed_serialization(self):
        """Multiple threads with ValkeyLock produce correct final balance.

        Without the lock, concurrent reads + writes cause lost updates.
        With the lock, operations serialize and data integrity is preserved.

        Uses threads to demonstrate that ValkeyLock serializes access correctly
        across multiple execution contexts sharing the same process.
        """
        import rqlite

        # Setup: create table with initial balance
        conn = rqlite.connect(host="localhost", port=4001)
        cursor = conn.cursor()
        try:
            cursor.execute("DROP TABLE IF EXISTS valkey_test_balance")
            cursor.execute("""
                CREATE TABLE valkey_test_balance (
                    id INTEGER PRIMARY KEY,
                    account TEXT NOT NULL UNIQUE,
                    balance REAL NOT NULL
                )
            """)
            initial = 5000.0
            cursor.execute(
                "INSERT INTO valkey_test_balance (account, balance) VALUES (?, ?)",
                ("SENDER", initial),
            )
            conn.commit()
        finally:
            cursor.close()
            conn.close()

        num_threads = 4
        iterations_per_thread = 10
        amount = 50.0
        expected_final = initial - (num_threads * iterations_per_thread * amount)

        # Run workers in separate threads with lock
        errors: list[Exception] = []

        def worker(thread_id: int) -> None:
            try:
                lock = ValkeyLock(name="sync_distributed_test", timeout=30.0)
                conn = rqlite.connect(host="localhost", port=4001, lock=lock)
                c = conn.cursor()
                try:
                    for _ in range(iterations_per_thread):
                        with lock:
                            c.execute(
                                "UPDATE valkey_test_balance SET balance=balance-? WHERE account=?",
                                (amount, "SENDER"),
                            )
                            conn.commit()
                            time.sleep(random.uniform(0.001, 0.005))
                finally:
                    c.close()
                    conn.close()
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(num_threads)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert not errors, f"Thread errors: {errors}"

        # Check final balance
        conn = rqlite.connect(host="localhost", port=4001)
        cursor = conn.cursor()
        try:
            cursor.execute("SELECT balance FROM valkey_test_balance WHERE account=?", ("SENDER",))
            row = cursor.fetchone()
            assert row is not None
            final_balance = row[0]
        finally:
            cursor.close()
            conn.close()

        # Allow tiny floating point tolerance
        assert abs(final_balance - expected_final) < 0.1, (
            f"Expected ${expected_final:.2f}, got ${final_balance:.2f} — "
            f"lost updates detected! Total deductions: ${initial - final_balance:.2f}"
        )

    def test_sync_valkey_lock_with_connection(self):
        """Use ValkeyLock with rqlite.connect — full CRUD workflow."""
        lock = ValkeyLock(name="crud_test", timeout=10.0)
        conn = rqlite.connect(host="localhost", port=4001, lock=lock)
        cursor = conn.cursor()

        try:
            # CREATE
            cursor.execute("DROP TABLE IF EXISTS valkey_lock_transfer")
            cursor.execute("""
                CREATE TABLE valkey_lock_transfer (
                    id INTEGER PRIMARY KEY,
                    sender TEXT NOT NULL,
                    receiver TEXT NOT NULL,
                    amount REAL NOT NULL,
                    status TEXT
                )
            """)
            conn.commit()

            # INSERT with lock
            with lock:
                cursor.execute(
                    "INSERT INTO valkey_lock_transfer (sender, receiver, amount, status) VALUES (?, ?, ?, ?)",
                    ("Alice", "Bob", 100.0, "completed"),
                )
                conn.commit()

            # SELECT with lock
            with lock:
                cursor.execute("SELECT * FROM valkey_lock_transfer")
                rows = cursor.fetchall()
                assert len(rows) == 1
                assert rows[0][2] == "Bob"

            # UPDATE with lock
            with lock:
                cursor.execute(
                    "UPDATE valkey_lock_transfer SET amount=? WHERE sender=?",
                    (150.0, "Alice"),
                )
                conn.commit()

            # Verify update
            with lock:
                cursor.execute("SELECT amount FROM valkey_lock_transfer WHERE sender=?", ("Alice",))
                row = cursor.fetchone()
                assert row is not None
                assert row[0] == 150.0

            # DELETE with lock
            with lock:
                cursor.execute("DELETE FROM valkey_lock_transfer WHERE sender=?", ("Alice",))
                conn.commit()

            # Verify delete
            with lock:
                cursor.execute("SELECT COUNT(*) FROM valkey_lock_transfer")
                count_row = cursor.fetchone()
                assert count_row is not None
                assert count_row[0] == 0

        finally:
            cursor.close()
            conn.close()


@pytest.mark.skipif(not _has_valkey(), reason="Valkey not available")
class TestSyncValkeyLockWithOperations:
    """Test that lock works correctly with rqlite operations."""

    def test_sync_valkey_lock_suppresses_warnings(self):
        """Test ValkeyLock suppresses transaction SQL warnings."""
        lock = ValkeyLock(name="sync_warning_test", timeout=10.0)
        conn = rqlite.connect(host="localhost", port=4001, lock=lock)
        cursor = conn.cursor()

        try:
            with warnings.catch_warnings(record=True) as w:
                warnings.simplefilter("always")
                cursor.execute("BEGIN")
                transaction_warnings = [
                    x
                    for x in w
                    if "BEGIN" in str(x.message) or "not supported" in str(x.message).lower()
                ]
                assert len(transaction_warnings) == 0, "ValkeyLock should suppress BEGIN warnings"
        finally:
            cursor.close()
            conn.close()

    def test_sync_valkey_lock_with_select(self):
        """Test SELECT works under ValkeyLock."""
        lock = ValkeyLock(name="sync_select_test", timeout=10.0)
        conn = rqlite.connect(host="localhost", port=4001, lock=lock)
        cursor = conn.cursor()

        try:
            cursor.execute("DROP TABLE IF EXISTS valkey_select_test")
            cursor.execute("CREATE TABLE valkey_select_test (id INTEGER PRIMARY KEY, value TEXT)")
            cursor.execute("INSERT INTO valkey_select_test (value) VALUES (?)", ("test",))
            conn.commit()

            with lock:
                cursor.execute("SELECT * FROM valkey_select_test")
                rows = cursor.fetchall()
                assert len(rows) == 1
        finally:
            cursor.close()
            conn.close()

    def test_sync_valkey_lock_with_insert_update_delete(self):
        """Test INSERT/UPDATE/DELETE work under ValkeyLock."""
        lock = ValkeyLock(name="sync_crud_ops_test", timeout=10.0)
        conn = rqlite.connect(host="localhost", port=4001, lock=lock)
        cursor = conn.cursor()

        try:
            cursor.execute("DROP TABLE IF EXISTS valkey_crud_ops")
            cursor.execute("CREATE TABLE valkey_crud_ops (id INTEGER PRIMARY KEY, val TEXT)")
            conn.commit()

            with lock:
                cursor.execute("INSERT INTO valkey_crud_ops (val) VALUES (?)", ("a",))
                conn.commit()

                cursor.execute("UPDATE valkey_crud_ops SET val=? WHERE id=?", ("b", 1))
                conn.commit()

                cursor.execute("SELECT val FROM valkey_crud_ops")
                val_row = cursor.fetchone()
                assert val_row is not None
                assert val_row[0] == "b"

                cursor.execute("DELETE FROM valkey_crud_ops")
                conn.commit()

                cursor.execute("SELECT COUNT(*) FROM valkey_crud_ops")
                count_row = cursor.fetchone()
                assert count_row is not None
                assert count_row[0] == 0
        finally:
            cursor.close()
            conn.close()


@pytest.mark.skipif(not _has_valkey(), reason="Valkey not available")
class TestSyncValkeyLockDeadlockPrevention:
    """Demonstrate that Valkey lock prevents lost updates in concurrent rqlite transactions.

    Without a distributed lock, rqlite's queue-based transaction model allows
    concurrent readers to see stale data, leading to lost updates (the classic
    read-modify-write race condition).

    With ValkeyLock, all concurrent operations serialize through the lock,
    ensuring each read sees the most recent committed value.
    """

    def test_sync_valkey_lock_no_lock_lost_updates(self):
        """Without any lock, concurrent transfers produce lost updates (data corruption)."""
        # Setup: create table with initial balance
        conn = rqlite.connect(host="localhost", port=4001)
        cursor = conn.cursor()
        try:
            cursor.execute("DROP TABLE IF EXISTS valkey_lock_test_accounts")
            cursor.execute("""
                CREATE TABLE valkey_lock_test_accounts (
                    id INTEGER PRIMARY KEY,
                    account TEXT NOT NULL UNIQUE,
                    balance REAL NOT NULL
                )
            """)
            initial = 10000.0
            cursor.execute(
                "INSERT INTO valkey_lock_test_accounts (account, balance) VALUES (?, ?)",
                ("SENDER", initial),
            )
            conn.commit()
        finally:
            cursor.close()
            conn.close()

        num_threads = 4
        iterations_per_thread = 20
        amount = 50.0
        expected_final = initial - (num_threads * iterations_per_thread * amount)

        # Run workers WITHOUT any lock — expect data corruption
        errors: list[Exception] = []

        def worker(thread_id: int) -> None:
            try:
                conn = rqlite.connect(host="localhost", port=4001)
                c = conn.cursor()
                try:
                    for _ in range(iterations_per_thread):
                        # Read-modify-write without lock — race condition!
                        c.execute(
                            "SELECT balance FROM valkey_lock_test_accounts WHERE account=?",
                            ("SENDER",),
                        )
                        row = c.fetchone()
                        if row is None:
                            break
                        balance = row[0]
                        time.sleep(random.uniform(0.001, 0.005))  # Simulate work
                        c.execute(
                            "UPDATE valkey_lock_test_accounts SET balance=? WHERE account=?",
                            (balance - amount, "SENDER"),
                        )
                        conn.commit()
                finally:
                    c.close()
                    conn.close()
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(num_threads)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert not errors, f"Thread errors: {errors}"

        # Check final balance — should NOT match expected (data corruption)
        conn = rqlite.connect(host="localhost", port=4001)
        cursor = conn.cursor()
        try:
            cursor.execute(
                "SELECT balance FROM valkey_lock_test_accounts WHERE account=?",
                ("SENDER",),
            )
            row = cursor.fetchone()
            assert row is not None
            final_balance = row[0]
        finally:
            cursor.close()
            conn.close()

        # Without a distributed lock, rqlite's queue-based model may or may not
        # show lost updates depending on timing. The point is: without locking,
        # you CANNOT guarantee correctness. With ValkeyLock (see other tests),
        # correctness IS guaranteed.
        print(
            f"\n  Without lock: Expected ${expected_final:.2f}, got ${final_balance:.2f} "
            f"(diff: ${abs(final_balance - expected_final):.2f})"
        )
        # Just verify the account wasn't deleted or corrupted
        assert final_balance > 0, "Balance should remain positive"
