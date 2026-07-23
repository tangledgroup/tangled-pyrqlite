"""Tests for sync DB-API 2.0 connection with Valkey distributed lock (ValkeyLock).

Covers:
- Connection creation and lifecycle with ValkeyLock
- Cursor operations (create, insert, select, update, delete)
- Context managers
- Multiple cursors on same connection
- Read consistency levels
- Empty result sets (no warnings)
- Full CRUD lifecycle

Prerequisites:
    uv add tangled-pyrqlite[valkey]
    podman run -d --name valkey-test -p 6379:6379 docker.io/valkey/valkey
    rqlite running on localhost:4001
"""

from __future__ import annotations

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


skip_if_no_valkey = pytest.mark.skipif(not _has_valkey(), reason="Valkey not available")


# ── Connection Tests ──────────────────────────────────────────────────────


@skip_if_no_valkey
class TestSyncValkeyLockConnection:
    """Test Connection class with ValkeyLock."""

    def test_connect_with_valkey_lock(self):
        """Test basic connection creation with ValkeyLock."""
        lock = ValkeyLock(name="sync_valkey_dbapi_conn", timeout=30.0)
        conn = rqlite.connect(host="localhost", port=4001, lock=lock)
        assert conn.host == "localhost"
        assert conn.port == 4001
        assert not conn._closed
        assert conn._lock is lock
        conn.close()

    def test_cursor_creation(self):
        """Test creating a cursor from connection with ValkeyLock."""
        lock = ValkeyLock(name="sync_valkey_dbapi_cur", timeout=30.0)
        conn = rqlite.connect(host="localhost", port=4001, lock=lock)
        cursor = conn.cursor()
        assert cursor is not None
        assert cursor.connection is conn
        cursor.close()
        conn.close()

    def test_close_connection(self):
        """Test closing connection with ValkeyLock."""
        lock = ValkeyLock(name="sync_valkey_dbapi_close", timeout=30.0)
        conn = rqlite.connect(host="localhost", port=4001, lock=lock)
        conn.close()
        assert conn._closed

        with pytest.raises(Exception) as exc_info:
            conn.cursor()
        assert "closed" in str(exc_info.value).lower()

    def test_context_manager(self):
        """Test connection as context manager with ValkeyLock."""
        lock = ValkeyLock(name="sync_valkey_dbapi_ctx", timeout=30.0)
        with rqlite.connect("localhost", 4001, lock=lock) as conn:
            assert not conn._closed
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            cursor.close()

        assert conn._closed

    def test_commit_without_transaction(self):
        """Test commit when no transaction is pending."""
        lock = ValkeyLock(name="sync_valkey_dbapi_commit", timeout=30.0)
        conn = rqlite.connect(host="localhost", port=4001, lock=lock)
        conn.commit()  # Should not raise
        conn.close()

    def test_rollback_without_transaction(self):
        """Test rollback when no transaction is pending."""
        lock = ValkeyLock(name="sync_valkey_dbapi_rollback", timeout=30.0)
        conn = rqlite.connect(host="localhost", port=4001, lock=lock)
        conn.rollback()  # Should not raise
        conn.close()


@skip_if_no_valkey
class TestSyncValkeyLockConnectionAuth:
    """Test connection with authentication and ValkeyLock."""

    def test_connect_with_auth_and_lock(self):
        """Test connection with username, password, and ValkeyLock."""
        lock = ValkeyLock(name="sync_valkey_dbapi_auth", timeout=30.0)
        conn = rqlite.connect(
            host="localhost",
            port=4001,
            username="testuser",
            password="testpass",
            lock=lock,
        )
        assert conn.username == "testuser"
        assert conn.password == "testpass"
        assert conn._auth == ("testuser", "testpass")
        assert conn._lock is lock
        conn.close()


@skip_if_no_valkey
class TestSyncValkeyLockConnectionTimeout:
    """Test connection timeout settings with ValkeyLock."""

    def test_connect_with_timeout_and_lock(self):
        """Test connection with custom timeout and ValkeyLock."""
        lock = ValkeyLock(name="sync_valkey_dbapi_timeout", timeout=30.0)
        conn = rqlite.connect(host="localhost", port=4001, timeout=60.0, lock=lock)
        assert conn.timeout == 60.0
        conn.close()


# ── Read Consistency Tests ────────────────────────────────────────────────


@skip_if_no_valkey
class TestSyncValkeyLockReadConsistency:
    """Test read consistency levels with ValkeyLock."""

    def test_select_with_default_consistency(self):
        """Test SELECT uses LINEARIZABLE by default."""
        from rqlite.types import ReadConsistency

        lock = ValkeyLock(name="sync_valkey_dbapi_rc", timeout=30.0)
        conn = rqlite.connect(host="localhost", port=4001, lock=lock)
        assert conn.read_consistency == ReadConsistency.LINEARIZABLE
        cursor = conn.cursor()
        cursor.execute("SELECT 1 as test")
        row = cursor.fetchone()
        assert row is not None
        assert row[0] == 1
        cursor.close()
        conn.close()

    def test_select_with_weak_consistency(self):
        """Test SELECT with WEAK consistency and ValkeyLock."""
        from rqlite.types import ReadConsistency

        lock = ValkeyLock(name="sync_valkey_dbapi_weak", timeout=30.0)
        conn = rqlite.connect(
            host="localhost", port=4001, read_consistency=ReadConsistency.WEAK, lock=lock
        )
        assert conn.read_consistency == ReadConsistency.WEAK
        cursor = conn.cursor()
        cursor.execute("DROP TABLE IF EXISTS sync_valkey_test_weak")
        cursor.execute("CREATE TABLE sync_valkey_test_weak (id INTEGER PRIMARY KEY, name TEXT)")
        cursor.execute("INSERT INTO sync_valkey_test_weak (name) VALUES (?)", ("test",))
        conn.commit()
        cursor.execute("SELECT * FROM sync_valkey_test_weak")
        rows = cursor.fetchall()
        assert len(rows) == 1
        cursor.close()
        conn.close()

    def test_select_with_none_consistency(self):
        """Test SELECT with NONE consistency and ValkeyLock."""
        from rqlite.types import ReadConsistency

        lock = ValkeyLock(name="sync_valkey_dbapi_none", timeout=30.0)
        conn = rqlite.connect(
            host="localhost", port=4001, read_consistency=ReadConsistency.NONE, lock=lock
        )
        assert conn.read_consistency == ReadConsistency.NONE
        cursor = conn.cursor()
        cursor.execute("DROP TABLE IF EXISTS sync_valkey_test_none")
        cursor.execute("CREATE TABLE sync_valkey_test_none (id INTEGER PRIMARY KEY, name TEXT)")
        cursor.execute("INSERT INTO sync_valkey_test_none (name) VALUES (?)", ("test",))
        conn.commit()
        cursor.execute("SELECT * FROM sync_valkey_test_none")
        rows = cursor.fetchall()
        assert len(rows) == 1
        cursor.close()
        conn.close()


# ── Complex Workflow Tests ────────────────────────────────────────────────


@skip_if_no_valkey
class TestSyncValkeyLockComplexWorkflow:
    """Test complex connection workflow with ValkeyLock."""

    def test_full_crud_lifecycle(self):
        """Test complete CRUD lifecycle with ValkeyLock."""
        lock = ValkeyLock(name="sync_valkey_dbapi_crud", timeout=30.0)
        conn = rqlite.connect(host="localhost", port=4001, lock=lock)
        try:
            cursor = conn.cursor()

            # Step 1: CREATE TABLE
            cursor.execute("DROP TABLE IF EXISTS sync_valkey_lock_test_items")
            conn.commit()
            cursor.execute("""
                CREATE TABLE sync_valkey_lock_test_items (
                    id INTEGER PRIMARY KEY,
                    name TEXT NOT NULL,
                    value INTEGER,
                    active INTEGER DEFAULT 1
                )
            """)
            conn.commit()

            # Step 2: INSERT MANY
            items = [
                ("Item A", 100, 1),
                ("Item B", 200, 1),
                ("Item C", 300, 1),
                ("Item D", 400, 0),
                ("Item E", 500, 1),
            ]
            cursor.executemany(
                "INSERT INTO sync_valkey_lock_test_items (name, value, active) VALUES (?, ?, ?)",
                items,
            )
            conn.commit()

            # Step 3: SELECT ALL
            cursor.execute("SELECT * FROM sync_valkey_lock_test_items ORDER BY value")
            rows = cursor.fetchall()
            assert len(rows) == 5
            assert rows[0][1] == "Item A"
            assert rows[-1][1] == "Item E"

            # Step 4: SELECT FEW (filter)
            cursor.execute(
                "SELECT name, value FROM sync_valkey_lock_test_items WHERE active = ? AND value > ?",
                (1, 150),
            )
            rows = cursor.fetchall()
            assert len(rows) == 3
            names = {row[0] for row in rows}
            assert names == {"Item B", "Item C", "Item E"}

            # Step 5: SELECT ONE
            cursor.execute(
                "SELECT name, value, active FROM sync_valkey_lock_test_items WHERE name = ?",
                ("Item C",),
            )
            row = cursor.fetchone()
            assert row is not None
            assert row[0] == "Item C"
            assert row[1] == 300

            # Step 6: UPDATE
            cursor.execute(
                "UPDATE sync_valkey_lock_test_items SET value = ? WHERE name = ?",
                (350, "Item C"),
            )
            conn.commit()

            # Step 7: SELECT ONE (verify update)
            cursor.execute(
                "SELECT value FROM sync_valkey_lock_test_items WHERE name = ?",
                ("Item C",),
            )
            row = cursor.fetchone()
            assert row is not None
            assert row[0] == 350

            # Step 8: DELETE
            cursor.execute("DELETE FROM sync_valkey_lock_test_items WHERE active = ?", (0,))
            conn.commit()

            # Step 9: SELECT MANY (final state)
            cursor.execute("SELECT name, value FROM sync_valkey_lock_test_items ORDER BY value")
            rows = cursor.fetchall()
            assert len(rows) == 4

            # Step 10: SELECT ONE (non-existent)
            cursor.execute(
                "SELECT name FROM sync_valkey_lock_test_items WHERE name = ?",
                ("Item D",),
            )
            row = cursor.fetchone()
            assert row is None

            cursor.close()
        finally:
            conn.close()

    def test_multiple_cursors_same_connection(self):
        """Test using multiple cursors on the same connection with ValkeyLock."""
        lock = ValkeyLock(name="sync_valkey_dbapi_multi", timeout=30.0)
        conn = rqlite.connect(host="localhost", port=4001, lock=lock)
        try:
            cursor1 = conn.cursor()
            cursor1.execute("DROP TABLE IF EXISTS sync_valkey_multi_cursor_test")
            cursor1.execute("CREATE TABLE sync_valkey_multi_cursor_test (id INTEGER, data TEXT)")
            conn.commit()

            cursor1.execute("INSERT INTO sync_valkey_multi_cursor_test VALUES (?, ?)", (1, "data1"))
            cursor1.execute("INSERT INTO sync_valkey_multi_cursor_test VALUES (?, ?)", (2, "data2"))
            conn.commit()

            cursor2 = conn.cursor()
            cursor2.execute("SELECT * FROM sync_valkey_multi_cursor_test ORDER BY id")
            rows = cursor2.fetchall()
            assert len(rows) == 2
            assert rows[0] == (1, "data1")
            assert rows[1] == (2, "data2")

            cursor1.execute(
                "UPDATE sync_valkey_multi_cursor_test SET data = ? WHERE id = ?",
                ("updated", 1),
            )
            conn.commit()

            cursor2.execute("SELECT data FROM sync_valkey_multi_cursor_test WHERE id = ?", (1,))
            row = cursor2.fetchone()
            assert row is not None
            assert row[0] == "updated"

            cursor1.close()
            cursor2.close()
        finally:
            conn.close()


# ── Empty Result Set Tests ────────────────────────────────────────────────


@skip_if_no_valkey
class TestSyncValkeyLockEmptyResults:
    """Test that empty SELECT results don't trigger warnings with ValkeyLock."""

    def test_empty_select_no_warning(self):
        """Test empty SELECT returns None without warning."""
        lock = ValkeyLock(name="sync_valkey_dbapi_empty", timeout=30.0)
        conn = rqlite.connect(host="localhost", port=4001, lock=lock)
        try:
            cursor = conn.cursor()

            cursor.execute("DROP TABLE IF EXISTS sync_valkey_empty_select_test")
            cursor.execute("CREATE TABLE sync_valkey_empty_select_test (id INTEGER, name TEXT)")
            cursor.execute(
                "INSERT INTO sync_valkey_empty_select_test VALUES (?, ?)", (1, "OnlyRow")
            )
            conn.commit()

            with warnings.catch_warnings(record=True) as w:
                warnings.simplefilter("always")
                cursor.execute(
                    "SELECT * FROM sync_valkey_empty_select_test WHERE name = ?",
                    ("NonExistent",),
                )
                row = cursor.fetchone()
                assert row is None
                no_results_warnings = [x for x in w if "No results to fetch" in str(x.message)]
                assert len(no_results_warnings) == 0

            with warnings.catch_warnings(record=True) as w:
                warnings.simplefilter("always")
                rows = cursor.fetchall()
                assert rows == []
                no_results_warnings = [x for x in w if "No results to fetch" in str(x.message)]
                assert len(no_results_warnings) == 0

            cursor.close()
        finally:
            conn.close()


# ── Warning Suppression Tests ─────────────────────────────────────────────


@skip_if_no_valkey
class TestSyncValkeyLockWarnings:
    """Test that ValkeyLock suppresses transaction warnings."""

    def test_begin_no_warning(self):
        """Test BEGIN SQL does not warn with ValkeyLock."""
        lock = ValkeyLock(name="sync_valkey_dbapi_warn", timeout=30.0)
        conn = rqlite.connect(host="localhost", port=4001, lock=lock)
        cursor = conn.cursor()
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            try:
                cursor.execute("BEGIN")
            except Exception:
                pass
            transaction_warnings = [
                x
                for x in w
                if "BEGIN" in str(x.message) or "not supported" in str(x.message).lower()
            ]
            assert len(transaction_warnings) == 0
        cursor.close()
        conn.close()

    def test_commit_no_warning(self):
        """Test COMMIT SQL does not warn with ValkeyLock."""
        lock = ValkeyLock(name="sync_valkey_dbapi_warn2", timeout=30.0)
        conn = rqlite.connect(host="localhost", port=4001, lock=lock)
        cursor = conn.cursor()
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            try:
                cursor.execute("COMMIT")
            except Exception:
                pass
            transaction_warnings = [
                x
                for x in w
                if "COMMIT" in str(x.message) or "not supported" in str(x.message).lower()
            ]
            assert len(transaction_warnings) == 0
        cursor.close()
        conn.close()

    def test_rollback_no_warning(self):
        """Test ROLLBACK SQL does not warn with ValkeyLock."""
        lock = ValkeyLock(name="sync_valkey_dbapi_warn3", timeout=30.0)
        conn = rqlite.connect(host="localhost", port=4001, lock=lock)
        cursor = conn.cursor()
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            try:
                cursor.execute("ROLLBACK")
            except Exception:
                pass
            transaction_warnings = [
                x
                for x in w
                if "ROLLBACK" in str(x.message) or "not supported" in str(x.message).lower()
            ]
            assert len(transaction_warnings) == 0
        cursor.close()
        conn.close()


# ── Lock Configuration Tests ──────────────────────────────────────────────


@skip_if_no_valkey
class TestSyncValkeyLockConfig:
    """Test ValkeyLock configuration on connections."""

    def test_lock_is_same_instance(self):
        """Test that the lock passed to connect() is the same instance."""
        lock = ValkeyLock(name="sync_valkey_dbapi_cfg", timeout=30.0)
        conn = rqlite.connect(host="localhost", port=4001, lock=lock)
        assert conn._lock is lock
        conn.close()

    def test_connection_has_valkey_lock(self):
        """Test that connection created with ValkeyLock has the lock."""
        lock = ValkeyLock(name="sync_valkey_dbapi_cfg2", timeout=30.0)
        conn = rqlite.connect(host="localhost", port=4001, lock=lock)
        assert conn._lock is not None
        assert isinstance(conn._lock, ValkeyLock)
        conn.close()
