"""Tests for sync ThreadLock in rqlite client.

Covers:
- Unit tests (lock creation, acquire/release, protocol compliance)
- Connection/cursor behavior with ThreadLock and threading.Lock
- Transaction warning suppression with ThreadLock
- CRUD operations under ThreadLock
- Multi-cursor lock sharing
- Context manager behavior
- Full integration workflow

Usage:
    pytest tests/test_sync_thread_lock.py -v
"""

import threading
import warnings

import rqlite
from rqlite.types import LockProtocol, ThreadLock


class TestSyncThreadLockUnit:
    """Tests for ThreadLock unit behavior."""

    def test_sync_thread_lock_creation(self):
        """Test ThreadLock can be created."""
        lock = ThreadLock()
        assert lock is not None

    def test_sync_thread_lock_acquire_release(self):
        """Test ThreadLock acquire and release."""
        lock = ThreadLock()
        acquired = lock.acquire()
        assert acquired is True
        lock.release()

    def test_sync_thread_lock_nonblocking_acquire(self):
        """Test ThreadLock non-blocking acquire."""
        lock = ThreadLock()
        # First acquire should succeed
        assert lock.acquire() is True
        # Second non-blocking acquire should fail
        assert lock.acquire(blocking=False) is False
        lock.release()

    def test_sync_thread_lock_context_manager(self):
        """Test ThreadLock as context manager."""
        lock = ThreadLock()
        with lock:
            pass  # Lock acquired, then released on exit
        # Should be able to acquire again after release
        assert lock.acquire() is True
        lock.release()

    def test_sync_thread_lock_satisfies_protocol(self):
        """Test ThreadLock satisfies LockProtocol."""
        lock = ThreadLock()
        assert isinstance(lock, LockProtocol)


class TestSyncThreadLockWithThreadingLock:
    """Test that threading.Lock works with rqlite (compatibility)."""

    def test_sync_thread_lock_threading_lock_satisfies_protocol(self):
        """Test threading.Lock satisfies LockProtocol."""
        lock = threading.Lock()
        assert isinstance(lock, LockProtocol)

    def test_sync_thread_lock_connection_with_threading_lock(self):
        """Test connection can be created with threading.Lock."""
        conn = rqlite.connect(host="localhost", port=4001, lock=threading.Lock())
        assert conn._lock is not None
        assert isinstance(conn._lock, LockProtocol)
        conn.close()

    def test_sync_thread_lock_cursor_inherits_threading_lock(self):
        """Test cursor inherits threading.Lock from connection."""
        conn = rqlite.connect(host="localhost", port=4001, lock=threading.Lock())
        cursor = conn.cursor()
        assert cursor._lock is not None
        assert isinstance(cursor._lock, LockProtocol)
        cursor.close()
        conn.close()


class TestSyncThreadLockWithConnection:
    """Test Connection behavior with ThreadLock."""

    def test_sync_thread_lock_connection_with_threadlock(self):
        """Test connection created with ThreadLock."""
        lock = ThreadLock()
        conn = rqlite.connect(host="localhost", port=4001, lock=lock)
        assert conn._lock is lock
        conn.close()

    def test_sync_thread_lock_connection_without_lock(self):
        """Test connection without lock has None."""
        conn = rqlite.connect(host="localhost", port=4001)
        assert conn._lock is None
        conn.close()

    def test_sync_thread_lock_cursor_inherits_lock_from_connection(self):
        """Test cursor inherits lock from connection."""
        lock = ThreadLock()
        conn = rqlite.connect(host="localhost", port=4001, lock=lock)
        cursor = conn.cursor()
        assert cursor._lock is lock
        cursor.close()
        conn.close()

    def test_sync_thread_lock_cursor_no_lock_when_connection_has_none(self):
        """Test cursor has no lock when connection has None."""
        conn = rqlite.connect(host="localhost", port=4001)
        cursor = conn.cursor()
        assert cursor._lock is None
        cursor.close()
        conn.close()


class TestSyncThreadLockWarningSuppression:
    """Test that providing ThreadLock suppresses transaction warnings."""

    def test_sync_thread_lock_begin_warning_without_lock(self, cursor):
        """Test BEGIN SQL raises warning without lock.

        Note: rqlite v9 does NOT raise an error for BEGIN/COMMIT/ROLLBACK SQL,
        it simply ignores these statements. Our client emits a warning to inform
        users that explicit transaction SQL is not supported in the traditional sense.
        """
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            cursor.execute("BEGIN")

            transaction_warnings = [
                x for x in w if "BEGIN" in str(x.message) or "not supported" in str(x.message).lower()
            ]
            assert len(transaction_warnings) == 1, (
                "BEGIN should warn when no lock is provided"
            )

    def test_sync_thread_lock_begin_no_warning_with_lock(self):
        """Test BEGIN SQL does not raise warning with ThreadLock."""
        lock = ThreadLock()
        conn = rqlite.connect(host="localhost", port=4001, lock=lock)
        cursor = conn.cursor()

        try:
            with warnings.catch_warnings(record=True) as w:
                warnings.simplefilter("always")
                cursor.execute("BEGIN")

                transaction_warnings = [
                    x for x in w if "BEGIN" in str(x.message) or "not supported" in str(x.message).lower()
                ]
                assert len(transaction_warnings) == 0, (
                    "BEGIN should not warn when lock is provided"
                )
        finally:
            cursor.close()
            conn.close()

    def test_sync_thread_lock_commit_warning_without_lock(self, cursor):
        """Test COMMIT SQL raises warning without lock."""
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            cursor.execute("COMMIT")
            assert len(w) == 1
            assert "COMMIT" in str(w[0].message) or "not supported" in str(w[0].message).lower()

    def test_sync_thread_lock_commit_no_warning_with_lock(self):
        """Test COMMIT SQL does not raise warning with ThreadLock."""
        lock = ThreadLock()
        conn = rqlite.connect(host="localhost", port=4001, lock=lock)
        cursor = conn.cursor()

        try:
            with warnings.catch_warnings(record=True) as w:
                warnings.simplefilter("always")
                cursor.execute("COMMIT")

                transaction_warnings = [
                    x for x in w if "COMMIT" in str(x.message) or "not supported" in str(x.message).lower()
                ]
                assert len(transaction_warnings) == 0
        finally:
            cursor.close()
            conn.close()

    def test_sync_thread_lock_rollback_warning_without_lock(self, cursor):
        """Test ROLLBACK SQL raises warning without lock."""
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            cursor.execute("ROLLBACK")
            assert len(w) == 1
            assert "ROLLBACK" in str(w[0].message) or "not supported" in str(w[0].message).lower()

    def test_sync_thread_lock_rollback_no_warning_with_lock(self):
        """Test ROLLBACK SQL does not raise warning with ThreadLock."""
        lock = ThreadLock()
        conn = rqlite.connect(host="localhost", port=4001, lock=lock)
        cursor = conn.cursor()

        try:
            with warnings.catch_warnings(record=True) as w:
                warnings.simplefilter("always")
                cursor.execute("ROLLBACK")
                transaction_warnings = [
                    x for x in w if "ROLLBACK" in str(x.message) or "not supported" in str(x.message).lower()
                ]
                assert len(transaction_warnings) == 0
        finally:
            cursor.close()
            conn.close()

    def test_sync_thread_lock_savepoint_warning_without_lock(self, cursor):
        """Test SAVEPOINT SQL raises warning without lock."""
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            cursor.execute("SAVEPOINT my_sp")
            assert len(w) == 1
            assert "SAVEPOINT" in str(w[0].message) or "not supported" in str(w[0].message).lower()

    def test_sync_thread_lock_savepoint_no_warning_with_lock(self):
        """Test SAVEPOINT SQL does not raise warning with ThreadLock."""
        lock = ThreadLock()
        conn = rqlite.connect(host="localhost", port=4001, lock=lock)
        cursor = conn.cursor()

        try:
            with warnings.catch_warnings(record=True) as w:
                warnings.simplefilter("always")
                cursor.execute("SAVEPOINT my_sp")
                transaction_warnings = [
                    x for x in w if "SAVEPOINT" in str(x.message) or "not supported" in str(x.message).lower()
                ]
                assert len(transaction_warnings) == 0
        finally:
            cursor.close()
            conn.close()


class TestSyncThreadLockWithOperations:
    """Test that normal operations work with ThreadLock."""

    def test_sync_thread_lock_select_with_lock(self):
        """Test SELECT works with ThreadLock."""
        lock = ThreadLock()
        conn = rqlite.connect(host="localhost", port=4001, lock=lock)
        cursor = conn.cursor()

        try:
            cursor.execute("DROP TABLE IF EXISTS sync_thread_lock_test_select")
            cursor.execute("CREATE TABLE sync_thread_lock_test_select (id INTEGER PRIMARY KEY, name TEXT)")
            conn.commit()

            cursor.execute("INSERT INTO sync_thread_lock_test_select (name) VALUES (?)", ("test",))
            conn.commit()

            cursor.execute("SELECT * FROM sync_thread_lock_test_select")
            rows = cursor.fetchall()
            assert len(rows) == 1
            assert rows[0][1] == "test"
        finally:
            cursor.close()
            conn.close()

    def test_sync_thread_lock_insert_with_lock(self):
        """Test INSERT works with ThreadLock."""
        lock = ThreadLock()
        conn = rqlite.connect(host="localhost", port=4001, lock=lock)
        cursor = conn.cursor()

        try:
            cursor.execute("DROP TABLE IF EXISTS sync_thread_lock_test_insert")
            cursor.execute("CREATE TABLE sync_thread_lock_test_insert (id INTEGER PRIMARY KEY, value TEXT)")
            conn.commit()

            cursor.execute("INSERT INTO sync_thread_lock_test_insert (value) VALUES (?)", ("hello",))
            conn.commit()

            cursor.execute("SELECT COUNT(*) FROM sync_thread_lock_test_insert")
            row = cursor.fetchone()
            assert row[0] == 1
        finally:
            cursor.close()
            conn.close()

    def test_sync_thread_lock_update_with_lock(self):
        """Test UPDATE works with ThreadLock."""
        lock = ThreadLock()
        conn = rqlite.connect(host="localhost", port=4001, lock=lock)
        cursor = conn.cursor()

        try:
            cursor.execute("DROP TABLE IF EXISTS sync_thread_lock_test_update")
            cursor.execute("CREATE TABLE sync_thread_lock_test_update (id INTEGER PRIMARY KEY, value TEXT)")
            conn.commit()

            cursor.execute("INSERT INTO sync_thread_lock_test_update (value) VALUES (?)", ("old",))
            conn.commit()

            cursor.execute("UPDATE sync_thread_lock_test_update SET value = ?", ("new",))
            conn.commit()

            cursor.execute("SELECT value FROM sync_thread_lock_test_update")
            row = cursor.fetchone()
            assert row[0] == "new"
        finally:
            cursor.close()
            conn.close()

    def test_sync_thread_lock_delete_with_lock(self):
        """Test DELETE works with ThreadLock."""
        lock = ThreadLock()
        conn = rqlite.connect(host="localhost", port=4001, lock=lock)
        cursor = conn.cursor()

        try:
            cursor.execute("DROP TABLE IF EXISTS sync_thread_lock_test_delete")
            cursor.execute("CREATE TABLE sync_thread_lock_test_delete (id INTEGER PRIMARY KEY, value TEXT)")
            conn.commit()

            cursor.execute("INSERT INTO sync_thread_lock_test_delete (value) VALUES (?)", ("to_delete",))
            conn.commit()

            cursor.execute("DELETE FROM sync_thread_lock_test_delete WHERE value = ?", ("to_delete",))
            conn.commit()

            cursor.execute("SELECT COUNT(*) FROM sync_thread_lock_test_delete")
            row = cursor.fetchone()
            assert row[0] == 0
        finally:
            cursor.close()
            conn.close()


class TestSyncThreadLockMultipleCursors:
    """Test lock behavior with multiple cursors."""

    def test_sync_thread_lock_multiple_cursors_share_lock(self):
        """Test that multiple cursors from same connection share the lock."""
        lock = ThreadLock()
        conn = rqlite.connect(host="localhost", port=4001, lock=lock)

        cursor1 = conn.cursor()
        cursor2 = conn.cursor()

        try:
            assert cursor1._lock is lock
            assert cursor2._lock is lock
            assert cursor1._lock is cursor2._lock
        finally:
            cursor1.close()
            cursor2.close()
            conn.close()

    def test_sync_thread_lock_multiple_cursors_operations_with_lock(self):
        """Test operations with multiple cursors and ThreadLock."""
        lock = ThreadLock()
        conn = rqlite.connect(host="localhost", port=4001, lock=lock)

        cursor1 = conn.cursor()
        cursor2 = conn.cursor()

        try:
            cursor1.execute("DROP TABLE IF EXISTS sync_thread_lock_multi_cursor_test")
            cursor1.execute("CREATE TABLE sync_thread_lock_multi_cursor_test (id INTEGER PRIMARY KEY, data TEXT)")
            conn.commit()

            cursor1.execute("INSERT INTO sync_thread_lock_multi_cursor_test (data) VALUES (?)", ("from_cursor1",))
            conn.commit()

            cursor2.execute("INSERT INTO sync_thread_lock_multi_cursor_test (data) VALUES (?)", ("from_cursor2",))
            conn.commit()

            cursor1.execute("SELECT COUNT(*) FROM sync_thread_lock_multi_cursor_test")
            row = cursor1.fetchone()
            assert row[0] == 2

            cursor2.execute("SELECT data FROM sync_thread_lock_multi_cursor_test ORDER BY data")
            rows = cursor2.fetchall()
            assert len(rows) == 2
        finally:
            cursor1.close()
            cursor2.close()
            conn.close()


class TestSyncThreadLockContextManager:
    """Test lock as context manager in various scenarios."""

    def test_sync_thread_lock_context_manager(self):
        """Test ThreadLock works as context manager."""
        lock = ThreadLock()

        with lock:
            assert lock.acquire(blocking=False) is False

        assert lock.acquire(blocking=False) is True
        lock.release()

    def test_sync_thread_lock_threading_lock_context_manager(self):
        """Test threading.Lock works as context manager."""
        lock = threading.Lock()

        with lock:
            assert lock.acquire(blocking=False) is False

        assert lock.acquire(blocking=False) is True
        lock.release()


class TestSyncThreadLockIntegration:
    """Integration tests for ThreadLock functionality."""

    def test_sync_thread_lock_full_workflow_with_threadlock(self):
        """Test complete CRUD workflow with ThreadLock."""
        lock = ThreadLock()
        conn = rqlite.connect(host="localhost", port=4001, lock=lock)
        cursor = conn.cursor()

        try:
            cursor.execute("DROP TABLE IF EXISTS sync_thread_lock_full_workflow")
            cursor.execute("""
                CREATE TABLE sync_thread_lock_full_workflow (
                    id INTEGER PRIMARY KEY,
                    name TEXT NOT NULL,
                    value INTEGER
                )
            """)
            conn.commit()

            items = [("A", 1), ("B", 2), ("C", 3)]
            for name, val in items:
                cursor.execute(
                    "INSERT INTO sync_thread_lock_full_workflow (name, value) VALUES (?, ?)",
                    (name, val),
                )
            conn.commit()

            cursor.execute("SELECT COUNT(*) FROM sync_thread_lock_full_workflow")
            row = cursor.fetchone()
            assert row[0] == 3

            cursor.execute("SELECT * FROM sync_thread_lock_full_workflow ORDER BY value")
            cursor.fetchall()

            cursor.execute("UPDATE sync_thread_lock_full_workflow SET value = ? WHERE name = ?", (10, "B"))
            conn.commit()

            cursor.execute("SELECT value FROM sync_thread_lock_full_workflow WHERE name = ?", ("B",))
            row = cursor.fetchone()
            assert row[0] == 10

            cursor.execute("DELETE FROM sync_thread_lock_full_workflow WHERE name = ?", ("C",))
            conn.commit()

            cursor.execute("SELECT COUNT(*) FROM sync_thread_lock_full_workflow")
            row = cursor.fetchone()
            assert row[0] == 2
        finally:
            cursor.close()
            conn.close()

    def test_sync_thread_lock_full_workflow_with_threading_lock(self):
        """Test complete CRUD workflow with threading.Lock."""
        lock = threading.Lock()
        conn = rqlite.connect(host="localhost", port=4001, lock=lock)
        cursor = conn.cursor()

        try:
            cursor.execute("DROP TABLE IF EXISTS sync_thread_lock_threading_workflow")
            cursor.execute("""
                CREATE TABLE sync_thread_lock_threading_workflow (
                    id INTEGER PRIMARY KEY,
                    data TEXT
                )
            """)
            conn.commit()

            cursor.execute("INSERT INTO sync_thread_lock_threading_workflow (data) VALUES (?)", ("test",))
            conn.commit()

            cursor.execute("SELECT * FROM sync_thread_lock_threading_workflow")
            rows = cursor.fetchall()
            assert len(rows) == 1
        finally:
            cursor.close()
            conn.close()
