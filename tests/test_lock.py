"""Tests for lock functionality in rqlite client."""


import threading
import warnings

import rqlite
from rqlite.types import LockProtocol, ThreadLock


class TestThreadLock:
    """Test ThreadLock implementation."""

    def test_thread_lock_creation(self):
        """Test ThreadLock can be created."""
        lock = ThreadLock()
        assert lock is not None

    def test_thread_lock_acquire_release(self):
        """Test ThreadLock acquire and release."""
        lock = ThreadLock()
        acquired = lock.acquire()
        assert acquired is True
        lock.release()

    def test_thread_lock_nonblocking_acquire(self):
        """Test ThreadLock non-blocking acquire."""
        lock = ThreadLock()
        # First acquire should succeed
        assert lock.acquire() is True
        # Second non-blocking acquire should fail
        assert lock.acquire(blocking=False) is False
        lock.release()

    def test_thread_lock_context_manager(self):
        """Test ThreadLock as context manager."""
        lock = ThreadLock()
        with lock:
            pass  # Lock acquired, then released on exit
        # Should be able to acquire again after release
        assert lock.acquire() is True
        lock.release()

    def test_thread_lock_satisfies_protocol(self):
        """Test ThreadLock satisfies LockProtocol."""
        lock = ThreadLock()
        assert isinstance(lock, LockProtocol)


class TestThreadingLockCompatibility:
    """Test that threading.Lock works with rqlite."""

    def test_threading_lock_satisfies_protocol(self):
        """Test threading.Lock satisfies LockProtocol."""
        lock = threading.Lock()
        assert isinstance(lock, LockProtocol)

    def test_connect_with_threading_lock(self):
        """Test connection can be created with threading.Lock."""
        conn = rqlite.connect(host="localhost", port=4001, lock=threading.Lock())
        assert conn._lock is not None
        assert isinstance(conn._lock, LockProtocol)
        conn.close()

    def test_cursor_inherits_threading_lock(self):
        """Test cursor inherits threading.Lock from connection."""
        conn = rqlite.connect(host="localhost", port=4001, lock=threading.Lock())
        cursor = conn.cursor()
        assert cursor._lock is not None
        assert isinstance(cursor._lock, LockProtocol)
        cursor.close()
        conn.close()


class TestConnectionWithLock:
    """Test Connection behavior with lock."""

    def test_connection_with_threadlock(self):
        """Test connection created with ThreadLock."""
        lock = ThreadLock()
        conn = rqlite.connect(host="localhost", port=4001, lock=lock)
        assert conn._lock is lock
        conn.close()

    def test_connection_without_lock(self):
        """Test connection without lock has None."""
        conn = rqlite.connect(host="localhost", port=4001)
        assert conn._lock is None
        conn.close()

    def test_cursor_inherits_lock_from_connection(self):
        """Test cursor inherits lock from connection."""
        lock = ThreadLock()
        conn = rqlite.connect(host="localhost", port=4001, lock=lock)
        cursor = conn.cursor()
        assert cursor._lock is lock
        cursor.close()
        conn.close()

    def test_cursor_no_lock_when_connection_has_none(self):
        """Test cursor has no lock when connection has None."""
        conn = rqlite.connect(host="localhost", port=4001)
        cursor = conn.cursor()
        assert cursor._lock is None
        cursor.close()
        conn.close()


class TestLockSuppressesWarnings:
    """Test that providing a lock suppresses transaction warnings."""

    def test_begin_warning_without_lock(self, cursor):
        """Test BEGIN SQL raises warning without lock.

        Note: rqlite v9 does NOT raise an error for BEGIN/COMMIT/ROLLBACK SQL,
        it simply ignores these statements. Our client emits a warning to inform
        users that explicit transaction SQL is not supported in the traditional sense.
        """
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            # rqlite v9 ignores BEGIN without raising an error
            cursor.execute("BEGIN")

            # Should have warned about unsupported SQL
            transaction_warnings = [
                x for x in w if "BEGIN" in str(x.message) or "not supported" in str(x.message).lower()
            ]
            assert len(transaction_warnings) == 1, (
                "BEGIN should warn when no lock is provided"
            )

    def test_begin_no_warning_with_lock(self):
        """Test BEGIN SQL does not raise warning with lock.

        Note: Our client skips execution of explicit transaction SQL and does NOT
        emit a warning when a lock is provided.
        """
        lock = ThreadLock()
        conn = rqlite.connect(host="localhost", port=4001, lock=lock)
        cursor = conn.cursor()

        try:
            with warnings.catch_warnings(record=True) as w:
                warnings.simplefilter("always")
                # BEGIN is skipped by our client when lock is provided
                cursor.execute("BEGIN")

                # Check that no warning was raised about unsupported SQL
                transaction_warnings = [
                    x for x in w if "BEGIN" in str(x.message) or "not supported" in str(x.message).lower()
                ]
                assert len(transaction_warnings) == 0, (
                    "BEGIN should not warn when lock is provided"
                )
        finally:
            cursor.close()
            conn.close()

    def test_commit_warning_without_lock(self, cursor):
        """Test COMMIT SQL raises warning without lock."""
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            cursor.execute("COMMIT")
            assert len(w) == 1
            assert "COMMIT" in str(w[0].message) or "not supported" in str(w[0].message).lower()

    def test_commit_no_warning_with_lock(self):
        """Test COMMIT SQL does not raise warning with lock.

        Note: Our client skips execution of explicit transaction SQL and does NOT
        emit a warning when a lock is provided.
        """
        lock = ThreadLock()
        conn = rqlite.connect(host="localhost", port=4001, lock=lock)
        cursor = conn.cursor()

        try:
            with warnings.catch_warnings(record=True) as w:
                warnings.simplefilter("always")
                # COMMIT is skipped by our client when lock is provided
                cursor.execute("COMMIT")

                # Check that no warning was raised about unsupported SQL
                transaction_warnings = [
                    x for x in w if "COMMIT" in str(x.message) or "not supported" in str(x.message).lower()
                ]
                assert len(transaction_warnings) == 0
        finally:
            cursor.close()
            conn.close()

    def test_rollback_warning_without_lock(self, cursor):
        """Test ROLLBACK SQL raises warning without lock."""
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            # ROLLBACK is skipped by our client and emits a warning
            cursor.execute("ROLLBACK")
            assert len(w) == 1
            assert "ROLLBACK" in str(w[0].message) or "not supported" in str(w[0].message).lower()

    def test_rollback_no_warning_with_lock(self):
        """Test ROLLBACK SQL does not raise warning with lock."""
        lock = ThreadLock()
        conn = rqlite.connect(host="localhost", port=4001, lock=lock)
        cursor = conn.cursor()

        try:
            with warnings.catch_warnings(record=True) as w:
                warnings.simplefilter("always")
                # ROLLBACK is skipped by our client when lock is provided
                cursor.execute("ROLLBACK")
                transaction_warnings = [
                    x for x in w if "ROLLBACK" in str(x.message) or "not supported" in str(x.message).lower()
                ]
                assert len(transaction_warnings) == 0
        finally:
            cursor.close()
            conn.close()

    def test_savepoint_warning_without_lock(self, cursor):
        """Test SAVEPOINT SQL raises warning without lock."""
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            cursor.execute("SAVEPOINT my_sp")
            assert len(w) == 1
            assert "SAVEPOINT" in str(w[0].message) or "not supported" in str(w[0].message).lower()

    def test_savepoint_no_warning_with_lock(self):
        """Test SAVEPOINT SQL does not raise warning with lock."""
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


class TestLockWithOperations:
    """Test that normal operations work with lock."""

    def test_select_with_lock(self):
        """Test SELECT works with lock."""
        lock = ThreadLock()
        conn = rqlite.connect(host="localhost", port=4001, lock=lock)
        cursor = conn.cursor()

        try:
            # Clean up and create table
            cursor.execute("DROP TABLE IF EXISTS lock_test_select")
            cursor.execute("CREATE TABLE lock_test_select (id INTEGER PRIMARY KEY, name TEXT)")
            conn.commit()

            # Insert data
            cursor.execute("INSERT INTO lock_test_select (name) VALUES (?)", ("test",))
            conn.commit()

            # Select data
            cursor.execute("SELECT * FROM lock_test_select")
            rows = cursor.fetchall()
            assert len(rows) == 1
            assert rows[0][1] == "test"
        finally:
            cursor.close()
            conn.close()

    def test_insert_with_lock(self):
        """Test INSERT works with lock."""
        lock = ThreadLock()
        conn = rqlite.connect(host="localhost", port=4001, lock=lock)
        cursor = conn.cursor()

        try:
            cursor.execute("DROP TABLE IF EXISTS lock_test_insert")
            cursor.execute("CREATE TABLE lock_test_insert (id INTEGER PRIMARY KEY, value TEXT)")
            conn.commit()

            cursor.execute("INSERT INTO lock_test_insert (value) VALUES (?)", ("hello",))
            conn.commit()

            cursor.execute("SELECT COUNT(*) FROM lock_test_insert")
            row = cursor.fetchone()
            assert row[0] == 1
        finally:
            cursor.close()
            conn.close()

    def test_update_with_lock(self):
        """Test UPDATE works with lock."""
        lock = ThreadLock()
        conn = rqlite.connect(host="localhost", port=4001, lock=lock)
        cursor = conn.cursor()

        try:
            cursor.execute("DROP TABLE IF EXISTS lock_test_update")
            cursor.execute("CREATE TABLE lock_test_update (id INTEGER PRIMARY KEY, value TEXT)")
            conn.commit()

            cursor.execute("INSERT INTO lock_test_update (value) VALUES (?)", ("old",))
            conn.commit()

            cursor.execute("UPDATE lock_test_update SET value = ?", ("new",))
            conn.commit()

            cursor.execute("SELECT value FROM lock_test_update")
            row = cursor.fetchone()
            assert row[0] == "new"
        finally:
            cursor.close()
            conn.close()

    def test_delete_with_lock(self):
        """Test DELETE works with lock."""
        lock = ThreadLock()
        conn = rqlite.connect(host="localhost", port=4001, lock=lock)
        cursor = conn.cursor()

        try:
            cursor.execute("DROP TABLE IF EXISTS lock_test_delete")
            cursor.execute("CREATE TABLE lock_test_delete (id INTEGER PRIMARY KEY, value TEXT)")
            conn.commit()

            cursor.execute("INSERT INTO lock_test_delete (value) VALUES (?)", ("to_delete",))
            conn.commit()

            cursor.execute("DELETE FROM lock_test_delete WHERE value = ?", ("to_delete",))
            conn.commit()

            cursor.execute("SELECT COUNT(*) FROM lock_test_delete")
            row = cursor.fetchone()
            assert row[0] == 0
        finally:
            cursor.close()
            conn.close()


class TestLockWithMultipleCursors:
    """Test lock behavior with multiple cursors."""

    def test_multiple_cursors_share_lock(self):
        """Test that multiple cursors from same connection share the lock."""
        lock = ThreadLock()
        conn = rqlite.connect(host="localhost", port=4001, lock=lock)

        cursor1 = conn.cursor()
        cursor2 = conn.cursor()

        try:
            # Both cursors should have the same lock
            assert cursor1._lock is lock
            assert cursor2._lock is lock
            assert cursor1._lock is cursor2._lock
        finally:
            cursor1.close()
            cursor2.close()
            conn.close()

    def test_multiple_cursors_operations_with_lock(self):
        """Test operations with multiple cursors and lock."""
        lock = ThreadLock()
        conn = rqlite.connect(host="localhost", port=4001, lock=lock)

        cursor1 = conn.cursor()
        cursor2 = conn.cursor()

        try:
            # Create table with cursor1
            cursor1.execute("DROP TABLE IF EXISTS multi_cursor_lock_test")
            cursor1.execute("CREATE TABLE multi_cursor_lock_test (id INTEGER PRIMARY KEY, data TEXT)")
            conn.commit()

            # Insert with cursor1
            cursor1.execute("INSERT INTO multi_cursor_lock_test (data) VALUES (?)", ("from_cursor1",))
            conn.commit()

            # Insert with cursor2
            cursor2.execute("INSERT INTO multi_cursor_lock_test (data) VALUES (?)", ("from_cursor2",))
            conn.commit()

            # Query with cursor1
            cursor1.execute("SELECT COUNT(*) FROM multi_cursor_lock_test")
            row = cursor1.fetchone()
            assert row[0] == 2

            # Query with cursor2
            cursor2.execute("SELECT data FROM multi_cursor_lock_test ORDER BY data")
            rows = cursor2.fetchall()
            assert len(rows) == 2
        finally:
            cursor1.close()
            cursor2.close()
            conn.close()


class TestLockContextManager:
    """Test lock as context manager in various scenarios."""

    def test_threadlock_context_manager(self):
        """Test ThreadLock works as context manager."""
        lock = ThreadLock()

        with lock:
            # Lock is acquired here
            assert lock.acquire(blocking=False) is False  # Can't acquire again

        # Lock is released here, can acquire again
        assert lock.acquire(blocking=False) is True
        lock.release()

    def test_threading_lock_context_manager(self):
        """Test threading.Lock works as context manager."""
        lock = threading.Lock()

        with lock:
            assert lock.acquire(blocking=False) is False

        assert lock.acquire(blocking=False) is True
        lock.release()


class TestLockIntegration:
    """Integration tests for lock functionality."""

    def test_full_workflow_with_threadlock(self):
        """Test complete CRUD workflow with ThreadLock."""
        lock = ThreadLock()
        conn = rqlite.connect(host="localhost", port=4001, lock=lock)
        cursor = conn.cursor()

        try:
            # CREATE
            cursor.execute("DROP TABLE IF EXISTS full_lock_workflow")
            cursor.execute("""
                CREATE TABLE full_lock_workflow (
                    id INTEGER PRIMARY KEY,
                    name TEXT NOT NULL,
                    value INTEGER
                )
            """)
            conn.commit()

            # INSERT MANY
            items = [("A", 1), ("B", 2), ("C", 3)]
            for name, val in items:
                cursor.execute(
                    "INSERT INTO full_lock_workflow (name, value) VALUES (?, ?)",
                    (name, val),
                )
            conn.commit()

            # SELECT ALL - verify all inserted
            cursor.execute("SELECT COUNT(*) FROM full_lock_workflow")
            row = cursor.fetchone()
            assert row[0] == 3

            cursor.execute("SELECT * FROM full_lock_workflow ORDER BY value")
            cursor.fetchall()

            # UPDATE
            cursor.execute("UPDATE full_lock_workflow SET value = ? WHERE name = ?", (10, "B"))
            conn.commit()

            # SELECT ONE
            cursor.execute("SELECT value FROM full_lock_workflow WHERE name = ?", ("B",))
            row = cursor.fetchone()
            assert row[0] == 10

            # DELETE
            cursor.execute("DELETE FROM full_lock_workflow WHERE name = ?", ("C",))
            conn.commit()

            # SELECT MANY (final)
            cursor.execute("SELECT COUNT(*) FROM full_lock_workflow")
            row = cursor.fetchone()
            assert row[0] == 2
        finally:
            cursor.close()
            conn.close()

    def test_full_workflow_with_threading_lock(self):
        """Test complete CRUD workflow with threading.Lock."""
        lock = threading.Lock()
        conn = rqlite.connect(host="localhost", port=4001, lock=lock)
        cursor = conn.cursor()

        try:
            cursor.execute("DROP TABLE IF EXISTS threading_lock_workflow")
            cursor.execute("""
                CREATE TABLE threading_lock_workflow (
                    id INTEGER PRIMARY KEY,
                    data TEXT
                )
            """)
            conn.commit()

            cursor.execute("INSERT INTO threading_lock_workflow (data) VALUES (?)", ("test",))
            conn.commit()

            cursor.execute("SELECT * FROM threading_lock_workflow")
            rows = cursor.fetchall()
            assert len(rows) == 1
        finally:
            cursor.close()
            conn.close()
