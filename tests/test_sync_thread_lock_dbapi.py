"""Tests for sync DB-API 2.0 connection with ThreadLock."""


import pytest

import rqlite


class TestSyncThreadLockConnection:
    """Test Connection class."""

    def test_connect(self, connection):
        """Test basic connection creation."""
        assert connection.host == "localhost"
        assert connection.port == 4001
        assert not connection._closed

    def test_cursor_creation(self, connection):
        """Test creating a cursor from connection."""
        cursor = connection.cursor()
        assert cursor is not None
        assert cursor.connection is connection
        cursor.close()

    def test_close_connection(self, connection):
        """Test closing connection."""
        connection.close()
        assert connection._closed

        # Creating cursor after close should raise error
        with pytest.raises(Exception) as exc_info:
            connection.cursor()
        assert "closed" in str(exc_info.value).lower()

    def test_context_manager(self):
        """Test connection as context manager."""
        with rqlite.connect("localhost", 4001) as conn:
            assert not conn._closed
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            cursor.close()

        assert conn._closed

    def test_commit_without_transaction(self, connection):
        """Test commit when no transaction is pending."""
        # Should not raise
        connection.commit()

    def test_rollback_without_transaction(self, connection):
        """Test rollback when no transaction is pending."""
        # Should not raise
        connection.rollback()


class TestSyncThreadLockConnectionAuthentication:
    """Test connection with authentication."""

    def test_connect_with_auth(self):
        """Test connection with username and password."""
        conn = rqlite.connect(
            host="localhost",
            port=4001,
            username="testuser",
            password="testpass",
        )
        assert conn.username == "testuser"
        assert conn.password == "testpass"
        assert conn._auth == ("testuser", "testpass")
        conn.close()


class TestSyncThreadLockConnectionTimeout:
    """Test connection timeout settings."""

    def test_connect_with_timeout(self):
        """Test connection with custom timeout."""
        conn = rqlite.connect(host="localhost", port=4001, timeout=60.0)
        assert conn.timeout == 60.0
        conn.close()


class TestSyncThreadLockComplexConnectionWorkflow:
    """Test complex connection workflow with multiple operations."""

    def test_full_crud_lifecycle(self):
        """Test complete CRUD lifecycle: create, read, update, delete."""
        conn = rqlite.connect(host="localhost", port=4001)
        try:
            cursor = conn.cursor()

            # Step 1: CREATE TABLE
            cursor.execute("DROP TABLE IF EXISTS test_items")
            conn.commit()
            cursor.execute("""
                CREATE TABLE test_items (
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
                "INSERT INTO test_items (name, value, active) VALUES (?, ?, ?)",
                items,
            )
            conn.commit()

            # Step 3: SELECT ALL
            cursor.execute("SELECT * FROM test_items ORDER BY value")
            rows = cursor.fetchall()
            assert len(rows) == 5
            assert rows[0][1] == "Item A"  # Lowest value
            assert rows[-1][1] == "Item E"  # Highest value

            # Step 4: SELECT FEW (filter)
            cursor.execute(
                "SELECT name, value FROM test_items WHERE active = ? AND value > ?",
                (1, 150),
            )
            rows = cursor.fetchall()
            assert len(rows) == 3  # Item B, C, E
            names = {row[0] for row in rows}
            assert names == {"Item B", "Item C", "Item E"}

            # Step 5: SELECT ONE
            cursor.execute(
                "SELECT name, value, active FROM test_items WHERE name = ?",
                ("Item C",),
            )
            row = cursor.fetchone()
            assert row is not None
            assert row[0] == "Item C"
            assert row[1] == 300

            # Step 6: UPDATE
            cursor.execute(
                "UPDATE test_items SET value = ? WHERE name = ?",
                (350, "Item C"),
            )
            conn.commit()

            # Step 7: SELECT ONE (verify update)
            cursor.execute(
                "SELECT value FROM test_items WHERE name = ?",
                ("Item C",),
            )
            row = cursor.fetchone()
            assert row[0] == 350

            # Step 8: DELETE
            cursor.execute("DELETE FROM test_items WHERE active = ?", (0,))
            conn.commit()

            # Step 9: SELECT MANY (final state)
            cursor.execute("SELECT name, value FROM test_items ORDER BY value")
            rows = cursor.fetchall()
            assert len(rows) == 4  # Item D was deleted

            # Step 10: SELECT ONE (non-existent)
            cursor.execute(
                "SELECT name FROM test_items WHERE name = ?",
                ("Item D",),
            )
            row = cursor.fetchone()
            assert row is None  # Item D was deleted

            cursor.close()
        finally:
            conn.close()

    def test_multiple_cursors_same_connection(self):
        """Test using multiple cursors on the same connection."""
        conn = rqlite.connect(host="localhost", port=4001)
        try:
            # Create table
            cursor1 = conn.cursor()
            cursor1.execute("DROP TABLE IF EXISTS multi_cursor_test")
            cursor1.execute("CREATE TABLE multi_cursor_test (id INTEGER, data TEXT)")
            conn.commit()

            # Use cursor1 to insert
            cursor1.execute("INSERT INTO multi_cursor_test VALUES (?, ?)", (1, "data1"))
            cursor1.execute("INSERT INTO multi_cursor_test VALUES (?, ?)", (2, "data2"))
            conn.commit()

            # Use cursor2 to query
            cursor2 = conn.cursor()
            cursor2.execute("SELECT * FROM multi_cursor_test ORDER BY id")
            rows = cursor2.fetchall()
            assert len(rows) == 2
            assert rows[0] == (1, "data1")
            assert rows[1] == (2, "data2")

            # Use cursor1 again to update
            cursor1.execute("UPDATE multi_cursor_test SET data = ? WHERE id = ?", ("updated", 1))
            conn.commit()

            # Use cursor2 to verify
            cursor2.execute("SELECT data FROM multi_cursor_test WHERE id = ?", (1,))
            row = cursor2.fetchone()
            assert row[0] == "updated"

            cursor1.close()
            cursor2.close()
        finally:
            conn.close()

    def test_empty_select_no_warning(self):
        """Test that empty SELECT results don't trigger warnings.

        This is a regression test for the issue where fetchone() would warn
        even when a SELECT was executed but returned no rows.
        """
        import warnings

        conn = rqlite.connect(host="localhost", port=4001)
        try:
            cursor = conn.cursor()

            # Create table and insert one row
            cursor.execute("DROP TABLE IF EXISTS empty_select_test")
            cursor.execute("CREATE TABLE empty_select_test (id INTEGER, name TEXT)")
            cursor.execute("INSERT INTO empty_select_test VALUES (?, ?)", (1, "OnlyRow"))
            conn.commit()

            # Execute SELECT that returns no rows
            with warnings.catch_warnings(record=True) as w:
                warnings.simplefilter("always")
                cursor.execute(
                    "SELECT * FROM empty_select_test WHERE name = ?",
                    ("NonExistent",),
                )
                row = cursor.fetchone()
                assert row is None
                # Should NOT have any warnings about "No results to fetch"
                no_results_warnings = [
                    x for x in w if "No results to fetch" in str(x.message)
                ]
                assert len(no_results_warnings) == 0, (
                    "Empty SELECT should not trigger 'No results to fetch' warning"
                )

            # fetchall() on empty result should also not warn
            with warnings.catch_warnings(record=True) as w:
                warnings.simplefilter("always")
                rows = cursor.fetchall()
                assert rows == []
                no_results_warnings = [
                    x for x in w if "No results to fetch" in str(x.message)
                ]
                assert len(no_results_warnings) == 0

            cursor.close()
        finally:
            conn.close()
