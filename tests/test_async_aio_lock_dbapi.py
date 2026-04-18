"""Tests for async DB-API 2.0 connection with AioLock.

Covers:
- Async connection creation and lifecycle
- Async cursor operations (create, insert, select, update, delete)
- Context managers for async connections
- Multiple cursors on same async connection

Usage:
    pytest tests/test_async_aio_lock_dbapi.py -v
"""

import asyncio

import rqlite


class TestAsyncAioLockConnection:
    """Test AsyncConnection class."""

    def test_async_connect(self):
        """Test basic async connection creation."""
        conn = rqlite.async_connect(host="localhost", port=4001)

        async def _test():
            assert conn.host == "localhost"
            assert conn.port == 4001
            assert not conn._closed

        asyncio.run(_test())

    def test_async_cursor_creation(self):
        """Test creating a cursor from async connection."""
        conn = rqlite.async_connect(host="localhost", port=4001)

        async def _test():
            cursor = await conn.cursor()
            assert cursor is not None
            assert cursor.connection is conn
            await cursor.close()

        asyncio.run(_test())

    def test_async_close_connection(self):
        """Test closing async connection."""
        conn = rqlite.async_connect(host="localhost", port=4001)

        async def _test():
            await conn.close()
            assert conn._closed

        asyncio.run(_test())

    def test_async_context_manager(self):
        """Test async connection as context manager."""
        async def _test():
            async with rqlite.async_connect("localhost", 4001) as conn:
                assert not conn._closed
                cursor = await conn.cursor()
                await cursor.execute("SELECT 1")
                await cursor.close()

        asyncio.run(_test())

    def test_async_commit_without_transaction(self):
        """Test commit when no transaction is pending."""
        conn = rqlite.async_connect(host="localhost", port=4001)

        async def _test():
            await conn.commit()

        asyncio.run(_test())
        conn.close()


class TestAsyncAioLockConnectionWithAuth:
    """Test async connection with authentication."""

    def test_async_connect_with_auth(self):
        """Test async connection with username and password."""
        conn = rqlite.async_connect(
            host="localhost",
            port=4001,
            username="testuser",
            password="testpass",
        )

        async def _test():
            assert conn.username == "testuser"
            assert conn.password == "testpass"
            assert conn._auth == ("testuser", "testpass")

        asyncio.run(_test())
        conn.close()


class TestAsyncAioLockConnectionWithTimeout:
    """Test async connection timeout settings."""

    def test_async_connect_with_timeout(self):
        """Test async connection with custom timeout."""
        conn = rqlite.async_connect(host="localhost", port=4001, timeout=60.0)

        async def _test():
            assert conn.timeout == 60.0

        asyncio.run(_test())
        conn.close()


class TestAsyncAioLockComplexConnectionWorkflow:
    """Test complex async connection workflow with multiple operations."""

    def test_async_full_crud_lifecycle(self):
        """Test complete CRUD lifecycle: create, read, update, delete."""
        conn = rqlite.async_connect(host="localhost", port=4001)

        async def _test():
            cursor = await conn.cursor()

            # Step 1: CREATE TABLE
            await cursor.execute("DROP TABLE IF EXISTS async_test_items")
            await conn.commit()
            await cursor.execute("""
                CREATE TABLE async_test_items (
                    id INTEGER PRIMARY KEY,
                    name TEXT NOT NULL,
                    value INTEGER,
                    active INTEGER DEFAULT 1
                )
            """)
            await conn.commit()

            # Step 2: INSERT MANY
            items = [
                ("Item A", 100, 1),
                ("Item B", 200, 1),
                ("Item C", 300, 1),
                ("Item D", 400, 0),
                ("Item E", 500, 1),
            ]
            await cursor.executemany(
                "INSERT INTO async_test_items (name, value, active) VALUES (?, ?, ?)",
                items,
            )
            await conn.commit()

            # Step 3: SELECT ALL
            await cursor.execute("SELECT * FROM async_test_items ORDER BY value")
            rows = cursor.fetchall()
            assert len(rows) == 5
            assert rows[0][1] == "Item A"
            assert rows[-1][1] == "Item E"

            # Step 4: SELECT FEW (filter)
            await cursor.execute(
                "SELECT name, value FROM async_test_items WHERE active = ? AND value > ?",
                (1, 150),
            )
            rows = cursor.fetchall()
            assert len(rows) == 3
            names = {row[0] for row in rows}
            assert names == {"Item B", "Item C", "Item E"}

            # Step 5: SELECT ONE
            await cursor.execute(
                "SELECT name, value, active FROM async_test_items WHERE name = ?",
                ("Item C",),
            )
            row = cursor.fetchone()
            assert row is not None
            assert row[0] == "Item C"
            assert row[1] == 300

            # Step 6: UPDATE
            await cursor.execute(
                "UPDATE async_test_items SET value = ? WHERE name = ?",
                (350, "Item C"),
            )
            await conn.commit()

            # Step 7: SELECT ONE (verify update)
            await cursor.execute(
                "SELECT value FROM async_test_items WHERE name = ?",
                ("Item C",),
            )
            row = cursor.fetchone()
            assert row[0] == 350

            # Step 8: DELETE
            await cursor.execute("DELETE FROM async_test_items WHERE active = ?", (0,))
            await conn.commit()

            # Step 9: SELECT MANY (final state)
            await cursor.execute("SELECT name, value FROM async_test_items ORDER BY value")
            rows = cursor.fetchall()
            assert len(rows) == 4

            # Step 10: SELECT ONE (non-existent)
            await cursor.execute(
                "SELECT name FROM async_test_items WHERE name = ?",
                ("Item D",),
            )
            row = cursor.fetchone()
            assert row is None

            await cursor.close()

        asyncio.run(_test())
        conn.close()

    def test_async_multiple_cursors_same_connection(self):
        """Test using multiple cursors on the same async connection."""
        conn = rqlite.async_connect(host="localhost", port=4001)

        async def _test():
            cursor1 = await conn.cursor()
            await cursor1.execute("DROP TABLE IF EXISTS async_multi_cursor_test")
            await cursor1.execute("CREATE TABLE async_multi_cursor_test (id INTEGER, data TEXT)")
            await conn.commit()

            await cursor1.execute("INSERT INTO async_multi_cursor_test VALUES (?, ?)", (1, "data1"))
            await cursor1.execute("INSERT INTO async_multi_cursor_test VALUES (?, ?)", (2, "data2"))
            await conn.commit()

            cursor2 = await conn.cursor()
            await cursor2.execute("SELECT * FROM async_multi_cursor_test ORDER BY id")
            rows = cursor2.fetchall()
            assert len(rows) == 2
            assert rows[0] == (1, "data1")
            assert rows[1] == (2, "data2")

            await cursor1.execute("UPDATE async_multi_cursor_test SET data = ? WHERE id = ?", ("updated", 1))
            await conn.commit()

            await cursor2.execute("SELECT data FROM async_multi_cursor_test WHERE id = ?", (1,))
            row = cursor2.fetchone()
            assert row[0] == "updated"

            await cursor1.close()
            await cursor2.close()

        asyncio.run(_test())
        conn.close()


class TestAsyncAioLockReadConsistency:
    """Test read consistency levels with async connection."""

    def test_async_select_with_default_consistency(self):
        """Test SELECT uses LINEARIZABLE by default."""
        from rqlite.types import ReadConsistency

        conn = rqlite.async_connect(host="localhost", port=4001)

        async def _test():
            assert conn.read_consistency == ReadConsistency.LINEARIZABLE
            cursor = await conn.cursor()
            await cursor.execute("SELECT 1 as test")
            row = cursor.fetchone()
            assert row[0] == 1
            await cursor.close()

        asyncio.run(_test())
        conn.close()

    def test_async_select_with_weak_consistency(self):
        """Test SELECT with WEAK consistency."""
        from rqlite.types import ReadConsistency

        conn = rqlite.async_connect(
            host="localhost", port=4001, read_consistency=ReadConsistency.WEAK
        )

        async def _test():
            assert conn.read_consistency == ReadConsistency.WEAK
            cursor = await conn.cursor()
            await cursor.execute("DROP TABLE IF EXISTS async_test_weak")
            await cursor.execute("CREATE TABLE async_test_weak (id INTEGER PRIMARY KEY, name TEXT)")
            await conn.commit()
            await cursor.execute("INSERT INTO async_test_weak (name) VALUES (?)", ("test",))
            await conn.commit()
            await cursor.execute("SELECT * FROM async_test_weak")
            rows = cursor.fetchall()
            assert len(rows) == 1
            await cursor.close()

        asyncio.run(_test())
        conn.close()

    def test_async_select_with_none_consistency(self):
        """Test SELECT with NONE consistency."""
        from rqlite.types import ReadConsistency

        conn = rqlite.async_connect(
            host="localhost", port=4001, read_consistency=ReadConsistency.NONE
        )

        async def _test():
            assert conn.read_consistency == ReadConsistency.NONE
            cursor = await conn.cursor()
            await cursor.execute("DROP TABLE IF EXISTS async_test_none")
            await cursor.execute("CREATE TABLE async_test_none (id INTEGER PRIMARY KEY, name TEXT)")
            await conn.commit()
            await cursor.execute("INSERT INTO async_test_none (name) VALUES (?)", ("test",))
            await conn.commit()
            await cursor.execute("SELECT * FROM async_test_none")
            rows = cursor.fetchall()
            assert len(rows) == 1
            await cursor.close()

        asyncio.run(_test())
        conn.close()


class TestAsyncAioLockEmptyResultSets:
    """Test that empty SELECT results don't trigger warnings in async."""

    def test_async_empty_select_no_warning(self):
        """Test empty SELECT returns None without warning."""
        import warnings

        conn = rqlite.async_connect(host="localhost", port=4001)

        async def _test():
            cursor = await conn.cursor()
            await cursor.execute("DROP TABLE IF EXISTS async_empty_test")
            await cursor.execute("CREATE TABLE async_empty_test (id INTEGER, name TEXT)")
            await cursor.execute("INSERT INTO async_empty_test VALUES (?, ?)", (1, "OnlyRow"))
            await conn.commit()

            with warnings.catch_warnings(record=True) as w:
                warnings.simplefilter("always")
                await cursor.execute(
                    "SELECT * FROM async_empty_test WHERE name = ?",
                    ("NonExistent",),
                )
                row = cursor.fetchone()
                assert row is None
                no_results_warnings = [
                    x for x in w if "No results to fetch" in str(x.message)
                ]
                assert len(no_results_warnings) == 0

            with warnings.catch_warnings(record=True) as w:
                warnings.simplefilter("always")
                rows = cursor.fetchall()
                assert rows == []
                no_results_warnings = [
                    x for x in w if "No results to fetch" in str(x.message)
                ]
                assert len(no_results_warnings) == 0

            await cursor.close()

        asyncio.run(_test())
        conn.close()
