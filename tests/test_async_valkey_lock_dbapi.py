"""Tests for async DB-API 2.0 connection with async Valkey distributed lock (AioValkeyLock).

Covers:
- Async connection creation and lifecycle with AioValkeyLock
- Async cursor operations (create, insert, select, update, delete)
- Context managers for async connections
- Multiple cursors on same async connection
- Read consistency levels
- Empty result sets (no warnings)
- Full CRUD lifecycle

Prerequisites:
    uv add tangled-pyrqlite[valkey]
    podman run -d --name valkey-test -p 6379:6379 docker.io/valkey/valkey
    rqlite running on localhost:4001
"""

from __future__ import annotations

import asyncio
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


def run_async(coro):
    """Helper to run async code in a new event loop."""
    try:
        return asyncio.get_running_loop().run_until_complete(coro)
    except RuntimeError:
        return asyncio.run(coro)


skip_if_no_valkey = pytest.mark.skipif(not _has_valkey(), reason="Valkey not available")


# ── Connection Tests ──────────────────────────────────────────────────────


@skip_if_no_valkey
class TestAsyncValkeyLockConnection:
    """Test AsyncConnection class with AioValkeyLock."""

    def test_async_connect_with_valkey_lock(self):
        """Test basic async connection creation with AioValkeyLock."""
        lock = AioValkeyLock(name="async_valkey_dbapi_conn", timeout=30.0)
        conn = rqlite.async_connect(host="localhost", port=4001, lock=lock)

        async def _test():
            assert conn.host == "localhost"
            assert conn.port == 4001
            assert not conn._closed
            assert conn._lock is lock

        run_async(_test())
        conn.close()  # ty: ignore[unused-awaitable]

    def test_async_cursor_creation(self):
        """Test creating a cursor from async connection with AioValkeyLock."""
        lock = AioValkeyLock(name="async_valkey_dbapi_cur", timeout=30.0)
        conn = rqlite.async_connect(host="localhost", port=4001, lock=lock)

        async def _test():
            cursor = await conn.cursor()
            assert cursor is not None
            assert cursor.connection is conn
            await cursor.close()

        run_async(_test())
        conn.close()  # ty: ignore[unused-awaitable]

    def test_async_close_connection(self):
        """Test closing async connection with AioValkeyLock."""
        lock = AioValkeyLock(name="async_valkey_dbapi_close", timeout=30.0)
        conn = rqlite.async_connect(host="localhost", port=4001, lock=lock)

        async def _test():
            await conn.close()
            assert conn._closed

        run_async(_test())

    def test_async_context_manager(self):
        """Test async connection as context manager with AioValkeyLock."""
        lock = AioValkeyLock(name="async_valkey_dbapi_ctx", timeout=30.0)

        async def _test():
            async with rqlite.async_connect("localhost", 4001, lock=lock) as conn:
                assert not conn._closed
                cursor = await conn.cursor()
                await cursor.execute("SELECT 1")
                await cursor.close()

        run_async(_test())

    def test_async_commit_without_transaction(self):
        """Test commit when no transaction is pending."""
        lock = AioValkeyLock(name="async_valkey_dbapi_commit", timeout=30.0)
        conn = rqlite.async_connect(host="localhost", port=4001, lock=lock)

        async def _test():
            await conn.commit()

        run_async(_test())
        conn.close()  # ty: ignore[unused-awaitable]

    def test_async_rollback_without_transaction(self):
        """Test rollback when no transaction is pending."""
        lock = AioValkeyLock(name="async_valkey_dbapi_rollback", timeout=30.0)
        conn = rqlite.async_connect(host="localhost", port=4001, lock=lock)

        async def _test():
            await conn.rollback()

        run_async(_test())
        conn.close()  # ty: ignore[unused-awaitable]


@skip_if_no_valkey
class TestAsyncValkeyLockConnectionAuth:
    """Test async connection with authentication and AioValkeyLock."""

    def test_async_connect_with_auth_and_lock(self):
        """Test async connection with username, password, and AioValkeyLock."""
        lock = AioValkeyLock(name="async_valkey_dbapi_auth", timeout=30.0)
        conn = rqlite.async_connect(
            host="localhost",
            port=4001,
            username="testuser",
            password="testpass",
            lock=lock,
        )

        async def _test():
            assert conn.username == "testuser"
            assert conn.password == "testpass"
            assert conn._auth is not None
            assert conn._auth.login == "testuser"
            assert conn._auth.password == "testpass"
            assert conn._lock is lock

        run_async(_test())
        conn.close()  # ty: ignore[unused-awaitable]


@skip_if_no_valkey
class TestAsyncValkeyLockConnectionTimeout:
    """Test async connection timeout settings with AioValkeyLock."""

    def test_async_connect_with_timeout_and_lock(self):
        """Test async connection with custom timeout and AioValkeyLock."""
        lock = AioValkeyLock(name="async_valkey_dbapi_timeout", timeout=30.0)
        conn = rqlite.async_connect(host="localhost", port=4001, timeout=60.0, lock=lock)

        async def _test():
            assert conn.timeout == 60.0

        run_async(_test())
        conn.close()  # ty: ignore[unused-awaitable]


# ── Read Consistency Tests ────────────────────────────────────────────────


@skip_if_no_valkey
class TestAsyncValkeyLockReadConsistency:
    """Test read consistency levels with AioValkeyLock."""

    def test_async_select_with_default_consistency(self):
        """Test SELECT uses LINEARIZABLE by default."""
        from rqlite.types import ReadConsistency

        lock = AioValkeyLock(name="async_valkey_dbapi_rc", timeout=30.0)
        conn = rqlite.async_connect(host="localhost", port=4001, lock=lock)

        async def _test():
            assert conn.read_consistency == ReadConsistency.LINEARIZABLE
            cursor = await conn.cursor()
            await cursor.execute("SELECT 1 as test")
            row = cursor.fetchone()
            assert row is not None
            assert row[0] == 1
            await cursor.close()

        run_async(_test())
        conn.close()  # ty: ignore[unused-awaitable]

    def test_async_select_with_weak_consistency(self):
        """Test SELECT with WEAK consistency and AioValkeyLock."""
        from rqlite.types import ReadConsistency

        lock = AioValkeyLock(name="async_valkey_dbapi_weak", timeout=30.0)
        conn = rqlite.async_connect(
            host="localhost", port=4001, read_consistency=ReadConsistency.WEAK, lock=lock
        )

        async def _test():
            assert conn.read_consistency == ReadConsistency.WEAK
            cursor = await conn.cursor()
            await cursor.execute("DROP TABLE IF EXISTS async_valkey_test_weak")
            await cursor.execute(
                "CREATE TABLE async_valkey_test_weak (id INTEGER PRIMARY KEY, name TEXT)"
            )
            await cursor.execute("INSERT INTO async_valkey_test_weak (name) VALUES (?)", ("test",))
            await conn.commit()
            await cursor.execute("SELECT * FROM async_valkey_test_weak")
            rows = cursor.fetchall()
            assert len(rows) == 1
            await cursor.close()

        run_async(_test())
        conn.close()  # ty: ignore[unused-awaitable]

    def test_async_select_with_none_consistency(self):
        """Test SELECT with NONE consistency and AioValkeyLock."""
        from rqlite.types import ReadConsistency

        lock = AioValkeyLock(name="async_valkey_dbapi_none", timeout=30.0)
        conn = rqlite.async_connect(
            host="localhost", port=4001, read_consistency=ReadConsistency.NONE, lock=lock
        )

        async def _test():
            assert conn.read_consistency == ReadConsistency.NONE
            cursor = await conn.cursor()
            await cursor.execute("DROP TABLE IF EXISTS async_valkey_test_none")
            await cursor.execute(
                "CREATE TABLE async_valkey_test_none (id INTEGER PRIMARY KEY, name TEXT)"
            )
            await cursor.execute("INSERT INTO async_valkey_test_none (name) VALUES (?)", ("test",))
            await conn.commit()
            await cursor.execute("SELECT * FROM async_valkey_test_none")
            rows = cursor.fetchall()
            assert len(rows) == 1
            await cursor.close()

        run_async(_test())
        conn.close()  # ty: ignore[unused-awaitable]


# ── Complex Workflow Tests ────────────────────────────────────────────────


@skip_if_no_valkey
class TestAsyncValkeyLockComplexWorkflow:
    """Test complex async connection workflow with AioValkeyLock."""

    def test_async_full_crud_lifecycle(self):
        """Test complete CRUD lifecycle with AioValkeyLock."""
        lock = AioValkeyLock(name="async_valkey_dbapi_crud", timeout=30.0)
        conn = rqlite.async_connect(host="localhost", port=4001, lock=lock)

        async def _test():
            cursor = await conn.cursor()

            # Step 1: CREATE TABLE
            await cursor.execute("DROP TABLE IF EXISTS async_valkey_lock_test_items")
            await conn.commit()
            await cursor.execute("""
                CREATE TABLE async_valkey_lock_test_items (
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
                "INSERT INTO async_valkey_lock_test_items (name, value, active) VALUES (?, ?, ?)",
                items,
            )
            await conn.commit()

            # Step 3: SELECT ALL
            await cursor.execute("SELECT * FROM async_valkey_lock_test_items ORDER BY value")
            rows = cursor.fetchall()
            assert len(rows) == 5
            assert rows[0][1] == "Item A"
            assert rows[-1][1] == "Item E"

            # Step 4: SELECT FEW (filter)
            await cursor.execute(
                "SELECT name, value FROM async_valkey_lock_test_items "
                "WHERE active = ? AND value > ?",
                (1, 150),
            )
            rows = cursor.fetchall()
            assert len(rows) == 3
            names = {row[0] for row in rows}
            assert names == {"Item B", "Item C", "Item E"}

            # Step 5: SELECT ONE
            await cursor.execute(
                "SELECT name, value, active FROM async_valkey_lock_test_items WHERE name = ?",
                ("Item C",),
            )
            row = cursor.fetchone()
            assert row is not None
            assert row[0] == "Item C"
            assert row[1] == 300

            # Step 6: UPDATE
            await cursor.execute(
                "UPDATE async_valkey_lock_test_items SET value = ? WHERE name = ?",
                (350, "Item C"),
            )
            await conn.commit()

            # Step 7: SELECT ONE (verify update)
            await cursor.execute(
                "SELECT value FROM async_valkey_lock_test_items WHERE name = ?",
                ("Item C",),
            )
            row = cursor.fetchone()
            assert row is not None
            assert row[0] == 350

            # Step 8: DELETE
            await cursor.execute("DELETE FROM async_valkey_lock_test_items WHERE active = ?", (0,))
            await conn.commit()

            # Step 9: SELECT MANY (final state)
            await cursor.execute(
                "SELECT name, value FROM async_valkey_lock_test_items ORDER BY value"
            )
            rows = cursor.fetchall()
            assert len(rows) == 4

            # Step 10: SELECT ONE (non-existent)
            await cursor.execute(
                "SELECT name FROM async_valkey_lock_test_items WHERE name = ?",
                ("Item D",),
            )
            row = cursor.fetchone()
            assert row is None

            await cursor.close()

        run_async(_test())
        conn.close()  # ty: ignore[unused-awaitable]

    def test_async_multiple_cursors_same_connection(self):
        """Test using multiple cursors on the same async connection with AioValkeyLock."""
        lock = AioValkeyLock(name="async_valkey_dbapi_multi", timeout=30.0)
        conn = rqlite.async_connect(host="localhost", port=4001, lock=lock)

        async def _test():
            cursor1 = await conn.cursor()
            await cursor1.execute("DROP TABLE IF EXISTS async_valkey_multi_cursor_test")
            await cursor1.execute(
                "CREATE TABLE async_valkey_multi_cursor_test (id INTEGER, data TEXT)"
            )
            await conn.commit()

            await cursor1.execute(
                "INSERT INTO async_valkey_multi_cursor_test VALUES (?, ?)", (1, "data1")
            )
            await cursor1.execute(
                "INSERT INTO async_valkey_multi_cursor_test VALUES (?, ?)", (2, "data2")
            )
            await conn.commit()

            cursor2 = await conn.cursor()
            await cursor2.execute("SELECT * FROM async_valkey_multi_cursor_test ORDER BY id")
            rows = cursor2.fetchall()
            assert len(rows) == 2
            assert rows[0] == (1, "data1")
            assert rows[1] == (2, "data2")

            await cursor1.execute(
                "UPDATE async_valkey_multi_cursor_test SET data = ? WHERE id = ?",
                ("updated", 1),
            )
            await conn.commit()

            await cursor2.execute(
                "SELECT data FROM async_valkey_multi_cursor_test WHERE id = ?", (1,)
            )
            row = cursor2.fetchone()
            assert row is not None
            assert row[0] == "updated"

            await cursor1.close()
            await cursor2.close()

        run_async(_test())
        conn.close()  # ty: ignore[unused-awaitable]


# ── Empty Result Set Tests ────────────────────────────────────────────────


@skip_if_no_valkey
class TestAsyncValkeyLockEmptyResults:
    """Test that empty SELECT results don't trigger warnings with AioValkeyLock."""

    def test_async_empty_select_no_warning(self):
        """Test empty SELECT returns None without warning."""
        lock = AioValkeyLock(name="async_valkey_dbapi_empty", timeout=30.0)
        conn = rqlite.async_connect(host="localhost", port=4001, lock=lock)

        async def _test():
            cursor = await conn.cursor()
            await cursor.execute("DROP TABLE IF EXISTS async_valkey_empty_test")
            await cursor.execute("CREATE TABLE async_valkey_empty_test (id INTEGER, name TEXT)")
            await cursor.execute(
                "INSERT INTO async_valkey_empty_test VALUES (?, ?)", (1, "OnlyRow")
            )
            await conn.commit()

            with warnings.catch_warnings(record=True) as w:
                warnings.simplefilter("always")
                await cursor.execute(
                    "SELECT * FROM async_valkey_empty_test WHERE name = ?",
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

            await cursor.close()

        run_async(_test())
        conn.close()  # ty: ignore[unused-awaitable]


# ── Warning Suppression Tests ─────────────────────────────────────────────


@skip_if_no_valkey
class TestAsyncValkeyLockWarnings:
    """Test that AioValkeyLock suppresses transaction warnings."""

    def test_async_begin_no_warning(self):
        """Test BEGIN SQL does not warn with AioValkeyLock."""
        lock = AioValkeyLock(name="async_valkey_dbapi_warn", timeout=30.0)
        conn = rqlite.async_connect(host="localhost", port=4001, lock=lock)

        async def _test():
            cursor = await conn.cursor()
            with warnings.catch_warnings(record=True) as w:
                warnings.simplefilter("always")
                try:
                    await cursor.execute("BEGIN")
                except Exception:
                    pass
                transaction_warnings = [
                    x
                    for x in w
                    if "BEGIN" in str(x.message) or "not supported" in str(x.message).lower()
                ]
                assert len(transaction_warnings) == 0
            await cursor.close()

        run_async(_test())
        conn.close()  # ty: ignore[unused-awaitable]

    def test_async_commit_no_warning(self):
        """Test COMMIT SQL does not warn with AioValkeyLock."""
        lock = AioValkeyLock(name="async_valkey_dbapi_warn2", timeout=30.0)
        conn = rqlite.async_connect(host="localhost", port=4001, lock=lock)

        async def _test():
            cursor = await conn.cursor()
            with warnings.catch_warnings(record=True) as w:
                warnings.simplefilter("always")
                try:
                    await cursor.execute("COMMIT")
                except Exception:
                    pass
                transaction_warnings = [
                    x
                    for x in w
                    if "COMMIT" in str(x.message) or "not supported" in str(x.message).lower()
                ]
                assert len(transaction_warnings) == 0
            await cursor.close()

        run_async(_test())
        conn.close()  # ty: ignore[unused-awaitable]

    def test_async_rollback_no_warning(self):
        """Test ROLLBACK SQL does not warn with AioValkeyLock."""
        lock = AioValkeyLock(name="async_valkey_dbapi_warn3", timeout=30.0)
        conn = rqlite.async_connect(host="localhost", port=4001, lock=lock)

        async def _test():
            cursor = await conn.cursor()
            with warnings.catch_warnings(record=True) as w:
                warnings.simplefilter("always")
                try:
                    await cursor.execute("ROLLBACK")
                except Exception:
                    pass
                transaction_warnings = [
                    x
                    for x in w
                    if "ROLLBACK" in str(x.message) or "not supported" in str(x.message).lower()
                ]
                assert len(transaction_warnings) == 0
            await cursor.close()

        run_async(_test())
        conn.close()  # ty: ignore[unused-awaitable]


# ── Lock Configuration Tests ──────────────────────────────────────────────


@skip_if_no_valkey
class TestAsyncValkeyLockConfig:
    """Test AioValkeyLock configuration on async connections."""

    def test_async_lock_is_same_instance(self):
        """Test that the lock passed to async_connect() is the same instance."""
        lock = AioValkeyLock(name="async_valkey_dbapi_cfg", timeout=30.0)
        conn = rqlite.async_connect(host="localhost", port=4001, lock=lock)

        async def _test():
            assert conn._lock is lock

        run_async(_test())
        conn.close()  # ty: ignore[unused-awaitable]

    def test_async_connection_has_valkey_lock(self):
        """Test that async connection created with AioValkeyLock has the lock."""
        lock = AioValkeyLock(name="async_valkey_dbapi_cfg2", timeout=30.0)
        conn = rqlite.async_connect(host="localhost", port=4001, lock=lock)

        async def _test():
            assert conn._lock is not None
            assert isinstance(conn._lock, AioValkeyLock)

        run_async(_test())
        conn.close()  # ty: ignore[unused-awaitable]
