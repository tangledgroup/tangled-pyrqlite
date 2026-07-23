"""Async DB-API 2.0 tests with Valkey distributed lock (AioValkeyLock).

Tests the rqlite AsyncConnection/AsyncCursor using AioValkeyLock for
distributed locking. Covers connection lifecycle, CRUD, parameterized queries,
fetch methods, cursor attributes, context managers, type handling, and edge
cases.

Prerequisites:
    rqlite running on localhost:4001
    valkey running on localhost:6379
"""

from __future__ import annotations

import asyncio
import warnings

import pytest

import rqlite
from rqlite import AioValkeyLock
from rqlite.exceptions import (
    DatabaseError,
    IntegrityError,
    InterfaceError,
    OperationalError,
    ProgrammingError,
)

# Helpers


def _has_valkey() -> bool:
    """Check if Valkey is reachable."""
    try:
        import valkey

        client = valkey.Redis(
            host="localhost", port=6379, db=0, socket_connect_timeout=1.0
        )
        return bool(client.ping())
    except Exception:
        return False


skip_if_no_valkey = pytest.mark.skipif(
    not _has_valkey(), reason="Valkey not available"
)

TABLE = "dbapi_async_valkey_test"


def _make_lock(name: str = "dbapi_async") -> AioValkeyLock:
    return AioValkeyLock(name=f"test_{name}", timeout=30.0)


async def _setup_table(cursor: rqlite.AsyncCursor) -> None:
    await cursor.execute(f"DROP TABLE IF EXISTS {TABLE}")
    await cursor.execute(f"""
        CREATE TABLE {TABLE} (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            email TEXT UNIQUE,
            age INTEGER,
            score REAL,
            active INTEGER DEFAULT 1,
            data BLOB,
            created_at TEXT
        )
    """)
    await cursor.connection.commit()


async def _teardown_table(cursor: rqlite.AsyncCursor) -> None:
    await cursor.execute(f"DROP TABLE IF EXISTS {TABLE}")
    await cursor.connection.commit()


# Connection Tests


@skip_if_no_valkey
class TestAsyncDBAPI_Connection:
    """Async DB-API connection tests with AioValkeyLock."""

    def test_connect_with_valkey_lock(self):
        async def _run():
            lock = _make_lock("conn")
            conn = rqlite.async_connect(host="localhost", port=4001, lock=lock)
            assert conn.host == "localhost"
            assert conn.port == 4001
            assert not conn._closed
            assert conn._lock is lock
            await conn.close()

        asyncio.run(_run())

    def test_connect_default_values(self):
        async def _run():
            lock = _make_lock("conn_default")
            conn = rqlite.async_connect(lock=lock)
            assert conn.host == "localhost"
            assert conn.port == 4001
            assert conn.timeout == 30.0
            await conn.close()

        asyncio.run(_run())

    def test_connect_custom_port(self):
        async def _run():
            lock = _make_lock("conn_port")
            conn = rqlite.async_connect(host="localhost", port=4002, lock=lock)
            assert conn.port == 4002
            await conn.close()

        asyncio.run(_run())

    def test_connect_with_auth(self):
        async def _run():
            lock = _make_lock("conn_auth")
            conn = rqlite.async_connect(
                host="localhost",
                port=4001,
                username="admin",
                password="secret",
                lock=lock,
            )
            assert conn.username == "admin"
            assert conn.password == "secret"
            await conn.close()

        asyncio.run(_run())

    def test_cursor_creation(self):
        async def _run():
            lock = _make_lock("cursor")
            conn = rqlite.async_connect(lock=lock)
            cursor = await conn.cursor()
            assert cursor is not None
            assert cursor.connection is conn
            await cursor.close()
            await conn.close()

        asyncio.run(_run())

    def test_multiple_cursors(self):
        async def _run():
            lock = _make_lock("multi_cursor")
            conn = rqlite.async_connect(lock=lock)
            c1 = await conn.cursor()
            c2 = await conn.cursor()
            assert c1 is not c2
            assert c1.connection is c2.connection
            await c1.close()
            await c2.close()
            await conn.close()

        asyncio.run(_run())

    def test_close_connection(self):
        async def _run():
            lock = _make_lock("close")
            conn = rqlite.async_connect(lock=lock)
            await conn.close()
            assert conn._closed
            with pytest.raises(Exception) as exc_info:
                await conn.cursor()
            assert "closed" in str(exc_info.value).lower()

        asyncio.run(_run())

    def test_double_close(self):
        async def _run():
            lock = _make_lock("dbl_close")
            conn = rqlite.async_connect(lock=lock)
            await conn.close()
            await conn.close()  # Should not raise
            assert conn._closed

        asyncio.run(_run())

    def test_context_manager(self):
        async def _run():
            lock = _make_lock("ctx")
            async with rqlite.async_connect(lock=lock) as conn:
                assert not conn._closed
            assert conn._closed

        asyncio.run(_run())

    def test_context_manager_rollback_on_error(self):
        async def _run():
            lock = _make_lock("ctx_err")
            with pytest.raises(ValueError):
                async with rqlite.async_connect(lock=lock) as conn:
                    raise ValueError("test error")
            assert conn._closed

        asyncio.run(_run())

    def test_lock_suppresses_warning(self):
        async def _run():
            lock = _make_lock("no_warn")
            with warnings.catch_warnings(record=True) as w:
                warnings.simplefilter("always")
                conn = rqlite.async_connect(lock=lock)
                await conn.close()
            tx_warnings = [
                x for x in w if "BEGIN/COMMIT/ROLLBACK" in str(x.message)
            ]
            assert len(tx_warnings) == 0

        asyncio.run(_run())

    def test_no_lock_warns(self):
        async def _run():
            with warnings.catch_warnings(record=True) as w:
                warnings.simplefilter("always")
                conn = rqlite.async_connect(host="localhost", port=4001)
                await conn.close()
            tx_warnings = [
                x for x in w if "BEGIN/COMMIT/ROLLBACK" in str(x.message)
            ]
            assert len(tx_warnings) >= 1

        asyncio.run(_run())


# Cursor Tests


@skip_if_no_valkey
class TestAsyncDBAPI_Cursor:
    """Async DB-API cursor tests with AioValkeyLock."""

    def _conn(self):
        lock = _make_lock("cur")
        return rqlite.async_connect(lock=lock)

    def test_cursor_attributes_initial(self):
        async def _run():
            conn = self._conn()
            cursor = await conn.cursor()
            assert cursor.description is None
            assert cursor.rowcount == -1
            assert cursor.arraysize == 1
            await cursor.close()
            await conn.close()

        asyncio.run(_run())

    def test_set_arraysize(self):
        async def _run():
            conn = self._conn()
            cursor = await conn.cursor()
            cursor.arraysize = 10
            assert cursor.arraysize == 10
            await cursor.close()
            await conn.close()

        asyncio.run(_run())

    def test_cursor_context_manager(self):
        async def _run():
            conn = self._conn()
            cursor = await conn.cursor()
            async with cursor:
                assert cursor.connection is conn
            await conn.close()

        asyncio.run(_run())

    def test_cursor_double_close(self):
        async def _run():
            conn = self._conn()
            cursor = await conn.cursor()
            await cursor.close()
            await cursor.close()  # Should not raise
            await conn.close()

        asyncio.run(_run())

    def test_execute_on_closed_cursor(self):
        async def _run():
            conn = self._conn()
            cursor = await conn.cursor()
            await cursor.close()
            with pytest.raises(InterfaceError):
                await cursor.execute("SELECT 1")
            await conn.close()

        asyncio.run(_run())

    def test_fetch_on_closed_cursor(self):
        async def _run():
            conn = self._conn()
            cursor = await conn.cursor()
            await cursor.close()
            with pytest.raises(InterfaceError):
                cursor.fetchone()
            await conn.close()

        asyncio.run(_run())

    def test_cursor_iterator(self):
        async def _run():
            conn = self._conn()
            cursor = await conn.cursor()
            await _setup_table(cursor)
            await cursor.execute(
                f"INSERT INTO {TABLE} (name, age) VALUES ('Alice', 30)"
            )
            await cursor.execute(
                f"INSERT INTO {TABLE} (name, age) VALUES ('Bob', 25)"
            )
            await cursor.execute(f"SELECT name, age FROM {TABLE} ORDER BY name")
            rows = list(cursor)
            assert len(rows) == 2
            assert rows[0][0] == "Alice"
            assert rows[1][0] == "Bob"
            await _teardown_table(cursor)
            await cursor.close()
            await conn.close()

        asyncio.run(_run())


# CRUD Tests


@skip_if_no_valkey
class TestAsyncDBAPI_CRUD:
    """Async DB-API CRUD operations with AioValkeyLock."""

    def _conn(self):
        lock = _make_lock("crud")
        return rqlite.async_connect(lock=lock)

    def test_create_table(self):
        async def _run():
            conn = self._conn()
            cursor = await conn.cursor()
            await _setup_table(cursor)
            await cursor.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
                (TABLE,),
            )
            assert cursor.fetchone() is not None
            await _teardown_table(cursor)
            await cursor.close()
            await conn.close()

        asyncio.run(_run())

    def test_insert_positional(self):
        async def _run():
            conn = self._conn()
            cursor = await conn.cursor()
            await _setup_table(cursor)
            await cursor.execute(
                f"INSERT INTO {TABLE} (name, age, score) VALUES (?, ?, ?)",
                ("Alice", 30, 95.5),
            )
            assert cursor.rowcount == 1
            await cursor.execute(f"SELECT name, age, score FROM {TABLE}")
            row = cursor.fetchone()
            assert row == ("Alice", 30, 95.5)
            await _teardown_table(cursor)
            await cursor.close()
            await conn.close()

        asyncio.run(_run())

    def test_insert_named(self):
        async def _run():
            conn = self._conn()
            cursor = await conn.cursor()
            await _setup_table(cursor)
            await cursor.execute(
                f"INSERT INTO {TABLE} (name, age) VALUES (:name, :age)",
                {"name": "Bob", "age": 25},
            )
            await cursor.execute(f"SELECT name, age FROM {TABLE}")
            row = cursor.fetchone()
            assert row == ("Bob", 25)
            await _teardown_table(cursor)
            await cursor.close()
            await conn.close()

        asyncio.run(_run())

    def test_insert_no_params(self):
        async def _run():
            conn = self._conn()
            cursor = await conn.cursor()
            await _setup_table(cursor)
            await cursor.execute(f"INSERT INTO {TABLE} (name) VALUES ('Charlie')")
            await cursor.execute(f"SELECT name FROM {TABLE}")
            assert cursor.fetchone() == ("Charlie",)
            await _teardown_table(cursor)
            await cursor.close()
            await conn.close()

        asyncio.run(_run())

    def test_insert_many(self):
        async def _run():
            conn = self._conn()
            cursor = await conn.cursor()
            await _setup_table(cursor)
            rows = [("Dave", 40), ("Eve", 35), ("Frank", 50)]
            await cursor.executemany(
                f"INSERT INTO {TABLE} (name, age) VALUES (?, ?)",
                rows,
            )
            await cursor.execute(f"SELECT COUNT(*) FROM {TABLE}")
            assert cursor.fetchone() == (3,)
            await _teardown_table(cursor)
            await cursor.close()
            await conn.close()

        asyncio.run(_run())

    def test_select_all(self):
        async def _run():
            conn = self._conn()
            cursor = await conn.cursor()
            await _setup_table(cursor)
            for name in ["A", "B", "C"]:
                await cursor.execute(
                    f"INSERT INTO {TABLE} (name) VALUES (?)", (name,)
                )
            await cursor.execute(f"SELECT name FROM {TABLE} ORDER BY name")
            rows = cursor.fetchall()
            assert [r[0] for r in rows] == ["A", "B", "C"]
            await _teardown_table(cursor)
            await cursor.close()
            await conn.close()

        asyncio.run(_run())

    def test_select_filtered(self):
        async def _run():
            conn = self._conn()
            cursor = await conn.cursor()
            await _setup_table(cursor)
            await cursor.execute(
                f"INSERT INTO {TABLE} (name, age) VALUES (?, ?)", ("Young", 20)
            )
            await cursor.execute(
                f"INSERT INTO {TABLE} (name, age) VALUES (?, ?)", ("Old", 60)
            )
            await cursor.execute(
                f"SELECT name FROM {TABLE} WHERE age > ? ORDER BY name", (30,)
            )
            rows = cursor.fetchall()
            assert len(rows) == 1
            assert rows[0][0] == "Old"
            await _teardown_table(cursor)
            await cursor.close()
            await conn.close()

        asyncio.run(_run())

    def test_update(self):
        async def _run():
            conn = self._conn()
            cursor = await conn.cursor()
            await _setup_table(cursor)
            await cursor.execute(
                f"INSERT INTO {TABLE} (name, age) VALUES (?, ?)", ("Alice", 30)
            )
            await cursor.execute(
                f"UPDATE {TABLE} SET age = ? WHERE name = ?", (31, "Alice")
            )
            assert cursor.rowcount == 1
            await cursor.execute(
                f"SELECT age FROM {TABLE} WHERE name = ?", ("Alice",)
            )
            assert cursor.fetchone() == (31,)
            await _teardown_table(cursor)
            await cursor.close()
            await conn.close()

        asyncio.run(_run())

    def test_delete(self):
        async def _run():
            conn = self._conn()
            cursor = await conn.cursor()
            await _setup_table(cursor)
            await cursor.execute(
                f"INSERT INTO {TABLE} (name) VALUES (?)", ("ToDelete",)
            )
            await cursor.execute(
                f"DELETE FROM {TABLE} WHERE name = ?", ("ToDelete",)
            )
            assert cursor.rowcount == 1
            await cursor.execute(f"SELECT COUNT(*) FROM {TABLE}")
            assert cursor.fetchone() == (0,)
            await _teardown_table(cursor)
            await cursor.close()
            await conn.close()

        asyncio.run(_run())


# Fetch Tests


@skip_if_no_valkey
class TestAsyncDBAPI_Fetch:
    """Async DB-API fetch method tests with AioValkeyLock."""

    def _conn(self):
        lock = _make_lock("fetch")
        return rqlite.async_connect(lock=lock)

    def test_fetchone(self):
        async def _run():
            conn = self._conn()
            cursor = await conn.cursor()
            await _setup_table(cursor)
            await cursor.execute(
                f"INSERT INTO {TABLE} (name) VALUES ('First')"
            )
            await cursor.execute(
                f"INSERT INTO {TABLE} (name) VALUES ('Second')"
            )
            await cursor.execute(f"SELECT name FROM {TABLE} ORDER BY name")
            assert cursor.fetchone() == ("First",)
            assert cursor.fetchone() == ("Second",)
            assert cursor.fetchone() is None
            await _teardown_table(cursor)
            await cursor.close()
            await conn.close()

        asyncio.run(_run())

    def test_fetchmany(self):
        async def _run():
            conn = self._conn()
            cursor = await conn.cursor()
            await _setup_table(cursor)
            for i in range(5):
                await cursor.execute(
                    f"INSERT INTO {TABLE} (name) VALUES (?)", (f"R{i}",)
                )
            await cursor.execute(f"SELECT name FROM {TABLE} ORDER BY name")
            batch = cursor.fetchmany(2)
            assert len(batch) == 2
            assert batch[0][0] == "R0"
            batch2 = cursor.fetchmany(2)
            assert len(batch2) == 2
            remaining = cursor.fetchall()
            assert len(remaining) == 1
            await _teardown_table(cursor)
            await cursor.close()
            await conn.close()

        asyncio.run(_run())

    def test_fetchmany_default_arraysize(self):
        async def _run():
            conn = self._conn()
            cursor = await conn.cursor()
            cursor.arraysize = 3
            await _setup_table(cursor)
            for i in range(5):
                await cursor.execute(
                    f"INSERT INTO {TABLE} (name) VALUES (?)", (f"X{i}",)
                )
            await cursor.execute(f"SELECT name FROM {TABLE} ORDER BY name")
            batch = cursor.fetchmany()
            assert len(batch) == 3
            await _teardown_table(cursor)
            await cursor.close()
            await conn.close()

        asyncio.run(_run())

    def test_fetchall(self):
        async def _run():
            conn = self._conn()
            cursor = await conn.cursor()
            await _setup_table(cursor)
            for name in ["A", "B", "C"]:
                await cursor.execute(
                    f"INSERT INTO {TABLE} (name) VALUES (?)", (name,)
                )
            await cursor.execute(f"SELECT name FROM {TABLE} ORDER BY name")
            rows = cursor.fetchall()
            assert len(rows) == 3
            assert [r[0] for r in rows] == ["A", "B", "C"]
            assert cursor.fetchall() == []
            await _teardown_table(cursor)
            await cursor.close()
            await conn.close()

        asyncio.run(_run())

    def test_fetch_empty_result(self):
        async def _run():
            conn = self._conn()
            cursor = await conn.cursor()
            await _setup_table(cursor)
            await cursor.execute(f"SELECT * FROM {TABLE}")
            assert cursor.fetchone() is None
            assert cursor.fetchall() == []
            await _teardown_table(cursor)
            await cursor.close()
            await conn.close()

        asyncio.run(_run())


# Cursor Attributes Tests


@skip_if_no_valkey
class TestAsyncDBAPI_CursorAttrs:
    """Async DB-API cursor attribute tests with AioValkeyLock."""

    def _conn(self):
        lock = _make_lock("attrs")
        return rqlite.async_connect(lock=lock)

    def test_description_after_select(self):
        async def _run():
            conn = self._conn()
            cursor = await conn.cursor()
            await _setup_table(cursor)
            await cursor.execute(f"SELECT name, age FROM {TABLE}")
            assert cursor.description is not None
            assert len(cursor.description) == 2
            assert cursor.description[0][0] == "name"
            assert cursor.description[1][0] == "age"
            await _teardown_table(cursor)
            await cursor.close()
            await conn.close()

        asyncio.run(_run())

    def test_description_none_after_write(self):
        async def _run():
            conn = self._conn()
            cursor = await conn.cursor()
            await _setup_table(cursor)
            await cursor.execute(f"INSERT INTO {TABLE} (name) VALUES ('Test')")
            assert cursor.description is None
            await _teardown_table(cursor)
            await cursor.close()
            await conn.close()

        asyncio.run(_run())

    def test_rowcount_after_write(self):
        async def _run():
            conn = self._conn()
            cursor = await conn.cursor()
            await _setup_table(cursor)
            await cursor.execute(f"INSERT INTO {TABLE} (name) VALUES ('Test')")
            assert cursor.rowcount == 1
            await cursor.execute(f"SELECT * FROM {TABLE}")
            assert cursor.rowcount == -1
            await _teardown_table(cursor)
            await cursor.close()
            await conn.close()

        asyncio.run(_run())

    def test_lastrowid(self):
        async def _run():
            conn = self._conn()
            cursor = await conn.cursor()
            await _setup_table(cursor)
            await cursor.execute(f"INSERT INTO {TABLE} (name) VALUES ('Test')")
            assert cursor.lastrowid is not None
            assert isinstance(cursor.lastrowid, int)
            await _teardown_table(cursor)
            await cursor.close()
            await conn.close()

        asyncio.run(_run())


# Type Handling Tests


@skip_if_no_valkey
class TestAsyncDBAPI_Types:
    """Async DB-API type handling tests with AioValkeyLock."""

    def _conn(self):
        lock = _make_lock("types")
        return rqlite.async_connect(lock=lock)

    def test_integer(self):
        async def _run():
            conn = self._conn()
            cursor = await conn.cursor()
            await _setup_table(cursor)
            await cursor.execute(
                f"INSERT INTO {TABLE} (name, age) VALUES (?, ?)", ("Int", 42)
            )
            await cursor.execute(f"SELECT age FROM {TABLE}")
            assert cursor.fetchone() == (42,)
            await _teardown_table(cursor)
            await cursor.close()
            await conn.close()

        asyncio.run(_run())

    def test_text(self):
        async def _run():
            conn = self._conn()
            cursor = await conn.cursor()
            await _setup_table(cursor)
            await cursor.execute(
                f"INSERT INTO {TABLE} (name, email) VALUES (?, ?)",
                ("Text", "test@example.com"),
            )
            await cursor.execute(f"SELECT email FROM {TABLE}")
            assert cursor.fetchone() == ("test@example.com",)
            await _teardown_table(cursor)
            await cursor.close()
            await conn.close()

        asyncio.run(_run())

    def test_real(self):
        async def _run():
            conn = self._conn()
            cursor = await conn.cursor()
            await _setup_table(cursor)
            await cursor.execute(
                f"INSERT INTO {TABLE} (name, score) VALUES (?, ?)",
                ("Real", 3.14159),
            )
            await cursor.execute(f"SELECT score FROM {TABLE}")
            row = cursor.fetchone()
            assert row is not None
            assert abs(row[0] - 3.14159) < 0.0001
            await _teardown_table(cursor)
            await cursor.close()
            await conn.close()

        asyncio.run(_run())

    def test_blob(self):
        async def _run():
            conn = self._conn()
            cursor = await conn.cursor()
            await _setup_table(cursor)
            data = b"\x00\x01\x02\xff\xfe"
            await cursor.execute(
                f"INSERT INTO {TABLE} (name, data) VALUES (?, ?)", ("Blob", data)
            )
            await cursor.execute(f"SELECT data FROM {TABLE}")
            row = cursor.fetchone()
            assert row is not None
            assert row[0] == data
            await _teardown_table(cursor)
            await cursor.close()
            await conn.close()

        asyncio.run(_run())

    def test_null(self):
        async def _run():
            conn = self._conn()
            cursor = await conn.cursor()
            await _setup_table(cursor)
            await cursor.execute(
                f"INSERT INTO {TABLE} (name, age) VALUES (?, ?)", ("Null", None)
            )
            await cursor.execute(f"SELECT age FROM {TABLE}")
            assert cursor.fetchone() == (None,)
            await _teardown_table(cursor)
            await cursor.close()
            await conn.close()

        asyncio.run(_run())

    def test_binary_type(self):
        async def _run():
            conn = self._conn()
            cursor = await conn.cursor()
            await _setup_table(cursor)
            binary_data = rqlite.Binary(b"\xde\xad\xbe\xef")
            await cursor.execute(
                f"INSERT INTO {TABLE} (name, data) VALUES (?, ?)",
                ("Bin", binary_data),
            )
            await cursor.execute(f"SELECT data FROM {TABLE}")
            row = cursor.fetchone()
            assert row is not None
            assert row[0] == b"\xde\xad\xbe\xef"
            await _teardown_table(cursor)
            await cursor.close()
            await conn.close()

        asyncio.run(_run())


# Error Handling Tests


@skip_if_no_valkey
class TestAsyncDBAPI_Errors:
    """Async DB-API error handling tests with AioValkeyLock."""

    def _conn(self):
        lock = _make_lock("err")
        return rqlite.async_connect(lock=lock)

    def test_invalid_sql(self):
        async def _run():
            conn = self._conn()
            cursor = await conn.cursor()
            with pytest.raises((ProgrammingError, OperationalError, DatabaseError)):
                await cursor.execute("INVALID SQL STATEMENT")
            await cursor.close()
            await conn.close()

        asyncio.run(_run())

    def test_unique_constraint(self):
        async def _run():
            conn = self._conn()
            cursor = await conn.cursor()
            await _setup_table(cursor)
            await cursor.execute(
                f"INSERT INTO {TABLE} (name, email) VALUES (?, ?)",
                ("U1", "a@b.c"),
            )
            with pytest.raises(IntegrityError):
                await cursor.execute(
                    f"INSERT INTO {TABLE} (name, email) VALUES (?, ?)",
                    ("U1", "a@b.c"),
                )
            await _teardown_table(cursor)
            await cursor.close()
            await conn.close()

        asyncio.run(_run())

    def test_not_null_constraint(self):
        async def _run():
            conn = self._conn()
            cursor = await conn.cursor()
            await _setup_table(cursor)
            with pytest.raises(IntegrityError):
                await cursor.execute(f"INSERT INTO {TABLE} (name) VALUES (NULL)")
            await _teardown_table(cursor)
            await cursor.close()
            await conn.close()

        asyncio.run(_run())


# Transaction SQL Warning Tests


@skip_if_no_valkey
class TestAsyncDBAPI_TransactionWarnings:
    """Async DB-API transaction SQL warning tests with AioValkeyLock."""

    def _conn(self):
        lock = _make_lock("tx_warn")
        return rqlite.async_connect(lock=lock)

    def test_begin_no_warning_with_lock(self):
        async def _run():
            conn = self._conn()
            cursor = await conn.cursor()
            await _setup_table(cursor)
            with warnings.catch_warnings(record=True) as w:
                warnings.simplefilter("always")
                await cursor.execute("BEGIN")
            assert len([x for x in w if "BEGIN" in str(x.message)]) == 0
            await _teardown_table(cursor)
            await cursor.close()
            await conn.close()

        asyncio.run(_run())

    def test_commit_no_warning_with_lock(self):
        async def _run():
            conn = self._conn()
            cursor = await conn.cursor()
            await _setup_table(cursor)
            with warnings.catch_warnings(record=True) as w:
                warnings.simplefilter("always")
                await cursor.execute("COMMIT")
            assert len([x for x in w if "COMMIT" in str(x.message)]) == 0
            await _teardown_table(cursor)
            await cursor.close()
            await conn.close()

        asyncio.run(_run())

    def test_rollback_no_warning_with_lock(self):
        async def _run():
            conn = self._conn()
            cursor = await conn.cursor()
            await _setup_table(cursor)
            with warnings.catch_warnings(record=True) as w:
                warnings.simplefilter("always")
                await cursor.execute("ROLLBACK")
            assert len([x for x in w if "ROLLBACK" in str(x.message)]) == 0
            await _teardown_table(cursor)
            await cursor.close()
            await conn.close()

        asyncio.run(_run())


# Read Consistency Tests


@skip_if_no_valkey
class TestAsyncDBAPI_ReadConsistency:
    """Async DB-API read consistency tests with AioValkeyLock."""

    def test_default_linearizable(self):
        async def _run():
            lock = _make_lock("rc_default")
            conn = rqlite.async_connect(lock=lock)
            assert conn.read_consistency == rqlite.ReadConsistency.LINEARIZABLE
            await conn.close()

        asyncio.run(_run())

    def test_weak_consistency(self):
        async def _run():
            lock = _make_lock("rc_weak")
            conn = rqlite.async_connect(
                read_consistency=rqlite.ReadConsistency.WEAK, lock=lock
            )
            assert conn.read_consistency == rqlite.ReadConsistency.WEAK
            await conn.close()

        asyncio.run(_run())

    def test_none_consistency(self):
        async def _run():
            lock = _make_lock("rc_none")
            conn = rqlite.async_connect(
                read_consistency=rqlite.ReadConsistency.NONE, lock=lock
            )
            assert conn.read_consistency == rqlite.ReadConsistency.NONE
            await conn.close()

        asyncio.run(_run())

    def test_strong_consistency(self):
        async def _run():
            lock = _make_lock("rc_strong")
            conn = rqlite.async_connect(
                read_consistency=rqlite.ReadConsistency.STRONG, lock=lock
            )
            assert conn.read_consistency == rqlite.ReadConsistency.STRONG
            await conn.close()

        asyncio.run(_run())


# Complex Workflow Tests


@skip_if_no_valkey
class TestAsyncDBAPI_Workflow:
    """Async DB-API complex workflow tests with AioValkeyLock."""

    def test_full_crud_lifecycle(self):
        async def _run():
            lock = _make_lock("workflow")
            conn = rqlite.async_connect(lock=lock)
            cursor = await conn.cursor()

            await _setup_table(cursor)

            items = [
                ("Widget", 10, 9.99, 1),
                ("Gadget", 25, 19.99, 1),
                ("Doohickey", 5, 4.99, 0),
            ]
            await cursor.executemany(
                f"INSERT INTO {TABLE} (name, age, score, active) VALUES (?, ?, ?, ?)",
                items,
            )

            await cursor.execute(f"SELECT COUNT(*) FROM {TABLE}")
            assert cursor.fetchone() == (3,)

            await cursor.execute(
                f"SELECT name, score FROM {TABLE} "
                f"WHERE active = ? ORDER BY score DESC",
                (1,),
            )
            rows = cursor.fetchall()
            assert len(rows) == 2
            assert rows[0][0] == "Gadget"

            await cursor.execute(
                f"UPDATE {TABLE} SET score = ? WHERE name = ?",
                (29.99, "Widget"),
            )
            assert cursor.rowcount == 1

            await cursor.execute(f"DELETE FROM {TABLE} WHERE active = ?", (0,))
            assert cursor.rowcount == 1

            await cursor.execute(f"SELECT COUNT(*) FROM {TABLE}")
            assert cursor.fetchone() == (2,)

            await _teardown_table(cursor)
            await cursor.close()
            await conn.close()

        asyncio.run(_run())

    def test_large_insert(self):
        async def _run():
            lock = _make_lock("large")
            conn = rqlite.async_connect(lock=lock)
            cursor = await conn.cursor()
            await _setup_table(cursor)

            count = 100
            for i in range(count):
                await cursor.execute(
                    f"INSERT INTO {TABLE} (name, age, score) VALUES (?, ?, ?)",
                    (f"User{i}", i, i * 0.1),
                )

            await cursor.execute(f"SELECT COUNT(*) FROM {TABLE}")
            assert cursor.fetchone() == (count,)

            await cursor.execute(f"SELECT AVG(age) FROM {TABLE}")
            row = cursor.fetchone()
            assert row is not None
            avg = row[0]
            assert abs(avg - 49.5) < 0.01

            await _teardown_table(cursor)
            await cursor.close()
            await conn.close()

        asyncio.run(_run())

    def test_multiple_operations_same_cursor(self):
        async def _run():
            lock = _make_lock("multi_op")
            conn = rqlite.async_connect(lock=lock)
            cursor = await conn.cursor()
            await _setup_table(cursor)

            await cursor.execute(f"INSERT INTO {TABLE} (name) VALUES ('A')")
            await cursor.execute(f"INSERT INTO {TABLE} (name) VALUES ('B')")
            await cursor.execute(f"SELECT name FROM {TABLE} ORDER BY name")
            assert len(cursor.fetchall()) == 2
            await cursor.execute(
                f"UPDATE {TABLE} SET name = ? WHERE name = ?", ("X", "A")
            )
            await cursor.execute(f"SELECT name FROM {TABLE} ORDER BY name")
            rows = cursor.fetchall()
            assert [r[0] for r in rows] == ["B", "X"]

            await _teardown_table(cursor)
            await cursor.close()
            await conn.close()

        asyncio.run(_run())
