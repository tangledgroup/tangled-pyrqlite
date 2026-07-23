"""Async DB-API 2.0 tests with Redis cluster lock (AioRedisLock).

Tests the rqlite AsyncConnection/AsyncCursor using AioRedisLock with
cluster=True for distributed locking against a Redis cluster.

Prerequisites:
    rqlite running on localhost:4001
    redis cluster running (seed node on localhost:6379)
"""

from __future__ import annotations

import asyncio
import warnings

import pytest

import rqlite
from rqlite import AioRedisLock
from rqlite.exceptions import (
    DatabaseError,
    IntegrityError,
    OperationalError,
    ProgrammingError,
)

# Helpers


def _has_redis_cluster() -> bool:
    """Check if a Redis cluster is reachable."""
    try:
        from rqlite.redis_cluster import is_cluster_mode

        return is_cluster_mode("localhost", 6379)
    except Exception:
        return False


skip_if_no_redis_cluster = pytest.mark.skipif(
    not _has_redis_cluster(), reason="Redis cluster not available"
)

TABLE = "dbapi_async_redis_cluster_test"


def _make_lock(name: str = "dbapi_async_cluster") -> AioRedisLock:
    return AioRedisLock(name=f"test_{name}", timeout=30.0, cluster=True)


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


@skip_if_no_redis_cluster
class TestAsyncDBAPI_RedisCluster_Connection:
    """Async DB-API connection tests with AioRedisLock (cluster=True)."""

    def test_connect_with_cluster_lock(self):
        async def _run():
            lock = _make_lock("conn")
            conn = rqlite.async_connect(host="localhost", port=4001, lock=lock)
            assert conn.host == "localhost"
            assert conn.port == 4001
            assert not conn._closed
            assert conn._lock is lock
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

    def test_context_manager(self):
        async def _run():
            lock = _make_lock("ctx")
            async with rqlite.async_connect(lock=lock) as conn:
                assert not conn._closed
            assert conn._closed

        asyncio.run(_run())

    def test_lock_suppresses_warning(self):
        async def _run():
            lock = _make_lock("no_warn")
            with warnings.catch_warnings(record=True) as w:
                warnings.simplefilter("always")
                conn = rqlite.async_connect(lock=lock)
                await conn.close()
            tx_warnings = [x for x in w if "BEGIN/COMMIT/ROLLBACK" in str(x.message)]
            assert len(tx_warnings) == 0

        asyncio.run(_run())


# CRUD Tests


@skip_if_no_redis_cluster
class TestAsyncDBAPI_RedisCluster_CRUD:
    """Async DB-API CRUD operations with AioRedisLock (cluster=True)."""

    def _conn(self):
        lock = _make_lock("crud")
        return rqlite.async_connect(lock=lock)

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


@skip_if_no_redis_cluster
class TestAsyncDBAPI_RedisCluster_Fetch:
    """Async DB-API fetch method tests with AioRedisLock (cluster=True)."""

    def _conn(self):
        lock = _make_lock("fetch")
        return rqlite.async_connect(lock=lock)

    def test_fetchone(self):
        async def _run():
            conn = self._conn()
            cursor = await conn.cursor()
            await _setup_table(cursor)
            await cursor.execute(f"INSERT INTO {TABLE} (name) VALUES ('First')")
            await cursor.execute(f"INSERT INTO {TABLE} (name) VALUES ('Second')")
            await cursor.execute(f"SELECT name FROM {TABLE} ORDER BY name")
            assert cursor.fetchone() == ("First",)
            assert cursor.fetchone() == ("Second",)
            assert cursor.fetchone() is None
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


# Type Handling Tests


@skip_if_no_redis_cluster
class TestAsyncDBAPI_RedisCluster_Types:
    """Async DB-API type handling tests with AioRedisLock (cluster=True)."""

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


# Error Handling Tests


@skip_if_no_redis_cluster
class TestAsyncDBAPI_RedisCluster_Errors:
    """Async DB-API error handling tests with AioRedisLock (cluster=True)."""

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


# Complex Workflow Tests


@skip_if_no_redis_cluster
class TestAsyncDBAPI_RedisCluster_Workflow:
    """Async DB-API complex workflow tests with AioRedisLock (cluster=True)."""

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
            assert abs(row[0] - 49.5) < 0.01

            await _teardown_table(cursor)
            await cursor.close()
            await conn.close()

        asyncio.run(_run())
