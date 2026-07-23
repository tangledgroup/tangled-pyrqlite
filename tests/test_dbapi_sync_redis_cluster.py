"""Sync DB-API 2.0 tests with Redis cluster lock.

Tests the rqlite Connection/Cursor (sync) using RedisLock with cluster=True
for distributed locking against a Redis cluster.

Prerequisites:
    rqlite running on localhost:4001
    redis cluster running (seed node on localhost:6379)
"""

from __future__ import annotations

import warnings
from datetime import datetime, timezone

import pytest

import rqlite
from rqlite import RedisLock
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

TABLE = "dbapi_sync_redis_cluster_test"


def _make_lock(name: str = "dbapi_sync_cluster") -> RedisLock:
    return RedisLock(name=f"test_{name}", timeout=30.0, cluster=True)


def _setup_table(cursor: rqlite.Cursor) -> None:
    cursor.execute(f"DROP TABLE IF EXISTS {TABLE}")
    cursor.execute(f"""
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
    cursor.connection.commit()


def _teardown_table(cursor: rqlite.Cursor) -> None:
    cursor.execute(f"DROP TABLE IF EXISTS {TABLE}")
    cursor.connection.commit()


# Connection Tests


@skip_if_no_redis_cluster
class TestSyncDBAPI_RedisCluster_Connection:
    """Sync DB-API connection tests with RedisLock (cluster=True)."""

    def test_connect_with_cluster_lock(self):
        lock = _make_lock("conn")
        conn = rqlite.connect(host="localhost", port=4001, lock=lock)
        assert conn.host == "localhost"
        assert conn.port == 4001
        assert not conn._closed
        assert conn._lock is lock
        conn.close()

    def test_cursor_creation(self):
        lock = _make_lock("cursor")
        conn = rqlite.connect(lock=lock)
        cursor = conn.cursor()
        assert cursor is not None
        assert cursor.connection is conn
        cursor.close()
        conn.close()

    def test_context_manager(self):
        lock = _make_lock("ctx")
        with rqlite.connect(lock=lock) as conn:
            assert not conn._closed
        assert conn._closed

    def test_lock_suppresses_warning(self):
        lock = _make_lock("no_warn")
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            conn = rqlite.connect(lock=lock)
            conn.close()
        tx_warnings = [x for x in w if "BEGIN/COMMIT/ROLLBACK" in str(x.message)]
        assert len(tx_warnings) == 0


# CRUD Tests


@skip_if_no_redis_cluster
class TestSyncDBAPI_RedisCluster_CRUD:
    """Sync DB-API CRUD operations with RedisLock (cluster=True)."""

    def _conn(self):
        lock = _make_lock("crud")
        return rqlite.connect(lock=lock)

    def test_insert_positional(self):
        conn = self._conn()
        cursor = conn.cursor()
        _setup_table(cursor)
        cursor.execute(
            f"INSERT INTO {TABLE} (name, age, score) VALUES (?, ?, ?)",
            ("Alice", 30, 95.5),
        )
        assert cursor.rowcount == 1
        cursor.execute(f"SELECT name, age, score FROM {TABLE}")
        row = cursor.fetchone()
        assert row == ("Alice", 30, 95.5)
        _teardown_table(cursor)
        cursor.close()
        conn.close()

    def test_insert_many(self):
        conn = self._conn()
        cursor = conn.cursor()
        _setup_table(cursor)
        rows = [("Dave", 40), ("Eve", 35), ("Frank", 50)]
        cursor.executemany(
            f"INSERT INTO {TABLE} (name, age) VALUES (?, ?)",
            rows,
        )
        cursor.execute(f"SELECT COUNT(*) FROM {TABLE}")
        assert cursor.fetchone() == (3,)
        _teardown_table(cursor)
        cursor.close()
        conn.close()

    def test_select_filtered(self):
        conn = self._conn()
        cursor = conn.cursor()
        _setup_table(cursor)
        cursor.execute(f"INSERT INTO {TABLE} (name, age) VALUES (?, ?)", ("Young", 20))
        cursor.execute(f"INSERT INTO {TABLE} (name, age) VALUES (?, ?)", ("Old", 60))
        cursor.execute(
            f"SELECT name FROM {TABLE} WHERE age > ? ORDER BY name", (30,)
        )
        rows = cursor.fetchall()
        assert len(rows) == 1
        assert rows[0][0] == "Old"
        _teardown_table(cursor)
        cursor.close()
        conn.close()

    def test_update(self):
        conn = self._conn()
        cursor = conn.cursor()
        _setup_table(cursor)
        cursor.execute(f"INSERT INTO {TABLE} (name, age) VALUES (?, ?)", ("Alice", 30))
        cursor.execute(f"UPDATE {TABLE} SET age = ? WHERE name = ?", (31, "Alice"))
        assert cursor.rowcount == 1
        cursor.execute(f"SELECT age FROM {TABLE} WHERE name = ?", ("Alice",))
        assert cursor.fetchone() == (31,)
        _teardown_table(cursor)
        cursor.close()
        conn.close()

    def test_delete(self):
        conn = self._conn()
        cursor = conn.cursor()
        _setup_table(cursor)
        cursor.execute(f"INSERT INTO {TABLE} (name) VALUES (?)", ("ToDelete",))
        cursor.execute(f"DELETE FROM {TABLE} WHERE name = ?", ("ToDelete",))
        assert cursor.rowcount == 1
        cursor.execute(f"SELECT COUNT(*) FROM {TABLE}")
        assert cursor.fetchone() == (0,)
        _teardown_table(cursor)
        cursor.close()
        conn.close()


# Fetch Tests


@skip_if_no_redis_cluster
class TestSyncDBAPI_RedisCluster_Fetch:
    """Sync DB-API fetch method tests with RedisLock (cluster=True)."""

    def _conn(self):
        lock = _make_lock("fetch")
        return rqlite.connect(lock=lock)

    def test_fetchone(self):
        conn = self._conn()
        cursor = conn.cursor()
        _setup_table(cursor)
        cursor.execute(f"INSERT INTO {TABLE} (name) VALUES ('First')")
        cursor.execute(f"INSERT INTO {TABLE} (name) VALUES ('Second')")
        cursor.execute(f"SELECT name FROM {TABLE} ORDER BY name")
        assert cursor.fetchone() == ("First",)
        assert cursor.fetchone() == ("Second",)
        assert cursor.fetchone() is None
        _teardown_table(cursor)
        cursor.close()
        conn.close()

    def test_fetchall(self):
        conn = self._conn()
        cursor = conn.cursor()
        _setup_table(cursor)
        for name in ["A", "B", "C"]:
            cursor.execute(f"INSERT INTO {TABLE} (name) VALUES (?)", (name,))
        cursor.execute(f"SELECT name FROM {TABLE} ORDER BY name")
        rows = cursor.fetchall()
        assert len(rows) == 3
        assert [r[0] for r in rows] == ["A", "B", "C"]
        assert cursor.fetchall() == []
        _teardown_table(cursor)
        cursor.close()
        conn.close()


# Type Handling Tests


@skip_if_no_redis_cluster
class TestSyncDBAPI_RedisCluster_Types:
    """Sync DB-API type handling tests with RedisLock (cluster=True)."""

    def _conn(self):
        lock = _make_lock("types")
        return rqlite.connect(lock=lock)

    def test_integer(self):
        conn = self._conn()
        cursor = conn.cursor()
        _setup_table(cursor)
        cursor.execute(f"INSERT INTO {TABLE} (name, age) VALUES (?, ?)", ("Int", 42))
        cursor.execute(f"SELECT age FROM {TABLE}")
        assert cursor.fetchone() == (42,)
        _teardown_table(cursor)
        cursor.close()
        conn.close()

    def test_blob(self):
        conn = self._conn()
        cursor = conn.cursor()
        _setup_table(cursor)
        data = b"\x00\x01\x02\xff\xfe"
        cursor.execute(f"INSERT INTO {TABLE} (name, data) VALUES (?, ?)", ("Blob", data))
        cursor.execute(f"SELECT data FROM {TABLE}")
        row = cursor.fetchone()
        assert row is not None
        assert row[0] == data
        _teardown_table(cursor)
        cursor.close()
        conn.close()

    def test_null(self):
        conn = self._conn()
        cursor = conn.cursor()
        _setup_table(cursor)
        cursor.execute(f"INSERT INTO {TABLE} (name, age) VALUES (?, ?)", ("Null", None))
        cursor.execute(f"SELECT age FROM {TABLE}")
        assert cursor.fetchone() == (None,)
        _teardown_table(cursor)
        cursor.close()
        conn.close()

    def test_datetime_text(self):
        conn = self._conn()
        cursor = conn.cursor()
        _setup_table(cursor)
        now = datetime.now(timezone.utc).isoformat()
        cursor.execute(
            f"INSERT INTO {TABLE} (name, created_at) VALUES (?, ?)", ("Time", now)
        )
        cursor.execute(f"SELECT created_at FROM {TABLE}")
        row = cursor.fetchone()
        assert row is not None
        assert "T" in row[0]
        _teardown_table(cursor)
        cursor.close()
        conn.close()


# Error Handling Tests


@skip_if_no_redis_cluster
class TestSyncDBAPI_RedisCluster_Errors:
    """Sync DB-API error handling tests with RedisLock (cluster=True)."""

    def _conn(self):
        lock = _make_lock("err")
        return rqlite.connect(lock=lock)

    def test_invalid_sql(self):
        conn = self._conn()
        cursor = conn.cursor()
        with pytest.raises((ProgrammingError, OperationalError, DatabaseError)):
            cursor.execute("INVALID SQL STATEMENT")
        cursor.close()
        conn.close()

    def test_unique_constraint(self):
        conn = self._conn()
        cursor = conn.cursor()
        _setup_table(cursor)
        cursor.execute(f"INSERT INTO {TABLE} (name, email) VALUES (?, ?)", ("U1", "a@b.c"))
        with pytest.raises(IntegrityError):
            cursor.execute(
                f"INSERT INTO {TABLE} (name, email) VALUES (?, ?)", ("U2", "a@b.c")
            )
        _teardown_table(cursor)
        cursor.close()
        conn.close()


# Complex Workflow Tests


@skip_if_no_redis_cluster
class TestSyncDBAPI_RedisCluster_Workflow:
    """Sync DB-API complex workflow tests with RedisLock (cluster=True)."""

    def test_full_crud_lifecycle(self):
        lock = _make_lock("workflow")
        conn = rqlite.connect(lock=lock)
        cursor = conn.cursor()

        _setup_table(cursor)

        items = [
            ("Widget", 10, 9.99, 1),
            ("Gadget", 25, 19.99, 1),
            ("Doohickey", 5, 4.99, 0),
        ]
        cursor.executemany(
            f"INSERT INTO {TABLE} (name, age, score, active) VALUES (?, ?, ?, ?)",
            items,
        )

        cursor.execute(f"SELECT COUNT(*) FROM {TABLE}")
        assert cursor.fetchone() == (3,)

        cursor.execute(
            f"SELECT name, score FROM {TABLE} WHERE active = ? ORDER BY score DESC",
            (1,),
        )
        rows = cursor.fetchall()
        assert len(rows) == 2
        assert rows[0][0] == "Gadget"

        cursor.execute(
            f"UPDATE {TABLE} SET score = ? WHERE name = ?", (29.99, "Widget")
        )
        assert cursor.rowcount == 1

        cursor.execute(f"DELETE FROM {TABLE} WHERE active = ?", (0,))
        assert cursor.rowcount == 1

        cursor.execute(f"SELECT COUNT(*) FROM {TABLE}")
        assert cursor.fetchone() == (2,)

        _teardown_table(cursor)
        cursor.close()
        conn.close()

    def test_large_insert(self):
        lock = _make_lock("large")
        conn = rqlite.connect(lock=lock)
        cursor = conn.cursor()
        _setup_table(cursor)

        count = 100
        for i in range(count):
            cursor.execute(
                f"INSERT INTO {TABLE} (name, age, score) VALUES (?, ?, ?)",
                (f"User{i}", i, i * 0.1),
            )

        cursor.execute(f"SELECT COUNT(*) FROM {TABLE}")
        assert cursor.fetchone() == (count,)

        cursor.execute(f"SELECT AVG(age) FROM {TABLE}")
        row = cursor.fetchone()
        assert row is not None
        assert abs(row[0] - 49.5) < 0.01

        _teardown_table(cursor)
        cursor.close()
        conn.close()
