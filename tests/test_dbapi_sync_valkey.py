"""Sync DB-API 2.0 tests with Valkey distributed lock.

Tests the rqlite Connection/Cursor (sync) using ValkeyLock for distributed
locking. Covers connection lifecycle, CRUD, parameterized queries, fetch
methods, cursor attributes, context managers, type handling, and edge cases.

Prerequisites:
    rqlite running on localhost:4001
    valkey running on localhost:6379
"""

from __future__ import annotations

import warnings
from datetime import datetime, timezone

import pytest

import rqlite
from rqlite import ValkeyLock
from rqlite.exceptions import (
    DatabaseError,
    IntegrityError,
    InterfaceError,
    OperationalError,
    ProgrammingError,
)

# ── Helpers ────────────────────────────────────────────────────────────────


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

TABLE = "dbapi_sync_valkey_test"


def _make_lock(name: str = "dbapi_sync") -> ValkeyLock:
    return ValkeyLock(name=f"test_{name}", timeout=30.0)


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


# ── Connection Tests ───────────────────────────────────────────────────────


@skip_if_no_valkey
class TestSyncDBAPI_Connection:
    """Sync DB-API connection tests with ValkeyLock."""

    def test_connect_with_valkey_lock(self):
        lock = _make_lock("conn")
        conn = rqlite.connect(host="localhost", port=4001, lock=lock)
        assert conn.host == "localhost"
        assert conn.port == 4001
        assert not conn._closed
        assert conn._lock is lock
        conn.close()

    def test_connect_default_values(self):
        lock = _make_lock("conn_default")
        conn = rqlite.connect(lock=lock)
        assert conn.host == "localhost"
        assert conn.port == 4001
        assert conn.timeout == 30.0
        conn.close()

    def test_connect_custom_port(self):
        lock = _make_lock("conn_port")
        conn = rqlite.connect(host="localhost", port=4002, lock=lock)
        assert conn.port == 4002
        conn.close()

    def test_connect_with_auth(self):
        lock = _make_lock("conn_auth")
        conn = rqlite.connect(
            host="localhost", port=4001, username="admin", password="secret", lock=lock
        )
        assert conn.username == "admin"
        assert conn.password == "secret"
        conn.close()

    def test_cursor_creation(self):
        lock = _make_lock("cursor")
        conn = rqlite.connect(lock=lock)
        cursor = conn.cursor()
        assert cursor is not None
        assert cursor.connection is conn
        cursor.close()
        conn.close()

    def test_multiple_cursors(self):
        lock = _make_lock("multi_cursor")
        conn = rqlite.connect(lock=lock)
        c1 = conn.cursor()
        c2 = conn.cursor()
        assert c1 is not c2
        assert c1.connection is c2.connection
        c1.close()
        c2.close()
        conn.close()

    def test_close_connection(self):
        lock = _make_lock("close")
        conn = rqlite.connect(lock=lock)
        conn.close()
        assert conn._closed
        with pytest.raises(Exception) as exc_info:
            conn.cursor()
        assert "closed" in str(exc_info.value).lower()

    def test_double_close(self):
        lock = _make_lock("dbl_close")
        conn = rqlite.connect(lock=lock)
        conn.close()
        conn.close()  # Should not raise
        assert conn._closed

    def test_context_manager(self):
        lock = _make_lock("ctx")
        with rqlite.connect(lock=lock) as conn:
            assert not conn._closed
        assert conn._closed

    def test_context_manager_rollback_on_error(self):
        lock = _make_lock("ctx_err")
        with pytest.raises(ValueError):
            with rqlite.connect(lock=lock) as conn:
                raise ValueError("test error")
        assert conn._closed

    def test_lock_suppresses_warning(self):
        lock = _make_lock("no_warn")
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            conn = rqlite.connect(lock=lock)
            conn.close()
        tx_warnings = [
            x for x in w if "BEGIN/COMMIT/ROLLBACK" in str(x.message)
        ]
        assert len(tx_warnings) == 0

    def test_no_lock_warns(self):
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            conn = rqlite.connect(host="localhost", port=4001)
            conn.close()
        tx_warnings = [
            x for x in w if "BEGIN/COMMIT/ROLLBACK" in str(x.message)
        ]
        assert len(tx_warnings) >= 1


# ── Cursor Tests ───────────────────────────────────────────────────────────


@skip_if_no_valkey
class TestSyncDBAPI_Cursor:
    """Sync DB-API cursor tests with ValkeyLock."""

    def _conn(self):
        lock = _make_lock("cur")
        return rqlite.connect(lock=lock)

    def test_cursor_attributes_initial(self):
        conn = self._conn()
        cursor = conn.cursor()
        assert cursor.description is None
        assert cursor.rowcount == -1
        assert cursor.arraysize == 1
        cursor.close()
        conn.close()

    def test_set_arraysize(self):
        conn = self._conn()
        cursor = conn.cursor()
        cursor.arraysize = 10
        assert cursor.arraysize == 10
        cursor.close()
        conn.close()

    def test_cursor_context_manager(self):
        conn = self._conn()
        with conn.cursor() as cursor:
            assert cursor.connection is conn
        conn.close()

    def test_cursor_double_close(self):
        conn = self._conn()
        cursor = conn.cursor()
        cursor.close()
        cursor.close()  # Should not raise
        conn.close()

    def test_execute_on_closed_cursor(self):
        conn = self._conn()
        cursor = conn.cursor()
        cursor.close()
        with pytest.raises(InterfaceError):
            cursor.execute("SELECT 1")
        conn.close()

    def test_fetch_on_closed_cursor(self):
        conn = self._conn()
        cursor = conn.cursor()
        cursor.close()
        with pytest.raises(InterfaceError):
            cursor.fetchone()
        conn.close()

    def test_cursor_iterator(self):
        conn = self._conn()
        cursor = conn.cursor()
        _setup_table(cursor)
        cursor.execute(f"INSERT INTO {TABLE} (name, age) VALUES ('Alice', 30)")
        cursor.execute(f"INSERT INTO {TABLE} (name, age) VALUES ('Bob', 25)")
        cursor.execute(f"SELECT name, age FROM {TABLE} ORDER BY name")
        rows = list(cursor)
        assert len(rows) == 2
        assert rows[0][0] == "Alice"
        assert rows[1][0] == "Bob"
        _teardown_table(cursor)
        cursor.close()
        conn.close()


# ── CRUD Tests ─────────────────────────────────────────────────────────────


@skip_if_no_valkey
class TestSyncDBAPI_CRUD:
    """Sync DB-API CRUD operations with ValkeyLock."""

    def _conn(self):
        lock = _make_lock("crud")
        return rqlite.connect(lock=lock)

    def test_create_table(self):
        conn = self._conn()
        cursor = conn.cursor()
        _setup_table(cursor)
        cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
            (TABLE,),
        )
        assert cursor.fetchone() is not None
        _teardown_table(cursor)
        cursor.close()
        conn.close()

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

    def test_insert_named(self):
        conn = self._conn()
        cursor = conn.cursor()
        _setup_table(cursor)
        cursor.execute(
            f"INSERT INTO {TABLE} (name, age) VALUES (:name, :age)",
            {"name": "Bob", "age": 25},
        )
        cursor.execute(f"SELECT name, age FROM {TABLE}")
        row = cursor.fetchone()
        assert row == ("Bob", 25)
        _teardown_table(cursor)
        cursor.close()
        conn.close()

    def test_insert_no_params(self):
        conn = self._conn()
        cursor = conn.cursor()
        _setup_table(cursor)
        cursor.execute(f"INSERT INTO {TABLE} (name) VALUES ('Charlie')")
        cursor.execute(f"SELECT name FROM {TABLE}")
        assert cursor.fetchone() == ("Charlie",)
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

    def test_select_all(self):
        conn = self._conn()
        cursor = conn.cursor()
        _setup_table(cursor)
        for name in ["A", "B", "C"]:
            cursor.execute(f"INSERT INTO {TABLE} (name) VALUES (?)", (name,))
        cursor.execute(f"SELECT name FROM {TABLE} ORDER BY name")
        rows = cursor.fetchall()
        assert [r[0] for r in rows] == ["A", "B", "C"]
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


# ── Fetch Tests ────────────────────────────────────────────────────────────


@skip_if_no_valkey
class TestSyncDBAPI_Fetch:
    """Sync DB-API fetch method tests with ValkeyLock."""

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

    def test_fetchmany(self):
        conn = self._conn()
        cursor = conn.cursor()
        _setup_table(cursor)
        for i in range(5):
            cursor.execute(f"INSERT INTO {TABLE} (name) VALUES (?)", (f"R{i}",))
        cursor.execute(f"SELECT name FROM {TABLE} ORDER BY name")
        batch = cursor.fetchmany(2)
        assert len(batch) == 2
        assert batch[0][0] == "R0"
        batch2 = cursor.fetchmany(2)
        assert len(batch2) == 2
        remaining = cursor.fetchall()
        assert len(remaining) == 1
        _teardown_table(cursor)
        cursor.close()
        conn.close()

    def test_fetchmany_default_arraysize(self):
        conn = self._conn()
        cursor = conn.cursor()
        cursor.arraysize = 3
        _setup_table(cursor)
        for i in range(5):
            cursor.execute(f"INSERT INTO {TABLE} (name) VALUES (?)", (f"X{i}",))
        cursor.execute(f"SELECT name FROM {TABLE} ORDER BY name")
        batch = cursor.fetchmany()
        assert len(batch) == 3
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
        # Second fetchall returns empty
        assert cursor.fetchall() == []
        _teardown_table(cursor)
        cursor.close()
        conn.close()

    def test_fetch_empty_result(self):
        conn = self._conn()
        cursor = conn.cursor()
        _setup_table(cursor)
        cursor.execute(f"SELECT * FROM {TABLE}")
        assert cursor.fetchone() is None
        assert cursor.fetchall() == []
        _teardown_table(cursor)
        cursor.close()
        conn.close()


# ── Cursor Attributes Tests ────────────────────────────────────────────────


@skip_if_no_valkey
class TestSyncDBAPI_CursorAttrs:
    """Sync DB-API cursor attribute tests with ValkeyLock."""

    def _conn(self):
        lock = _make_lock("attrs")
        return rqlite.connect(lock=lock)

    def test_description_after_select(self):
        conn = self._conn()
        cursor = conn.cursor()
        _setup_table(cursor)
        cursor.execute(f"SELECT name, age FROM {TABLE}")
        assert cursor.description is not None
        assert len(cursor.description) == 2
        assert cursor.description[0][0] == "name"
        assert cursor.description[1][0] == "age"
        _teardown_table(cursor)
        cursor.close()
        conn.close()

    def test_description_none_after_write(self):
        conn = self._conn()
        cursor = conn.cursor()
        _setup_table(cursor)
        cursor.execute(f"INSERT INTO {TABLE} (name) VALUES ('Test')")
        assert cursor.description is None
        _teardown_table(cursor)
        cursor.close()
        conn.close()

    def test_rowcount_after_write(self):
        conn = self._conn()
        cursor = conn.cursor()
        _setup_table(cursor)
        cursor.execute(f"INSERT INTO {TABLE} (name) VALUES ('Test')")
        assert cursor.rowcount == 1
        cursor.execute(f"SELECT * FROM {TABLE}")
        assert cursor.rowcount == -1
        _teardown_table(cursor)
        cursor.close()
        conn.close()

    def test_lastrowid(self):
        conn = self._conn()
        cursor = conn.cursor()
        _setup_table(cursor)
        cursor.execute(f"INSERT INTO {TABLE} (name) VALUES ('Test')")
        assert cursor.lastrowid is not None
        assert isinstance(cursor.lastrowid, int)
        _teardown_table(cursor)
        cursor.close()
        conn.close()


# ── Type Handling Tests ────────────────────────────────────────────────────


@skip_if_no_valkey
class TestSyncDBAPI_Types:
    """Sync DB-API type handling tests with ValkeyLock."""

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

    def test_text(self):
        conn = self._conn()
        cursor = conn.cursor()
        _setup_table(cursor)
        cursor.execute(
            f"INSERT INTO {TABLE} (name, email) VALUES (?, ?)",
            ("Text", "test@example.com"),
        )
        cursor.execute(f"SELECT email FROM {TABLE}")
        assert cursor.fetchone() == ("test@example.com",)
        _teardown_table(cursor)
        cursor.close()
        conn.close()

    def test_real(self):
        conn = self._conn()
        cursor = conn.cursor()
        _setup_table(cursor)
        cursor.execute(f"INSERT INTO {TABLE} (name, score) VALUES (?, ?)", ("Real", 3.14159))
        cursor.execute(f"SELECT score FROM {TABLE}")
        row = cursor.fetchone()
        assert row is not None
        assert abs(row[0] - 3.14159) < 0.0001
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

    def test_boolean_as_integer(self):
        conn = self._conn()
        cursor = conn.cursor()
        _setup_table(cursor)
        cursor.execute(f"INSERT INTO {TABLE} (name, active) VALUES (?, ?)", ("Bool", 1))
        cursor.execute(f"SELECT active FROM {TABLE}")
        assert cursor.fetchone() == (1,)
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

    def test_binary_type(self):
        conn = self._conn()
        cursor = conn.cursor()
        _setup_table(cursor)
        binary_data = rqlite.Binary(b"\xde\xad\xbe\xef")
        cursor.execute(
            f"INSERT INTO {TABLE} (name, data) VALUES (?, ?)", ("Bin", binary_data)
        )
        cursor.execute(f"SELECT data FROM {TABLE}")
        row = cursor.fetchone()
        assert row is not None
        assert row[0] == b"\xde\xad\xbe\xef"
        _teardown_table(cursor)
        cursor.close()
        conn.close()


# ── Error Handling Tests ───────────────────────────────────────────────────


@skip_if_no_valkey
class TestSyncDBAPI_Errors:
    """Sync DB-API error handling tests with ValkeyLock."""

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

    def test_not_null_constraint(self):
        conn = self._conn()
        cursor = conn.cursor()
        _setup_table(cursor)
        with pytest.raises(IntegrityError):
            cursor.execute(f"INSERT INTO {TABLE} (name) VALUES (NULL)")
        _teardown_table(cursor)
        cursor.close()
        conn.close()

    def test_commit_rollback(self):
        conn = self._conn()
        cursor = conn.cursor()
        _setup_table(cursor)
        conn.commit()  # No-op, should not raise
        conn.rollback()  # No-op, should not raise
        _teardown_table(cursor)
        cursor.close()
        conn.close()


# ── Transaction SQL Warning Tests ──────────────────────────────────────────


@skip_if_no_valkey
class TestSyncDBAPI_TransactionWarnings:
    """Sync DB-API transaction SQL warning tests with ValkeyLock."""

    def _conn(self):
        lock = _make_lock("tx_warn")
        return rqlite.connect(lock=lock)

    def test_begin_no_warning_with_lock(self):
        conn = self._conn()
        cursor = conn.cursor()
        _setup_table(cursor)
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            cursor.execute("BEGIN")
        assert len([x for x in w if "BEGIN" in str(x.message)]) == 0
        _teardown_table(cursor)
        cursor.close()
        conn.close()

    def test_commit_no_warning_with_lock(self):
        conn = self._conn()
        cursor = conn.cursor()
        _setup_table(cursor)
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            cursor.execute("COMMIT")
        assert len([x for x in w if "COMMIT" in str(x.message)]) == 0
        _teardown_table(cursor)
        cursor.close()
        conn.close()

    def test_rollback_no_warning_with_lock(self):
        conn = self._conn()
        cursor = conn.cursor()
        _setup_table(cursor)
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            cursor.execute("ROLLBACK")
        assert len([x for x in w if "ROLLBACK" in str(x.message)]) == 0
        _teardown_table(cursor)
        cursor.close()
        conn.close()


# ── Read Consistency Tests ────────────────────────────────────────────────


@skip_if_no_valkey
class TestSyncDBAPI_ReadConsistency:
    """Sync DB-API read consistency tests with ValkeyLock."""

    def test_default_linearizable(self):
        lock = _make_lock("rc_default")
        conn = rqlite.connect(lock=lock)
        assert conn.read_consistency == rqlite.ReadConsistency.LINEARIZABLE
        conn.close()

    def test_weak_consistency(self):
        lock = _make_lock("rc_weak")
        conn = rqlite.connect(
            read_consistency=rqlite.ReadConsistency.WEAK, lock=lock
        )
        assert conn.read_consistency == rqlite.ReadConsistency.WEAK
        conn.close()

    def test_none_consistency(self):
        lock = _make_lock("rc_none")
        conn = rqlite.connect(
            read_consistency=rqlite.ReadConsistency.NONE, lock=lock
        )
        assert conn.read_consistency == rqlite.ReadConsistency.NONE
        conn.close()

    def test_strong_consistency(self):
        lock = _make_lock("rc_strong")
        conn = rqlite.connect(
            read_consistency=rqlite.ReadConsistency.STRONG, lock=lock
        )
        assert conn.read_consistency == rqlite.ReadConsistency.STRONG
        conn.close()

    def test_string_consistency(self):
        lock = _make_lock("rc_str")
        conn = rqlite.connect(
            read_consistency="weak", lock=lock
        )
        assert conn.read_consistency == rqlite.ReadConsistency.WEAK
        conn.close()


# ── Complex Workflow Tests ────────────────────────────────────────────────


@skip_if_no_valkey
class TestSyncDBAPI_Workflow:
    """Sync DB-API complex workflow tests with ValkeyLock."""

    def test_full_crud_lifecycle(self):
        lock = _make_lock("workflow")
        conn = rqlite.connect(lock=lock)
        cursor = conn.cursor()

        # Create
        _setup_table(cursor)

        # Insert batch
        items = [
            ("Widget", 10, 9.99, 1),
            ("Gadget", 25, 19.99, 1),
            ("Doohickey", 5, 4.99, 0),
        ]
        cursor.executemany(
            f"INSERT INTO {TABLE} (name, age, score, active) VALUES (?, ?, ?, ?)",
            items,
        )

        # Read
        cursor.execute(f"SELECT COUNT(*) FROM {TABLE}")
        assert cursor.fetchone() == (3,)

        cursor.execute(
            f"SELECT name, score FROM {TABLE} WHERE active = ? ORDER BY score DESC",
            (1,),
        )
        rows = cursor.fetchall()
        assert len(rows) == 2
        assert rows[0][0] == "Gadget"

        # Update
        cursor.execute(
            f"UPDATE {TABLE} SET score = ? WHERE name = ?", (29.99, "Widget")
        )
        assert cursor.rowcount == 1

        # Delete
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
        avg = row[0]
        assert abs(avg - 49.5) < 0.01

        _teardown_table(cursor)
        cursor.close()
        conn.close()

    def test_multiple_operations_same_cursor(self):
        lock = _make_lock("multi_op")
        conn = rqlite.connect(lock=lock)
        cursor = conn.cursor()
        _setup_table(cursor)

        cursor.execute(f"INSERT INTO {TABLE} (name) VALUES ('A')")
        cursor.execute(f"INSERT INTO {TABLE} (name) VALUES ('B')")
        cursor.execute(f"SELECT name FROM {TABLE} ORDER BY name")
        assert len(cursor.fetchall()) == 2
        cursor.execute(f"UPDATE {TABLE} SET name = ? WHERE name = ?", ("X", "A"))
        cursor.execute(f"SELECT name FROM {TABLE} ORDER BY name")
        rows = cursor.fetchall()
        assert [r[0] for r in rows] == ["B", "X"]

        _teardown_table(cursor)
        cursor.close()
        conn.close()
