"""Tests for BLOB (Binary/LargeBinary) support in rqlite DB-API.

Covers:
- Binary() constructor (DB-API 2.0)
- BLOB column insert via raw cursor
- BLOB column read back as Python bytes
- blob_array decoding (int arrays -> bytes)
- Empty BLOB, large BLOB, mixed text+blob columns
- Async cursor BLOB support

Usage:
    pytest tests/test_blob_dbapi.py -v
"""

import pytest

import rqlite


@pytest.fixture(scope="function")
def blob_table(cursor):
    """Create and cleanup blob_test table."""
    cursor.execute("DROP TABLE IF EXISTS blob_test")
    cursor.execute(
        "CREATE TABLE blob_test ("
        "id INTEGER PRIMARY KEY, "
        "name TEXT NOT NULL, "
        "data BLOB"
        ")"
    )
    cursor.connection.commit()
    yield
    cursor.execute("DROP TABLE IF EXISTS blob_test")
    cursor.connection.commit()


@pytest.fixture(scope="function")
def strict_blob_table(cursor):
    """Create and cleanup strict_blob_test table (STRICT mode)."""
    cursor.execute("DROP TABLE IF EXISTS strict_blob_test")
    cursor.execute(
        "CREATE TABLE strict_blob_test ("
        "id INTEGER PRIMARY KEY, "
        "label TEXT NOT NULL, "
        "blob_data BLOB NOT NULL"
        ") STRICT"
    )
    cursor.connection.commit()
    yield
    cursor.execute("DROP TABLE IF EXISTS strict_blob_test")
    cursor.connection.commit()


class TestBinaryConstructor:
    """Test DB-API 2.0 Binary() constructor."""

    def test_binary_exists_on_module(self):
        """rqlite module exposes Binary constructor."""
        assert hasattr(rqlite, "Binary")
        assert callable(rqlite.Binary)

    def test_binary_returns_memoryview(self):
        """Binary() returns memoryview matching sqlite3 behavior."""
        result = rqlite.Binary(b"hello")
        assert isinstance(result, memoryview)
        assert bytes(result) == b"hello"

    def test_binary_with_bytearray(self):
        """Binary() works with bytearray input."""
        result = rqlite.Binary(bytearray(b"test"))
        assert isinstance(result, memoryview)
        assert bytes(result) == b"test"

    def test_binary_empty(self):
        """Binary() handles empty bytes."""
        result = rqlite.Binary(b"")
        assert isinstance(result, memoryview)
        assert bytes(result) == b""

    def test_binary_existing_bytes(self):
        """BINARY type constant is still available."""
        assert rqlite.BINARY is bytes


class TestAdaptValueBytes:
    """Test adapt_value serializes bytes as int arrays for rqlite."""

    def test_adapt_bytes_to_int_array(self):
        """bytes -> list of integers (rqlite BLOB format)."""
        from rqlite.types import adapt_value

        result = adapt_value(b"SQLite")
        assert result == [83, 81, 76, 105, 116, 101]
        assert isinstance(result, list)

    def test_adapt_bytearray_to_int_array(self):
        """bytearray -> list of integers."""
        from rqlite.types import adapt_value

        result = adapt_value(bytearray(b"hi"))
        assert result == [104, 105]

    def test_adapt_memoryview_to_int_array(self):
        """memoryview -> list of integers."""
        from rqlite.types import adapt_value

        result = adapt_value(memoryview(b"abc"))
        assert result == [97, 98, 99]

    def test_adapt_empty_bytes(self):
        """Empty bytes -> empty list."""
        from rqlite.types import adapt_value

        result = adapt_value(b"")
        assert result == []


class TestBlobInsertRead:
    """Test BLOB insert and read via raw cursor."""

    def test_insert_and_read_blob(self, cursor, blob_table):
        """Insert BLOB data and read it back as Python bytes."""
        original = b"SQLite"
        cursor.execute(
            "INSERT INTO blob_test (name, data) VALUES (?, ?)",
            ("hello", original),
        )
        cursor.connection.commit()

        cursor.execute("SELECT name, data FROM blob_test WHERE name = ?", ("hello",))
        row = cursor.fetchone()
        assert row is not None
        assert row[0] == "hello"
        assert row[1] == original
        assert isinstance(row[1], bytes)

    def test_insert_empty_blob(self, cursor, blob_table):
        """Insert and read empty BLOB."""
        cursor.execute(
            "INSERT INTO blob_test (name, data) VALUES (?, ?)",
            ("empty", b""),
        )
        cursor.connection.commit()

        cursor.execute("SELECT data FROM blob_test WHERE name = ?", ("empty",))
        row = cursor.fetchone()
        assert row is not None
        assert row[0] == b""
        assert isinstance(row[0], bytes)

    def test_insert_large_blob(self, cursor, blob_table):
        """Insert and read a larger BLOB (>1KB)."""
        original = bytes(range(256)) * 10  # 2.5KB of all byte values
        cursor.execute(
            "INSERT INTO blob_test (name, data) VALUES (?, ?)",
            ("large", original),
        )
        cursor.connection.commit()

        cursor.execute("SELECT data FROM blob_test WHERE name = ?", ("large",))
        row = cursor.fetchone()
        assert row is not None
        assert row[0] == original
        assert isinstance(row[0], bytes)

    def test_insert_binary_constructor(self, cursor, blob_table):
        """Insert using Binary() constructor and read back."""
        data = rqlite.Binary(b"constructed")
        cursor.execute(
            "INSERT INTO blob_test (name, data) VALUES (?, ?)",
            ("binary", data),
        )
        cursor.connection.commit()

        cursor.execute("SELECT data FROM blob_test WHERE name = ?", ("binary",))
        row = cursor.fetchone()
        assert row is not None
        assert row[0] == b"constructed"

    def test_multiple_blobs(self, cursor, blob_table):
        """Insert multiple BLOB rows and read them all."""
        payloads = [
            ("first", b"alpha"),
            ("second", bytes([0x00, 0xFF, 0x80])),
            ("third", b"gamma\x00\x01\x02"),
        ]
        for name, data in payloads:
            cursor.execute(
                "INSERT INTO blob_test (name, data) VALUES (?, ?)",
                (name, data),
            )
        cursor.connection.commit()

        cursor.execute("SELECT name, data FROM blob_test ORDER BY name")
        rows = cursor.fetchall()
        assert len(rows) == 3
        for row, (expected_name, expected_data) in zip(rows, payloads):
            assert row[0] == expected_name
            assert row[1] == expected_data
            assert isinstance(row[1], bytes)

    def test_blob_with_null(self, cursor, blob_table):
        """BLOB column can be NULL."""
        cursor.execute(
            "INSERT INTO blob_test (name, data) VALUES (?, ?)",
            ("nullable", None),
        )
        cursor.connection.commit()

        cursor.execute("SELECT data FROM blob_test WHERE name = ?", ("nullable",))
        row = cursor.fetchone()
        assert row is not None
        assert row[0] is None

    def test_strict_blob_table(self, cursor, strict_blob_table):
        """BLOB works with STRICT tables."""
        original = b"strict data"
        cursor.execute(
            "INSERT INTO strict_blob_test (label, blob_data) VALUES (?, ?)",
            ("test", original),
        )
        cursor.connection.commit()

        cursor.execute(
            "SELECT label, blob_data FROM strict_blob_test WHERE label = ?",
            ("test",),
        )
        row = cursor.fetchone()
        assert row is not None
        assert row[0] == "test"
        assert row[1] == original
        assert isinstance(row[1], bytes)

    def test_update_blob(self, cursor, blob_table):
        """UPDATE BLOB column."""
        cursor.execute(
            "INSERT INTO blob_test (name, data) VALUES (?, ?)",
            ("update_me", b"old"),
        )
        cursor.connection.commit()

        cursor.execute(
            "UPDATE blob_test SET data = ? WHERE name = ?",
            (b"new", "update_me"),
        )
        cursor.connection.commit()

        cursor.execute("SELECT data FROM blob_test WHERE name = ?", ("update_me",))
        row = cursor.fetchone()
        assert row is not None
        assert row[0] == b"new"

    def test_select_blob_with_where(self, cursor, blob_table):
        """SELECT BLOB with WHERE clause on text column."""
        cursor.execute(
            "INSERT INTO blob_test (name, data) VALUES (?, ?)",
            ("filter", b"filtered"),
        )
        cursor.connection.commit()

        cursor.execute(
            "SELECT data FROM blob_test WHERE name = 'filter'",
        )
        row = cursor.fetchone()
        assert row is not None
        assert row[0] == b"filtered"


class TestBlobDescription:
    """Test cursor.description for BLOB columns."""

    def test_description_blob_type(self, cursor, blob_table):
        """cursor.description shows blob type for BLOB column."""
        cursor.execute("SELECT name, data FROM blob_test")
        assert cursor.description is not None
        assert len(cursor.description) == 2
        # Column names
        assert cursor.description[0][0] == "name"
        assert cursor.description[1][0] == "data"


class TestAsyncBlob:
    """Test async cursor BLOB support."""

    @pytest.mark.asyncio
    async def test_async_insert_and_read_blob(self):
        """Async: Insert BLOB and read back as Python bytes."""
        conn = rqlite.async_connect(
            host="localhost", port=4001, lock=rqlite.AioLock()
        )
        try:
            cur = await conn.cursor()
            await cur.execute("DROP TABLE IF EXISTS async_blob_test")
            await cur.execute(
                "CREATE TABLE async_blob_test ("
                "id INTEGER PRIMARY KEY, "
                "name TEXT NOT NULL, "
                "data BLOB"
                ")"
            )
            await conn.commit()

            original = b"async blob data"
            await cur.execute(
                "INSERT INTO async_blob_test (name, data) VALUES (?, ?)",
                ("test", original),
            )
            await conn.commit()

            await cur.execute(
                "SELECT name, data FROM async_blob_test WHERE name = ?",
                ("test",),
            )
            row = cur.fetchone()
            assert row is not None
            assert row[0] == "test"
            assert row[1] == original
            assert isinstance(row[1], bytes)

            await cur.execute("DROP TABLE IF EXISTS async_blob_test")
            await conn.commit()
        finally:
            await conn.close()

    @pytest.mark.asyncio
    async def test_async_empty_blob(self):
        """Async: Empty BLOB."""
        conn = rqlite.async_connect(
            host="localhost", port=4001, lock=rqlite.AioLock()
        )
        try:
            cur = await conn.cursor()
            await cur.execute("DROP TABLE IF EXISTS async_blob_test2")
            await cur.execute(
                "CREATE TABLE async_blob_test2 (id INTEGER PRIMARY KEY, data BLOB)"
            )
            await conn.commit()

            await cur.execute(
                "INSERT INTO async_blob_test2 (data) VALUES (?)",
                (b"",),
            )
            await conn.commit()

            await cur.execute("SELECT data FROM async_blob_test2")
            row = cur.fetchone()
            assert row is not None
            assert row[0] == b""
            assert isinstance(row[0], bytes)

            await cur.execute("DROP TABLE IF EXISTS async_blob_test2")
            await conn.commit()
        finally:
            await conn.close()

    @pytest.mark.asyncio
    async def test_async_large_blob(self):
        """Async: Large BLOB (>1KB)."""
        conn = rqlite.async_connect(
            host="localhost", port=4001, lock=rqlite.AioLock()
        )
        try:
            cur = await conn.cursor()
            await cur.execute("DROP TABLE IF EXISTS async_blob_test3")
            await cur.execute(
                "CREATE TABLE async_blob_test3 (id INTEGER PRIMARY KEY, data BLOB)"
            )
            await conn.commit()

            original = bytes(range(256)) * 10
            await cur.execute(
                "INSERT INTO async_blob_test3 (data) VALUES (?)",
                (original,),
            )
            await conn.commit()

            await cur.execute("SELECT data FROM async_blob_test3")
            row = cur.fetchone()
            assert row is not None
            assert row[0] == original
            assert isinstance(row[0], bytes)

            await cur.execute("DROP TABLE IF EXISTS async_blob_test3")
            await conn.commit()
        finally:
            await conn.close()
