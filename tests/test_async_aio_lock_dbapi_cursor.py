"""Tests for async DB-API 2.0 cursor with AioLock.

Covers:
- Async cursor attributes (description, rowcount, arraysize, lastrowid)
- Async execute operations (create, insert, select, update, delete)
- Async fetch operations (fetchone, fetchmany, fetchall, iteration)
- Executemany with async cursors

Usage:
    pytest tests/test_async_aio_lock_dbapi_cursor.py -v
"""

import asyncio

import rqlite


class TestAsyncAioLockCursorBasic:
    """Test basic async cursor operations."""

    def test_async_description_attribute(self):
        """Test description attribute before execute."""
        conn = rqlite.async_connect(host="localhost", port=4001)

        async def _test():
            cursor = await conn.cursor()
            assert cursor.description is None
            await cursor.close()

        asyncio.run(_test())
        conn.close()

    def test_async_rowcount_initial(self):
        """Test rowcount initial value."""
        conn = rqlite.async_connect(host="localhost", port=4001)

        async def _test():
            cursor = await conn.cursor()
            assert cursor.rowcount == -1
            await cursor.close()

        asyncio.run(_test())
        conn.close()

    def test_async_arraysize_default(self):
        """Test default arraysize."""
        conn = rqlite.async_connect(host="localhost", port=4001)

        async def _test():
            cursor = await conn.cursor()
            assert cursor.arraysize == 1
            await cursor.close()

        asyncio.run(_test())
        conn.close()

    def test_async_arraysize_setter(self):
        """Test setting arraysize."""
        conn = rqlite.async_connect(host="localhost", port=4001)

        async def _test():
            cursor = await conn.cursor()
            cursor.arraysize = 10
            assert cursor.arraysize == 10
            await cursor.close()

        asyncio.run(_test())
        conn.close()


class TestAsyncAioLockCursorExecute:
    """Test async cursor execute operations."""

    def test_async_execute_create_table(self):
        """Test executing CREATE TABLE."""
        conn = rqlite.async_connect(host="localhost", port=4001)

        async def _test():
            cursor = await conn.cursor()
            await cursor.execute("DROP TABLE IF EXISTS async_test_table")
            await cursor.execute("""
                CREATE TABLE async_test_table (
                    id INTEGER PRIMARY KEY,
                    name TEXT
                )
            """)
            await conn.commit()
            await cursor.close()

        asyncio.run(_test())
        conn.close()

    def test_async_execute_insert_positional(self):
        """Test INSERT with positional parameters."""
        conn = rqlite.async_connect(host="localhost", port=4001)

        async def _test():
            cursor = await conn.cursor()
            await cursor.execute("DROP TABLE IF EXISTS async_users")
            await cursor.execute("""
                CREATE TABLE async_users (
                    id INTEGER PRIMARY KEY,
                    name TEXT NOT NULL,
                    email TEXT UNIQUE,
                    age INTEGER
                )
            """)
            await conn.commit()

            await cursor.execute(
                "INSERT INTO async_users (name, email, age) VALUES (?, ?, ?)",
                ("Alice", "alice@example.com", 30),
            )
            await conn.commit()

            await cursor.execute("SELECT * FROM async_users WHERE name=?", ("Alice",))
            row = cursor.fetchone()
            assert row is not None
            assert row[1] == "Alice"
            assert row[2] == "alice@example.com"
            assert row[3] == 30

            await cursor.close()

        asyncio.run(_test())
        conn.close()

    def test_async_execute_insert_named(self):
        """Test INSERT with named parameters."""
        conn = rqlite.async_connect(host="localhost", port=4001)

        async def _test():
            cursor = await conn.cursor()
            await cursor.execute("DROP TABLE IF EXISTS async_users_named")
            await cursor.execute("""
                CREATE TABLE async_users_named (
                    id INTEGER PRIMARY KEY,
                    name TEXT NOT NULL,
                    email TEXT UNIQUE,
                    age INTEGER
                )
            """)
            await conn.commit()

            await cursor.execute(
                "INSERT INTO async_users_named (name, email, age) VALUES (:name, :email, :age)",
                {"name": "Bob", "email": "bob@example.com", "age": 25},
            )
            await conn.commit()

            await cursor.execute("SELECT * FROM async_users_named WHERE name=:name", {"name": "Bob"})
            row = cursor.fetchone()
            assert row is not None
            assert row[1] == "Bob"

            await cursor.close()

        asyncio.run(_test())
        conn.close()

    def test_async_execute_select(self):
        """Test SELECT query."""
        conn = rqlite.async_connect(host="localhost", port=4001)

        async def _test():
            cursor = await conn.cursor()
            await cursor.execute("DROP TABLE IF EXISTS async_users_sel")
            await cursor.execute("""
                CREATE TABLE async_users_sel (
                    id INTEGER PRIMARY KEY,
                    name TEXT NOT NULL,
                    email TEXT UNIQUE,
                    age INTEGER
                )
            """)
            await conn.commit()

            await cursor.execute(
                "INSERT INTO async_users_sel (name, email) VALUES (?, ?)",
                ("Charlie", "charlie@example.com"),
            )
            await conn.commit()

            await cursor.execute("SELECT * FROM async_users_sel")
            rows = cursor.fetchall()
            assert len(rows) == 1
            assert rows[0][1] == "Charlie"

            await cursor.close()

        asyncio.run(_test())
        conn.close()

    def test_async_execute_update(self):
        """Test UPDATE statement."""
        conn = rqlite.async_connect(host="localhost", port=4001)

        async def _test():
            cursor = await conn.cursor()
            await cursor.execute("DROP TABLE IF EXISTS async_users_upd")
            await cursor.execute("""
                CREATE TABLE async_users_upd (
                    id INTEGER PRIMARY KEY,
                    name TEXT NOT NULL,
                    email TEXT UNIQUE,
                    age INTEGER
                )
            """)
            await conn.commit()

            await cursor.execute(
                "INSERT INTO async_users_upd (name, age) VALUES (?, ?)",
                ("David", 20),
            )
            await conn.commit()

            await cursor.execute("UPDATE async_users_upd SET age=? WHERE name=?", (30, "David"))
            await conn.commit()

            await cursor.execute("SELECT age FROM async_users_upd WHERE name=?", ("David",))
            row = cursor.fetchone()
            assert row[0] == 30

            await cursor.close()

        asyncio.run(_test())
        conn.close()

    def test_async_execute_delete(self):
        """Test DELETE statement."""
        conn = rqlite.async_connect(host="localhost", port=4001)

        async def _test():
            cursor = await conn.cursor()
            await cursor.execute("DROP TABLE IF EXISTS async_users_del")
            await cursor.execute("""
                CREATE TABLE async_users_del (
                    id INTEGER PRIMARY KEY,
                    name TEXT NOT NULL,
                    email TEXT UNIQUE,
                    age INTEGER
                )
            """)
            await conn.commit()

            await cursor.execute(
                "INSERT INTO async_users_del (name) VALUES (?)",
                ("Eve",),
            )
            await conn.commit()

            await cursor.execute("DELETE FROM async_users_del WHERE name=?", ("Eve",))
            await conn.commit()

            await cursor.execute("SELECT COUNT(*) FROM async_users_del")
            row = cursor.fetchone()
            assert row[0] == 0

            await cursor.close()

        asyncio.run(_test())
        conn.close()


class TestAsyncAioLockCursorFetch:
    """Test async cursor fetch operations."""

    def test_async_fetchone(self):
        """Test fetchone()."""
        conn = rqlite.async_connect(host="localhost", port=4001)

        async def _test():
            cursor = await conn.cursor()
            await cursor.execute("DROP TABLE IF EXISTS async_users_fo")
            await cursor.execute("""
                CREATE TABLE async_users_fo (
                    id INTEGER PRIMARY KEY,
                    name TEXT NOT NULL,
                    email TEXT UNIQUE,
                    age INTEGER
                )
            """)
            await conn.commit()

            await cursor.execute(
                "INSERT INTO async_users_fo (name) VALUES (?)",
                ("Frank",),
            )
            await conn.commit()

            await cursor.execute("SELECT name FROM async_users_fo")
            row = cursor.fetchone()
            assert row == ("Frank",)

            await cursor.close()

        asyncio.run(_test())
        conn.close()

    def test_async_fetchmany(self):
        """Test fetchmany()."""
        conn = rqlite.async_connect(host="localhost", port=4001)

        async def _test():
            cursor = await conn.cursor()
            await cursor.execute("DROP TABLE IF EXISTS async_users_fm")
            await cursor.execute("""
                CREATE TABLE async_users_fm (
                    id INTEGER PRIMARY KEY,
                    name TEXT NOT NULL,
                    email TEXT UNIQUE,
                    age INTEGER
                )
            """)
            await conn.commit()

            for name in ["Grace", "Henry", "Ivy", "Jack"]:
                await cursor.execute("INSERT INTO async_users_fm (name) VALUES (?)", (name,))
            await conn.commit()

            await cursor.execute("SELECT name FROM async_users_fm")
            cursor.arraysize = 2
            rows = cursor.fetchmany()
            assert len(rows) == 2

            rows = cursor.fetchmany(2)
            assert len(rows) == 2

            await cursor.close()

        asyncio.run(_test())
        conn.close()

    def test_async_fetchall(self):
        """Test fetchall()."""
        conn = rqlite.async_connect(host="localhost", port=4001)

        async def _test():
            cursor = await conn.cursor()
            await cursor.execute("DROP TABLE IF EXISTS async_users_fa")
            await cursor.execute("""
                CREATE TABLE async_users_fa (
                    id INTEGER PRIMARY KEY,
                    name TEXT NOT NULL,
                    email TEXT UNIQUE,
                    age INTEGER
                )
            """)
            await conn.commit()

            for name in ["Kate", "Leo", "Mia"]:
                await cursor.execute("INSERT INTO async_users_fa (name) VALUES (?)", (name,))
            await conn.commit()

            await cursor.execute("SELECT name FROM async_users_fa")
            rows = cursor.fetchall()
            assert len(rows) == 3

            await cursor.close()

        asyncio.run(_test())
        conn.close()

    def test_async_iteration(self):
        """Test cursor iteration."""
        conn = rqlite.async_connect(host="localhost", port=4001)

        async def _test():
            cursor = await conn.cursor()
            await cursor.execute("DROP TABLE IF EXISTS async_users_iter")
            await cursor.execute("""
                CREATE TABLE async_users_iter (
                    id INTEGER PRIMARY KEY,
                    name TEXT NOT NULL,
                    email TEXT UNIQUE,
                    age INTEGER
                )
            """)
            await conn.commit()

            for name in ["Nick", "Olivia", "Paul"]:
                await cursor.execute("INSERT INTO async_users_iter (name) VALUES (?)", (name,))
            await conn.commit()

            await cursor.execute("SELECT name FROM async_users_iter")
            names = [row[0] for row in cursor]
            assert len(names) == 3

            await cursor.close()

        asyncio.run(_test())
        conn.close()


class TestAsyncAioLockCursorDescription:
    """Test async cursor description attribute."""

    def test_async_description_after_select(self):
        """Test description is populated after SELECT."""
        conn = rqlite.async_connect(host="localhost", port=4001)

        async def _test():
            cursor = await conn.cursor()
            await cursor.execute("DROP TABLE IF EXISTS async_desc_sel")
            await cursor.execute("""
                CREATE TABLE async_desc_sel (
                    id INTEGER PRIMARY KEY,
                    name TEXT NOT NULL,
                    email TEXT UNIQUE,
                    age INTEGER
                )
            """)
            await conn.commit()

            await cursor.execute("SELECT id, name, email FROM async_desc_sel")
            assert cursor.description is not None
            assert len(cursor.description) == 3
            assert cursor.description[0][0] == "id"
            assert cursor.description[1][0] == "name"
            assert cursor.description[2][0] == "email"

            await cursor.close()

        asyncio.run(_test())
        conn.close()


class TestAsyncAioLockCursorRowcount:
    """Test async cursor rowcount attribute."""

    def test_async_rowcount_after_insert(self):
        """Test rowcount after INSERT."""
        conn = rqlite.async_connect(host="localhost", port=4001)

        async def _test():
            cursor = await conn.cursor()
            await cursor.execute("DROP TABLE IF EXISTS async_rc_ins")
            await cursor.execute("""
                CREATE TABLE async_rc_ins (
                    id INTEGER PRIMARY KEY,
                    name TEXT NOT NULL,
                    email TEXT UNIQUE,
                    age INTEGER
                )
            """)
            await conn.commit()

            await cursor.execute(
                "INSERT INTO async_rc_ins (name) VALUES (?)",
                ("Ryan",),
            )
            assert cursor.rowcount == 1

            await cursor.close()

        asyncio.run(_test())
        conn.close()


class TestAsyncAioLockCursorExecutemany:
    """Test async executemany() method."""

    def test_async_executemany_positional(self):
        """Test executemany with positional parameters."""
        conn = rqlite.async_connect(host="localhost", port=4001)

        async def _test():
            cursor = await conn.cursor()
            await cursor.execute("DROP TABLE IF EXISTS async_users_em")
            await cursor.execute("""
                CREATE TABLE async_users_em (
                    id INTEGER PRIMARY KEY,
                    name TEXT NOT NULL,
                    email TEXT UNIQUE,
                    age INTEGER
                )
            """)
            await conn.commit()

            params = [
                ("Uma", "uma@example.com"),
                ("Victor", "victor@example.com"),
                ("Wendy", "wendy@example.com"),
            ]

            await cursor.executemany(
                "INSERT INTO async_users_em (name, email) VALUES (?, ?)",
                params,
            )
            await conn.commit()

            await cursor.execute("SELECT COUNT(*) FROM async_users_em")
            row = cursor.fetchone()
            assert row[0] == 3

            await cursor.close()

        asyncio.run(_test())
        conn.close()


class TestAsyncAioLockCursorClose:
    """Test async cursor close method."""

    def test_async_close_cursor(self):
        """Test closing cursor."""
        conn = rqlite.async_connect(host="localhost", port=4001)

        async def _test():
            cursor = await conn.cursor()
            await cursor.close()

        asyncio.run(_test())
        conn.close()


class TestAsyncAioLockComplexCursorWorkflow:
    """Test complex async cursor operations with multiple query patterns."""

    def test_async_comprehensive_crud_workflow(self):
        """Test complete CRUD workflow with various query patterns."""
        conn = rqlite.async_connect(host="localhost", port=4001)

        async def _test():
            cursor = await conn.cursor()

            await cursor.execute("DROP TABLE IF EXISTS async_comprehensive_test")
            await cursor.execute("""
                CREATE TABLE async_comprehensive_test (
                    id INTEGER PRIMARY KEY,
                    category TEXT NOT NULL,
                    score REAL,
                    rank INTEGER
                )
            """)
            await conn.commit()

            data = [
                ("Alpha", 85.5, 1),
                ("Beta", 92.0, 2),
                ("Gamma", 78.3, 3),
                ("Delta", 95.7, 4),
                ("Epsilon", 88.2, 5),
            ]
            await cursor.executemany(
                "INSERT INTO async_comprehensive_test (category, score, rank) VALUES (?, ?, ?)",
                data,
            )
            await conn.commit()

            await cursor.execute("SELECT * FROM async_comprehensive_test ORDER BY score DESC")
            rows = cursor.fetchall()
            assert len(rows) == 5
            assert rows[0][1] == "Delta"
            assert rows[-1][1] == "Gamma"

            await cursor.close()

        asyncio.run(_test())
        conn.close()


class TestAsyncAioLockCursorEmptyResults:
    """Test that empty result sets don't trigger warnings in async cursor."""

    def test_async_empty_result_no_warning(self):
        """Test fetchone on empty SELECT doesn't warn."""
        import warnings

        conn = rqlite.async_connect(host="localhost", port=4001)

        async def _test():
            cursor = await conn.cursor()
            await cursor.execute("DROP TABLE IF EXISTS async_empty_result")
            await cursor.execute("""
                CREATE TABLE async_empty_result (
                    id INTEGER PRIMARY KEY,
                    name TEXT NOT NULL,
                    email TEXT UNIQUE,
                    age INTEGER
                )
            """)
            await conn.commit()

            await cursor.execute(
                "INSERT INTO async_empty_result (name) VALUES (?)", ("exists",)
            )
            await conn.commit()

            with warnings.catch_warnings(record=True) as w:
                warnings.simplefilter("always")
                await cursor.execute(
                    "SELECT * FROM async_empty_result WHERE name = ?",
                    ("nonexistent",),
                )
                result = cursor.fetchone()
                assert result is None
                our_warnings = [
                    x for x in w if "No results to fetch" in str(x.message)
                ]
                assert len(our_warnings) == 0

            await cursor.close()

        asyncio.run(_test())
        conn.close()
