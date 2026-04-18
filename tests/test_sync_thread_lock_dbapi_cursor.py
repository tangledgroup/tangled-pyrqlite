"""Tests for sync DB-API 2.0 cursor with ThreadLock."""

import pytest


class TestSyncThreadLockCursorBasic:
    """Test basic cursor operations."""

    def test_description_attribute(self, cursor):
        """Test description attribute before execute."""
        assert cursor.description is None

    def test_rowcount_initial(self, cursor):
        """Test rowcount initial value."""
        assert cursor.rowcount == -1

    def test_arraysize_default(self, cursor):
        """Test default arraysize."""
        assert cursor.arraysize == 1

    def test_arraysize_setter(self, cursor):
        """Test setting arraysize."""
        cursor.arraysize = 10
        assert cursor.arraysize == 10


class TestSyncThreadLockCursorExecute:
    """Test cursor execute operations."""

    def test_execute_create_table(self, cursor):
        """Test executing CREATE TABLE."""
        cursor.execute("DROP TABLE IF EXISTS test_table")
        cursor.execute("""
            CREATE TABLE test_table (
                id INTEGER PRIMARY KEY,
                name TEXT
            )
        """)
        cursor.connection.commit()

    def test_execute_insert_positional(self, users_table, cursor):
        """Test INSERT with positional parameters."""
        cursor.execute(
            "INSERT INTO users (name, email, age) VALUES (?, ?, ?)",
            ("Alice", "alice@example.com", 30),
        )
        cursor.connection.commit()

        # Verify insert
        cursor.execute("SELECT * FROM users WHERE name=?", ("Alice",))
        row = cursor.fetchone()
        assert row is not None
        assert row[1] == "Alice"
        assert row[2] == "alice@example.com"
        assert row[3] == 30

    def test_execute_insert_named(self, users_table, cursor):
        """Test INSERT with named parameters."""
        cursor.execute(
            "INSERT INTO users (name, email, age) VALUES (:name, :email, :age)",
            {"name": "Bob", "email": "bob@example.com", "age": 25},
        )
        cursor.connection.commit()

        # Verify insert
        cursor.execute("SELECT * FROM users WHERE name=:name", {"name": "Bob"})
        row = cursor.fetchone()
        assert row is not None
        assert row[1] == "Bob"

    def test_execute_select(self, users_table, cursor):
        """Test SELECT query."""
        # Insert test data
        cursor.execute(
            "INSERT INTO users (name, email) VALUES (?, ?)",
            ("Charlie", "charlie@example.com"),
        )
        cursor.connection.commit()

        # Select
        cursor.execute("SELECT * FROM users")
        rows = cursor.fetchall()
        assert len(rows) == 1
        assert rows[0][1] == "Charlie"

    def test_execute_update(self, users_table, cursor):
        """Test UPDATE statement."""
        # Insert and update
        cursor.execute(
            "INSERT INTO users (name, age) VALUES (?, ?)",
            ("David", 20),
        )
        cursor.connection.commit()

        cursor.execute("UPDATE users SET age=? WHERE name=?", (30, "David"))
        cursor.connection.commit()

        # Verify update
        cursor.execute("SELECT age FROM users WHERE name=?", ("David",))
        row = cursor.fetchone()
        assert row[0] == 30

    def test_execute_delete(self, users_table, cursor):
        """Test DELETE statement."""
        # Insert and delete
        cursor.execute(
            "INSERT INTO users (name) VALUES (?)",
            ("Eve",),
        )
        cursor.connection.commit()

        cursor.execute("DELETE FROM users WHERE name=?", ("Eve",))
        cursor.connection.commit()

        # Verify delete
        cursor.execute("SELECT COUNT(*) FROM users")
        row = cursor.fetchone()
        assert row[0] == 0

    def test_execute_no_parameters(self, users_table, cursor):
        """Test execute without parameters."""
        cursor.execute("DELETE FROM users")
        cursor.connection.commit()


class TestSyncThreadLockCursorFetch:
    """Test cursor fetch operations."""

    def test_fetchone(self, users_table, cursor):
        """Test fetchone()."""
        cursor.execute(
            "INSERT INTO users (name) VALUES (?)",
            ("Frank",),
        )
        cursor.connection.commit()

        cursor.execute("SELECT name FROM users")
        row = cursor.fetchone()
        assert row == ("Frank",)

    def test_fetchmany(self, users_table, cursor):
        """Test fetchmany()."""
        # Insert multiple rows
        for name in ["Grace", "Henry", "Ivy", "Jack"]:
            cursor.execute("INSERT INTO users (name) VALUES (?)", (name,))
        cursor.connection.commit()

        cursor.execute("SELECT name FROM users")
        cursor.arraysize = 2
        rows = cursor.fetchmany()
        assert len(rows) == 2

        rows = cursor.fetchmany(2)
        assert len(rows) == 2

    def test_fetchall(self, users_table, cursor):
        """Test fetchall()."""
        for name in ["Kate", "Leo", "Mia"]:
            cursor.execute("INSERT INTO users (name) VALUES (?)", (name,))
        cursor.connection.commit()

        cursor.execute("SELECT name FROM users")
        rows = cursor.fetchall()
        assert len(rows) == 3

    def test_fetch_after_no_execute(self, cursor):
        """Test fetch without prior execute."""
        import warnings

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            result = cursor.fetchone()
            assert result is None
            assert len(w) == 1
            assert "No results to fetch" in str(w[0].message)

    def test_iteration(self, users_table, cursor):
        """Test cursor iteration."""
        for name in ["Nick", "Olivia", "Paul"]:
            cursor.execute("INSERT INTO users (name) VALUES (?)", (name,))
        cursor.connection.commit()

        cursor.execute("SELECT name FROM users")
        names = [row[0] for row in cursor]
        assert len(names) == 3


class TestSyncThreadLockCursorDescription:
    """Test cursor description attribute."""

    def test_description_after_select(self, users_table, cursor):
        """Test description is populated after SELECT."""
        cursor.execute("SELECT id, name, email FROM users")

        assert cursor.description is not None
        assert len(cursor.description) == 3
        assert cursor.description[0][0] == "id"
        assert cursor.description[1][0] == "name"
        assert cursor.description[2][0] == "email"

    def test_description_after_insert(self, users_table, cursor):
        """Test description is None after INSERT."""
        cursor.execute(
            "INSERT INTO users (name) VALUES (?)",
            ("Quinn",),
        )
        cursor.connection.commit()

        assert cursor.description is None


class TestSyncThreadLockCursorRowcount:
    """Test cursor rowcount attribute."""

    def test_rowcount_after_insert(self, users_table, cursor):
        """Test rowcount after INSERT."""
        cursor.execute(
            "INSERT INTO users (name) VALUES (?)",
            ("Ryan",),
        )
        # Single statement executes immediately
        assert cursor.rowcount == 1

    def test_rowcount_after_select(self, users_table, cursor):
        """Test rowcount after SELECT is -1."""
        cursor.execute(
            "INSERT INTO users (name) VALUES (?)",
            ("Sarah",),
        )
        cursor.connection.commit()

        cursor.execute("SELECT * FROM users")
        assert cursor.rowcount == -1


class TestSyncThreadLockCursorLastrowid:
    """Test cursor lastrowid attribute."""

    def test_lastrowid_after_insert(self, users_table, cursor):
        """Test lastrowid is set after INSERT."""
        cursor.execute(
            "INSERT INTO users (name) VALUES (?)",
            ("Tom",),
        )
        cursor.connection.commit()

        assert cursor.lastrowid is not None
        assert isinstance(cursor.lastrowid, int)


class TestSyncThreadLockCursorExecutemany:
    """Test executemany() method."""

    def test_executemany_positional(self, users_table, cursor):
        """Test executemany with positional parameters."""
        params = [
            ("Uma", "uma@example.com"),
            ("Victor", "victor@example.com"),
            ("Wendy", "wendy@example.com"),
        ]

        cursor.executemany(
            "INSERT INTO users (name, email) VALUES (?, ?)",
            params,
        )
        cursor.connection.commit()

        cursor.execute("SELECT COUNT(*) FROM users")
        row = cursor.fetchone()
        assert row[0] == 3

    def test_executemany_named(self, users_table, cursor):
        """Test executemany with named parameters."""
        params = [
            {"name": "Xavier", "email": "xavier@example.com"},
            {"name": "Yolanda", "email": "yolanda@example.com"},
        ]

        cursor.executemany(
            "INSERT INTO users (name, email) VALUES (:name, :email)",
            params,
        )
        cursor.connection.commit()

        cursor.execute("SELECT COUNT(*) FROM users")
        row = cursor.fetchone()
        assert row[0] == 2


class TestSyncThreadLockCursorClose:
    """Test cursor close method."""

    def test_close_cursor(self, cursor):
        """Test closing cursor."""
        cursor.close()

        # Should raise error after close
        with pytest.raises(Exception) as exc_info:
            cursor.execute("SELECT 1")
        assert "closed" in str(exc_info.value).lower()

    def test_double_close(self, cursor):
        """Test closing cursor twice doesn't raise."""
        cursor.close()
        cursor.close()  # Should not raise


class TestSyncThreadLockComplexCursorWorkflow:
    """Test complex cursor operations with multiple query patterns."""

    def test_comprehensive_crud_workflow(self, cursor):
        """Test complete CRUD workflow with various query patterns."""
        # Step 1: CREATE TABLE
        cursor.execute("DROP TABLE IF EXISTS comprehensive_test")
        cursor.execute("""
            CREATE TABLE comprehensive_test (
                id INTEGER PRIMARY KEY,
                category TEXT NOT NULL,
                score REAL,
                rank INTEGER
            )
        """)
        cursor.connection.commit()

        # Step 2: INSERT MANY with executemany
        data = [
            ("Alpha", 85.5, 1),
            ("Beta", 92.0, 2),
            ("Gamma", 78.3, 3),
            ("Delta", 95.7, 4),
            ("Epsilon", 88.2, 5),
        ]
        cursor.executemany(
            "INSERT INTO comprehensive_test (category, score, rank) VALUES (?, ?, ?)",
            data,
        )
        cursor.connection.commit()

        # Step 3: SELECT ALL ordered
        cursor.execute("SELECT * FROM comprehensive_test ORDER BY score DESC")
        rows = cursor.fetchall()
        assert len(rows) == 5
        assert rows[0][1] == "Delta"  # Highest score
        assert rows[-1][1] == "Gamma"  # Lowest score

        # Step 4: SELECT FEW with range filter
        cursor.execute(
            "SELECT category, score FROM comprehensive_test WHERE score BETWEEN ? AND ?",
            (85.0, 90.0),
        )
        rows = cursor.fetchall()
        assert len(rows) == 2  # Alpha and Epsilon
        categories = {row[0] for row in rows}
        assert categories == {"Alpha", "Epsilon"}

        # Step 5: SELECT ONE with fetchone
        cursor.execute(
            "SELECT category, score, rank FROM comprehensive_test WHERE category = ?",
            ("Beta",),
        )
        row = cursor.fetchone()
        assert row is not None
        assert row[0] == "Beta"
        assert row[1] == 92.0

        # Step 6: UPDATE specific row
        cursor.execute(
            "UPDATE comprehensive_test SET score = ? WHERE category = ?",
            (86.0, "Alpha"),
        )
        cursor.connection.commit()

        # Step 7: SELECT ONE to verify update
        cursor.execute(
            "SELECT score FROM comprehensive_test WHERE category = ?",
            ("Alpha",),
        )
        row = cursor.fetchone()
        assert row[0] == 86.0

        # Step 8: DELETE based on condition
        cursor.execute("DELETE FROM comprehensive_test WHERE rank < ?", (3,))
        cursor.connection.commit()

        # Step 9: SELECT MANY to verify remaining
        cursor.execute("SELECT category, rank FROM comprehensive_test ORDER BY rank")
        rows = cursor.fetchall()
        assert len(rows) == 3  # Gamma, Delta, Epsilon remain
        ranks = [row[1] for row in rows]
        assert ranks == [3, 4, 5]

        # Step 10: SELECT ONE for deleted item (should be None)
        cursor.execute(
            "SELECT category FROM comprehensive_test WHERE category = ?",
            ("Alpha",),
        )
        row = cursor.fetchone()
        assert row is None  # Alpha was deleted

    def test_fetch_patterns(self, cursor):
        """Test various fetch patterns: fetchone, fetchmany, fetchall, iteration."""
        # Setup
        cursor.execute("DROP TABLE IF EXISTS fetch_test")
        cursor.execute("CREATE TABLE fetch_test (id INTEGER, letter TEXT)")
        for i, letter in enumerate(["A", "B", "C", "D", "E", "F", "G", "H"], 1):
            cursor.execute("INSERT INTO fetch_test VALUES (?, ?)", (i, letter))
        cursor.connection.commit()

        # Test fetchone
        cursor.execute("SELECT letter FROM fetch_test WHERE id = 1")
        row = cursor.fetchone()
        assert row == ("A",)

        # Test fetchmany with arraysize
        cursor.execute("SELECT letter FROM fetch_test ORDER BY id")
        cursor.arraysize = 3
        batch1 = cursor.fetchmany()
        assert len(batch1) == 3
        batch2 = cursor.fetchmany(2)
        assert len(batch2) == 2
        batch3 = cursor.fetchmany()
        assert len(batch3) == 3  # Remaining

        # Test fetchall
        cursor.execute("SELECT letter FROM fetch_test WHERE id > 6")
        rows = cursor.fetchall()
        assert len(rows) == 2  # G, H

        # Test iteration
        cursor.execute("SELECT letter FROM fetch_test WHERE id <= 3")
        letters = [row[0] for row in cursor]
        assert letters == ["A", "B", "C"]

    def test_cursor_description_various_queries(self, cursor):
        """Test cursor.description with different query types."""
        # CREATE - no description
        cursor.execute("DROP TABLE IF EXISTS desc_test")
        assert cursor.description is None

        cursor.execute("CREATE TABLE desc_test (col1 INTEGER, col2 TEXT, col3 REAL)")
        assert cursor.description is None

        # INSERT - no description
        cursor.execute("INSERT INTO desc_test VALUES (?, ?, ?)", (1, "test", 3.14))
        assert cursor.description is None

        # SELECT - has description
        cursor.execute("SELECT * FROM desc_test")
        assert cursor.description is not None
        assert len(cursor.description) == 3
        assert cursor.description[0][0] == "col1"
        assert cursor.description[1][0] == "col2"
        assert cursor.description[2][0] == "col3"

        # SELECT with specific columns
        cursor.execute("SELECT col2, col1 FROM desc_test")
        assert cursor.description is not None
        assert len(cursor.description) == 2
        assert cursor.description[0][0] == "col2"
        assert cursor.description[1][0] == "col1"

        # UPDATE - no description
        cursor.execute("UPDATE desc_test SET col1 = ?", (999,))
        assert cursor.description is None

        # DELETE - no description
        cursor.execute("DELETE FROM desc_test")
        assert cursor.description is None

    def test_empty_result_sets_no_warnings(self, cursor):
        """Test that empty result sets from SELECT don't trigger warnings.

        Regression test: fetchone/fetchall on empty SELECT results should not warn.
        """
        import warnings

        # Create table with one row
        cursor.execute("DROP TABLE IF EXISTS empty_result_test")
        cursor.execute("CREATE TABLE empty_result_test (id INTEGER, value TEXT)")
        cursor.execute("INSERT INTO empty_result_test VALUES (?, ?)", (1, "exists"))
        cursor.connection.commit()

        # SELECT with no matching rows - fetchone should not warn
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            cursor.execute(
                "SELECT * FROM empty_result_test WHERE value = ?",
                ("nonexistent",),
            )
            result = cursor.fetchone()
            assert result is None
            # Filter for our specific warning
            our_warnings = [x for x in w if "No results to fetch" in str(x.message)]
            assert len(our_warnings) == 0, "Empty SELECT result should not trigger warning"

        # Same test with fetchall
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            cursor.execute(
                "SELECT * FROM empty_result_test WHERE id > ?",
                (999,),
            )
            results = cursor.fetchall()
            assert results == []
            our_warnings = [x for x in w if "No results to fetch" in str(x.message)]
            assert len(our_warnings) == 0

        # Same test with iteration
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            cursor.execute(
                "SELECT * FROM empty_result_test WHERE id = ?",
                (999,),
            )
            results = list(cursor)
            assert results == []
            our_warnings = [x for x in w if "No results to fetch" in str(x.message)]
            assert len(our_warnings) == 0
