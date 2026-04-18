"""Tests for sync DB-API 2.0 read consistency with ThreadLock."""


import pytest

import rqlite
from rqlite.types import ReadConsistency


class TestSyncThreadLockReadConsistencyEnum:
    """Test ReadConsistency enum values and methods."""

    def test_weak_consistency_value(self):
        """Test WEAK consistency level value."""
        assert ReadConsistency.WEAK.value == "weak"
        assert ReadConsistency.WEAK.to_query_param() == "weak"

    def test_linearizable_consistency_value(self):
        """Test LINEARIZABLE consistency level value."""
        assert ReadConsistency.LINEARIZABLE.value == "linearizable"
        assert ReadConsistency.LINEARIZABLE.to_query_param() == "linearizable"

    def test_none_consistency_value(self):
        """Test NONE consistency level value."""
        assert ReadConsistency.NONE.value == "none"
        assert ReadConsistency.NONE.to_query_param() == "none"

    def test_strong_consistency_value(self):
        """Test STRONG consistency level value."""
        assert ReadConsistency.STRONG.value == "strong"
        assert ReadConsistency.STRONG.to_query_param() == "strong"

    def test_auto_consistency_value(self):
        """Test AUTO consistency level value."""
        assert ReadConsistency.AUTO.value == "auto"
        assert ReadConsistency.AUTO.to_query_param() == "auto"

    def test_all_consistency_levels(self):
        """Test all consistency levels are defined."""
        expected = {"weak", "linearizable", "none", "strong", "auto"}
        actual = {level.value for level in ReadConsistency}
        assert actual == expected


class TestSyncThreadLockConnectionReadConsistency:
    """Test read consistency in Connection class."""

    def test_default_read_consistency_is_linearizable(self):
        """Test that default read consistency is LINEARIZABLE."""
        conn = rqlite.connect(host="localhost", port=4001)
        assert conn.read_consistency == ReadConsistency.LINEARIZABLE
        conn.close()

    def test_explicit_weak_consistency_enum(self):
        """Test setting WEAK consistency explicitly with enum."""
        conn = rqlite.connect(
            host="localhost",
            port=4001,
            read_consistency=ReadConsistency.WEAK,
        )
        assert conn.read_consistency == ReadConsistency.WEAK
        conn.close()

    def test_explicit_weak_consistency_string(self):
        """Test setting WEAK consistency with string."""
        conn = rqlite.connect(
            host="localhost",
            port=4001,
            read_consistency="weak",
        )
        assert conn.read_consistency == ReadConsistency.WEAK
        conn.close()

    def test_explicit_none_consistency_string(self):
        """Test setting NONE consistency with string."""
        conn = rqlite.connect(
            host="localhost",
            port=4001,
            read_consistency="none",
        )
        assert conn.read_consistency == ReadConsistency.NONE
        conn.close()

    def test_explicit_strong_consistency_string(self):
        """Test setting STRONG consistency with string."""
        conn = rqlite.connect(
            host="localhost",
            port=4001,
            read_consistency="strong",
        )
        assert conn.read_consistency == ReadConsistency.STRONG
        conn.close()

    def test_explicit_auto_consistency_string(self):
        """Test setting AUTO consistency with string."""
        conn = rqlite.connect(
            host="localhost",
            port=4001,
            read_consistency="auto",
        )
        assert conn.read_consistency == ReadConsistency.AUTO
        conn.close()

    def test_explicit_linearizable_consistency_string(self):
        """Test setting LINEARIZABLE consistency with string."""
        conn = rqlite.connect(
            host="localhost",
            port=4001,
            read_consistency="linearizable",
        )
        assert conn.read_consistency == ReadConsistency.LINEARIZABLE
        conn.close()

    def test_invalid_consistency_string_raises_error(self):
        """Test that invalid consistency string raises ValueError."""
        with pytest.raises(ValueError, match="Invalid read_consistency"):
            rqlite.connect(host="localhost", port=4001, read_consistency="invalid")


class TestSyncThreadLockCursorReadConsistency:
    """Test that cursor uses connection's read consistency for SELECT queries."""

    def test_select_query_includes_consistency_level(self, connection):
        """Test that SELECT queries include the consistency level parameter."""
        conn = connection
        assert conn.read_consistency == ReadConsistency.LINEARIZABLE

        cursor = conn.cursor()

        # Create a test table first
        cursor.execute("CREATE TABLE IF NOT EXISTS test_consistency (id INTEGER PRIMARY KEY, name TEXT)")
        conn.commit()

        # Execute SELECT - should use LINEARIZABLE by default
        cursor.execute("SELECT * FROM test_consistency")
        rows = cursor.fetchall()

        # Query should succeed with linearizable consistency
        assert isinstance(rows, list)
        cursor.close()

    def test_select_with_weak_consistency(self):
        """Test SELECT query with WEAK consistency."""
        conn = rqlite.connect(
            host="localhost",
            port=4001,
            read_consistency=ReadConsistency.WEAK,
        )
        cursor = conn.cursor()

        # Clean up and create test table
        try:
            cursor.execute("DROP TABLE IF EXISTS test_weak")
            conn.commit()
        except Exception:
            pass

        cursor.execute("CREATE TABLE test_weak (id INTEGER PRIMARY KEY, name TEXT)")
        conn.commit()

        # Insert test data
        cursor.execute("INSERT INTO test_weak (name) VALUES (?)", ("test",))
        conn.commit()

        # SELECT should work with weak consistency
        cursor.execute("SELECT * FROM test_weak")
        rows = cursor.fetchall()
        assert len(rows) == 1
        assert rows[0][1] == "test"

        cursor.close()
        conn.close()

    def test_select_with_none_consistency(self):
        """Test SELECT query with NONE consistency."""
        conn = rqlite.connect(
            host="localhost",
            port=4001,
            read_consistency=ReadConsistency.NONE,
        )
        cursor = conn.cursor()

        # Clean up and create test table
        try:
            cursor.execute("DROP TABLE IF EXISTS test_none")
            conn.commit()
        except Exception:
            pass

        cursor.execute("CREATE TABLE test_none (id INTEGER PRIMARY KEY, name TEXT)")
        conn.commit()

        # Insert test data
        cursor.execute("INSERT INTO test_none (name) VALUES (?)", ("test",))
        conn.commit()

        # SELECT should work with none consistency
        cursor.execute("SELECT * FROM test_none")
        rows = cursor.fetchall()
        assert len(rows) == 1

        cursor.close()
        conn.close()

    def test_select_with_strong_consistency(self):
        """Test SELECT query with STRONG consistency."""
        conn = rqlite.connect(
            host="localhost",
            port=4001,
            read_consistency=ReadConsistency.STRONG,
        )
        cursor = conn.cursor()

        # Clean up and create test table
        try:
            cursor.execute("DROP TABLE IF EXISTS test_strong")
            conn.commit()
        except Exception:
            pass

        cursor.execute("CREATE TABLE test_strong (id INTEGER PRIMARY KEY, name TEXT)")
        conn.commit()

        # Insert test data
        cursor.execute("INSERT INTO test_strong (name) VALUES (?)", ("test",))
        conn.commit()

        # SELECT should work with strong consistency
        cursor.execute("SELECT * FROM test_strong")
        rows = cursor.fetchall()
        assert len(rows) == 1

        cursor.close()
        conn.close()

    def test_select_with_auto_consistency(self):
        """Test SELECT query with AUTO consistency."""
        conn = rqlite.connect(
            host="localhost",
            port=4001,
            read_consistency=ReadConsistency.AUTO,
        )
        cursor = conn.cursor()

        # Clean up and create test table
        try:
            cursor.execute("DROP TABLE IF EXISTS test_auto")
            conn.commit()
        except Exception:
            pass

        cursor.execute("CREATE TABLE test_auto (id INTEGER PRIMARY KEY, name TEXT)")
        conn.commit()

        # Insert test data
        cursor.execute("INSERT INTO test_auto (name) VALUES (?)", ("test",))
        conn.commit()

        # SELECT should work with auto consistency
        cursor.execute("SELECT * FROM test_auto")
        rows = cursor.fetchall()
        assert len(rows) == 1

        cursor.close()
        conn.close()

    def test_write_operations_not_affected_by_consistency(self, connection):
        """Test that INSERT/UPDATE/DELETE operations work regardless of consistency level."""
        conn = rqlite.connect(
            host="localhost",
            port=4001,
            read_consistency=ReadConsistency.LINEARIZABLE,
        )
        cursor = conn.cursor()

        # Create table
        cursor.execute("CREATE TABLE IF NOT EXISTS test_write (id INTEGER PRIMARY KEY, name TEXT)")
        conn.commit()

        # INSERT should work
        cursor.execute("INSERT INTO test_write (name) VALUES (?)", ("Alice",))
        conn.commit()

        # UPDATE should work
        cursor.execute("UPDATE test_write SET name=? WHERE id=?", ("Bob", 1))
        conn.commit()

        # DELETE should work
        cursor.execute("DELETE FROM test_write WHERE id=?", (1,))
        conn.commit()

        # Verify deletion
        cursor.execute("SELECT COUNT(*) FROM test_write")
        count = cursor.fetchone()[0]
        assert count == 0

        cursor.close()
        conn.close()


class TestSyncThreadLockSQLAlchemyReadConsistency:
    """Test read consistency with SQLAlchemy dialect."""

    def test_sqlalchemy_default_consistency(self, connection):
        """Test that SQLAlchemy uses LINEARIZABLE by default."""
        from sqlalchemy import create_engine, text

        # Create test table
        engine = create_engine("rqlite://localhost:4001")
        with engine.connect() as conn:
            conn.execute(text("DROP TABLE IF EXISTS sa_test"))
            conn.commit()
            conn.execute(text("CREATE TABLE sa_test (id INTEGER PRIMARY KEY, name TEXT)"))
            conn.commit()

            # Insert data
            conn.execute(text("INSERT INTO sa_test (name) VALUES (:name)"), {"name": "test"})
            conn.commit()

            # SELECT should work with default consistency
            result = conn.execute(text("SELECT * FROM sa_test"))
            rows = result.fetchall()
            assert len(rows) == 1

        engine.dispose()

    def test_sqlalchemy_consistency_via_url(self):
        """Test setting consistency via SQLAlchemy URL query parameter."""
        from sqlalchemy import create_engine, text

        # Test with weak consistency in URL
        engine = create_engine("rqlite://localhost:4001?read_consistency=weak")

        with engine.connect() as conn:
            conn.execute(text("DROP TABLE IF EXISTS sa_weak"))
            conn.commit()
            conn.execute(text("CREATE TABLE sa_weak (id INTEGER PRIMARY KEY, name TEXT)"))
            conn.commit()

            conn.execute(text("INSERT INTO sa_weak (name) VALUES (:name)"), {"name": "test"})
            conn.commit()

            result = conn.execute(text("SELECT * FROM sa_weak"))
            rows = result.fetchall()
            assert len(rows) == 1

        engine.dispose()

    def test_sqlalchemy_consistency_via_dialect_kwarg(self):
        """Test setting consistency via dialect keyword argument."""
        from sqlalchemy import create_engine, text

        # Test with none consistency via kwargs
        engine = create_engine(
            "rqlite://localhost:4001",
            connect_args={"read_consistency": ReadConsistency.NONE},
        )

        with engine.connect() as conn:
            conn.execute(text("DROP TABLE IF EXISTS sa_none"))
            conn.commit()
            conn.execute(text("CREATE TABLE sa_none (id INTEGER PRIMARY KEY, name TEXT)"))
            conn.commit()

            conn.execute(text("INSERT INTO sa_none (name) VALUES (:name)"), {"name": "test"})
            conn.commit()

            result = conn.execute(text("SELECT * FROM sa_none"))
            rows = result.fetchall()
            assert len(rows) == 1

        engine.dispose()

    def test_sqlalchemy_invalid_consistency_fallback(self):
        """Test that invalid consistency value falls back to LINEARIZABLE."""
        import warnings

        from sqlalchemy import create_engine, text

        # Test with invalid consistency in URL - should warn and use LINEARIZABLE
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            engine = create_engine("rqlite://localhost:4001?read_consistency=invalid")

            # Check that a warning was raised
            assert len(w) == 1
            assert "Invalid read_consistency" in str(w[0].message)

        # Should still work with fallback to LINEARIZABLE
        with engine.connect() as conn:
            conn.execute(text("DROP TABLE IF EXISTS sa_invalid"))
            conn.commit()
            conn.execute(text("CREATE TABLE sa_invalid (id INTEGER PRIMARY KEY, name TEXT)"))
            conn.commit()

            conn.execute(text("INSERT INTO sa_invalid (name) VALUES (:name)"), {"name": "test"})
            conn.commit()

            result = conn.execute(text("SELECT * FROM sa_invalid"))
            rows = result.fetchall()
            assert len(rows) == 1

        engine.dispose()


class TestSyncThreadLockReadConsistencyIntegration:
    """Integration tests for read consistency levels."""

    def test_all_consistency_levels_work(self):
        """Test that all consistency levels can be used successfully."""
        for consistency in ReadConsistency:
            # Create connection with specific consistency level
            conn = rqlite.connect(
                host="localhost",
                port=4001,
                read_consistency=consistency,
            )
            cursor = conn.cursor()

            # Create table with unique name per consistency level
            table_name = f"test_{consistency.value}"

            # Drop existing table to ensure clean state
            try:
                cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
                conn.commit()
            except Exception:
                pass

            cursor.execute(f"CREATE TABLE {table_name} (id INTEGER PRIMARY KEY, name TEXT)")
            conn.commit()

            # Insert test data
            cursor.execute(f"INSERT INTO {table_name} (name) VALUES (?)", (consistency.value,))
            conn.commit()

            # Query should succeed with this consistency level
            cursor.execute(f"SELECT * FROM {table_name}")
            rows = cursor.fetchall()
            assert len(rows) == 1
            assert rows[0][1] == consistency.value

            cursor.close()
            conn.close()

    def test_consistency_level_persists_across_queries(self):
        """Test that consistency level is used for all queries on a connection."""
        conn = rqlite.connect(
            host="localhost",
            port=4001,
            read_consistency=ReadConsistency.WEAK,
        )
        cursor = conn.cursor()

        # All SELECT queries should use WEAK consistency
        for _ in range(3):
            cursor.execute("SELECT 1 as test")
            result = cursor.fetchone()
            assert result[0] == 1

        cursor.close()
        conn.close()
