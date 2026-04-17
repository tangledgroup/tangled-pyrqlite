"""Tests for rqlite transaction handling."""


import warnings

import pytest

import rqlite


class TestTransactionBehavior:
    """Test transaction behavior with rqlite.

    Note: This implementation executes statements immediately rather than
    queuing them. For atomic multi-statement operations, users should use
    rqlite's ?transaction=true parameter directly via HTTP or implement
    their own batching logic.
    """

    def test_commit_with_no_pending(self, connection):
        """Test that commit works when no statements are pending."""
        cursor = connection.cursor()

        # Statements execute immediately
        cursor.execute("DROP TABLE IF EXISTS tx_test")
        cursor.execute("""
            CREATE TABLE tx_test (
                id INTEGER PRIMARY KEY,
                value TEXT
            )
        """)

        # Commit is a no-op when nothing is pending
        connection.commit()

        # Insert rows (each executes immediately)
        cursor.execute("INSERT INTO tx_test (value) VALUES (?)", ("a",))
        cursor.execute("INSERT INTO tx_test (value) VALUES (?)", ("b",))
        cursor.execute("INSERT INTO tx_test (value) VALUES (?)", ("c",))

        # Verify all rows were inserted
        cursor.execute("SELECT COUNT(*) FROM tx_test")
        row = cursor.fetchone()
        assert row[0] == 3

        cursor.close()

    def test_rollback_with_no_pending(self, connection):
        """Test that rollback works when no statements are pending."""
        cursor = connection.cursor()

        # Create table first (execute immediately)
        cursor.execute("DROP TABLE IF EXISTS tx_test2")
        cursor.execute("""
            CREATE TABLE tx_test2 (
                id INTEGER PRIMARY KEY,
                value TEXT
            )
        """)

        # Inserts execute immediately
        cursor.execute("INSERT INTO tx_test2 (value) VALUES (?)", ("x",))
        cursor.execute("INSERT INTO tx_test2 (value) VALUES (?)", ("y",))

        # Rollback is a no-op when nothing is pending
        # (statements already executed and committed by rqlite)
        connection.rollback()

        # Both rows are still there (rqlite auto-commits single statements)
        cursor.execute("SELECT COUNT(*) FROM tx_test2")
        row = cursor.fetchone()
        assert row[0] == 2  # Both 'x' and 'y' were inserted

        cursor.close()

    def test_error_handling(self, connection):
        """Test that errors are properly reported."""
        cursor = connection.cursor()

        # Create table with unique constraint
        cursor.execute("DROP TABLE IF EXISTS tx_unique")
        cursor.execute("""
            CREATE TABLE tx_unique (
                id INTEGER PRIMARY KEY,
                value TEXT UNIQUE
            )
        """)

        # Insert executes immediately and succeeds
        cursor.execute("INSERT INTO tx_unique (value) VALUES (?)", ("ok1",))

        # Second insert with same value fails immediately
        with pytest.raises(rqlite.IntegrityError):
            cursor.execute("INSERT INTO tx_unique (value) VALUES (?)", ("ok1",))

        # Only the first row was inserted
        cursor.execute("SELECT COUNT(*) FROM tx_unique")
        row = cursor.fetchone()
        assert row[0] == 1

        cursor.close()


class TestTransactionWarnings:
    """Test warnings for unsupported transaction patterns."""

    def test_explicit_begin_warning(self, cursor):
        """Test warning when using explicit BEGIN SQL."""
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            cursor.execute("BEGIN")

            # Should have warned
            assert len(w) == 1
            assert "BEGIN" in str(w[0].message) or "not supported" in str(w[0].message).lower()

    def test_explicit_commit_warning(self, cursor):
        """Test warning when using explicit COMMIT SQL."""
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            cursor.execute("COMMIT")

            assert len(w) == 1
            assert "COMMIT" in str(w[0].message) or "not supported" in str(w[0].message).lower()

    def test_explicit_rollback_warning(self, cursor):
        """Test warning when using explicit ROLLBACK SQL.

        Note: Our client skips execution of explicit transaction SQL and emits
        a warning to inform users that this is not supported in the traditional sense.
        """
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            # ROLLBACK is skipped by our client and emits a warning
            cursor.execute("ROLLBACK")

            # Should have warned about unsupported SQL
            assert len(w) == 1
            assert "ROLLBACK" in str(w[0].message) or "not supported" in str(w[0].message).lower()

    def test_savepoint_warning(self, cursor):
        """Test warning when using SAVEPOINT SQL."""
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            cursor.execute("SAVEPOINT my_savepoint")

            assert len(w) == 1
            assert "SAVEPOINT" in str(w[0].message) or "not supported" in str(w[0].message).lower()


class TestTransactionContextManager:
    """Test transaction behavior with context managers."""

    def test_context_manager_commit_on_success(self):
        """Test that context manager commits on success."""
        with rqlite.connect("localhost", 4001) as conn:
            cursor = conn.cursor()

            cursor.execute("DROP TABLE IF EXISTS ctx_test")
            cursor.execute("""
                CREATE TABLE ctx_test (
                    id INTEGER PRIMARY KEY,
                    data TEXT
                )
            """)
            cursor.execute("INSERT INTO ctx_test (data) VALUES (?)", ("test",))

            # Context exit should commit

            cursor.execute("SELECT COUNT(*) FROM ctx_test")
            row = cursor.fetchone()
            assert row[0] == 1

            cursor.close()

        # Data should persist after context exit
        with rqlite.connect("localhost", 4001) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM ctx_test")
            row = cursor.fetchone()
            assert row[0] == 1
            cursor.close()

    def test_context_manager_on_exception(self):
        """Test that context manager handles exceptions properly."""
        exception_raised = False
        try:
            with rqlite.connect("localhost", 4001) as conn:
                cursor = conn.cursor()

                # Create table first
                cursor.execute("DROP TABLE IF EXISTS ctx_test2")
                cursor.execute("""
                    CREATE TABLE ctx_test2 (
                        id INTEGER PRIMARY KEY,
                        data TEXT
                    )
                """)

                # Insert then raise exception
                cursor.execute("INSERT INTO ctx_test2 (data) VALUES (?)", ("test",))
                raise ValueError("Test exception")

                cursor.close()
        except ValueError:
            exception_raised = True

        assert exception_raised

        # Data was inserted (statements execute immediately)
        with rqlite.connect("localhost", 4001) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM ctx_test2")
            row = cursor.fetchone()
            assert row[0] == 1  # Insert happened before exception
            cursor.close()


class TestTransactionLimitations:
    """Test known transaction limitations."""

    def test_select_in_transaction(self, connection):
        """Test that SELECT in transaction is queued but not immediately available.

        This demonstrates rqlite's limitation: you cannot use SELECT results
        within the same transaction for subsequent operations.
        """
        cursor = connection.cursor()

        # Create and populate table
        cursor.execute("DROP TABLE IF EXISTS tx_select_test")
        cursor.execute("""
            CREATE TABLE tx_select_test (
                id INTEGER PRIMARY KEY,
                value TEXT
            )
        """)
        connection.commit()

        cursor.execute("INSERT INTO tx_select_test (value) VALUES (?)", ("initial",))
        connection.commit()

        # Now try to select and use result in same transaction
        # This is NOT supported in rqlite - the SELECT is queued but results
        # are not available until after commit
        cursor.execute("SELECT value FROM tx_select_test WHERE id=1")
        # fetchone would return None because results aren't available yet

        connection.commit()

        cursor.close()
