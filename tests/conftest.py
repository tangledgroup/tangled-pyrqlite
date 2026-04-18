"""Pytest fixtures for rqlite tests."""

import pytest

import rqlite

# Configure pytest-asyncio
pytest_plugins = ("pytest_asyncio",)

# List of all test tables that need cleanup
TEST_TABLES = [
    "comprehensive_test",
    "ctx_test",
    "ctx_test2",
    "dbapi_test_products",
    "desc_test",
    "empty_result_test",
    "empty_select_test",
    "fetch_test",
    "full_lock_workflow",
    "lock_test_delete",
    "lock_test_insert",
    "lock_test_select",
    "lock_test_update",
    "multi_cursor_lock_test",
    "multi_cursor_test",
    "products",
    "sa_consistency",
    "sa_invalid",
    "sa_lock_products",
    "sa_lock_users",
    "sa_none",
    "sa_orders",
    "sa_products",
    "sa_strong",
    "sa_test",
    "sa_users",
    "sa_weak",
    "test_auto",
    "test_consistency",
    "test_items",
    "test_linearizable",
    "test_none",
    "test_strong",
    "test_table",
    "test_weak",
    "test_write",
    "threading_lock_workflow",
    "tx_select_test",
    "tx_test",
    "tx_test2",
    "tx_unique",
    "users",
]


@pytest.fixture(scope="function", autouse=True)
def cleanup_tables():
    """Automatically cleanup all test tables after each test.

    This fixture runs automatically after every test to ensure
    a clean database state and prevent test interference.
    Uses ThreadLock to suppress transaction warnings.
    """
    # Yield first to allow the test to run
    yield

    # Cleanup happens AFTER the test runs (teardown phase)
    conn = rqlite.connect(host="localhost", port=4001, lock=rqlite.ThreadLock())
    try:
        cursor = conn.cursor()
        for table in TEST_TABLES:
            cursor.execute(f"DROP TABLE IF EXISTS {table}")
        conn.commit()
    except Exception:
        # Ignore cleanup errors - we want tests to pass even if cleanup fails
        pass
    finally:
        conn.close()


@pytest.fixture(scope="function")
def connection():
    """Create a new connection for each test.

    Yields:
        rqlite.Connection: A new database connection.
    """
    conn = rqlite.connect(host="localhost", port=4001)
    yield conn
    conn.close()


@pytest.fixture(scope="function")
def cursor(connection):
    """Create a cursor for testing.

    Args:
        connection: Connection fixture.

    Yields:
        rqlite.Cursor: A new cursor.
    """
    cur = connection.cursor()
    yield cur
    cur.close()


@pytest.fixture(scope="function")
def users_table(cursor):
    """Create and cleanup users table for tests.

    Args:
        cursor: Cursor fixture.

    Yields:
        None
    """
    # Drop if exists and create fresh
    cursor.execute("DROP TABLE IF EXISTS users")
    cursor.execute("""
        CREATE TABLE users (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            email TEXT UNIQUE,
            age INTEGER
        )
    """)
    cursor.connection.commit()

    yield

    # Cleanup
    cursor.execute("DROP TABLE IF EXISTS users")
    cursor.connection.commit()


@pytest.fixture(scope="function")
def products_table(cursor):
    """Create and cleanup products table for tests.

    Args:
        cursor: Cursor fixture.

    Yields:
        None
    """
    # Drop if exists and create fresh
    cursor.execute("DROP TABLE IF EXISTS products")
    cursor.execute("""
        CREATE TABLE products (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            price REAL,
            quantity INTEGER DEFAULT 0
        )
    """)
    cursor.connection.commit()

    yield

    # Cleanup
    cursor.execute("DROP TABLE IF EXISTS products")
    cursor.connection.commit()
