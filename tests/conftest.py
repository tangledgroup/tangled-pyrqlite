"""Pytest fixtures for rqlite tests."""

import pytest

import rqlite

# Configure pytest-asyncio
pytest_plugins = ("pytest_asyncio",)

# List of all test tables that need cleanup
TEST_TABLES = [
    "async_blob_test",
    "async_blob_test2",
    "async_blob_test3",
    "async_valkey_empty_test",
    "async_valkey_lock_test_items",
    "async_valkey_multi_cursor_test",
    "async_valkey_test_none",
    "async_valkey_test_weak",
    "blob_test",
    "dbapi_async_redis_cluster_test",
    "dbapi_async_valkey_cluster_test",
    "dbapi_async_valkey_test",
    "dbapi_sync_redis_cluster_test",
    "dbapi_sync_valkey_cluster_test",
    "dbapi_sync_valkey_test",
    "sa_async_core_del",
    "sa_async_core_ins",
    "sa_async_core_sel",
    "sa_async_core_test",
    "sa_async_core_upd",
    "sa_async_has_tbl",
    "sa_async_reflect",
    "sa_async_redis_cluster_del",
    "sa_async_redis_cluster_ins",
    "sa_async_redis_cluster_sel",
    "sa_async_redis_cluster_upd",
    "sa_async_redis_cluster_products",
    "sa_async_redis_cluster_users",
    "sa_async_valkey_cluster_del",
    "sa_async_valkey_cluster_ins",
    "sa_async_valkey_cluster_sel",
    "sa_async_valkey_cluster_upd",
    "sa_async_valkey_cluster_products",
    "sa_async_valkey_cluster_users",
    "sa_async_valkey_products",
    "sa_async_valkey_users",
    "sa_async_users_rc",
    "sa_async_users_t1",
    "sa_async_users_t2",
    "sa_async_users_t3",
    "sa_async_blob_attachment",
    "sa_async_blob_test",
    "sa_blob_attachment",
    "sa_blob_test",
    "sa_consistency",
    "sa_invalid",
    "sa_lock_products",
    "sa_lock_users",
    "sa_none",
    "sa_orders",
    "sa_products",
    "sa_strong",
    "sa_sync_core_del",
    "sa_sync_core_ins",
    "sa_sync_core_sel",
    "sa_sync_core_test",
    "sa_sync_core_upd",
    "sa_sync_has_tbl",
    "sa_sync_reflect",
    "sa_sync_redis_cluster_del",
    "sa_sync_redis_cluster_ins",
    "sa_sync_redis_cluster_sel",
    "sa_sync_redis_cluster_upd",
    "sa_sync_redis_cluster_products",
    "sa_sync_redis_cluster_users",
    "sa_sync_valkey_cluster_del",
    "sa_sync_valkey_cluster_ins",
    "sa_sync_valkey_cluster_sel",
    "sa_sync_valkey_cluster_upd",
    "sa_sync_valkey_cluster_products",
    "sa_sync_valkey_cluster_users",
    "sa_sync_valkey_products",
    "sa_sync_valkey_users",
    "sa_test",
    "sa_users",
    "sa_valkey_products",
    "sa_valkey_users",
    "sa_weak",
    "strict_blob_test",
    "sync_valkey_empty_select_test",
    "sync_valkey_lock_test_items",
    "sync_valkey_multi_cursor_test",
    "sync_valkey_test_none",
    "sync_valkey_test_weak",
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
