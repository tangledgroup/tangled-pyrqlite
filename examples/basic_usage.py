"""Basic usage examples for rqlite DB-API 2.0 client.

This example demonstrates fundamental CRUD operations using the rqlite
DB-API 2.0 compliant client.

Prerequisites:
    - rqlite server running on localhost:4001

    Start with Podman (recommended):
        podman run -d --name rqlite-test -p 4001:4001 docker.io/rqlite/rqlite

    Or with Docker:
        docker run -d --name rqlite-test -p 4001:4001 rqlite/rqlite

Usage:
    # Without lock (shows transaction warnings):
    uv run python -B examples/basic_usage.py

    # With lock (no transaction warnings):
    uv run python -B examples/basic_usage.py --with-lock
"""

import argparse
import functools
from typing import Any
from collections.abc import Callable

import rqlite
from rqlite import ThreadLock


def print_docstring(func: Callable) -> Callable:
    """Decorator that prints the function's docstring when called."""
    @functools.wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        if func.__doc__:
            print(f"\n{'─' * 60}")
            print(f"📝 {func.__name__}: {func.__doc__.strip()}")
            print("─" * 60)
        return func(*args, **kwargs)
    return wrapper


@print_docstring
def create_table(use_lock: bool = False):
    """Create a table."""
    lock = ThreadLock() if use_lock else None
    conn = rqlite.connect(host="localhost", port=4001, lock=lock)
    cursor = conn.cursor()

    # Drop existing table for clean demo
    cursor.execute("DROP TABLE IF EXISTS users")

    # Create new table
    cursor.execute("""
        CREATE TABLE users (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            email TEXT UNIQUE,
            age INTEGER
        )
    """)

    conn.commit()
    print("✓ Table 'users' created")

    cursor.close()
    conn.close()


@print_docstring
def insert_data(use_lock: bool = False):
    """Insert data into the table."""
    lock = ThreadLock() if use_lock else None
    conn = rqlite.connect(lock=lock)
    cursor = conn.cursor()

    # Insert single row with positional parameters (?)
    cursor.execute(
        "INSERT INTO users (name, email, age) VALUES (?, ?, ?)",
        ("Alice", "alice@example.com", 30),
    )

    # Insert with named parameters (:name)
    cursor.execute(
        "INSERT INTO users (name, email, age) VALUES (:name, :email, :age)",
        {"name": "Bob", "email": "bob@example.com", "age": 25},
    )

    conn.commit()
    print(f"✓ Inserted {cursor.rowcount} row(s)")

    cursor.close()
    conn.close()


@print_docstring
def query_data(use_lock: bool = False):
    """Query data from the table."""
    lock = ThreadLock() if use_lock else None
    conn = rqlite.connect(lock=lock)
    cursor = conn.cursor()

    # Select all users
    cursor.execute("SELECT * FROM users")

    print("\nAll users:")
    print("-" * 60)
    for row in cursor:
        print(f"  ID: {row[0]}, Name: {row[1]}, Email: {row[2]}, Age: {row[3]}")

    # Select specific user
    print("\nUser named Alice:")
    cursor.execute("SELECT * FROM users WHERE name=?", ("Alice",))
    row = cursor.fetchone()
    if row:
        print(f"  Found: {row}")

    cursor.close()
    conn.close()


@print_docstring
def update_data(use_lock: bool = False):
    """Update existing data."""
    lock = ThreadLock() if use_lock else None
    conn = rqlite.connect(lock=lock)
    cursor = conn.cursor()

    # Update Bob's age
    cursor.execute("UPDATE users SET age=? WHERE name=?", (30, "Bob"))
    conn.commit()

    print(f"✓ Updated {cursor.rowcount} row(s)")

    cursor.close()
    conn.close()


@print_docstring
def delete_data(use_lock: bool = False):
    """Delete data from the table."""
    lock = ThreadLock() if use_lock else None
    conn = rqlite.connect(lock=lock)
    cursor = conn.cursor()

    # Delete Alice
    cursor.execute("DELETE FROM users WHERE name=?", ("Alice",))
    conn.commit()

    print(f"✓ Deleted {cursor.rowcount} row(s)")

    cursor.close()
    conn.close()


@print_docstring
def batch_insert(use_lock: bool = False):
    """Insert multiple rows efficiently."""
    lock = ThreadLock() if use_lock else None
    conn = rqlite.connect(lock=lock)
    cursor = conn.cursor()

    # Using executemany for multiple inserts
    users = [
        ("Charlie", "charlie@example.com", 35),
        ("Diana", "diana@example.com", 28),
        ("Eve", "eve@example.com", 32),
    ]

    cursor.executemany(
        "INSERT INTO users (name, email, age) VALUES (?, ?, ?)",
        users,
    )
    conn.commit()

    print(f"✓ Inserted {len(users)} users")

    cursor.close()
    conn.close()


@print_docstring
def context_manager_example(use_lock: bool = False):
    """Using context managers for automatic resource cleanup."""
    # Connection as context manager
    lock = ThreadLock() if use_lock else None
    with rqlite.connect(lock=lock) as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) FROM users")
            count = cursor.fetchone()[0]
            print(f"✓ Total users: {count}")

    # Connection and cursor are automatically closed


@print_docstring
def complex_workflow(use_lock: bool = False):
    """Complex workflow: comprehensive CRUD operations with multiple query patterns."""
    lock = ThreadLock() if use_lock else None
    conn = rqlite.connect(host="localhost", port=4001, lock=lock)

    try:
        cursor = conn.cursor()

        # Step 1: CREATE TABLE
        print("\n[STEP 1] CREATE: Drop and create products table")
        cursor.execute("DROP TABLE IF EXISTS complex_products")
        cursor.execute("""
            CREATE TABLE complex_products (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                category TEXT NOT NULL,
                price REAL NOT NULL,
                stock INTEGER DEFAULT 0,
                active INTEGER DEFAULT 1
            )
        """)
        conn.commit()
        print("✓ Table created")

        # Step 2: INSERT MANY - Add products in bulk
        print("\n[STEP 2] INSERT MANY: Adding 8 products")
        products = [
            ("Laptop Pro", "Electronics", 1299.99, 15, 1),
            ("Wireless Mouse", "Electronics", 49.99, 50, 1),
            ("Office Chair", "Furniture", 249.99, 20, 1),
            ("Desk Lamp", "Furniture", 39.99, 30, 1),
            ("USB-C Hub", "Electronics", 79.99, 40, 1),
            ("Notebook Set", "Stationery", 12.99, 100, 1),
            ("Mechanical Keyboard", "Electronics", 149.99, 25, 1),
            ("Monitor Stand", "Furniture", 89.99, 18, 0),  # Inactive
        ]
        cursor.executemany(
            """INSERT INTO complex_products
               (name, category, price, stock, active)
               VALUES (?, ?, ?, ?, ?)""",
            products,
        )
        conn.commit()
        print(f"✓ Inserted {len(products)} products")

        # Step 3: SELECT MANY - All products ordered by price
        print("\n[STEP 3] SELECT MANY: All products by price (descending)")
        cursor.execute(
            "SELECT name, category, price FROM complex_products ORDER BY price DESC"
        )
        rows = cursor.fetchall()
        for name, category, price in rows:
            print(f"  ${price:>8.2f} | {category:<12} | {name}")
        print(f"✓ Retrieved {len(rows)} products")

        # Step 4: SELECT FEW - Filter by category and price range
        print("\n[STEP 4] SELECT FEW: Electronics between $50-$150")
        cursor.execute(
            """SELECT name, price FROM complex_products
               WHERE category = ? AND price BETWEEN ? AND ?
               AND active = ?""",
            ("Electronics", 50.0, 150.0, 1),
        )
        rows = cursor.fetchall()
        for name, price in rows:
            print(f"  {name}: ${price:.2f}")
        print(f"✓ Found {len(rows)} products matching criteria")

        # Step 5: SELECT ONE - Find specific product
        print("\n[STEP 5] SELECT ONE: Find 'Wireless Mouse'")
        cursor.execute(
            """SELECT name, category, price, stock
               FROM complex_products WHERE name = ?""",
            ("Wireless Mouse",),
        )
        row = cursor.fetchone()
        if row:
            print(f"  Found: {row[0]}")
            print(f"  Category: {row[1]}, Price: ${row[2]:.2f}, Stock: {row[3]}")
            print("✓ Single product retrieved")

        # Step 6: UPDATE - Increase prices for Electronics by 10%
        print("\n[STEP 6] UPDATE: Increase Electronics prices by 10%")
        cursor.execute(
            "UPDATE complex_products SET price = price * 1.10 WHERE category = ?",
            ("Electronics",),
        )
        conn.commit()
        print(f"✓ Updated {cursor.rowcount} products")

        # Step 7: SELECT ONE (verify update)
        print("\n[STEP 7] SELECT ONE: Verify Laptop Pro new price")
        cursor.execute(
            "SELECT name, price FROM complex_products WHERE name = ?",
            ("Laptop Pro",),
        )
        row = cursor.fetchone()
        if row:
            print(f"  {row[0]}: ${row[1]:.2f} (was $1299.99)")
            print("✓ Price increase verified")

        # Step 8: UPDATE - Restock low inventory items
        print("\n[STEP 8] UPDATE: Add 10 units to products with stock < 25")
        cursor.execute(
            "UPDATE complex_products SET stock = stock + 10 WHERE stock < ? AND active = ?",
            (25, 1),
        )
        conn.commit()
        print(f"✓ Restocked {cursor.rowcount} products")

        # Step 9: DELETE - Remove inactive products
        print("\n[STEP 9] DELETE: Remove inactive products")
        cursor.execute(
            "SELECT name FROM complex_products WHERE active = ?",
            (0,),
        )
        inactive = cursor.fetchall()
        for name, in inactive:
            print(f"  Removing: {name}")
        cursor.execute("DELETE FROM complex_products WHERE active = ?", (0,))
        conn.commit()
        print(f"✓ Deleted {len(inactive)} inactive products")

        # Step 10: SELECT MANY - Final inventory summary by category
        print("\n[STEP 10] SELECT MANY: Inventory summary by category")
        cursor.execute(
            """SELECT category, COUNT(*) as count, SUM(stock) as total_stock,
                      AVG(price) as avg_price
               FROM complex_products
               GROUP BY category ORDER BY avg_price DESC"""
        )
        rows = cursor.fetchall()
        print(f"  {'Category':<15} {'Count':>6} {'Stock':>8} {'Avg Price':>10}")
        print("  " + "-" * 42)
        for category, count, total_stock, avg_price in rows:
            print(f"  {category:<15} {count:>6} {total_stock:>8} ${avg_price:>9.2f}")
        print(f"✓ Summary generated for {len(rows)} categories")

        # Step 11: SELECT ONE (deleted item - should be None)
        print("\n[STEP 11] SELECT ONE: Query deleted product (Monitor Stand)")
        cursor.execute(
            "SELECT name FROM complex_products WHERE name = ?",
            ("Monitor Stand",),
        )
        row = cursor.fetchone()
        if row is None:
            print("  Monitor Stand not found (successfully deleted)")
            print("✓ Non-existent query returns None as expected")

        # Step 12: SELECT with fetchmany - Paginated results
        print("\n[STEP 12] FETCH MANY: Paginated product list (page size=3)")
        cursor.execute("SELECT name, price FROM complex_products ORDER BY price")
        cursor.arraysize = 3
        page_num = 1
        while True:
            page = cursor.fetchmany()
            if not page:
                break
            print(f"  Page {page_num}:")
            for name, price in page:
                print(f"    - {name}: ${price:.2f}")
            page_num += 1
        print("✓ Pagination completed")

        cursor.close()
        print("\n✅ Complex workflow completed successfully!")

    finally:
        conn.close()


def main():
    """Run all examples."""
    # Parse command line arguments
    parser = argparse.ArgumentParser(
        description="rqlite basic usage examples",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  uv run python -B examples/basic_usage.py              # Without lock (shows warnings)
  uv run python -B examples/basic_usage.py --with-lock  # With lock (no warnings)
        """
    )
    parser.add_argument(
        "--with-lock",
        action="store_true",
        help="Use ThreadLock to suppress transaction warnings"
    )
    args = parser.parse_args()

    use_lock = args.with_lock
    mode = "WITH LOCK" if use_lock else "WITHOUT LOCK"

    print("=" * 60)
    print(f"rqlite Basic Usage Examples ({mode})")
    print("=" * 60)

    try:
        create_table(use_lock=use_lock)
        insert_data(use_lock=use_lock)
        query_data(use_lock=use_lock)
        update_data(use_lock=use_lock)
        batch_insert(use_lock=use_lock)
        query_data(use_lock=use_lock)
        delete_data(use_lock=use_lock)
        context_manager_example(use_lock=use_lock)
        complex_workflow(use_lock=use_lock)

        print("\n" + "=" * 60)
        print("All examples completed successfully!")
        print("=" * 60)
    except rqlite.OperationalError as e:
        print(f"\n✗ Database error: {e}")
        print("Make sure rqlite is running on localhost:4001")
    except rqlite.ProgrammingError as e:
        print(f"\n✗ SQL error: {e}")


if __name__ == "__main__":
    main()
