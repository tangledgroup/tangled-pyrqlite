# ty: ignore[unresolved-attribute]
"""BLOB (Binary/LargeBinary) examples for rqlite.

This example demonstrates working with BLOB data using the raw DB-API 2.0
interface, both sync and async.

Key concepts:
    - Binary() constructor (DB-API 2.0) wraps bytes as memoryview
    - BLOB columns store binary data transparently
    - rqlite serializes bytes as JSON int arrays for unambiguous storage
    - On read, BLOB data is decoded back to Python bytes

Prerequisites:
    - rqlite server running on localhost:4001

Usage:
    uv run python -B examples/blob_basic_usage.py
"""

from __future__ import annotations

import argparse
import asyncio
import functools
from collections.abc import Callable
from typing import Any

import rqlite


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


# ---------------------------------------------------------------------------
# Sync examples
# ---------------------------------------------------------------------------

@print_docstring
def demo_binary_constructor():
    """Demonstrate Binary() constructor (DB-API 2.0)."""
    # Binary() wraps bytes as memoryview — matches sqlite3.Binary behavior
    blob = rqlite.Binary(b"Hello BLOB")
    print(f"  Binary(b'Hello BLOB') → {blob}")
    print(f"  Type: {type(blob).__name__}")
    print(f"  Back to bytes: {bytes(blob)!r}")

    # Also works with bytearray and memoryview
    blob2 = rqlite.Binary(bytearray(b"bytearray"))
    print(f"  Binary(bytearray(...)) → type={type(blob2).__name__}")


@print_docstring
def demo_blob_create_table():
    """Create a table with BLOB column."""
    conn = rqlite.connect(host="localhost", port=4001, lock=rqlite.ThreadLock())
    try:
        cur = conn.cursor()
        cur.execute("DROP TABLE IF EXISTS blob_demo")
        cur.execute(
            "CREATE TABLE blob_demo ("
            "id INTEGER PRIMARY KEY, "
            "filename TEXT NOT NULL, "
            "data BLOB"
            ")"
        )
        conn.commit()
        print("✓ Table 'blob_demo' created with BLOB column")
    finally:
        conn.close()


@print_docstring
def demo_blob_insert():
    """Insert BLOB data using Python bytes."""
    conn = rqlite.connect(host="localhost", port=4001, lock=rqlite.ThreadLock())
    try:
        cur = conn.cursor()

        # Insert plain bytes
        cur.execute(
            "INSERT INTO blob_demo (filename, data) VALUES (?, ?)",
            ("hello.txt", b"Hello, World!"),
        )

        # Insert using Binary() constructor
        cur.execute(
            "INSERT INTO blob_demo (filename, data) VALUES (?, ?)",
            ("binary.bin", rqlite.Binary(b"\x00\x01\x02\x03\x04")),
        )

        # Insert large binary data
        image_data = bytes(range(256)) * 10  # 2.5KB
        cur.execute(
            "INSERT INTO blob_demo (filename, data) VALUES (?, ?)",
            ("large.bin", image_data),
        )

        # Insert empty BLOB
        cur.execute(
            "INSERT INTO blob_demo (filename, data) VALUES (?, ?)",
            ("empty.bin", b""),
        )

        conn.commit()
        print("✓ Inserted 4 BLOB rows")
    finally:
        conn.close()


@print_docstring
def demo_blob_select():
    """Select and read BLOB data back as Python bytes."""
    conn = rqlite.connect(host="localhost", port=4001, lock=rqlite.ThreadLock())
    try:
        cur = conn.cursor()
        cur.execute("SELECT filename, data FROM blob_demo ORDER BY id")
        rows = cur.fetchall()

        for filename, data in rows:
            size = len(data) if data is not None else 0
            preview = data[:20] if data and len(data) > 20 else data
            print(f"  {filename}: {size} bytes, preview={preview!r}")

        # Verify exact round-trip
        cur.execute(
            "SELECT data FROM blob_demo WHERE filename = ?",
            ("hello.txt",),
        )
        row = cur.fetchone()
        assert row[0] == b"Hello, World!", "BLOB round-trip failed!"
        print("✓ BLOB round-trip verified: data matches exactly")
    finally:
        conn.close()


@print_docstring
def demo_blob_update():
    """Update BLOB data."""
    conn = rqlite.connect(host="localhost", port=4001, lock=rqlite.ThreadLock())
    try:
        cur = conn.cursor()

        # Read original
        cur.execute(
            "SELECT data FROM blob_demo WHERE filename = ?",
            ("hello.txt",),
        )
        original = cur.fetchone()[0]
        print(f"  Original: {original!r}")

        # Update
        cur.execute(
            "UPDATE blob_demo SET data = ? WHERE filename = ?",
            (b"Updated content!", "hello.txt"),
        )
        conn.commit()

        # Verify
        cur.execute(
            "SELECT data FROM blob_demo WHERE filename = ?",
            ("hello.txt",),
        )
        updated = cur.fetchone()[0]
        print(f"  Updated:  {updated!r}")
        assert updated == b"Updated content!"
        print("✓ BLOB update verified")
    finally:
        conn.close()


@print_docstring
def demo_blob_cleanup():
    """Clean up demo table."""
    conn = rqlite.connect(host="localhost", port=4001, lock=rqlite.ThreadLock())
    try:
        cur = conn.cursor()
        cur.execute("DROP TABLE IF EXISTS blob_demo")
        conn.commit()
        print("✓ Cleanup complete")
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Async examples
# ---------------------------------------------------------------------------

@print_docstring
async def demo_async_blob():
    """Async: Insert and read BLOB data."""
    conn = rqlite.async_connect(
        host="localhost", port=4001, lock=rqlite.AioLock()
    )
    try:
        cur = await conn.cursor()

        # Create table
        await cur.execute("DROP TABLE IF EXISTS async_blob_demo")
        await cur.execute(
            "CREATE TABLE async_blob_demo ("
            "id INTEGER PRIMARY KEY, "
            "filename TEXT NOT NULL, "
            "data BLOB"
            ")"
        )
        await conn.commit()

        # Insert
        await cur.execute(
            "INSERT INTO async_blob_demo (filename, data) VALUES (?, ?)",
            ("async.txt", b"Async BLOB data!"),
        )
        await conn.commit()

        # Read back
        await cur.execute(
            "SELECT filename, data FROM async_blob_demo WHERE filename = ?",
            ("async.txt",),
        )
        row = cur.fetchone()
        print(f"  {row[0]}: {row[1]!r}")
        assert row[1] == b"Async BLOB data!"
        print("✓ Async BLOB round-trip verified")

        # Cleanup
        await cur.execute("DROP TABLE IF EXISTS async_blob_demo")
        await conn.commit()
    finally:
        await conn.close()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    """Run all BLOB examples."""
    parser = argparse.ArgumentParser(
        description="rqlite BLOB (Binary/LargeBinary) examples",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  uv run python -B examples/blob_basic_usage.py
        """,
    )
    args = parser.parse_args()

    print("=" * 60)
    print("rqlite BLOB Examples (Sync + Async)")
    print("=" * 60)

    try:
        # Sync demos
        demo_binary_constructor()
        demo_blob_create_table()
        demo_blob_insert()
        demo_blob_select()
        demo_blob_update()

        # Async demo
        asyncio.run(demo_async_blob())

        # Cleanup
        demo_blob_cleanup()

        print("\n" + "=" * 60)
        print("All BLOB examples completed!")
        print("=" * 60)
    except Exception as e:
        print(f"\n✗ Error: {e}")
        print("Make sure rqlite is running on localhost:4001")
        import traceback

        traceback.print_exc()


if __name__ == "__main__":
    main()
