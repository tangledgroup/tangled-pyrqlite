# ty: ignore[unresolved-attribute]
"""BLOB (LargeBinary) SQLAlchemy ORM examples for rqlite.

This example demonstrates using SQLAlchemy ORM with LargeBinary columns
for storing BLOB data, both sync and async.

Key concepts:
    - LargeBinary maps to SQLite BLOB type
    - Python bytes are stored/retrieved transparently
    - Works with both sync (create_engine) and async (create_async_engine)
    - Binary() constructor available via dialect.dbapi.Binary

Prerequisites:
    - rqlite server running on localhost:4001

Usage:
    uv run python -B examples/blob_sqlalchemy_orm.py
"""

from __future__ import annotations

import argparse
import asyncio
import functools
from collections.abc import Callable
from typing import Any

from sqlalchemy import LargeBinary, String, create_engine, select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column

from rqlite import AioLock, ThreadLock


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
# Models
# ---------------------------------------------------------------------------

class Base(DeclarativeBase):
    """Base class for all models."""


class Attachment(Base):
    """File attachment model with LargeBinary column.

    This mirrors a real-world use case: storing file contents (PDFs,
    images, documents) in the database alongside metadata.
    """

    __tablename__ = "sa_blob_demo"

    id: Mapped[int] = mapped_column(primary_key=True)
    filename: Mapped[str] = mapped_column(String(255), nullable=False)
    mimetype: Mapped[str] = mapped_column(String(255), nullable=False)
    data: Mapped[bytes] = mapped_column(LargeBinary, nullable=False)
    size: Mapped[int] = mapped_column(nullable=False)


class AsyncAttachment(Base):
    """Same model for async engine (separate Base avoids conflicts)."""

    __tablename__ = "sa_async_blob_demo"

    id: Mapped[int] = mapped_column(primary_key=True)
    filename: Mapped[str] = mapped_column(String(255), nullable=False)
    mimetype: Mapped[str] = mapped_column(String(255), nullable=False)
    data: Mapped[bytes] = mapped_column(LargeBinary, nullable=False)
    size: Mapped[int] = mapped_column(nullable=False)


# ---------------------------------------------------------------------------
# Sync examples
# ---------------------------------------------------------------------------

@print_docstring
def create_tables():
    """Create database tables."""
    engine = create_engine(
        "rqlite://localhost:4001",
        connect_args={"lock": ThreadLock()},
        echo=False,
    )
    with engine.connect() as conn:
        conn.execute(text("DROP TABLE IF EXISTS sa_blob_demo"))
        conn.commit()
    Base.metadata.create_all(engine)
    engine.dispose()
    print("✓ Tables created")


@print_docstring
def add_attachments():
    """Add file attachments with binary data."""
    engine = create_engine(
        "rqlite://localhost:4001",
        connect_args={"lock": ThreadLock()},
        echo=False,
    )

    with Session(engine) as session:
        # Text file
        text_data = b"Hello, this is a text file stored in the database!"
        session.add(Attachment(
            filename="hello.txt",
            mimetype="text/plain",
            data=text_data,
            size=len(text_data),
        ))

        # Binary data (simulated image header)
        png_header = bytes([
            0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A,
            0x00, 0x00, 0x00, 0x0D, 0x49, 0x48, 0x44, 0x52,
        ])
        session.add(Attachment(
            filename="image.png",
            mimetype="image/png",
            data=png_header,
            size=len(png_header),
        ))

        # Large binary data
        large_data = bytes(range(256)) * 10  # 2.5KB
        session.add(Attachment(
            filename="large.bin",
            mimetype="application/octet-stream",
            data=large_data,
            size=len(large_data),
        ))

        session.commit()
        print(f"✓ Added {session.query(Attachment).count()} attachments")

    engine.dispose()


@print_docstring
def query_attachments():
    """Query and display attachments."""
    engine = create_engine(
        "rqlite://localhost:4001",
        connect_args={"lock": ThreadLock()},
        echo=False,
    )

    with Session(engine) as session:
        print("\nAll attachments:")
        for att in session.query(Attachment).order_by(Attachment.filename).all():
            preview = att.data[:30] if len(att.data) > 30 else att.data
            print(f"  {att.filename} ({att.mimetype})")
            print(f"    size: {att.size} bytes")
            print(f"    preview: {preview!r}")

        # Find by mimetype
        print("\nBinary files (application/octet-stream):")
        binary_files = (
            session.query(Attachment)
            .filter(Attachment.mimetype == "application/octet-stream")
            .all()
        )
        for att in binary_files:
            print(f"  {att.filename}: {att.size} bytes")

        # Verify round-trip
        print("\nVerifying data integrity:")
        hello = session.query(Attachment).filter_by(filename="hello.txt").first()
        assert hello is not None
        print(f"  ✓ {hello.filename}: {len(hello.data)} bytes retrieved")

    engine.dispose()


@print_docstring
def update_attachment():
    """Update attachment data."""
    engine = create_engine(
        "rqlite://localhost:4001",
        connect_args={"lock": ThreadLock()},
        echo=False,
    )

    with Session(engine) as session:
        att = session.query(Attachment).filter_by(filename="hello.txt").first()
        if att:
            old_data = att.data
            att.data = b"Updated content - the file has been modified!"
            att.size = len(att.data)
            session.commit()
            print(f"  Updated {att.filename}")
            print(f"  Old: {old_data!r}")
            print(f"  New: {att.data!r}")

    engine.dispose()


@print_docstring
def delete_attachment():
    """Delete an attachment."""
    engine = create_engine(
        "rqlite://localhost:4001",
        connect_args={"lock": ThreadLock()},
        echo=False,
    )

    with Session(engine) as session:
        att = session.query(Attachment).filter_by(filename="large.bin").first()
        if att:
            session.delete(att)
            session.commit()
            print(f"✓ Deleted {att.filename}")

    engine.dispose()


@print_docstring
def cleanup():
    """Clean up tables."""
    engine = create_engine(
        "rqlite://localhost:4001",
        connect_args={"lock": ThreadLock()},
        echo=False,
    )
    Base.metadata.drop_all(engine)
    engine.dispose()
    print("✓ Cleanup complete")


# ---------------------------------------------------------------------------
# Async examples
# ---------------------------------------------------------------------------

@print_docstring
async def async_blob_workflow():
    """Async: Complete BLOB workflow with SQLAlchemy async engine."""
    engine = create_async_engine(
        "rqlite+aiorqlite://localhost:4001",
        connect_args={"lock": AioLock()},
        echo=False,
    )
    async_session = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

    # Create table
    async with engine.begin() as conn:
        await conn.execute(text("DROP TABLE IF EXISTS sa_async_blob_demo"))

        def create_all(c):
            Base.metadata.create_all(c)

        await conn.run_sync(create_all)

    # Insert and query
    async with async_session() as session:
        # Insert
        data = b"Async SQLAlchemy BLOB data!"
        att = AsyncAttachment(
            filename="async_file.bin",
            mimetype="application/octet-stream",
            data=data,
            size=len(data),
        )
        session.add(att)
        await session.commit()

        # Query back
        result = await session.execute(
            select(AsyncAttachment).where(AsyncAttachment.filename == "async_file.bin")
        )
        row = result.scalar_one_or_none()
        assert row is not None
        print(f"  {row.filename}: {len(row.data)} bytes")
        print(f"  data: {row.data!r}")
        assert row.data == data
        print("✓ Async BLOB round-trip verified")

    # Cleanup
    async with engine.begin() as conn:
        await conn.execute(text("DROP TABLE IF EXISTS sa_async_blob_demo"))

    await engine.dispose()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    """Run all BLOB SQLAlchemy examples."""
    parser = argparse.ArgumentParser(
        description="rqlite BLOB SQLAlchemy ORM examples",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  uv run python -B examples/blob_sqlalchemy_orm.py
        """,
    )
    args = parser.parse_args()

    print("=" * 60)
    print("rqlite BLOB SQLAlchemy ORM Examples (Sync + Async)")
    print("=" * 60)

    try:
        # Sync demos
        create_tables()
        add_attachments()
        query_attachments()
        update_attachment()
        delete_attachment()
        query_attachments()

        # Async demo
        asyncio.run(async_blob_workflow())

        # Cleanup
        cleanup()

        print("\n" + "=" * 60)
        print("All BLOB SQLAlchemy examples completed!")
        print("=" * 60)
    except Exception as e:
        print(f"\n✗ Error: {e}")
        print("Make sure rqlite is running on localhost:4001")
        import traceback

        traceback.print_exc()


if __name__ == "__main__":
    main()
