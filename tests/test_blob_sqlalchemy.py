"""Tests for BLOB (LargeBinary) support in rqlite SQLAlchemy dialect.

Covers:
- Sync SQLAlchemy with LargeBinary column
- Async SQLAlchemy with LargeBinary column
- ORM model with LargeBinary
- Core insert/select with LargeBinary
- Binary() availability on dbapi module
- Empty BLOB, large BLOB, multiple rows

Usage:
    pytest tests/test_blob_sqlalchemy.py -v
"""

import asyncio

import pytest
from sqlalchemy import (
    LargeBinary,
    String,
    create_engine,
    insert,
    select,
    text,
)
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column

import rqlite


def run_async(coro):
    """Helper to run async code in a new event loop."""
    try:
        return asyncio.get_running_loop().run_until_complete(coro)
    except RuntimeError:
        return asyncio.run(coro)


# ---------------------------------------------------------------------------
# Sync models
# ---------------------------------------------------------------------------

class BaseSync(DeclarativeBase):
    """Base class for sync SQLAlchemy models."""


class BlobAttachment(BaseSync):
    """Model with LargeBinary column."""

    __tablename__ = "sa_blob_attachment"

    id: Mapped[int] = mapped_column(primary_key=True)
    filename: Mapped[str] = mapped_column(String(255), nullable=False)
    mimetype: Mapped[str] = mapped_column(String(255), nullable=False)
    data: Mapped[bytes] = mapped_column(LargeBinary, nullable=False)
    size: Mapped[int] = mapped_column(nullable=False)


# ---------------------------------------------------------------------------
# Async models
# ---------------------------------------------------------------------------

class BaseAsync(DeclarativeBase):
    """Base class for async SQLAlchemy models."""


class AsyncBlobAttachment(BaseAsync):
    """Async model with LargeBinary column."""

    __tablename__ = "sa_async_blob_attachment"

    id: Mapped[int] = mapped_column(primary_key=True)
    filename: Mapped[str] = mapped_column(String(255), nullable=False)
    mimetype: Mapped[str] = mapped_column(String(255), nullable=False)
    data: Mapped[bytes] = mapped_column(LargeBinary, nullable=False)
    size: Mapped[int] = mapped_column(nullable=False)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="function")
def sync_engine():
    """Create sync SQLAlchemy engine with lock."""
    engine = create_engine(
        "rqlite://localhost:4001",
        connect_args={"lock": rqlite.ThreadLock()},
        echo=False,
    )
    yield engine
    engine.dispose()


@pytest.fixture(scope="function")
def sync_tables(sync_engine):
    """Create and cleanup sync tables."""
    with sync_engine.connect() as conn:
        conn.execute(text("DROP TABLE IF EXISTS sa_blob_attachment"))
        conn.commit()
    BaseSync.metadata.create_all(sync_engine)
    yield
    BaseSync.metadata.drop_all(sync_engine)


@pytest.fixture(scope="function")
def async_engine():
    """Create async SQLAlchemy engine with lock."""
    engine = create_async_engine(
        "rqlite+aiorqlite://localhost:4001",
        connect_args={"lock": rqlite.AioLock()},
        echo=False,
    )
    yield engine
    run_async(engine.dispose())  # ty: ignore[unused-awaitable]


# ---------------------------------------------------------------------------
# Tests: dbapi.Binary availability
# ---------------------------------------------------------------------------

class TestDBAPIBinaryAvailability:
    """Test that dbapi.Binary is available for both dialects."""

    def test_sync_dialect_has_binary(self):
        """Sync dialect's dbapi exposes Binary."""
        engine = create_engine("rqlite://localhost:4001")
        assert hasattr(engine.dialect.dbapi, "Binary")
        engine.dispose()

    def test_async_dialect_has_binary(self):
        """Async dialect's dbapi exposes Binary."""
        engine = create_async_engine("rqlite+aiorqlite://localhost:4001")
        # The dbapi is accessed via the driver's import_dbapi
        from rqlite.sqlalchemy.async_dialect import AioRQLiteDBAPI

        dbapi = AioRQLiteDBAPI()
        assert hasattr(dbapi, "Binary")
        assert callable(dbapi.Binary)
        result = dbapi.Binary(b"test")
        assert isinstance(result, memoryview)


# ---------------------------------------------------------------------------
# Tests: Sync SQLAlchemy LargeBinary
# ---------------------------------------------------------------------------

class TestSyncSQLAlchemyLargeBinary:
    """Test sync SQLAlchemy with LargeBinary columns."""

    def test_create_table_with_large_binary(self, sync_engine):
        """CREATE TABLE with LargeBinary column works."""
        BaseSync.metadata.create_all(sync_engine)
        with sync_engine.connect() as conn:
            result = conn.execute(
                text("SELECT name FROM sqlite_master WHERE type='table'")
            )
            tables = [row[0] for row in result]
            assert "sa_blob_attachment" in tables

    def test_insert_and_select_large_binary(self, sync_engine, sync_tables):
        """INSERT and SELECT LargeBinary via Core."""
        original = b"Hello BLOB World!"
        with sync_engine.connect() as conn:
            stmt = insert(BlobAttachment).values(
                filename="test.bin",
                mimetype="application/octet-stream",
                data=original,
                size=len(original),
            )
            conn.execute(stmt)
            conn.commit()

            result = conn.execute(
                select(BlobAttachment).where(BlobAttachment.filename == "test.bin")
            )
            row = result.fetchone()
            assert row is not None
            assert row.data == original
            assert isinstance(row.data, bytes)
            assert row.filename == "test.bin"

    def test_insert_and_select_orm(self, sync_engine, sync_tables):
        """INSERT and SELECT LargeBinary via ORM Session."""
        original = b"ORM blob data"
        with Session(sync_engine) as session:
            att = BlobAttachment(
                filename="orm.bin",
                mimetype="application/octet-stream",
                data=original,
                size=len(original),
            )
            session.add(att)
            session.commit()

            result = (
                session.query(BlobAttachment)
                .filter_by(filename="orm.bin")
                .first()
            )
            assert result is not None
            assert result.data == original
            assert isinstance(result.data, bytes)

    def test_empty_large_binary(self, sync_engine, sync_tables):
        """Empty LargeBinary value."""
        with Session(sync_engine) as session:
            att = BlobAttachment(
                filename="empty.bin",
                mimetype="application/octet-stream",
                data=b"",
                size=0,
            )
            session.add(att)
            session.commit()

            result = session.query(BlobAttachment).filter_by(filename="empty.bin").first()
            assert result is not None
            assert result.data == b""
            assert isinstance(result.data, bytes)

    def test_large_binary_data(self, sync_engine, sync_tables):
        """Large BLOB data (>1KB)."""
        original = bytes(range(256)) * 10  # 2.5KB
        with Session(sync_engine) as session:
            att = BlobAttachment(
                filename="large.bin",
                mimetype="application/octet-stream",
                data=original,
                size=len(original),
            )
            session.add(att)
            session.commit()

            result = session.query(BlobAttachment).filter_by(filename="large.bin").first()
            assert result is not None
            assert result.data == original
            assert isinstance(result.data, bytes)

    def test_multiple_large_binary_rows(self, sync_engine, sync_tables):
        """Multiple rows with LargeBinary."""
        payloads = [
            ("alpha.bin", b"alpha"),
            ("beta.bin", bytes([0x00, 0xFF, 0x80])),
            ("gamma.bin", b"gamma\x00\x01\x02"),
        ]
        with Session(sync_engine) as session:
            for fname, data in payloads:
                att = BlobAttachment(
                    filename=fname,
                    mimetype="application/octet-stream",
                    data=data,
                    size=len(data),
                )
                session.add(att)
            session.commit()

            results = (
                session.query(BlobAttachment)
                .order_by(BlobAttachment.filename)
                .all()
            )
            assert len(results) == 3
            for row, (expected_name, expected_data) in zip(results, payloads):
                assert row.filename == expected_name
                assert row.data == expected_data
                assert isinstance(row.data, bytes)

    def test_update_large_binary(self, sync_engine, sync_tables):
        """UPDATE LargeBinary column."""
        with Session(sync_engine) as session:
            att = BlobAttachment(
                filename="update.bin",
                mimetype="application/octet-stream",
                data=b"old",
                size=3,
            )
            session.add(att)
            session.commit()

            # Update
            att.data = b"new data"
            att.size = len(b"new data")
            session.commit()

            result = session.query(BlobAttachment).filter_by(filename="update.bin").first()
            assert result is not None
            assert result.data == b"new data"

    def test_binary_constructor_via_sqlalchemy(self, sync_engine, sync_tables):
        """Use Binary() constructor with SQLAlchemy."""
        original = rqlite.Binary(b"constructed via Binary")
        with Session(sync_engine) as session:
            att = BlobAttachment(
                filename="binary.bin",
                mimetype="application/octet-stream",
                data=original,
                size=len(original),
            )
            session.add(att)
            session.commit()

            result = session.query(BlobAttachment).filter_by(filename="binary.bin").first()
            assert result is not None
            assert result.data == b"constructed via Binary"


# ---------------------------------------------------------------------------
# Tests: Async SQLAlchemy LargeBinary
# ---------------------------------------------------------------------------

class TestAsyncSQLAlchemyLargeBinary:
    """Test async SQLAlchemy with LargeBinary columns."""

    def _create_tables(self, conn):
        """Sync callable for run_sync."""
        BaseAsync.metadata.create_all(conn)

    def test_async_insert_and_select_large_binary(self, async_engine):
        """Async: INSERT and SELECT LargeBinary via Core."""
        original = b"Async BLOB data"

        async def _test():
            async with async_engine.begin() as conn:
                await conn.execute(text("DROP TABLE IF EXISTS sa_async_blob_attachment"))
                await conn.run_sync(self._create_tables)

            async_session = async_sessionmaker(
                async_engine, class_=AsyncSession, expire_on_commit=False
            )
            async with async_session() as session:
                att = AsyncBlobAttachment(
                    filename="async.bin",
                    mimetype="application/octet-stream",
                    data=original,
                    size=len(original),
                )
                session.add(att)
                await session.commit()

                result = await session.execute(
                    AsyncBlobAttachment.__table__.select().where(
                        AsyncBlobAttachment.filename == "async.bin"
                    )
                )
                row = result.fetchone()
                assert row is not None
                assert row.data == original
                assert isinstance(row.data, bytes)
                assert row.filename == "async.bin"

        run_async(_test())

    def test_async_empty_large_binary(self, async_engine):
        """Async: Empty LargeBinary value."""

        async def _test():
            async with async_engine.begin() as conn:
                await conn.execute(text("DROP TABLE IF EXISTS sa_async_blob_attachment"))
                await conn.run_sync(self._create_tables)

            async_session = async_sessionmaker(
                async_engine, class_=AsyncSession, expire_on_commit=False
            )
            async with async_session() as session:
                att = AsyncBlobAttachment(
                    filename="async_empty.bin",
                    mimetype="application/octet-stream",
                    data=b"",
                    size=0,
                )
                session.add(att)
                await session.commit()

                result = await session.execute(
                    AsyncBlobAttachment.__table__.select().where(
                        AsyncBlobAttachment.filename == "async_empty.bin"
                    )
                )
                row = result.fetchone()
                assert row is not None
                assert row.data == b""
                assert isinstance(row.data, bytes)

        run_async(_test())

    def test_async_large_binary_data(self, async_engine):
        """Async: Large BLOB data (>1KB)."""
        original = bytes(range(256)) * 10  # 2.5KB

        async def _test():
            async with async_engine.begin() as conn:
                await conn.execute(text("DROP TABLE IF EXISTS sa_async_blob_attachment"))
                await conn.run_sync(self._create_tables)

            async_session = async_sessionmaker(
                async_engine, class_=AsyncSession, expire_on_commit=False
            )
            async with async_session() as session:
                att = AsyncBlobAttachment(
                    filename="async_large.bin",
                    mimetype="application/octet-stream",
                    data=original,
                    size=len(original),
                )
                session.add(att)
                await session.commit()

                result = await session.execute(
                    AsyncBlobAttachment.__table__.select().where(
                        AsyncBlobAttachment.filename == "async_large.bin"
                    )
                )
                row = result.fetchone()
                assert row is not None
                assert row.data == original
                assert isinstance(row.data, bytes)

        run_async(_test())

    def test_async_multiple_large_binary_rows(self, async_engine):
        """Async: Multiple rows with LargeBinary."""
        payloads = [
            ("async_alpha.bin", b"alpha"),
            ("async_beta.bin", bytes([0x00, 0xFF, 0x80])),
        ]

        async def _test():
            async with async_engine.begin() as conn:
                await conn.execute(text("DROP TABLE IF EXISTS sa_async_blob_attachment"))
                await conn.run_sync(self._create_tables)

            async_session = async_sessionmaker(
                async_engine, class_=AsyncSession, expire_on_commit=False
            )
            async with async_session() as session:
                for fname, data in payloads:
                    att = AsyncBlobAttachment(
                        filename=fname,
                        mimetype="application/octet-stream",
                        data=data,
                        size=len(data),
                    )
                    session.add(att)
                await session.commit()

                result = await session.execute(
                    select(AsyncBlobAttachment).order_by(AsyncBlobAttachment.filename)
                )
                rows = result.scalars().all()
                assert len(rows) == 2
                for row, (expected_name, expected_data) in zip(rows, payloads):
                    assert row.filename == expected_name
                    assert row.data == expected_data
                    assert isinstance(row.data, bytes)

        run_async(_test())
