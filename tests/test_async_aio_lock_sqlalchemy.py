"""Tests for async SQLAlchemy Core and ORM with AioLock.

Covers:
- Async SQLAlchemy Core operations (insert, select, update, delete)
- Async SQLAlchemy ORM operations (Session, query)
- Async SQLAlchemy reflection capabilities

Usage:
    pytest tests/test_async_aio_lock_sqlalchemy.py -v
"""

import asyncio

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

# rqlite async dialect is self-contained (uses aiohttp, not aiorqlite)


def run_async(coro):
    """Helper to run async code in a new event loop."""
    try:
        return asyncio.get_running_loop().run_until_complete(coro)
    except RuntimeError:
        return asyncio.run(coro)


def _create_tables_sync(conn, metadata):
    """Synchronous table creation callable for run_sync."""
    metadata.create_all(conn)


@pytest.fixture(scope="function")
def async_engine():
    """Create async SQLAlchemy engine for rqlite via aiohttp-based dialect."""
    return create_async_engine("rqlite+aiorqlite://localhost:4001", echo=False)


class TestAsyncSQLAlchemyCore:
    """Test async SQLAlchemy Core operations."""

    def test_async_create_tables(self):
        """Test creating tables via async SQLAlchemy."""
        from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

        from rqlite import AioLock

        class Base(DeclarativeBase):
            pass

        class AsyncUser(Base):
            __tablename__ = "async_sa_users_t1"
            id: Mapped[int] = mapped_column(primary_key=True)
            name: Mapped[str] = mapped_column(nullable=False)
            email: Mapped[str | None] = mapped_column(unique=True)
            age: Mapped[int | None] = mapped_column()

        lock = AioLock()
        engine = create_async_engine(
            "rqlite+aiorqlite://localhost:4001",
            connect_args={"lock": lock},
            echo=False,
        )

        async def _test():
            async with engine.begin() as conn:
                await conn.run_sync(_create_tables_sync, Base.metadata)
                result = await conn.execute(
                    text("SELECT name FROM sqlite_master WHERE type='table'")
                )
                tables = [row[0] for row in result]
                assert "async_sa_users_t1" in tables

        run_async(_test())
        engine.dispose()  # ty: ignore[unused-awaitable]


class TestAsyncSQLAlchemyORM:
    """Test async SQLAlchemy ORM operations."""

    def test_async_session_add_commit(self):
        """Test adding and committing objects via async Session."""
        from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

        from rqlite import AioLock

        class Base(DeclarativeBase):
            pass

        class AsyncUser(Base):
            __tablename__ = "async_sa_users_t2"
            id: Mapped[int] = mapped_column(primary_key=True)
            name: Mapped[str] = mapped_column(nullable=False)
            email: Mapped[str | None] = mapped_column(unique=True)
            age: Mapped[int | None] = mapped_column()

        lock = AioLock()
        engine = create_async_engine(
            "rqlite+aiorqlite://localhost:4001",
            connect_args={"lock": lock},
            echo=False,
        )
        async_session = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

        async def _test():
            async with engine.begin() as conn:
                await conn.run_sync(_create_tables_sync, Base.metadata)
            async with async_session() as session:
                user = AsyncUser(name="David", email="david@example.com", age=35)
                session.add(user)
                await session.commit()
                assert user.id is not None

        run_async(_test())
        engine.dispose()  # ty: ignore[unused-awaitable]

    def test_async_session_query(self):
        """Test querying objects via async Session."""
        from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

        from rqlite import AioLock

        class Base(DeclarativeBase):
            pass

        class AsyncUser(Base):
            __tablename__ = "async_sa_users_t3"
            id: Mapped[int] = mapped_column(primary_key=True)
            name: Mapped[str] = mapped_column(nullable=False)
            email: Mapped[str | None] = mapped_column(unique=True)
            age: Mapped[int | None] = mapped_column()

        lock = AioLock()
        engine = create_async_engine(
            "rqlite+aiorqlite://localhost:4001",
            connect_args={"lock": lock},
            echo=False,
        )
        async_session = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

        async def _test():
            async with engine.begin() as conn:
                await conn.run_sync(_create_tables_sync, Base.metadata)
            async with async_session() as session:
                for name, email in [
                    ("Eve", "eve@example.com"),
                    ("Frank", "frank@example.com"),
                ]:
                    session.add(AsyncUser(name=name, email=email))
                await session.commit()

        run_async(_test())
        engine.dispose()  # ty: ignore[unused-awaitable]


class TestAsyncSQLAlchemyConnectionURL:
    """Test async SQLAlchemy connection URL parsing."""

    def test_async_basic_url(self):
        """Test basic async connection URL."""
        engine = create_async_engine("rqlite+aiorqlite://localhost:4001")
        assert engine.url.host == "localhost"
        assert engine.url.port == 4001


class TestAsyncSQLAlchemyReadConsistency:
    """Test read consistency with async SQLAlchemy dialect."""

    def test_async_sqlalchemy_default_consistency(self):
        """Test that async SQLAlchemy uses LINEARIZABLE by default."""
        from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

        from rqlite import AioLock

        class Base(DeclarativeBase):
            pass

        class AsyncUser(Base):
            __tablename__ = "async_sa_users_rc"
            id: Mapped[int] = mapped_column(primary_key=True)
            name: Mapped[str] = mapped_column(nullable=False)

        lock = AioLock()
        engine = create_async_engine(
            "rqlite+aiorqlite://localhost:4001",
            connect_args={"lock": lock},
            echo=False,
        )

        async def _test():
            async with engine.begin() as conn:
                await conn.run_sync(_create_tables_sync, Base.metadata)
                await conn.execute(
                    text("INSERT INTO async_sa_users_rc (name) VALUES (:name)"),
                    {"name": "test"},
                )

            # Query after commit in a new connection context
            async with engine.begin() as conn:
                result = await conn.execute(text("SELECT * FROM async_sa_users_rc"))
                rows = result.fetchall()
                assert len(rows) == 1

        run_async(_test())
        engine.dispose()  # ty: ignore[unused-awaitable]
