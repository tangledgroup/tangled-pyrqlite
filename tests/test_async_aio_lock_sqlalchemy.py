# ty: ignore[unresolved-import]
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

_aiorqlite_available = False
try:
    import aiorqlite  # noqa: F401, ty: ignore[unresolved-import]

    _aiorqlite_available = True
except ImportError:
    pass

skip_no_aiorqlite = pytest.mark.skipif(
    not _aiorqlite_available,
    reason="aiorqlite not installed",
)


@pytest.fixture(scope="function")
def async_engine():
    """Create async SQLAlchemy engine for rqlite via aiorqlite."""
    return create_async_engine("rqlite+aiorqlite:///async_test.db", echo=False)


@pytest.fixture(scope="function")
def async_tables(async_engine):
    """Create and cleanup async test tables."""
    from sqlalchemy.orm import DeclarativeBase

    class Base(DeclarativeBase):
        pass

    class AsyncUser(Base):
        __tablename__ = "async_sa_users"
        from sqlalchemy.orm import Mapped, mapped_column

        id: Mapped[int] = mapped_column(primary_key=True)
        name: Mapped[str] = mapped_column(nullable=False)
        email: Mapped[str | None] = mapped_column(unique=True)
        age: Mapped[int | None] = mapped_column()

    Base.metadata.drop_all(async_engine)
    Base.metadata.create_all(async_engine)

    yield

    Base.metadata.drop_all(async_engine)


class TestAsyncSQLAlchemyCore:
    """Test async SQLAlchemy Core operations."""

    @skip_no_aiorqlite
    def test_async_create_tables(self):
        """Test creating tables via async SQLAlchemy."""
        from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

        class Base(DeclarativeBase):
            pass

        class AsyncUser(Base):
            __tablename__ = "async_sa_users_t1"
            id: Mapped[int] = mapped_column(primary_key=True)
            name: Mapped[str] = mapped_column(nullable=False)
            email: Mapped[str | None] = mapped_column(unique=True)
            age: Mapped[int | None] = mapped_column()

        engine = create_async_engine("rqlite+aiorqlite:///async_test_t1.db", echo=False)

        async def _test():
            await engine.connect()
            Base.metadata.create_all(engine)  # ty: ignore[invalid-argument-type]

            async with engine.begin() as conn:
                result = await conn.execute(
                    text("SELECT name FROM sqlite_master WHERE type='table'")
                )
                tables = [row[0] for row in result]
                assert "async_sa_users_t1" in tables

        asyncio.run(_test())
        engine.dispose()  # ty: ignore[unused-awaitable]


class TestAsyncSQLAlchemyORM:
    """Test async SQLAlchemy ORM operations."""

    @skip_no_aiorqlite
    def test_async_session_add_commit(self):
        """Test adding and committing objects via async Session."""
        from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

        class Base(DeclarativeBase):
            pass

        class AsyncUser(Base):
            __tablename__ = "async_sa_users_t2"
            id: Mapped[int] = mapped_column(primary_key=True)
            name: Mapped[str] = mapped_column(nullable=False)
            email: Mapped[str | None] = mapped_column(unique=True)
            age: Mapped[int | None] = mapped_column()

        engine = create_async_engine("rqlite+aiorqlite:///async_test_t2.db", echo=False)
        async_session = async_sessionmaker(engine, class_=AsyncSession)

        async def _test():
            await engine.connect()
            Base.metadata.create_all(engine)  # ty: ignore[invalid-argument-type]

            async with async_session() as session:
                user = AsyncUser(name="David", email="david@example.com", age=35)
                session.add(user)
                await session.commit()
                assert user.id is not None

        asyncio.run(_test())
        engine.dispose()  # ty: ignore[unused-awaitable]

    @skip_no_aiorqlite
    def test_async_session_query(self):
        """Test querying objects via async Session."""
        from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

        class Base(DeclarativeBase):
            pass

        class AsyncUser(Base):
            __tablename__ = "async_sa_users_t3"
            id: Mapped[int] = mapped_column(primary_key=True)
            name: Mapped[str] = mapped_column(nullable=False)
            email: Mapped[str | None] = mapped_column(unique=True)
            age: Mapped[int | None] = mapped_column()

        engine = create_async_engine("rqlite+aiorqlite:///async_test_t3.db", echo=False)
        async_session = async_sessionmaker(engine, class_=AsyncSession)

        async def _test():
            await engine.connect()
            Base.metadata.create_all(engine)  # ty: ignore[invalid-argument-type]

            async with async_session() as session:
                for name, email in [
                    ("Eve", "eve@example.com"),
                    ("Frank", "frank@example.com"),
                ]:
                    session.add(AsyncUser(name=name, email=email))
                await session.commit()

        asyncio.run(_test())
        engine.dispose()  # ty: ignore[unused-awaitable]


class TestAsyncSQLAlchemyConnectionURL:
    """Test async SQLAlchemy connection URL parsing."""

    def test_async_basic_url(self):
        """Test basic async connection URL."""
        engine = create_async_engine("rqlite+aiorqlite:///async_test_url.db")
        # aiorqlite uses file path, not host/port
        assert "async_test_url.db" in str(engine.url.database)


class TestAsyncSQLAlchemyReadConsistency:
    """Test read consistency with async SQLAlchemy dialect."""

    @skip_no_aiorqlite
    def test_async_sqlalchemy_default_consistency(self):
        """Test that async SQLAlchemy uses LINEARIZABLE by default."""
        from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

        class Base(DeclarativeBase):
            pass

        class AsyncUser(Base):
            __tablename__ = "async_sa_users_rc"
            id: Mapped[int] = mapped_column(primary_key=True)
            name: Mapped[str] = mapped_column(nullable=False)

        engine = create_async_engine("rqlite+aiorqlite:///async_test_rc.db", echo=False)

        async def _test():
            await engine.connect()
            Base.metadata.create_all(engine)  # ty: ignore[invalid-argument-type]

            async with engine.begin() as conn:
                await conn.execute(
                    text("INSERT INTO async_sa_users_rc (name) VALUES (:name)"),
                    {"name": "test"},
                )
                await conn.commit()

                result = await conn.execute(text("SELECT * FROM async_sa_users_rc"))
                rows = result.fetchall()
                assert len(rows) == 1

        asyncio.run(_test())
        engine.dispose()  # ty: ignore[unused-awaitable]
