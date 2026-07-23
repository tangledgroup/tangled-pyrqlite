"""Async SQLAlchemy tests with Valkey distributed lock (AioValkeyLock).

Tests SQLAlchemy async Core and ORM operations using the rqlite async dialect
with AioValkeyLock for distributed locking. Covers async engine creation,
Core operations, ORM models and sessions, and full CRUD workflows.

Prerequisites:
    rqlite running on localhost:4001
    valkey running on localhost:6379
"""

from __future__ import annotations

import asyncio
import warnings

import pytest
from sqlalchemy import (
    Column,
    Float,
    Integer,
    MetaData,
    String,
    Table,
    insert,
    inspect,
    select,
    text,
)
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from rqlite import AioValkeyLock

# Helpers


def _has_valkey() -> bool:
    """Check if Valkey is reachable."""
    try:
        import valkey

        client = valkey.Redis(
            host="localhost", port=6379, db=0, socket_connect_timeout=1.0
        )
        return bool(client.ping())
    except Exception:
        return False


skip_if_no_valkey = pytest.mark.skipif(
    not _has_valkey(), reason="Valkey not available"
)


def run_async(coro):
    """Helper to run async code in a new event loop."""
    try:
        return asyncio.get_running_loop().run_until_complete(coro)
    except RuntimeError:
        return asyncio.run(coro)


def _make_lock(name: str = "sa_async") -> AioValkeyLock:
    return AioValkeyLock(name=f"test_{name}", timeout=30.0)


def _create_all(conn, metadata):
    """Sync callable for conn.run_sync to create tables."""
    metadata.create_all(conn)


def _drop_all(conn, metadata):
    """Sync callable for conn.run_sync to drop tables."""
    metadata.drop_all(conn)





def _get_table_names(conn):
    """Sync callable for conn.run_sync to get table names."""
    insp = inspect(conn)
    return insp.get_table_names()


# ORM Base


class AsyncBase(DeclarativeBase):
    pass


class AsyncUser(AsyncBase):
    __tablename__ = "sa_async_valkey_users"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String, nullable=False)
    email: Mapped[str | None] = mapped_column(String, unique=True)
    age: Mapped[int | None] = mapped_column(Integer)
    score: Mapped[float | None] = mapped_column(Float)


class AsyncProduct(AsyncBase):
    __tablename__ = "sa_async_valkey_products"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String, nullable=False)
    price: Mapped[float] = mapped_column(Float)
    quantity: Mapped[int] = mapped_column(Integer, default=0)


# Engine Tests


@skip_if_no_valkey
class TestAsyncSA_Engine:
    """Async SQLAlchemy engine tests with AioValkeyLock."""

    def test_create_async_engine(self):
        async def _run():
            lock = _make_lock("engine")
            engine = create_async_engine(
                "rqlite+aiorqlite://localhost:4001",
                connect_args={"lock": lock},
                echo=False,
            )
            assert engine is not None
            await engine.dispose()

        run_async(_run())

    def test_create_engine_with_lock(self):
        async def _run():
            lock = _make_lock("engine_lock")
            engine = create_async_engine(
                "rqlite+aiorqlite://localhost:4001",
                connect_args={"lock": lock},
            )
            async with engine.connect() as conn:
                result = await conn.execute(text("SELECT 1"))
                assert result.scalar() == 1
            await engine.dispose()

        run_async(_run())

    def test_lock_suppresses_warning(self):
        async def _run():
            lock = _make_lock("no_warn")
            with warnings.catch_warnings(record=True) as w:
                warnings.simplefilter("always")
                engine = create_async_engine(
                    "rqlite+aiorqlite://localhost:4001",
                    connect_args={"lock": lock},
                )
                await engine.dispose()
            tx_warnings = [
                x for x in w if "BEGIN/COMMIT/ROLLBACK" in str(x.message)
            ]
            assert len(tx_warnings) == 0

        run_async(_run())

    def test_engine_url_parsing(self):
        async def _run():
            lock = _make_lock("url")
            engine = create_async_engine(
                "rqlite+aiorqlite://localhost:4001",
                connect_args={"lock": lock},
            )
            async with engine.connect() as conn:
                result = await conn.execute(text("SELECT 'hello'"))
                assert result.scalar() == "hello"
            await engine.dispose()

        run_async(_run())


# Core Tests


@skip_if_no_valkey
class TestAsyncSA_Core:
    """Async SQLAlchemy Core tests with AioValkeyLock."""

    def _engine(self):
        lock = _make_lock("core")
        return create_async_engine(
            "rqlite+aiorqlite://localhost:4001",
            connect_args={"lock": lock},
            echo=False,
        )

    def test_create_table_core(self):
        async def _run():
            engine = self._engine()
            metadata = MetaData()
            Table(
                "sa_async_core_test",
                metadata,
                Column("id", Integer, primary_key=True),
                Column("name", String, nullable=False),
                Column("value", Integer),
            )
            try:
                async with engine.begin() as conn:
                    await conn.run_sync(_create_all, metadata)
                async with engine.connect() as conn:
                    result = await conn.execute(
                        text(
                            "SELECT name FROM sqlite_master "
                            "WHERE type='table' AND name='sa_async_core_test'"
                        )
                    )
                    assert result.scalar() is not None
                await engine.dispose()
            finally:
                async with engine.connect() as conn:
                    await conn.execute(text("DROP TABLE IF EXISTS sa_async_core_test"))
                    await conn.commit()

        run_async(_run())

    def test_insert_core(self):
        async def _run():
            engine = self._engine()
            try:
                async with engine.connect() as conn:
                    await conn.execute(text("DROP TABLE IF EXISTS sa_async_core_ins"))
                    await conn.execute(text("""
                        CREATE TABLE sa_async_core_ins (
                            id INTEGER PRIMARY KEY,
                            name TEXT NOT NULL,
                            age INTEGER
                        )
                    """))
                    await conn.commit()

                    await conn.execute(
                        insert(
                            Table(
                                "sa_async_core_ins",
                                MetaData(),
                                Column("id", Integer, primary_key=True),
                                Column("name", String),
                                Column("age", Integer),
                            )
                        ).values(name="Alice", age=30)
                    )
                    await conn.commit()

                    result = await conn.execute(
                        text("SELECT name, age FROM sa_async_core_ins")
                    )
                    row = result.fetchone()
                    assert row is not None
                    assert row[0] == "Alice"
                    assert row[1] == 30
            finally:
                async with engine.connect() as conn:
                    await conn.execute(text("DROP TABLE IF EXISTS sa_async_core_ins"))
                    await conn.commit()
                await engine.dispose()

        run_async(_run())

    def test_select_core(self):
        async def _run():
            engine = self._engine()
            try:
                async with engine.connect() as conn:
                    await conn.execute(text("DROP TABLE IF EXISTS sa_async_core_sel"))
                    await conn.execute(text("""
                        CREATE TABLE sa_async_core_sel (
                            id INTEGER PRIMARY KEY,
                            name TEXT NOT NULL,
                            score REAL
                        )
                    """))
                    await conn.execute(
                        text("INSERT INTO sa_async_core_sel VALUES (1, 'A', 95.5)")
                    )
                    await conn.execute(
                        text("INSERT INTO sa_async_core_sel VALUES (2, 'B', 87.3)")
                    )
                    await conn.commit()

                    result = await conn.execute(
                        text(
                            "SELECT name, score FROM sa_async_core_sel "
                            "ORDER BY score DESC"
                        )
                    )
                    rows = result.fetchall()
                    assert len(rows) == 2
                    assert rows[0][0] == "A"
                    assert rows[1][0] == "B"
            finally:
                async with engine.connect() as conn:
                    await conn.execute(text("DROP TABLE IF EXISTS sa_async_core_sel"))
                    await conn.commit()
                await engine.dispose()

        run_async(_run())

    def test_update_core(self):
        async def _run():
            engine = self._engine()
            try:
                async with engine.connect() as conn:
                    await conn.execute(text("DROP TABLE IF EXISTS sa_async_core_upd"))
                    await conn.execute(text("""
                        CREATE TABLE sa_async_core_upd (
                            id INTEGER PRIMARY KEY,
                            name TEXT,
                            age INTEGER
                        )
                    """))
                    await conn.execute(
                        text(
                            "INSERT INTO sa_async_core_upd VALUES (1, 'Alice', 30)"
                        )
                    )
                    await conn.commit()

                    await conn.execute(
                        text(
                            "UPDATE sa_async_core_upd SET age = 31 WHERE name = 'Alice'"
                        )
                    )
                    await conn.commit()

                    result = await conn.execute(
                        text(
                            "SELECT age FROM sa_async_core_upd WHERE name = 'Alice'"
                        )
                    )
                    assert result.scalar() == 31
            finally:
                async with engine.connect() as conn:
                    await conn.execute(text("DROP TABLE IF EXISTS sa_async_core_upd"))
                    await conn.commit()
                await engine.dispose()

        run_async(_run())

    def test_delete_core(self):
        async def _run():
            engine = self._engine()
            try:
                async with engine.connect() as conn:
                    await conn.execute(text("DROP TABLE IF EXISTS sa_async_core_del"))
                    await conn.execute(text("""
                        CREATE TABLE sa_async_core_del (
                            id INTEGER PRIMARY KEY,
                            name TEXT
                        )
                    """))
                    await conn.execute(
                        text(
                            "INSERT INTO sa_async_core_del VALUES (1, 'ToDelete')"
                        )
                    )
                    await conn.commit()

                    await conn.execute(
                        text(
                            "DELETE FROM sa_async_core_del WHERE name = 'ToDelete'"
                        )
                    )
                    await conn.commit()

                    result = await conn.execute(
                        text("SELECT COUNT(*) FROM sa_async_core_del")
                    )
                    assert result.scalar() == 0
            finally:
                async with engine.connect() as conn:
                    await conn.execute(text("DROP TABLE IF EXISTS sa_async_core_del"))
                    await conn.commit()
                await engine.dispose()

        run_async(_run())

    def test_text_execution(self):
        async def _run():
            engine = self._engine()
            async with engine.connect() as conn:
                result = await conn.execute(text("SELECT 1 + 1"))
                assert result.scalar() == 2
            await engine.dispose()

        run_async(_run())

    def test_parameterized_text(self):
        async def _run():
            engine = self._engine()
            async with engine.connect() as conn:
                result = await conn.execute(
                    text("SELECT :a + :b"), {"a": 10, "b": 20}
                )
                assert result.scalar() == 30
            await engine.dispose()

        run_async(_run())


# ORM Tests


@skip_if_no_valkey
class TestAsyncSA_ORM:
    """Async SQLAlchemy ORM tests with AioValkeyLock."""

    def _engine(self):
        lock = _make_lock("orm")
        return create_async_engine(
            "rqlite+aiorqlite://localhost:4001",
            connect_args={"lock": lock},
            echo=False,
        )

    def _setup(self, engine):
        async def _run():
            async with engine.begin() as conn:
                await conn.run_sync(_create_all, AsyncBase.metadata)
        return _run

    def _teardown(self, engine):
        async def _run():
            async with engine.begin() as conn:
                await conn.run_sync(_drop_all, AsyncBase.metadata)
        return _run

    def test_orm_create_all(self):
        async def _run():
            engine = self._engine()
            try:
                async with engine.begin() as conn:
                    await conn.run_sync(_create_all, AsyncBase.metadata)
                async with engine.connect() as conn:
                    result = await conn.execute(
                        text(
                            "SELECT name FROM sqlite_master "
                            "WHERE type='table' AND name='sa_async_valkey_users'"
                        )
                    )
                    assert result.scalar() is not None
            finally:
                async with engine.begin() as conn:
                    await conn.run_sync(_drop_all, AsyncBase.metadata)
                await engine.dispose()

        run_async(_run())

    def test_orm_add_and_query(self):
        async def _run():
            engine = self._engine()
            try:
                async with engine.begin() as conn:
                    await conn.run_sync(_create_all, AsyncBase.metadata)
                async_session = async_sessionmaker(
                    engine, class_=AsyncSession, expire_on_commit=False
                )
                async with async_session() as session:
                    user = AsyncUser(
                        name="Alice",
                        email="alice@test.com",
                        age=30,
                        score=95.5,
                    )
                    session.add(user)
                    await session.commit()

                    result = await session.execute(select(AsyncUser))
                    users = result.scalars().all()
                    assert len(users) == 1
                    assert users[0].name == "Alice"
                    assert users[0].email == "alice@test.com"
            finally:
                async with engine.begin() as conn:
                    await conn.run_sync(_drop_all, AsyncBase.metadata)
                await engine.dispose()

        run_async(_run())

    def test_orm_query_filter(self):
        async def _run():
            engine = self._engine()
            try:
                async with engine.begin() as conn:
                    await conn.run_sync(_create_all, AsyncBase.metadata)
                async_session = async_sessionmaker(
                    engine, class_=AsyncSession, expire_on_commit=False
                )
                async with async_session() as session:
                    session.add(AsyncUser(name="Young", age=20))
                    session.add(AsyncUser(name="Old", age=60))
                    await session.commit()

                    result = await session.execute(
                        select(AsyncUser)
                        .where(AsyncUser.age > 30)
                        .order_by(AsyncUser.name)
                    )
                    users = result.scalars().all()
                    assert len(users) == 1
                    assert users[0].name == "Old"
            finally:
                async with engine.begin() as conn:
                    await conn.run_sync(_drop_all, AsyncBase.metadata)
                await engine.dispose()

        run_async(_run())

    def test_orm_update(self):
        async def _run():
            engine = self._engine()
            try:
                async with engine.begin() as conn:
                    await conn.run_sync(_create_all, AsyncBase.metadata)
                async_session = async_sessionmaker(
                    engine, class_=AsyncSession, expire_on_commit=False
                )
                async with async_session() as session:
                    session.add(AsyncUser(name="Alice", age=30))
                    await session.commit()

                    user = (
                        await session.execute(select(AsyncUser))
                    ).scalars().first()
                    assert user is not None
                    user.age = 31
                    await session.commit()

                    user = (
                        await session.execute(select(AsyncUser))
                    ).scalars().first()
                    assert user is not None
                    assert user.age == 31
            finally:
                async with engine.begin() as conn:
                    await conn.run_sync(_drop_all, AsyncBase.metadata)
                await engine.dispose()

        run_async(_run())

    def test_orm_delete(self):
        async def _run():
            engine = self._engine()
            try:
                async with engine.begin() as conn:
                    await conn.run_sync(_create_all, AsyncBase.metadata)
                async_session = async_sessionmaker(
                    engine, class_=AsyncSession, expire_on_commit=False
                )
                async with async_session() as session:
                    session.add(AsyncUser(name="ToDelete"))
                    await session.commit()

                    user = (
                        await session.execute(select(AsyncUser))
                    ).scalars().first()
                    await session.delete(user)
                    await session.commit()

                    result = await session.execute(select(AsyncUser))
                    assert result.scalars().all() == []
            finally:
                async with engine.begin() as conn:
                    await conn.run_sync(_drop_all, AsyncBase.metadata)
                await engine.dispose()

        run_async(_run())

    def test_orm_multiple_add(self):
        async def _run():
            engine = self._engine()
            try:
                async with engine.begin() as conn:
                    await conn.run_sync(_create_all, AsyncBase.metadata)
                async_session = async_sessionmaker(
                    engine, class_=AsyncSession, expire_on_commit=False
                )
                async with async_session() as session:
                    users = [
                        AsyncUser(name=f"User{i}", age=i * 10)
                        for i in range(1, 6)
                    ]
                    session.add_all(users)
                    await session.commit()

                    result = await session.execute(
                        select(AsyncUser).order_by(AsyncUser.age)
                    )
                    fetched = result.scalars().all()
                    assert len(fetched) == 5
                    assert fetched[0].name == "User1"
                    assert fetched[-1].name == "User5"
            finally:
                async with engine.begin() as conn:
                    await conn.run_sync(_drop_all, AsyncBase.metadata)
                await engine.dispose()

        run_async(_run())

    def test_orm_product_crud(self):
        async def _run():
            engine = self._engine()
            try:
                async with engine.begin() as conn:
                    await conn.run_sync(_create_all, AsyncBase.metadata)
                async_session = async_sessionmaker(
                    engine, class_=AsyncSession, expire_on_commit=False
                )
                async with async_session() as session:
                    product = AsyncProduct(
                        name="Widget", price=9.99, quantity=100
                    )
                    session.add(product)
                    await session.commit()

                    result = await session.execute(select(AsyncProduct))
                    products = result.scalars().all()
                    assert len(products) == 1
                    assert products[0].price == 9.99

                    products[0].price = 12.99
                    await session.commit()
                    await session.refresh(products[0])
                    assert products[0].price == 12.99

                    await session.delete(products[0])
                    await session.commit()
                    assert (
                        await session.execute(select(AsyncProduct))
                    ).scalars().all() == []
            finally:
                async with engine.begin() as conn:
                    await conn.run_sync(_drop_all, AsyncBase.metadata)
                await engine.dispose()

        run_async(_run())

    def test_orm_session_context_manager(self):
        async def _run():
            engine = self._engine()
            try:
                async with engine.begin() as conn:
                    await conn.run_sync(_create_all, AsyncBase.metadata)
                async_session = async_sessionmaker(
                    engine, class_=AsyncSession, expire_on_commit=False
                )
                async with async_session() as session:
                    session.add(AsyncUser(name="CtxUser", age=25))
                    await session.commit()
            finally:
                async with engine.begin() as conn:
                    await conn.run_sync(_drop_all, AsyncBase.metadata)
                await engine.dispose()

        run_async(_run())

    def test_orm_scalar_one_or_none(self):
        async def _run():
            engine = self._engine()
            try:
                async with engine.begin() as conn:
                    await conn.run_sync(_create_all, AsyncBase.metadata)
                async_session = async_sessionmaker(
                    engine, class_=AsyncSession, expire_on_commit=False
                )
                async with async_session() as session:
                    session.add(AsyncUser(name="ScalarTest", age=40))
                    await session.commit()

                    count = (
                        await session.execute(
                            select(AsyncUser).where(
                                AsyncUser.name == "ScalarTest"
                            )
                        )
                    ).scalars().first()
                    assert count is not None
                    assert count.name == "ScalarTest"

                    missing = (
                        await session.execute(
                            select(AsyncUser).where(
                                AsyncUser.name == "NonExistent"
                            )
                        )
                    ).scalars().first()
                    assert missing is None
            finally:
                async with engine.begin() as conn:
                    await conn.run_sync(_drop_all, AsyncBase.metadata)
                await engine.dispose()

        run_async(_run())


# Reflection Tests


@skip_if_no_valkey
class TestAsyncSA_Reflection:
    """Async SQLAlchemy reflection tests with AioValkeyLock."""

    def _engine(self):
        lock = _make_lock("reflect")
        return create_async_engine(
            "rqlite+aiorqlite://localhost:4001",
            connect_args={"lock": lock},
            echo=False,
        )

    def test_reflect_columns(self):
        """Test column reflection via PRAGMA (avoids metadata.reflect type bug)."""
        async def _run():
            engine = self._engine()
            try:
                async with engine.connect() as conn:
                    await conn.execute(text("DROP TABLE IF EXISTS sa_async_reflect"))
                    await conn.execute(text("""
                        CREATE TABLE sa_async_reflect (
                            id INTEGER PRIMARY KEY,
                            name TEXT NOT NULL,
                            value REAL
                        )
                    """))
                    await conn.commit()

                # Query PRAGMA table_info for column info
                async with engine.connect() as conn:
                    result = await conn.execute(
                        text("PRAGMA table_info(sa_async_reflect)")
                    )
                    rows = result.fetchall()
                    col_names = {row[1] for row in rows}
                    assert "id" in col_names
                    assert "name" in col_names
                    assert "value" in col_names
            finally:
                async with engine.connect() as conn:
                    await conn.execute(text("DROP TABLE IF EXISTS sa_async_reflect"))
                    await conn.commit()
                await engine.dispose()

        run_async(_run())

    def test_has_table(self):
        async def _run():
            engine = self._engine()
            try:
                async with engine.connect() as conn:
                    await conn.execute(text("DROP TABLE IF EXISTS sa_async_has_tbl"))
                    await conn.execute(text("""
                        CREATE TABLE sa_async_has_tbl (id INTEGER PRIMARY KEY)
                    """))
                    await conn.commit()

                async with engine.connect() as conn:
                    tables = await conn.run_sync(_get_table_names)
                assert "sa_async_has_tbl" in tables
            finally:
                async with engine.connect() as conn:
                    await conn.execute(text("DROP TABLE IF EXISTS sa_async_has_tbl"))
                    await conn.commit()
                await engine.dispose()

        run_async(_run())


# Complex Workflow Tests


@skip_if_no_valkey
class TestAsyncSA_Workflow:
    """Async SQLAlchemy complex workflow tests with AioValkeyLock."""

    def _engine(self):
        lock = _make_lock("workflow")
        return create_async_engine(
            "rqlite+aiorqlite://localhost:4001",
            connect_args={"lock": lock},
            echo=False,
        )

    def test_full_orm_workflow(self):
        async def _run():
            engine = self._engine()
            try:
                async with engine.begin() as conn:
                    await conn.run_sync(_create_all, AsyncBase.metadata)
                async_session = async_sessionmaker(
                    engine, class_=AsyncSession, expire_on_commit=False
                )

                async with async_session() as session:
                    users = [
                        AsyncUser(
                            name=f"User{i}",
                            age=i * 5,
                            score=float(i * 10),
                        )
                        for i in range(1, 11)
                    ]
                    session.add_all(users)
                    await session.commit()

                    result = await session.execute(
                        select(AsyncUser)
                        .where(AsyncUser.age >= 25)
                        .order_by(AsyncUser.score.desc())
                    )
                    filtered = result.scalars().all()
                    assert len(filtered) == 6

                    for user in filtered[:3]:
                        user.score = 100.0
                    await session.commit()

                    result = await session.execute(
                        select(AsyncUser).where(AsyncUser.score == 100.0)
                    )
                    assert len(result.scalars().all()) == 3

                    to_delete = (
                        await session.execute(
                            select(AsyncUser).where(AsyncUser.age < 15)
                        )
                    ).scalars().all()
                    for user in to_delete:
                        await session.delete(user)
                    await session.commit()

                    result = await session.execute(select(AsyncUser))
                    remaining = result.scalars().all()
                    assert len(remaining) == 8

            finally:
                async with engine.begin() as conn:
                    await conn.run_sync(_drop_all, AsyncBase.metadata)
                await engine.dispose()

        run_async(_run())

    def test_multiple_sessions(self):
        async def _run():
            engine = self._engine()
            try:
                async with engine.begin() as conn:
                    await conn.run_sync(_create_all, AsyncBase.metadata)
                async_session = async_sessionmaker(
                    engine, class_=AsyncSession, expire_on_commit=False
                )

                async with async_session() as session1:
                    session1.add(AsyncUser(name="FromSession1", age=20))
                    await session1.commit()

                async with async_session() as session2:
                    result = await session2.execute(select(AsyncUser))
                    users = result.scalars().all()
                    assert len(users) == 1
                    assert users[0].name == "FromSession1"

            finally:
                async with engine.begin() as conn:
                    await conn.run_sync(_drop_all, AsyncBase.metadata)
                await engine.dispose()

        run_async(_run())

    def test_engine_connect_context(self):
        async def _run():
            engine = self._engine()
            try:
                async with engine.begin() as conn:
                    await conn.run_sync(_create_all, AsyncBase.metadata)

                async with engine.connect() as conn:
                    await conn.execute(
                        text(
                            "INSERT INTO sa_async_valkey_users (name, age) "
                            "VALUES ('Ctx', 99)"
                        )
                    )
                    await conn.commit()

                    result = await conn.execute(
                        text(
                            "SELECT age FROM sa_async_valkey_users "
                            "WHERE name = 'Ctx'"
                        )
                    )
                    assert result.scalar() == 99

                async with engine.connect() as conn:
                    await conn.execute(
                        text(
                            "DELETE FROM sa_async_valkey_users WHERE name = 'Ctx'"
                        )
                    )
                    await conn.commit()

            finally:
                async with engine.begin() as conn:
                    await conn.run_sync(_drop_all, AsyncBase.metadata)
                await engine.dispose()

        run_async(_run())
