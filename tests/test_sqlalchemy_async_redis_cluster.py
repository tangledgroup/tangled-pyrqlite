"""Async SQLAlchemy tests with Redis cluster lock (AioRedisLock).

Tests SQLAlchemy async Core and ORM operations using the rqlite async dialect
with AioRedisLock (cluster=True) for distributed locking.

Prerequisites:
    rqlite running on localhost:4001
    redis cluster running (seed node on localhost:6379)
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
    select,
    text,
)
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from rqlite import AioRedisLock

# Helpers


def _has_redis_cluster() -> bool:
    """Check if a Redis cluster is reachable."""
    try:
        from rqlite.redis_cluster import is_cluster_mode

        return is_cluster_mode("localhost", 6379)
    except Exception:
        return False


skip_if_no_redis_cluster = pytest.mark.skipif(
    not _has_redis_cluster(), reason="Redis cluster not available"
)


def run_async(coro):
    """Helper to run async code in a new event loop."""
    try:
        return asyncio.get_running_loop().run_until_complete(coro)
    except RuntimeError:
        return asyncio.run(coro)


def _make_lock(name: str = "sa_async_redis_cluster") -> AioRedisLock:
    return AioRedisLock(name=f"test_{name}", timeout=30.0, cluster=True)


def _create_all(conn, metadata):
    """Sync callable for conn.run_sync to create tables."""
    metadata.create_all(conn)


def _drop_all(conn, metadata):
    """Sync callable for conn.run_sync to drop tables."""
    metadata.drop_all(conn)


# ORM Base


class AsyncRedisClusterBase(DeclarativeBase):
    pass


class AsyncRedisClusterUser(AsyncRedisClusterBase):
    __tablename__ = "sa_async_redis_cluster_users"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String, nullable=False)
    email: Mapped[str | None] = mapped_column(String, unique=True)
    age: Mapped[int | None] = mapped_column(Integer)
    score: Mapped[float | None] = mapped_column(Float)


class AsyncRedisClusterProduct(AsyncRedisClusterBase):
    __tablename__ = "sa_async_redis_cluster_products"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String, nullable=False)
    price: Mapped[float] = mapped_column(Float)
    quantity: Mapped[int] = mapped_column(Integer, default=0)


# Engine Tests


@skip_if_no_redis_cluster
class TestAsyncSA_RedisCluster_Engine:
    """Async SQLAlchemy engine tests with AioRedisLock (cluster=True)."""

    def test_create_engine_with_cluster_lock(self):
        async def _run():
            lock = _make_lock("engine")
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
            tx_warnings = [x for x in w if "BEGIN/COMMIT/ROLLBACK" in str(x.message)]
            assert len(tx_warnings) == 0

        run_async(_run())


# Core Tests


@skip_if_no_redis_cluster
class TestAsyncSA_RedisCluster_Core:
    """Async SQLAlchemy Core tests with AioRedisLock (cluster=True)."""

    def _engine(self):
        lock = _make_lock("core")
        return create_async_engine(
            "rqlite+aiorqlite://localhost:4001",
            connect_args={"lock": lock},
            echo=False,
        )

    def test_insert_core(self):
        async def _run():
            engine = self._engine()
            try:
                async with engine.connect() as conn:
                    await conn.execute(text("DROP TABLE IF EXISTS sa_async_redis_cluster_ins"))
                    await conn.execute(text("""
                        CREATE TABLE sa_async_redis_cluster_ins (
                            id INTEGER PRIMARY KEY,
                            name TEXT NOT NULL,
                            age INTEGER
                        )
                    """))
                    await conn.commit()

                    await conn.execute(
                        insert(
                            Table(
                                "sa_async_redis_cluster_ins",
                                MetaData(),
                                Column("id", Integer, primary_key=True),
                                Column("name", String),
                                Column("age", Integer),
                            )
                        ).values(name="Alice", age=30)
                    )
                    await conn.commit()

                    result = await conn.execute(
                        text("SELECT name, age FROM sa_async_redis_cluster_ins")
                    )
                    row = result.fetchone()
                    assert row is not None
                    assert row[0] == "Alice"
                    assert row[1] == 30
            finally:
                async with engine.connect() as conn:
                    await conn.execute(text("DROP TABLE IF EXISTS sa_async_redis_cluster_ins"))
                    await conn.commit()
                await engine.dispose()

        run_async(_run())

    def test_select_core(self):
        async def _run():
            engine = self._engine()
            try:
                async with engine.connect() as conn:
                    await conn.execute(text("DROP TABLE IF EXISTS sa_async_redis_cluster_sel"))
                    await conn.execute(text("""
                        CREATE TABLE sa_async_redis_cluster_sel (
                            id INTEGER PRIMARY KEY,
                            name TEXT NOT NULL,
                            score REAL
                        )
                    """))
                    await conn.execute(
                        text("INSERT INTO sa_async_redis_cluster_sel VALUES (1, 'A', 95.5)")
                    )
                    await conn.execute(
                        text("INSERT INTO sa_async_redis_cluster_sel VALUES (2, 'B', 87.3)")
                    )
                    await conn.commit()

                    result = await conn.execute(
                        text("SELECT name, score FROM sa_async_redis_cluster_sel ORDER BY score DESC")
                    )
                    rows = result.fetchall()
                    assert len(rows) == 2
                    assert rows[0][0] == "A"
            finally:
                async with engine.connect() as conn:
                    await conn.execute(text("DROP TABLE IF EXISTS sa_async_redis_cluster_sel"))
                    await conn.commit()
                await engine.dispose()

        run_async(_run())

    def test_update_core(self):
        async def _run():
            engine = self._engine()
            try:
                async with engine.connect() as conn:
                    await conn.execute(text("DROP TABLE IF EXISTS sa_async_redis_cluster_upd"))
                    await conn.execute(text("""
                        CREATE TABLE sa_async_redis_cluster_upd (
                            id INTEGER PRIMARY KEY,
                            name TEXT,
                            age INTEGER
                        )
                    """))
                    await conn.execute(
                        text("INSERT INTO sa_async_redis_cluster_upd VALUES (1, 'Alice', 30)")
                    )
                    await conn.commit()

                    await conn.execute(
                        text("UPDATE sa_async_redis_cluster_upd SET age = 31 WHERE name = 'Alice'")
                    )
                    await conn.commit()

                    result = await conn.execute(
                        text("SELECT age FROM sa_async_redis_cluster_upd WHERE name = 'Alice'")
                    )
                    assert result.scalar() == 31
            finally:
                async with engine.connect() as conn:
                    await conn.execute(text("DROP TABLE IF EXISTS sa_async_redis_cluster_upd"))
                    await conn.commit()
                await engine.dispose()

        run_async(_run())

    def test_delete_core(self):
        async def _run():
            engine = self._engine()
            try:
                async with engine.connect() as conn:
                    await conn.execute(text("DROP TABLE IF EXISTS sa_async_redis_cluster_del"))
                    await conn.execute(text("""
                        CREATE TABLE sa_async_redis_cluster_del (
                            id INTEGER PRIMARY KEY,
                            name TEXT
                        )
                    """))
                    await conn.execute(
                        text("INSERT INTO sa_async_redis_cluster_del VALUES (1, 'ToDelete')")
                    )
                    await conn.commit()

                    await conn.execute(
                        text("DELETE FROM sa_async_redis_cluster_del WHERE name = 'ToDelete'")
                    )
                    await conn.commit()

                    result = await conn.execute(
                        text("SELECT COUNT(*) FROM sa_async_redis_cluster_del")
                    )
                    assert result.scalar() == 0
            finally:
                async with engine.connect() as conn:
                    await conn.execute(text("DROP TABLE IF EXISTS sa_async_redis_cluster_del"))
                    await conn.commit()
                await engine.dispose()

        run_async(_run())


# ORM Tests


@skip_if_no_redis_cluster
class TestAsyncSA_RedisCluster_ORM:
    """Async SQLAlchemy ORM tests with AioRedisLock (cluster=True)."""

    def _engine(self):
        lock = _make_lock("orm")
        return create_async_engine(
            "rqlite+aiorqlite://localhost:4001",
            connect_args={"lock": lock},
            echo=False,
        )

    def test_orm_add_and_query(self):
        async def _run():
            engine = self._engine()
            try:
                async with engine.begin() as conn:
                    await conn.run_sync(_create_all, AsyncRedisClusterBase.metadata)
                async_session = async_sessionmaker(
                    engine, class_=AsyncSession, expire_on_commit=False
                )
                async with async_session() as session:
                    user = AsyncRedisClusterUser(
                        name="Alice",
                        email="alice@test.com",
                        age=30,
                        score=95.5,
                    )
                    session.add(user)
                    await session.commit()

                    result = await session.execute(select(AsyncRedisClusterUser))
                    users = result.scalars().all()
                    assert len(users) == 1
                    assert users[0].name == "Alice"
            finally:
                async with engine.begin() as conn:
                    await conn.run_sync(_drop_all, AsyncRedisClusterBase.metadata)
                await engine.dispose()

        run_async(_run())

    def test_orm_update(self):
        async def _run():
            engine = self._engine()
            try:
                async with engine.begin() as conn:
                    await conn.run_sync(_create_all, AsyncRedisClusterBase.metadata)
                async_session = async_sessionmaker(
                    engine, class_=AsyncSession, expire_on_commit=False
                )
                async with async_session() as session:
                    session.add(AsyncRedisClusterUser(name="Alice", age=30))
                    await session.commit()

                    user = (
                        await session.execute(select(AsyncRedisClusterUser))
                    ).scalars().first()
                    assert user is not None
                    user.age = 31
                    await session.commit()

                    user = (
                        await session.execute(select(AsyncRedisClusterUser))
                    ).scalars().first()
                    assert user is not None
                    assert user.age == 31
            finally:
                async with engine.begin() as conn:
                    await conn.run_sync(_drop_all, AsyncRedisClusterBase.metadata)
                await engine.dispose()

        run_async(_run())

    def test_orm_product_crud(self):
        async def _run():
            engine = self._engine()
            try:
                async with engine.begin() as conn:
                    await conn.run_sync(_create_all, AsyncRedisClusterBase.metadata)
                async_session = async_sessionmaker(
                    engine, class_=AsyncSession, expire_on_commit=False
                )
                async with async_session() as session:
                    product = AsyncRedisClusterProduct(
                        name="Widget", price=9.99, quantity=100
                    )
                    session.add(product)
                    await session.commit()

                    result = await session.execute(select(AsyncRedisClusterProduct))
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
                        await session.execute(select(AsyncRedisClusterProduct))
                    ).scalars().all() == []
            finally:
                async with engine.begin() as conn:
                    await conn.run_sync(_drop_all, AsyncRedisClusterBase.metadata)
                await engine.dispose()

        run_async(_run())


# Complex Workflow Tests


@skip_if_no_redis_cluster
class TestAsyncSA_RedisCluster_Workflow:
    """Async SQLAlchemy complex workflow tests with AioRedisLock (cluster=True)."""

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
                    await conn.run_sync(_create_all, AsyncRedisClusterBase.metadata)
                async_session = async_sessionmaker(
                    engine, class_=AsyncSession, expire_on_commit=False
                )

                async with async_session() as session:
                    users = [
                        AsyncRedisClusterUser(
                            name=f"User{i}",
                            age=i * 5,
                            score=float(i * 10),
                        )
                        for i in range(1, 11)
                    ]
                    session.add_all(users)
                    await session.commit()

                    result = await session.execute(
                        select(AsyncRedisClusterUser)
                        .where(AsyncRedisClusterUser.age >= 25)
                        .order_by(AsyncRedisClusterUser.score.desc())
                    )
                    filtered = result.scalars().all()
                    assert len(filtered) == 6

                    for user in filtered[:3]:
                        user.score = 100.0
                    await session.commit()

                    result = await session.execute(
                        select(AsyncRedisClusterUser).where(
                            AsyncRedisClusterUser.score == 100.0
                        )
                    )
                    assert len(result.scalars().all()) == 3

                    to_delete = (
                        await session.execute(
                            select(AsyncRedisClusterUser).where(
                                AsyncRedisClusterUser.age < 15
                            )
                        )
                    ).scalars().all()
                    for user in to_delete:
                        await session.delete(user)
                    await session.commit()

                    result = await session.execute(select(AsyncRedisClusterUser))
                    remaining = result.scalars().all()
                    assert len(remaining) == 8

            finally:
                async with engine.begin() as conn:
                    await conn.run_sync(_drop_all, AsyncRedisClusterBase.metadata)
                await engine.dispose()

        run_async(_run())
