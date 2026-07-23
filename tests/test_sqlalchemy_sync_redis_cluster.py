"""Sync SQLAlchemy tests with Redis cluster lock.

Tests SQLAlchemy Core and ORM operations using the rqlite sync dialect
with RedisLock (cluster=True) for distributed locking.

Prerequisites:
    rqlite running on localhost:4001
    redis cluster running (seed node on localhost:6379)
"""

from __future__ import annotations

import warnings

import pytest
from sqlalchemy import (
    Column,
    Float,
    Integer,
    MetaData,
    String,
    Table,
    create_engine,
    insert,
    select,
    text,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column

from rqlite import RedisLock

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


def _make_lock(name: str = "sa_sync_redis_cluster") -> RedisLock:
    return RedisLock(name=f"test_{name}", timeout=30.0, cluster=True)


# ORM Base


class RedisClusterBase(DeclarativeBase):
    pass


class RedisClusterUser(RedisClusterBase):
    __tablename__ = "sa_sync_redis_cluster_users"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String, nullable=False)
    email: Mapped[str | None] = mapped_column(String, unique=True)
    age: Mapped[int | None] = mapped_column(Integer)
    score: Mapped[float | None] = mapped_column(Float)


class RedisClusterProduct(RedisClusterBase):
    __tablename__ = "sa_sync_redis_cluster_products"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String, nullable=False)
    price: Mapped[float] = mapped_column(Float)
    quantity: Mapped[int] = mapped_column(Integer, default=0)


# Engine Tests


@skip_if_no_redis_cluster
class TestSyncSA_RedisCluster_Engine:
    """Sync SQLAlchemy engine tests with RedisLock (cluster=True)."""

    def test_create_engine_with_cluster_lock(self):
        lock = _make_lock("engine")
        engine = create_engine(
            "rqlite://localhost:4001",
            connect_args={"lock": lock},
        )
        with engine.connect() as conn:
            result = conn.execute(text("SELECT 1"))
            assert result.scalar() == 1
        engine.dispose()

    def test_lock_suppresses_warning(self):
        lock = _make_lock("no_warn")
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            engine = create_engine(
                "rqlite://localhost:4001",
                connect_args={"lock": lock},
            )
            engine.dispose()
        tx_warnings = [x for x in w if "BEGIN/COMMIT/ROLLBACK" in str(x.message)]
        assert len(tx_warnings) == 0


# Core Tests


@skip_if_no_redis_cluster
class TestSyncSA_RedisCluster_Core:
    """Sync SQLAlchemy Core tests with RedisLock (cluster=True)."""

    def _engine(self):
        lock = _make_lock("core")
        return create_engine(
            "rqlite://localhost:4001",
            connect_args={"lock": lock},
            echo=False,
        )

    def test_insert_core(self):
        engine = self._engine()
        try:
            with engine.connect() as conn:
                conn.execute(text("DROP TABLE IF EXISTS sa_sync_redis_cluster_ins"))
                conn.execute(text("""
                    CREATE TABLE sa_sync_redis_cluster_ins (
                        id INTEGER PRIMARY KEY,
                        name TEXT NOT NULL,
                        age INTEGER
                    )
                """))
                conn.commit()

                conn.execute(
                    insert(
                        Table(
                            "sa_sync_redis_cluster_ins",
                            MetaData(),
                            Column("id", Integer, primary_key=True),
                            Column("name", String),
                            Column("age", Integer),
                        )
                    ).values(name="Alice", age=30)
                )
                conn.commit()

                result = conn.execute(
                    text("SELECT name, age FROM sa_sync_redis_cluster_ins")
                )
                row = result.fetchone()
                assert row is not None
                assert row[0] == "Alice"
                assert row[1] == 30
        finally:
            with engine.connect() as conn:
                conn.execute(text("DROP TABLE IF EXISTS sa_sync_redis_cluster_ins"))
                conn.commit()
            engine.dispose()

    def test_select_core(self):
        engine = self._engine()
        try:
            with engine.connect() as conn:
                conn.execute(text("DROP TABLE IF EXISTS sa_sync_redis_cluster_sel"))
                conn.execute(text("""
                    CREATE TABLE sa_sync_redis_cluster_sel (
                        id INTEGER PRIMARY KEY,
                        name TEXT NOT NULL,
                        score REAL
                    )
                """))
                conn.execute(text("INSERT INTO sa_sync_redis_cluster_sel VALUES (1, 'A', 95.5)"))
                conn.execute(text("INSERT INTO sa_sync_redis_cluster_sel VALUES (2, 'B', 87.3)"))
                conn.commit()

                result = conn.execute(
                    text("SELECT name, score FROM sa_sync_redis_cluster_sel ORDER BY score DESC")
                )
                rows = result.fetchall()
                assert len(rows) == 2
                assert rows[0][0] == "A"
        finally:
            with engine.connect() as conn:
                conn.execute(text("DROP TABLE IF EXISTS sa_sync_redis_cluster_sel"))
                conn.commit()
            engine.dispose()

    def test_update_core(self):
        engine = self._engine()
        try:
            with engine.connect() as conn:
                conn.execute(text("DROP TABLE IF EXISTS sa_sync_redis_cluster_upd"))
                conn.execute(text("""
                    CREATE TABLE sa_sync_redis_cluster_upd (
                        id INTEGER PRIMARY KEY,
                        name TEXT,
                        age INTEGER
                    )
                """))
                conn.execute(text("INSERT INTO sa_sync_redis_cluster_upd VALUES (1, 'Alice', 30)"))
                conn.commit()

                conn.execute(text("UPDATE sa_sync_redis_cluster_upd SET age = 31 WHERE name = 'Alice'"))
                conn.commit()

                result = conn.execute(
                    text("SELECT age FROM sa_sync_redis_cluster_upd WHERE name = 'Alice'")
                )
                assert result.scalar() == 31
        finally:
            with engine.connect() as conn:
                conn.execute(text("DROP TABLE IF EXISTS sa_sync_redis_cluster_upd"))
                conn.commit()
            engine.dispose()

    def test_delete_core(self):
        engine = self._engine()
        try:
            with engine.connect() as conn:
                conn.execute(text("DROP TABLE IF EXISTS sa_sync_redis_cluster_del"))
                conn.execute(text("""
                    CREATE TABLE sa_sync_redis_cluster_del (
                        id INTEGER PRIMARY KEY,
                        name TEXT
                    )
                """))
                conn.execute(text("INSERT INTO sa_sync_redis_cluster_del VALUES (1, 'ToDelete')"))
                conn.commit()

                conn.execute(text("DELETE FROM sa_sync_redis_cluster_del WHERE name = 'ToDelete'"))
                conn.commit()

                result = conn.execute(text("SELECT COUNT(*) FROM sa_sync_redis_cluster_del"))
                assert result.scalar() == 0
        finally:
            with engine.connect() as conn:
                conn.execute(text("DROP TABLE IF EXISTS sa_sync_redis_cluster_del"))
                conn.commit()
            engine.dispose()


# ORM Tests


@skip_if_no_redis_cluster
class TestSyncSA_RedisCluster_ORM:
    """Sync SQLAlchemy ORM tests with RedisLock (cluster=True)."""

    def _engine(self):
        lock = _make_lock("orm")
        return create_engine(
            "rqlite://localhost:4001",
            connect_args={"lock": lock},
            echo=False,
        )

    def test_orm_add_and_query(self):
        engine = self._engine()
        try:
            RedisClusterBase.metadata.create_all(engine)
            with Session(engine) as session:
                user = RedisClusterUser(
                    name="Alice", email="alice@test.com", age=30, score=95.5
                )
                session.add(user)
                session.commit()

                result = session.execute(select(RedisClusterUser))
                users = result.scalars().all()
                assert len(users) == 1
                assert users[0].name == "Alice"
        finally:
            RedisClusterBase.metadata.drop_all(engine)
            engine.dispose()

    def test_orm_update(self):
        engine = self._engine()
        try:
            RedisClusterBase.metadata.create_all(engine)
            with Session(engine) as session:
                session.add(RedisClusterUser(name="Alice", age=30))
                session.commit()

                user = session.execute(select(RedisClusterUser)).scalars().first()
                assert user is not None
                user.age = 31
                session.commit()

                user = session.execute(select(RedisClusterUser)).scalars().first()
                assert user is not None
                assert user.age == 31
        finally:
            RedisClusterBase.metadata.drop_all(engine)
            engine.dispose()

    def test_orm_product_crud(self):
        engine = self._engine()
        try:
            RedisClusterBase.metadata.create_all(engine)
            with Session(engine) as session:
                product = RedisClusterProduct(name="Widget", price=9.99, quantity=100)
                session.add(product)
                session.commit()

                result = session.execute(select(RedisClusterProduct))
                products = result.scalars().all()
                assert len(products) == 1
                assert products[0].price == 9.99

                products[0].price = 12.99
                session.commit()
                session.refresh(products[0])
                assert products[0].price == 12.99

                session.delete(products[0])
                session.commit()
                assert session.execute(select(RedisClusterProduct)).scalars().all() == []
        finally:
            RedisClusterBase.metadata.drop_all(engine)
            engine.dispose()


# Complex Workflow Tests


@skip_if_no_redis_cluster
class TestSyncSA_RedisCluster_Workflow:
    """Sync SQLAlchemy complex workflow tests with RedisLock (cluster=True)."""

    def _engine(self):
        lock = _make_lock("workflow")
        return create_engine(
            "rqlite://localhost:4001",
            connect_args={"lock": lock},
            echo=False,
        )

    def test_full_orm_workflow(self):
        engine = self._engine()
        try:
            RedisClusterBase.metadata.create_all(engine)

            with Session(engine) as session:
                users = [
                    RedisClusterUser(name=f"User{i}", age=i * 5, score=float(i * 10))
                    for i in range(1, 11)
                ]
                session.add_all(users)
                session.commit()

                result = session.execute(
                    select(RedisClusterUser)
                    .where(RedisClusterUser.age >= 25)
                    .order_by(RedisClusterUser.score.desc())
                )
                filtered = result.scalars().all()
                assert len(filtered) == 6

                for user in filtered[:3]:
                    user.score = 100.0
                session.commit()

                result = session.execute(
                    select(RedisClusterUser).where(RedisClusterUser.score == 100.0)
                )
                assert len(result.scalars().all()) == 3

                to_delete = session.execute(
                    select(RedisClusterUser).where(RedisClusterUser.age < 15)
                ).scalars().all()
                for user in to_delete:
                    session.delete(user)
                session.commit()

                result = session.execute(select(RedisClusterUser))
                remaining = result.scalars().all()
                assert len(remaining) == 8

        finally:
            RedisClusterBase.metadata.drop_all(engine)
            engine.dispose()
