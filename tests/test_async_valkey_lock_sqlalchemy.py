"""Tests for async SQLAlchemy Core and ORM with async Valkey distributed lock (AioValkeyLock).

Covers:
- Async SQLAlchemy Core operations (insert, select, update, delete) with AioValkeyLock
- Async SQLAlchemy ORM operations (AsyncSession, query) with AioValkeyLock
- Lock suppresses transaction warnings
- Full CRUD workflows

Prerequisites:
    uv add tangled-pyrqlite[valkey]
    podman run -d --name valkey-test -p 6379:6379 docker.io/valkey/valkey
    rqlite running on localhost:4001
"""

from __future__ import annotations

import asyncio
import warnings

import pytest
from sqlalchemy import delete, insert, select, text, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

import rqlite
from rqlite import AioValkeyLock


def _has_valkey() -> bool:
    """Check if Valkey is reachable."""
    try:
        import valkey

        client = valkey.Redis(host="localhost", port=6379, db=0, socket_connect_timeout=1.0)
        return bool(client.ping())
    except Exception:
        return False


def run_async(coro):
    """Helper to run async code in a new event loop."""
    try:
        return asyncio.get_running_loop().run_until_complete(coro)
    except RuntimeError:
        return asyncio.run(coro)


def _create_tables_sync(conn, metadata):
    """Synchronous table creation callable for run_sync."""
    metadata.create_all(conn)


# Models


class Base(DeclarativeBase):
    """Base class for async SQLAlchemy models."""


class AsyncUserVSA(Base):
    """User model for async ValkeyLock SQLAlchemy tests."""

    __tablename__ = "async_sa_valkey_users"

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(nullable=False)
    email: Mapped[str | None] = mapped_column(unique=True)
    age: Mapped[int | None] = mapped_column()


class AsyncProductVSA(Base):
    """Product model for async ValkeyLock SQLAlchemy tests."""

    __tablename__ = "async_sa_valkey_products"

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(nullable=False)
    price: Mapped[int] = mapped_column()
    quantity: Mapped[int] = mapped_column(default=0)


# Skip marker

skip_if_no_valkey = pytest.mark.skipif(not _has_valkey(), reason="Valkey not available")


# Async SQLAlchemy Core Tests


@skip_if_no_valkey
class TestAsyncValkeyLockSQLAlchemyCore:
    """Test async SQLAlchemy Core operations with AioValkeyLock."""

    def test_async_create_tables(self):
        """Test creating tables via async SQLAlchemy with AioValkeyLock."""
        lock = AioValkeyLock(name="async_valkey_sa_core", timeout=30.0)
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
                assert "async_sa_valkey_users" in tables
                assert "async_sa_valkey_products" in tables

        run_async(_test())
        run_async(engine.dispose())

    def test_async_insert_select(self):
        """Test async INSERT and SELECT with AioValkeyLock."""
        lock = AioValkeyLock(name="async_valkey_sa_is", timeout=30.0)
        engine = create_async_engine(
            "rqlite+aiorqlite://localhost:4001",
            connect_args={"lock": lock},
            echo=False,
        )

        async def _test():
            async with engine.begin() as conn:
                await conn.run_sync(_create_tables_sync, Base.metadata)
                stmt = insert(AsyncUserVSA).values(name="Alice", email="alice@test.com", age=30)
                await conn.execute(stmt)

            async with engine.begin() as conn:
                result = await conn.execute(
                    select(AsyncUserVSA).where(AsyncUserVSA.name == "Alice")
                )
                row = result.fetchone()
                assert row is not None
                assert row.name == "Alice"
                assert row.age == 30

        run_async(_test())
        run_async(engine.dispose())

    def test_async_update(self):
        """Test async UPDATE with AioValkeyLock."""
        lock = AioValkeyLock(name="async_valkey_sa_update", timeout=30.0)
        engine = create_async_engine(
            "rqlite+aiorqlite://localhost:4001",
            connect_args={"lock": lock},
            echo=False,
        )

        async def _test():
            async with engine.begin() as conn:
                await conn.run_sync(_create_tables_sync, Base.metadata)
                stmt = insert(AsyncUserVSA).values(name="Bob", age=25)
                await conn.execute(stmt)

            async with engine.begin() as conn:
                stmt = update(AsyncUserVSA).where(AsyncUserVSA.name == "Bob").values(age=30)
                await conn.execute(stmt)

            async with engine.begin() as conn:
                result = await conn.execute(
                    select(AsyncUserVSA.age).where(AsyncUserVSA.name == "Bob")
                )
                row = result.fetchone()
                assert row[0] == 30

        run_async(_test())
        run_async(engine.dispose())

    def test_async_delete(self):
        """Test async DELETE with AioValkeyLock."""
        lock = AioValkeyLock(name="async_valkey_sa_delete", timeout=30.0)
        engine = create_async_engine(
            "rqlite+aiorqlite://localhost:4001",
            connect_args={"lock": lock},
            echo=False,
        )

        async def _test():
            async with engine.begin() as conn:
                await conn.run_sync(_create_tables_sync, Base.metadata)
                stmt = insert(AsyncUserVSA).values(name="Charlie", email="charlie@test.com")
                await conn.execute(stmt)

            async with engine.begin() as conn:
                stmt = delete(AsyncUserVSA).where(AsyncUserVSA.name == "Charlie")
                await conn.execute(stmt)

            async with engine.begin() as conn:
                result = await conn.execute(
                    select(AsyncUserVSA).where(AsyncUserVSA.name == "Charlie")
                )
                row = result.fetchone()
                assert row is None

        run_async(_test())
        run_async(engine.dispose())

    def test_async_text_sql(self):
        """Test raw text SQL with async AioValkeyLock."""
        lock = AioValkeyLock(name="async_valkey_sa_text", timeout=30.0)
        engine = create_async_engine(
            "rqlite+aiorqlite://localhost:4001",
            connect_args={"lock": lock},
            echo=False,
        )

        async def _test():
            async with engine.begin() as conn:
                await conn.run_sync(_create_tables_sync, Base.metadata)
                await conn.execute(
                    text("INSERT INTO async_sa_valkey_users (name, email) VALUES (:n, :e)"),
                    {"n": "Grace", "e": "grace@test.com"},
                )

            async with engine.begin() as conn:
                result = await conn.execute(
                    text("SELECT name, email FROM async_sa_valkey_users WHERE name = :n"),
                    {"n": "Grace"},
                )
                row = result.fetchone()
                assert row is not None
                assert row[0] == "Grace"

        run_async(_test())
        run_async(engine.dispose())

    def test_async_executemany(self):
        """Test bulk async insert with AioValkeyLock."""
        lock = AioValkeyLock(name="async_valkey_sa_bulk", timeout=30.0)
        engine = create_async_engine(
            "rqlite+aiorqlite://localhost:4001",
            connect_args={"lock": lock},
            echo=False,
        )

        async def _test():
            async with engine.begin() as conn:
                await conn.run_sync(_create_tables_sync, Base.metadata)
                users = [
                    {"name": "Diana", "email": "diana@test.com", "age": 28},
                    {"name": "Eve", "email": "eve@test.com", "age": 32},
                    {"name": "Frank", "email": "frank@test.com", "age": 45},
                ]
                await conn.execute(insert(AsyncUserVSA), users)

            async with engine.begin() as conn:
                result = await conn.execute(
                    select(AsyncUserVSA).order_by(AsyncUserVSA.age)
                )
                rows = result.fetchall()
                assert len(rows) == 3
                assert rows[0].name == "Diana"
                assert rows[-1].name == "Frank"

        run_async(_test())
        run_async(engine.dispose())


# Async SQLAlchemy ORM Tests


@skip_if_no_valkey
class TestAsyncValkeyLockSQLAlchemyORM:
    """Test async SQLAlchemy ORM operations with AioValkeyLock."""

    def test_async_session_add_commit(self):
        """Test adding and committing via async Session with AioValkeyLock."""
        lock = AioValkeyLock(name="async_valkey_sa_orm1", timeout=30.0)
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
                user = AsyncUserVSA(name="David", email="david@test.com", age=35)
                session.add(user)
                await session.commit()
                assert user.id is not None

        run_async(_test())
        run_async(engine.dispose())

    def test_async_session_add_all(self):
        """Test async Session.add_all() with AioValkeyLock."""
        lock = AioValkeyLock(name="async_valkey_sa_orm2", timeout=30.0)
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
                users = [
                    AsyncUserVSA(name="Eve", email="eve@test.com"),
                    AsyncUserVSA(name="Frank", email="frank@test.com"),
                ]
                session.add_all(users)
                await session.commit()

        run_async(_test())
        run_async(engine.dispose())

    def test_async_session_query(self):
        """Test querying via async Session with AioValkeyLock."""
        lock = AioValkeyLock(name="async_valkey_sa_orm3", timeout=30.0)
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
                    ("Ivy", "ivy@test.com"),
                    ("Jack", "jack@test.com"),
                ]:
                    session.add(AsyncUserVSA(name=name, email=email))
                await session.commit()

            async with async_session() as session:
                result = await session.execute(select(AsyncUserVSA))
                users = result.scalars().all()
                assert len(users) == 2

                result = await session.execute(
                    select(AsyncUserVSA).where(AsyncUserVSA.name == "Ivy")
                )
                ivy = result.scalar_one_or_none()
                assert ivy is not None
                assert ivy.email == "ivy@test.com"

        run_async(_test())
        run_async(engine.dispose())

    def test_async_session_update(self):
        """Test async Session update with AioValkeyLock."""
        lock = AioValkeyLock(name="async_valkey_sa_orm4", timeout=30.0)
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
                user = AsyncUserVSA(name="Kate", email="kate@test.com", age=25)
                session.add(user)
                await session.commit()

                user.age = 30
                await session.commit()

                result = await session.execute(
                    select(AsyncUserVSA).where(AsyncUserVSA.name == "Kate")
                )
                updated = result.scalar_one_or_none()
                assert updated is not None
                assert updated.age == 30

        run_async(_test())
        run_async(engine.dispose())

    def test_async_session_delete(self):
        """Test async Session delete with AioValkeyLock."""
        lock = AioValkeyLock(name="async_valkey_sa_orm5", timeout=30.0)
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
                user = AsyncUserVSA(name="Leo", email="leo@test.com")
                session.add(user)
                await session.commit()

                await session.delete(user)
                await session.commit()

                result = await session.execute(
                    select(AsyncUserVSA).where(AsyncUserVSA.name == "Leo")
                )
                found = result.scalar_one_or_none()
                assert found is None

        run_async(_test())
        run_async(engine.dispose())

    def test_async_session_product_crud(self):
        """Test full async CRUD with Product model and AioValkeyLock."""
        lock = AioValkeyLock(name="async_valkey_sa_orm6", timeout=30.0)
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
                product = AsyncProductVSA(name="Widget", price=999, quantity=100)
                session.add(product)
                await session.commit()
                assert product.id is not None

                result = await session.execute(
                    select(AsyncProductVSA).where(AsyncProductVSA.name == "Widget")
                )
                found = result.scalar_one_or_none()
                assert found is not None
                assert found.price == 999
                assert found.quantity == 100

                found.price = 1299
                found.quantity = 95
                await session.commit()

                result = await session.execute(
                    select(AsyncProductVSA).where(AsyncProductVSA.name == "Widget")
                )
                updated = result.scalar_one_or_none()
                assert updated is not None
                assert updated.price == 1299
                assert updated.quantity == 95

                await session.delete(updated)
                await session.commit()

                result = await session.execute(
                    select(AsyncProductVSA).where(AsyncProductVSA.name == "Widget")
                )
                assert result.scalar_one_or_none() is None

        run_async(_test())
        run_async(engine.dispose())


# Warning suppression Tests


@skip_if_no_valkey
class TestAsyncValkeyLockSQLAlchemyWarnings:
    """Test that AioValkeyLock suppresses transaction warnings in async SQLAlchemy."""

    def test_async_begin_no_warning(self):
        """Test BEGIN SQL does not warn with AioValkeyLock."""
        lock = AioValkeyLock(name="async_valkey_sa_warn1", timeout=30.0)
        engine = create_async_engine(
            "rqlite+aiorqlite://localhost:4001",
            connect_args={"lock": lock},
            echo=False,
        )

        async def _test():
            async with engine.begin() as conn:
                with warnings.catch_warnings(record=True) as w:
                    warnings.simplefilter("always")
                    try:
                        await conn.execute(text("BEGIN"))
                    except Exception:
                        pass

                    transaction_warnings = [
                        x
                        for x in w
                        if "BEGIN" in str(x.message) or "not supported" in str(x.message).lower()
                    ]
                    assert len(transaction_warnings) == 0

        run_async(_test())
        run_async(engine.dispose())

    def test_async_commit_no_warning(self):
        """Test COMMIT SQL does not warn with AioValkeyLock."""
        lock = AioValkeyLock(name="async_valkey_sa_warn2", timeout=30.0)
        engine = create_async_engine(
            "rqlite+aiorqlite://localhost:4001",
            connect_args={"lock": lock},
            echo=False,
        )

        async def _test():
            async with engine.begin() as conn:
                with warnings.catch_warnings(record=True) as w:
                    warnings.simplefilter("always")
                    try:
                        await conn.execute(text("COMMIT"))
                    except Exception:
                        pass

                    transaction_warnings = [
                        x
                        for x in w
                        if "COMMIT" in str(x.message) or "not supported" in str(x.message).lower()
                    ]
                    assert len(transaction_warnings) == 0

        run_async(_test())
        run_async(engine.dispose())


# Full workflow Tests


@skip_if_no_valkey
class TestAsyncValkeyLockSQLAlchemyFullWorkflow:
    """Test complete async CRUD workflows with AioValkeyLock."""

    def test_async_full_core_workflow(self):
        """Test complete async Core CRUD lifecycle with AioValkeyLock."""
        lock = AioValkeyLock(name="async_valkey_sa_wf1", timeout=30.0)
        engine = create_async_engine(
            "rqlite+aiorqlite://localhost:4001",
            connect_args={"lock": lock},
            echo=False,
        )

        async def _test():
            async with engine.begin() as conn:
                await conn.run_sync(_create_tables_sync, Base.metadata)

            # INSERT
            async with engine.begin() as conn:
                users = [
                    ("Quinn", "quinn@test.com", 30),
                    ("Ryan", "ryan@test.com", 35),
                    ("Sarah", "sarah@test.com", 40),
                ]
                for name, email, age in users:
                    await conn.execute(
                        insert(AsyncUserVSA).values(name=name, email=email, age=age)
                    )

            # SELECT ALL
            async with engine.begin() as conn:
                result = await conn.execute(select(AsyncUserVSA).order_by(AsyncUserVSA.age))
                rows = result.fetchall()
                assert len(rows) == 3
                assert rows[0].name == "Quinn"

            # SELECT ONE
            async with engine.begin() as conn:
                result = await conn.execute(
                    select(AsyncUserVSA).where(AsyncUserVSA.name == "Ryan")
                )
                row = result.fetchone()
                assert row is not None
                assert row.age == 35

            # UPDATE
            async with engine.begin() as conn:
                await conn.execute(
                    update(AsyncUserVSA).where(AsyncUserVSA.name == "Quinn").values(age=32)
                )

            # VERIFY
            async with engine.begin() as conn:
                result = await conn.execute(
                    select(AsyncUserVSA.age).where(AsyncUserVSA.name == "Quinn")
                )
                row = result.fetchone()
                assert row[0] == 32

            # DELETE
            async with engine.begin() as conn:
                await conn.execute(delete(AsyncUserVSA).where(AsyncUserVSA.name == "Sarah"))

            # FINAL COUNT
            async with engine.begin() as conn:
                result = await conn.execute(select(AsyncUserVSA))
                rows = result.fetchall()
                assert len(rows) == 2

        run_async(_test())
        run_async(engine.dispose())

    def test_async_full_orm_workflow(self):
        """Test complete async ORM CRUD lifecycle with AioValkeyLock."""
        lock = AioValkeyLock(name="async_valkey_sa_wf2", timeout=30.0)
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
                # CREATE
                new_users = [
                    AsyncUserVSA(name="Wendy", email="wendy@test.com", age=28),
                    AsyncUserVSA(name="Xavier", email="xavier@test.com", age=35),
                    AsyncUserVSA(name="Yolanda", email="yolanda@test.com", age=42),
                ]
                session.add_all(new_users)
                await session.commit()

                # SELECT MANY
                result = await session.execute(
                    select(AsyncUserVSA).order_by(AsyncUserVSA.age.asc())
                )
                all_users = result.scalars().all()
                assert len(all_users) == 3
                assert all_users[0].name == "Wendy"

                # SELECT ONE
                result = await session.execute(
                    select(AsyncUserVSA).where(AsyncUserVSA.name == "Xavier")
                )
                xavier = result.scalar_one_or_none()
                assert xavier is not None
                assert xavier.age == 35

                # UPDATE
                result = await session.execute(
                    select(AsyncUserVSA).where(AsyncUserVSA.name == "Wendy")
                )
                wendy = result.scalar_one_or_none()
                assert wendy is not None and wendy.age is not None
                wendy.age += 2
                await session.commit()

                result = await session.execute(
                    select(AsyncUserVSA).where(AsyncUserVSA.name == "Wendy")
                )
                wendy_updated = result.scalar_one_or_none()
                assert wendy_updated is not None
                assert wendy_updated.age == 30

                # DELETE
                result = await session.execute(
                    select(AsyncUserVSA).where(AsyncUserVSA.name == "Yolanda")
                )
                yolanda = result.scalar_one_or_none()
                await session.delete(yolanda)
                await session.commit()

                # FINAL
                result = await session.execute(select(AsyncUserVSA))
                remaining = result.scalars().all()
                assert len(remaining) == 2

        run_async(_test())
        run_async(engine.dispose())


# Engine lock configuration Tests


@skip_if_no_valkey
class TestAsyncValkeyLockSQLAlchemyEngineConfig:
    """Test async engine lock configuration with AioValkeyLock."""

    def test_async_connection_has_valkey_lock(self):
        """Test that async connection created with AioValkeyLock has the lock."""
        lock = AioValkeyLock(name="async_valkey_sa_cfg", timeout=30.0)
        engine = create_async_engine(
            "rqlite+aiorqlite://localhost:4001",
            connect_args={"lock": lock},
            echo=False,
        )

        async def _test():
            async with engine.connect() as conn:
                raw = await conn.get_raw_connection()
                rqlite_conn = raw.driver_connection
                assert rqlite_conn._lock is not None
                assert isinstance(rqlite_conn._lock, AioValkeyLock)

        run_async(_test())
        run_async(engine.dispose())

    def test_async_lock_is_same_instance(self):
        """Test that the lock passed to connect_args is the same instance."""
        lock = AioValkeyLock(name="async_valkey_sa_cfg2", timeout=30.0)
        engine = create_async_engine(
            "rqlite+aiorqlite://localhost:4001",
            connect_args={"lock": lock},
            echo=False,
        )

        async def _test():
            async with engine.connect() as conn:
                raw = await conn.get_raw_connection()
                rqlite_conn = raw.driver_connection
                assert rqlite_conn._lock is lock

        run_async(_test())
        run_async(engine.dispose())


# Connection URL Tests


@skip_if_no_valkey
class TestAsyncValkeyLockSQLAlchemyURL:
    """Test async SQLAlchemy connection URL parsing with AioValkeyLock."""

    def test_async_basic_url(self):
        """Test basic async connection URL."""
        engine = create_async_engine("rqlite+aiorqlite://localhost:4001")
        assert engine.url.host == "localhost"
        assert engine.url.port == 4001

    def test_async_url_with_lock(self):
        """Test async connection URL with AioValkeyLock in connect_args."""
        lock = AioValkeyLock(name="async_valkey_sa_url", timeout=30.0)
        engine = create_async_engine(
            "rqlite+aiorqlite://localhost:4001",
            connect_args={"lock": lock},
            echo=False,
        )
        assert engine.url.host == "localhost"
        assert engine.url.port == 4001
        run_async(engine.dispose())
