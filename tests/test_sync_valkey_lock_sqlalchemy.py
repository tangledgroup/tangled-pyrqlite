"""Tests for SQLAlchemy Core and ORM with sync Valkey distributed lock.

Covers:
- SQLAlchemy Core operations (insert, select, update, delete) with ValkeyLock
- SQLAlchemy ORM operations (Session, query) with ValkeyLock
- Lock suppresses transaction warnings
- Full CRUD workflows

Prerequisites:
    uv add tangled-pyrqlite[valkey]
    podman run -d --name valkey-test -p 6379:6379 docker.io/valkey/valkey
    rqlite running on localhost:4001
"""

from __future__ import annotations

import warnings

import pytest
from sqlalchemy import (
    create_engine,
    delete,
    insert,
    select,
    text,
    update,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column

import rqlite
from rqlite import ValkeyLock


def _has_valkey() -> bool:
    """Check if Valkey is reachable."""
    try:
        import valkey

        client = valkey.Redis(host="localhost", port=6379, db=0, socket_connect_timeout=1.0)
        return bool(client.ping())
    except Exception:
        return False


# Models


class Base(DeclarativeBase):
    """Base class for SQLAlchemy models."""


class UserVSA(Base):
    """User model for sync ValkeyLock SQLAlchemy tests."""

    __tablename__ = "sa_valkey_users"

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(nullable=False)
    email: Mapped[str | None] = mapped_column(unique=True)
    age: Mapped[int | None] = mapped_column()


class ProductVSA(Base):
    """Product model for sync ValkeyLock SQLAlchemy tests."""

    __tablename__ = "sa_valkey_products"

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(nullable=False)
    price: Mapped[int] = mapped_column()
    quantity: Mapped[int] = mapped_column(default=0)


# Fixtures


@pytest.fixture(scope="function")
def engine_valkey():
    """Create SQLAlchemy engine with ValkeyLock."""
    lock = ValkeyLock(name="sync_valkey_sa", timeout=30.0)
    engine = create_engine(
        "rqlite://localhost:4001",
        connect_args={"lock": lock},
        echo=False,
    )
    yield engine, lock
    engine.dispose()


@pytest.fixture(scope="function")
def tables_valkey(engine_valkey):
    """Create and cleanup test tables for ValkeyLock engine."""
    engine, _ = engine_valkey
    with engine.connect() as conn:
        conn.execute(text("DROP TABLE IF EXISTS sa_valkey_users"))
        conn.execute(text("DROP TABLE IF EXISTS sa_valkey_products"))
        conn.commit()

    Base.metadata.create_all(engine)
    yield
    Base.metadata.drop_all(engine)


# Skip marker

skip_if_no_valkey = pytest.mark.skipif(not _has_valkey(), reason="Valkey not available")


# SQLAlchemy Core Tests


@skip_if_no_valkey
class TestSyncValkeyLockSQLAlchemyCore:
    """Test SQLAlchemy Core operations with ValkeyLock."""

    def test_create_tables(self, engine_valkey):
        """Test table creation with ValkeyLock."""
        engine, _ = engine_valkey
        Base.metadata.create_all(engine)

        with engine.connect() as conn:
            result = conn.execute(text("SELECT name FROM sqlite_master WHERE type='table'"))
            tables = [row[0] for row in result]
            assert "sa_valkey_users" in tables
            assert "sa_valkey_products" in tables

        Base.metadata.drop_all(engine)

    def test_insert_select(self, engine_valkey, tables_valkey):
        """Test INSERT and SELECT with ValkeyLock."""
        engine, _ = engine_valkey

        with engine.connect() as conn:
            stmt = insert(UserVSA).values(name="Alice", email="alice@test.com", age=30)
            conn.execute(stmt)
            conn.commit()

            result = conn.execute(select(UserVSA).where(UserVSA.name == "Alice"))
            row = result.fetchone()
            assert row is not None
            assert row.name == "Alice"
            assert row.age == 30

    def test_update(self, engine_valkey, tables_valkey):
        """Test UPDATE with ValkeyLock."""
        engine, _ = engine_valkey

        with engine.connect() as conn:
            stmt = insert(UserVSA).values(name="Bob", age=25)
            conn.execute(stmt)
            conn.commit()

            stmt = update(UserVSA).where(UserVSA.name == "Bob").values(age=30)
            conn.execute(stmt)
            conn.commit()

            result = conn.execute(select(UserVSA.age).where(UserVSA.name == "Bob"))
            row = result.fetchone()
            assert row[0] == 30

    def test_delete(self, engine_valkey, tables_valkey):
        """Test DELETE with ValkeyLock."""
        engine, _ = engine_valkey

        with engine.connect() as conn:
            stmt = insert(UserVSA).values(name="Charlie", email="charlie@test.com")
            conn.execute(stmt)
            conn.commit()

            stmt = delete(UserVSA).where(UserVSA.name == "Charlie")
            conn.execute(stmt)
            conn.commit()

            result = conn.execute(select(UserVSA).where(UserVSA.name == "Charlie"))
            row = result.fetchone()
            assert row is None

    def test_executemany(self, engine_valkey, tables_valkey):
        """Test bulk insert with ValkeyLock."""
        engine, _ = engine_valkey

        with engine.connect() as conn:
            users = [
                {"name": "Diana", "email": "diana@test.com", "age": 28},
                {"name": "Eve", "email": "eve@test.com", "age": 32},
                {"name": "Frank", "email": "frank@test.com", "age": 45},
            ]
            conn.execute(insert(UserVSA), users)
            conn.commit()

            result = conn.execute(select(UserVSA).order_by(UserVSA.age))
            rows = result.fetchall()
            assert len(rows) == 3
            assert rows[0].name == "Diana"
            assert rows[-1].name == "Frank"

    def test_text_sql(self, engine_valkey, tables_valkey):
        """Test raw text SQL with ValkeyLock."""
        engine, _ = engine_valkey

        with engine.connect() as conn:
            conn.execute(text("INSERT INTO sa_valkey_users (name, email) VALUES (:n, :e)"), {
                "n": "Grace",
                "e": "grace@test.com",
            })
            conn.commit()

            result = conn.execute(
                text("SELECT name, email FROM sa_valkey_users WHERE name = :n"), {"n": "Grace"}
            )
            row = result.fetchone()
            assert row is not None
            assert row[0] == "Grace"


# SQLAlchemy ORM Tests


@skip_if_no_valkey
class TestSyncValkeyLockSQLAlchemyORM:
    """Test SQLAlchemy ORM operations with ValkeyLock."""

    def test_session_add_commit(self, engine_valkey, tables_valkey):
        """Test Session.add() and commit with ValkeyLock."""
        engine, _ = engine_valkey

        with Session(engine) as session:
            user = UserVSA(name="Henry", email="henry@test.com", age=45)
            session.add(user)
            session.commit()
            assert user.id is not None

    def test_session_add_all(self, engine_valkey, tables_valkey):
        """Test Session.add_all() with ValkeyLock."""
        engine, _ = engine_valkey

        with Session(engine) as session:
            users = [
                UserVSA(name="Ivy", email="ivy@test.com"),
                UserVSA(name="Jack", email="jack@test.com"),
            ]
            session.add_all(users)
            session.commit()

        with Session(engine) as session:
            users = session.query(UserVSA).all()
            assert len(users) == 2

    def test_session_query_filter(self, engine_valkey, tables_valkey):
        """Test Session query with filter_by and first."""
        engine, _ = engine_valkey

        with Session(engine) as session:
            for name, email in [
                ("Kate", "kate@test.com"),
                ("Leo", "leo@test.com"),
            ]:
                session.add(UserVSA(name=name, email=email))
            session.commit()

        with Session(engine) as session:
            kate = session.query(UserVSA).filter_by(name="Kate").first()
            assert kate is not None
            assert kate.email == "kate@test.com"

    def test_session_update(self, engine_valkey, tables_valkey):
        """Test Session update with ValkeyLock."""
        engine, _ = engine_valkey

        with Session(engine) as session:
            user = UserVSA(name="Mia", email="mia@test.com", age=25)
            session.add(user)
            session.commit()

            user.age = 30
            session.commit()

            updated = session.query(UserVSA).filter_by(name="Mia").first()
            assert updated is not None
            assert updated.age == 30

    def test_session_delete(self, engine_valkey, tables_valkey):
        """Test Session delete with ValkeyLock."""
        engine, _ = engine_valkey

        with Session(engine) as session:
            user = UserVSA(name="Noah", email="noah@test.com")
            session.add(user)
            session.commit()

            session.delete(user)
            session.commit()

            found = session.query(UserVSA).filter_by(name="Noah").first()
            assert found is None

    def test_session_product_crud(self, engine_valkey, tables_valkey):
        """Test full CRUD with Product model and ValkeyLock."""
        engine, _ = engine_valkey

        with Session(engine) as session:
            # Create
            product = ProductVSA(name="Widget", price=999, quantity=100)
            session.add(product)
            session.commit()
            assert product.id is not None

            # Read
            found = session.query(ProductVSA).filter_by(name="Widget").first()
            assert found is not None
            assert found.price == 999
            assert found.quantity == 100

            # Update
            found.price = 1299
            found.quantity = 95
            session.commit()

            updated = session.query(ProductVSA).filter_by(name="Widget").first()
            assert updated is not None
            assert updated.price == 1299
            assert updated.quantity == 95

            # Delete
            session.delete(updated)
            session.commit()

            assert session.query(ProductVSA).filter_by(name="Widget").first() is None


# Warning suppression Tests


@skip_if_no_valkey
class TestSyncValkeyLockSQLAlchemyWarnings:
    """Test that ValkeyLock suppresses transaction warnings in SQLAlchemy."""

    def test_begin_no_warning(self, engine_valkey):
        """Test BEGIN SQL does not warn with ValkeyLock."""
        engine, _ = engine_valkey

        with engine.connect() as conn:
            with warnings.catch_warnings(record=True) as w:
                warnings.simplefilter("always")
                try:
                    conn.execute(text("BEGIN"))
                except Exception:
                    pass

                transaction_warnings = [
                    x
                    for x in w
                    if "BEGIN" in str(x.message) or "not supported" in str(x.message).lower()
                ]
                assert len(transaction_warnings) == 0

    def test_commit_no_warning(self, engine_valkey):
        """Test COMMIT SQL does not warn with ValkeyLock."""
        engine, _ = engine_valkey

        with engine.connect() as conn:
            with warnings.catch_warnings(record=True) as w:
                warnings.simplefilter("always")
                try:
                    conn.execute(text("COMMIT"))
                except Exception:
                    pass

                transaction_warnings = [
                    x
                    for x in w
                    if "COMMIT" in str(x.message) or "not supported" in str(x.message).lower()
                ]
                assert len(transaction_warnings) == 0

    def test_rollback_no_warning(self, engine_valkey):
        """Test ROLLBACK SQL does not warn with ValkeyLock."""
        engine, _ = engine_valkey

        with engine.connect() as conn:
            with warnings.catch_warnings(record=True) as w:
                warnings.simplefilter("always")
                try:
                    conn.execute(text("ROLLBACK"))
                except Exception:
                    pass

                transaction_warnings = [
                    x
                    for x in w
                    if "ROLLBACK" in str(x.message) or "not supported" in str(x.message).lower()
                ]
                assert len(transaction_warnings) == 0


# Full workflow Tests


@skip_if_no_valkey
class TestSyncValkeyLockSQLAlchemyFullWorkflow:
    """Test complete CRUD workflows with ValkeyLock."""

    def test_full_core_workflow(self, engine_valkey):
        """Test complete Core CRUD lifecycle with ValkeyLock."""
        engine, _ = engine_valkey

        Base.metadata.drop_all(engine)
        Base.metadata.create_all(engine)

        try:
            with engine.connect() as conn:
                # INSERT
                users = [
                    ("Quinn", "quinn@test.com", 30),
                    ("Ryan", "ryan@test.com", 35),
                    ("Sarah", "sarah@test.com", 40),
                ]
                for name, email, age in users:
                    conn.execute(insert(UserVSA).values(name=name, email=email, age=age))
                conn.commit()

                # SELECT ALL
                result = conn.execute(select(UserVSA).order_by(UserVSA.age))
                rows = result.fetchall()
                assert len(rows) == 3
                assert rows[0].name == "Quinn"

                # SELECT ONE
                result = conn.execute(select(UserVSA).where(UserVSA.name == "Ryan"))
                row = result.fetchone()
                assert row is not None
                assert row.age == 35

                # UPDATE
                conn.execute(update(UserVSA).where(UserVSA.name == "Quinn").values(age=32))
                conn.commit()

                # VERIFY
                result = conn.execute(select(UserVSA.age).where(UserVSA.name == "Quinn"))
                row = result.fetchone()
                assert row[0] == 32

                # DELETE
                conn.execute(delete(UserVSA).where(UserVSA.name == "Sarah"))
                conn.commit()

                # FINAL COUNT
                result = conn.execute(select(UserVSA))
                rows = result.fetchall()
                assert len(rows) == 2
        finally:
            Base.metadata.drop_all(engine)

    def test_full_orm_workflow(self, engine_valkey):
        """Test complete ORM CRUD lifecycle with ValkeyLock."""
        engine, _ = engine_valkey

        Base.metadata.drop_all(engine)
        Base.metadata.create_all(engine)

        try:
            with Session(engine) as session:
                # CREATE
                new_users = [
                    UserVSA(name="Wendy", email="wendy@test.com", age=28),
                    UserVSA(name="Xavier", email="xavier@test.com", age=35),
                    UserVSA(name="Yolanda", email="yolanda@test.com", age=42),
                ]
                session.add_all(new_users)
                session.commit()

                # SELECT MANY
                all_users = session.query(UserVSA).order_by(UserVSA.age.asc()).all()
                assert len(all_users) == 3
                assert all_users[0].name == "Wendy"

                # SELECT ONE
                xavier = session.query(UserVSA).filter_by(name="Xavier").first()
                assert xavier is not None
                assert xavier.age == 35

                # UPDATE
                wendy = session.query(UserVSA).filter_by(name="Wendy").first()
                assert wendy is not None and wendy.age is not None
                wendy.age += 2
                session.commit()

                wendy_updated = session.query(UserVSA).filter_by(name="Wendy").first()
                assert wendy_updated is not None
                assert wendy_updated.age == 30

                # DELETE
                yolanda = session.query(UserVSA).filter_by(name="Yolanda").first()
                session.delete(yolanda)
                session.commit()

                # FINAL
                remaining = session.query(UserVSA).all()
                assert len(remaining) == 2
        finally:
            Base.metadata.drop_all(engine)


# Engine lock configuration Tests


@skip_if_no_valkey
class TestSyncValkeyLockSQLAlchemyEngineConfig:
    """Test engine lock configuration with ValkeyLock."""

    def test_connection_has_valkey_lock(self, engine_valkey):
        """Test that connection created with ValkeyLock has the lock."""
        engine, lock = engine_valkey

        with engine.connect() as conn:
            dbapi_conn = conn.connection
            rqlite_conn = dbapi_conn._conn
            assert rqlite_conn._lock is not None
            assert isinstance(rqlite_conn._lock, ValkeyLock)

    def test_lock_is_same_instance(self, engine_valkey):
        """Test that the lock passed to connect_args is the same instance."""
        engine, lock = engine_valkey

        with engine.connect() as conn:
            dbapi_conn = conn.connection
            rqlite_conn = dbapi_conn._conn
            assert rqlite_conn._lock is lock
