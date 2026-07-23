"""Sync SQLAlchemy tests with Valkey distributed lock.

Tests SQLAlchemy Core and ORM operations using the rqlite sync dialect
with ValkeyLock for distributed locking. Covers engine creation, Core
operations (insert, select, update, delete), ORM models and sessions,
reflection, and full CRUD workflows.

Prerequisites:
    rqlite running on localhost:4001
    valkey running on localhost:6379
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
    inspect,
    select,
    text,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column

from rqlite import ValkeyLock

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


def _make_lock(name: str = "sa_sync") -> ValkeyLock:
    return ValkeyLock(name=f"test_{name}", timeout=30.0)


def _make_engine(
    lock: ValkeyLock | None = None,
    read_consistency=None,
) -> None:
    """Create engine — returns nothing, caller creates inline."""
    pass  # Factory pattern: callers build inline


# ORM Base


class Base(DeclarativeBase):
    pass


class User(Base):
    __tablename__ = "sa_sync_valkey_users"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String, nullable=False)
    email: Mapped[str | None] = mapped_column(String, unique=True)
    age: Mapped[int | None] = mapped_column(Integer)
    score: Mapped[float | None] = mapped_column(Float)


class Product(Base):
    __tablename__ = "sa_sync_valkey_products"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String, nullable=False)
    price: Mapped[float] = mapped_column(Float)
    quantity: Mapped[int] = mapped_column(Integer, default=0)


# Engine Tests


@skip_if_no_valkey
class TestSyncSA_Engine:
    """Sync SQLAlchemy engine tests with ValkeyLock."""

    def test_create_engine(self):
        lock = _make_lock("engine")
        engine = create_engine(
            "rqlite://localhost:4001",
            connect_args={"lock": lock},
            echo=False,
        )
        assert engine is not None
        engine.dispose()

    def test_create_engine_with_lock(self):
        lock = _make_lock("engine_lock")
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
        tx_warnings = [
            x for x in w if "BEGIN/COMMIT/ROLLBACK" in str(x.message)
        ]
        assert len(tx_warnings) == 0

    def test_engine_url_parsing(self):
        lock = _make_lock("url")
        engine = create_engine(
            "rqlite://localhost:4001",
            connect_args={"lock": lock},
        )
        with engine.connect() as conn:
            result = conn.execute(text("SELECT 'hello'"))
            assert result.scalar() == "hello"
        engine.dispose()


# Core Tests


@skip_if_no_valkey
class TestSyncSA_Core:
    """Sync SQLAlchemy Core tests with ValkeyLock."""

    def _engine(self):
        lock = _make_lock("core")
        return create_engine(
            "rqlite://localhost:4001",
            connect_args={"lock": lock},
            echo=False,
        )

    def test_create_table_core(self):
        engine = self._engine()
        metadata = MetaData()
        Table(
            "sa_sync_core_test",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("name", String, nullable=False),
            Column("value", Integer),
        )
        try:
            metadata.create_all(engine)
            with engine.connect() as conn:
                result = conn.execute(
                    text(
                        "SELECT name FROM sqlite_master "
                        "WHERE type='table' AND name='sa_sync_core_test'"
                    )
                )
                assert result.scalar() is not None
            engine.dispose()
        finally:
            with engine.connect() as conn:
                conn.execute(text("DROP TABLE IF EXISTS sa_sync_core_test"))
                conn.commit()

    def test_insert_core(self):
        engine = self._engine()
        try:
            with engine.connect() as conn:
                conn.execute(text("DROP TABLE IF EXISTS sa_sync_core_ins"))
                conn.execute(text("""
                    CREATE TABLE sa_sync_core_ins (
                        id INTEGER PRIMARY KEY,
                        name TEXT NOT NULL,
                        age INTEGER
                    )
                """))
                conn.commit()

                conn.execute(
                    insert(
                        Table(
                            "sa_sync_core_ins",
                            MetaData(),
                            Column("id", Integer, primary_key=True),
                            Column("name", String),
                            Column("age", Integer),
                        )
                    ).values(name="Alice", age=30)
                )
                conn.commit()

                result = conn.execute(
                    text("SELECT name, age FROM sa_sync_core_ins")
                )
                row = result.fetchone()
                assert row is not None
                assert row[0] == "Alice"
                assert row[1] == 30
        finally:
            with engine.connect() as conn:
                conn.execute(text("DROP TABLE IF EXISTS sa_sync_core_ins"))
                conn.commit()
            engine.dispose()

    def test_select_core(self):
        engine = self._engine()
        try:
            with engine.connect() as conn:
                conn.execute(text("DROP TABLE IF EXISTS sa_sync_core_sel"))
                conn.execute(text("""
                    CREATE TABLE sa_sync_core_sel (
                        id INTEGER PRIMARY KEY,
                        name TEXT NOT NULL,
                        score REAL
                    )
                """))
                conn.execute(
                    text("INSERT INTO sa_sync_core_sel VALUES (1, 'A', 95.5)")
                )
                conn.execute(
                    text("INSERT INTO sa_sync_core_sel VALUES (2, 'B', 87.3)")
                )
                conn.commit()

                result = conn.execute(
                    text("SELECT name, score FROM sa_sync_core_sel ORDER BY score DESC")
                )
                rows = result.fetchall()
                assert len(rows) == 2
                assert rows[0][0] == "A"
                assert rows[1][0] == "B"
        finally:
            with engine.connect() as conn:
                conn.execute(text("DROP TABLE IF EXISTS sa_sync_core_sel"))
                conn.commit()
            engine.dispose()

    def test_update_core(self):
        engine = self._engine()
        try:
            with engine.connect() as conn:
                conn.execute(text("DROP TABLE IF EXISTS sa_sync_core_upd"))
                conn.execute(text("""
                    CREATE TABLE sa_sync_core_upd (
                        id INTEGER PRIMARY KEY,
                        name TEXT,
                        age INTEGER
                    )
                """))
                conn.execute(
                    text("INSERT INTO sa_sync_core_upd VALUES (1, 'Alice', 30)")
                )
                conn.commit()

                conn.execute(
                    text("UPDATE sa_sync_core_upd SET age = 31 WHERE name = 'Alice'")
                )
                conn.commit()

                result = conn.execute(
                    text("SELECT age FROM sa_sync_core_upd WHERE name = 'Alice'")
                )
                assert result.scalar() == 31
        finally:
            with engine.connect() as conn:
                conn.execute(text("DROP TABLE IF EXISTS sa_sync_core_upd"))
                conn.commit()
            engine.dispose()

    def test_delete_core(self):
        engine = self._engine()
        try:
            with engine.connect() as conn:
                conn.execute(text("DROP TABLE IF EXISTS sa_sync_core_del"))
                conn.execute(text("""
                    CREATE TABLE sa_sync_core_del (
                        id INTEGER PRIMARY KEY,
                        name TEXT
                    )
                """))
                conn.execute(
                    text("INSERT INTO sa_sync_core_del VALUES (1, 'ToDelete')")
                )
                conn.commit()

                conn.execute(
                    text("DELETE FROM sa_sync_core_del WHERE name = 'ToDelete'")
                )
                conn.commit()

                result = conn.execute(
                    text("SELECT COUNT(*) FROM sa_sync_core_del")
                )
                assert result.scalar() == 0
        finally:
            with engine.connect() as conn:
                conn.execute(text("DROP TABLE IF EXISTS sa_sync_core_del"))
                conn.commit()
            engine.dispose()

    def test_text_execution(self):
        engine = self._engine()
        with engine.connect() as conn:
            result = conn.execute(text("SELECT 1 + 1"))
            assert result.scalar() == 2
        engine.dispose()

    def test_parameterized_text(self):
        engine = self._engine()
        with engine.connect() as conn:
            result = conn.execute(
                text("SELECT :a + :b"), {"a": 10, "b": 20}
            )
            assert result.scalar() == 30
        engine.dispose()


# ORM Tests


@skip_if_no_valkey
class TestSyncSA_ORM:
    """Sync SQLAlchemy ORM tests with ValkeyLock."""

    def _engine(self):
        lock = _make_lock("orm")
        return create_engine(
            "rqlite://localhost:4001",
            connect_args={"lock": lock},
            echo=False,
        )

    def test_orm_create_all(self):
        engine = self._engine()
        try:
            Base.metadata.create_all(engine)
            with engine.connect() as conn:
                result = conn.execute(
                    text(
                        "SELECT name FROM sqlite_master "
                        "WHERE type='table' AND name='sa_sync_valkey_users'"
                    )
                )
                assert result.scalar() is not None
        finally:
            Base.metadata.drop_all(engine)
            engine.dispose()

    def test_orm_add_and_query(self):
        engine = self._engine()
        try:
            Base.metadata.create_all(engine)
            with Session(engine) as session:
                user = User(name="Alice", email="alice@test.com", age=30, score=95.5)
                session.add(user)
                session.commit()

                result = session.execute(select(User))
                users = result.scalars().all()
                assert len(users) == 1
                assert users[0].name == "Alice"
                assert users[0].email == "alice@test.com"
        finally:
            Base.metadata.drop_all(engine)
            engine.dispose()

    def test_orm_query_filter(self):
        engine = self._engine()
        try:
            Base.metadata.create_all(engine)
            with Session(engine) as session:
                session.add(User(name="Young", age=20))
                session.add(User(name="Old", age=60))
                session.commit()

                result = session.execute(
                    select(User).where(User.age > 30).order_by(User.name)
                )
                users = result.scalars().all()
                assert len(users) == 1
                assert users[0].name == "Old"
        finally:
            Base.metadata.drop_all(engine)
            engine.dispose()

    def test_orm_update(self):
        engine = self._engine()
        try:
            Base.metadata.create_all(engine)
            with Session(engine) as session:
                session.add(User(name="Alice", age=30))
                session.commit()

                user = session.execute(select(User)).scalars().first()
                assert user is not None
                user.age = 31
                session.commit()

                user = session.execute(select(User)).scalars().first()
                assert user is not None
                assert user.age == 31
        finally:
            Base.metadata.drop_all(engine)
            engine.dispose()

    def test_orm_delete(self):
        engine = self._engine()
        try:
            Base.metadata.create_all(engine)
            with Session(engine) as session:
                session.add(User(name="ToDelete"))
                session.commit()

                user = session.execute(select(User)).scalars().first()
                session.delete(user)
                session.commit()

                result = session.execute(select(User))
                assert result.scalars().all() == []
        finally:
            Base.metadata.drop_all(engine)
            engine.dispose()

    def test_orm_multiple_add(self):
        engine = self._engine()
        try:
            Base.metadata.create_all(engine)
            with Session(engine) as session:
                users = [
                    User(name=f"User{i}", age=i * 10)
                    for i in range(1, 6)
                ]
                session.add_all(users)
                session.commit()

                result = session.execute(
                    select(User).order_by(User.age)
                )
                fetched = result.scalars().all()
                assert len(fetched) == 5
                assert fetched[0].name == "User1"
                assert fetched[-1].name == "User5"
        finally:
            Base.metadata.drop_all(engine)
            engine.dispose()

    def test_orm_product_crud(self):
        engine = self._engine()
        try:
            Base.metadata.create_all(engine)
            with Session(engine) as session:
                # Create
                product = Product(name="Widget", price=9.99, quantity=100)
                session.add(product)
                session.commit()

                # Read
                result = session.execute(select(Product))
                products = result.scalars().all()
                assert len(products) == 1
                assert products[0].price == 9.99

                # Update
                products[0].price = 12.99
                session.commit()
                session.refresh(products[0])
                assert products[0].price == 12.99

                # Delete
                session.delete(products[0])
                session.commit()
                assert session.execute(select(Product)).scalars().all() == []
        finally:
            Base.metadata.drop_all(engine)
            engine.dispose()

    def test_orm_session_context_manager(self):
        engine = self._engine()
        try:
            Base.metadata.create_all(engine)
            with Session(engine) as session:
                session.add(User(name="CtxUser", age=25))
                session.commit()
            # Session closed after context
        finally:
            Base.metadata.drop_all(engine)
            engine.dispose()

    def test_orm_scalar_one_or_none(self):
        engine = self._engine()
        try:
            Base.metadata.create_all(engine)
            with Session(engine) as session:
                session.add(User(name="ScalarTest", age=40))
                session.commit()

                count = session.execute(
                    select(User).where(User.name == "ScalarTest")
                ).scalars().first()
                assert count is not None
                assert count.name == "ScalarTest"

                missing = session.execute(
                    select(User).where(User.name == "NonExistent")
                ).scalars().first()
                assert missing is None
        finally:
            Base.metadata.drop_all(engine)
            engine.dispose()


# Reflection Tests


@skip_if_no_valkey
class TestSyncSA_Reflection:
    """Sync SQLAlchemy reflection tests with ValkeyLock."""

    def _engine(self):
        lock = _make_lock("reflect")
        return create_engine(
            "rqlite://localhost:4001",
            connect_args={"lock": lock},
            echo=False,
        )

    def test_reflect_table(self):
        engine = self._engine()
        try:
            with engine.connect() as conn:
                conn.execute(text("DROP TABLE IF EXISTS sa_sync_reflect"))
                conn.execute(text("""
                    CREATE TABLE sa_sync_reflect (
                        id INTEGER PRIMARY KEY,
                        name TEXT NOT NULL,
                        value REAL
                    )
                """))
                conn.commit()

            metadata = MetaData()
            metadata.reflect(bind=engine)
            assert "sa_sync_reflect" in metadata.tables
            table = metadata.tables["sa_sync_reflect"]
            assert "id" in table.columns
            assert "name" in table.columns
            assert "value" in table.columns
        finally:
            with engine.connect() as conn:
                conn.execute(text("DROP TABLE IF EXISTS sa_sync_reflect"))
                conn.commit()
            engine.dispose()

    def test_has_table(self):
        engine = self._engine()
        try:
            with engine.connect() as conn:
                conn.execute(text("DROP TABLE IF EXISTS sa_sync_has_tbl"))
                conn.execute(text("""
                    CREATE TABLE sa_sync_has_tbl (id INTEGER PRIMARY KEY)
                """))
                conn.commit()

            insp = inspect(engine)
            tables = insp.get_table_names()
            assert "sa_sync_has_tbl" in tables
        finally:
            with engine.connect() as conn:
                conn.execute(text("DROP TABLE IF EXISTS sa_sync_has_tbl"))
                conn.commit()
            engine.dispose()


# Complex Workflow Tests


@skip_if_no_valkey
class TestSyncSA_Workflow:
    """Sync SQLAlchemy complex workflow tests with ValkeyLock."""

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
            Base.metadata.create_all(engine)

            with Session(engine) as session:
                # Batch insert
                users = [
                    User(name=f"User{i}", age=i * 5, score=float(i * 10))
                    for i in range(1, 11)
                ]
                session.add_all(users)
                session.commit()

                # Query with filter
                result = session.execute(
                    select(User).where(User.age >= 25).order_by(User.score.desc())
                )
                filtered = result.scalars().all()
                assert len(filtered) == 6

                # Update
                for user in filtered[:3]:
                    user.score = 100.0
                session.commit()

                # Verify update
                result = session.execute(
                    select(User).where(User.score == 100.0)
                )
                assert len(result.scalars().all()) == 3

                # Delete (age < 15 means age 5, 10 => 2 users)
                to_delete = session.execute(
                    select(User).where(User.age < 15)
                ).scalars().all()
                for user in to_delete:
                    session.delete(user)
                session.commit()

                # Final count: started with 10, deleted 2 => 8
                result = session.execute(select(User))
                remaining = result.scalars().all()
                assert len(remaining) == 8

        finally:
            Base.metadata.drop_all(engine)
            engine.dispose()

    def test_multiple_sessions(self):
        engine = self._engine()
        try:
            Base.metadata.create_all(engine)

            with Session(engine) as session1:
                session1.add(User(name="FromSession1", age=20))
                session1.commit()

            with Session(engine) as session2:
                result = session2.execute(select(User))
                users = result.scalars().all()
                assert len(users) == 1
                assert users[0].name == "FromSession1"

        finally:
            Base.metadata.drop_all(engine)
            engine.dispose()

    def test_engine_connect_context(self):
        engine = self._engine()
        try:
            Base.metadata.create_all(engine)

            with engine.connect() as conn:
                conn.execute(
                    text(
                        "INSERT INTO sa_sync_valkey_users "
                        "(name, age) VALUES ('Ctx', 99)"
                    )
                )
                conn.commit()

                result = conn.execute(
                    text("SELECT age FROM sa_sync_valkey_users WHERE name = 'Ctx'")
                )
                assert result.scalar() == 99

            # Cleanup
            with engine.connect() as conn:
                conn.execute(text("DELETE FROM sa_sync_valkey_users WHERE name = 'Ctx'"))
                conn.commit()

        finally:
            Base.metadata.drop_all(engine)
            engine.dispose()
