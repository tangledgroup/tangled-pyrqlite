"""Tests for SQLAlchemy dialect with lock functionality.

This module tests SQLAlchemy Core and ORM operations with and without locks,
ensuring proper integration with rqlite's transaction support.
"""

import threading
import warnings

import pytest
from sqlalchemy import (
    create_engine,
    insert,
    select,
    text,
    update,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column

from rqlite.types import ThreadLock


class Base(DeclarativeBase):
    """Base class for SQLAlchemy models."""


class UserSA(Base):
    """Test user model for SQLAlchemy tests."""

    __tablename__ = "sa_lock_users"

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(nullable=False)
    email: Mapped[str | None] = mapped_column(unique=True)
    age: Mapped[int | None] = mapped_column()


class ProductSA(Base):
    """Test product model for SQLAlchemy tests."""

    __tablename__ = "sa_lock_products"

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(nullable=False)
    price: Mapped[int] = mapped_column()  # cents
    quantity: Mapped[int] = mapped_column(default=0)


# Fixtures for engines with and without locks
@pytest.fixture(scope="function")
def engine_without_lock():
    """Create SQLAlchemy engine without lock."""
    engine = create_engine("rqlite://localhost:4001", echo=False)
    yield engine
    engine.dispose()


@pytest.fixture(scope="function")
def engine_with_threadlock():
    """Create SQLAlchemy engine with ThreadLock."""
    lock = ThreadLock()
    engine = create_engine(
        "rqlite://localhost:4001",
        connect_args={"lock": lock},
        echo=False,
    )
    yield engine
    engine.dispose()


@pytest.fixture(scope="function")
def engine_with_threading_lock():
    """Create SQLAlchemy engine with threading.Lock."""
    lock = threading.Lock()
    engine = create_engine(
        "rqlite://localhost:4001",
        connect_args={"lock": lock},
        echo=False,
    )
    yield engine
    engine.dispose()


@pytest.fixture(scope="function")
def tables_without_lock(engine_without_lock):
    """Create and cleanup test tables for engine without lock."""
    # Drop all sa_lock_* tables first to ensure clean state
    with engine_without_lock.connect() as conn:
        conn.execute(text("DROP TABLE IF EXISTS sa_lock_users"))
        conn.execute(text("DROP TABLE IF EXISTS sa_lock_products"))
        conn.commit()

    Base.metadata.create_all(engine_without_lock)
    yield
    Base.metadata.drop_all(engine_without_lock)


@pytest.fixture(scope="function")
def tables_with_lock(engine_with_threadlock):
    """Create and cleanup test tables for engine with lock."""
    # Drop all sa_lock_* tables first to ensure clean state
    with engine_with_threadlock.connect() as conn:
        conn.execute(text("DROP TABLE IF EXISTS sa_lock_users"))
        conn.execute(text("DROP TABLE IF EXISTS sa_lock_products"))
        conn.commit()

    Base.metadata.create_all(engine_with_threadlock)
    yield
    Base.metadata.drop_all(engine_with_threadlock)


class TestSyncThreadLockSQLAlchemyCoreWithoutLock:
    """Test SQLAlchemy Core operations without lock."""

    def test_create_tables(self, engine_without_lock):
        """Test table creation without lock."""
        Base.metadata.create_all(engine_without_lock)

        with engine_without_lock.connect() as conn:
            result = conn.execute(text("SELECT name FROM sqlite_master WHERE type='table'"))
            tables = [row[0] for row in result]
            assert "sa_lock_users" in tables
            assert "sa_lock_products" in tables

    def test_insert_select(self, engine_without_lock, tables_without_lock):
        """Test INSERT and SELECT without lock."""
        with engine_without_lock.connect() as conn:
            stmt = insert(UserSA).values(name="Alice", email="alice@test.com", age=30)
            conn.execute(stmt)
            conn.commit()

            result = conn.execute(select(UserSA).where(UserSA.name == "Alice"))
            row = result.fetchone()
            assert row is not None
            assert row.name == "Alice"
            assert row.age == 30

    def test_update(self, engine_without_lock, tables_without_lock):
        """Test UPDATE without lock."""
        with engine_without_lock.connect() as conn:
            # Insert first
            stmt = insert(UserSA).values(name="Bob", age=25)
            conn.execute(stmt)
            conn.commit()

            # Update
            stmt = update(UserSA).where(UserSA.name == "Bob").values(age=30)
            conn.execute(stmt)
            conn.commit()

            # Verify
            result = conn.execute(select(UserSA.age).where(UserSA.name == "Bob"))
            row = result.fetchone()
            assert row[0] == 30

    def test_delete(self, engine_without_lock, tables_without_lock):
        """Test DELETE without lock."""
        from sqlalchemy import delete as delete_stmt

        with engine_without_lock.connect() as conn:
            # Insert
            stmt = insert(UserSA).values(name="Charlie", email="charlie@test.com")
            conn.execute(stmt)
            conn.commit()

            # Delete
            stmt = delete_stmt(UserSA).where(UserSA.name == "Charlie")
            conn.execute(stmt)
            conn.commit()

            # Verify deleted
            result = conn.execute(select(UserSA).where(UserSA.name == "Charlie"))
            row = result.fetchone()
            assert row is None


class TestSyncThreadLockSQLAlchemyCoreWithLock:
    """Test SQLAlchemy Core operations with lock."""

    def test_create_tables_with_threadlock(self, engine_with_threadlock):
        """Test table creation with ThreadLock."""
        Base.metadata.create_all(engine_with_threadlock)

        with engine_with_threadlock.connect() as conn:
            result = conn.execute(text("SELECT name FROM sqlite_master WHERE type='table'"))
            tables = [row[0] for row in result]
            assert "sa_lock_users" in tables
            assert "sa_lock_products" in tables

    def test_insert_select_with_threadlock(self, engine_with_threadlock, tables_with_lock):
        """Test INSERT and SELECT with ThreadLock."""
        with engine_with_threadlock.connect() as conn:
            stmt = insert(UserSA).values(name="David", email="david@test.com", age=35)
            conn.execute(stmt)
            conn.commit()

            result = conn.execute(select(UserSA).where(UserSA.name == "David"))
            row = result.fetchone()
            assert row is not None
            assert row.name == "David"
            assert row.age == 35

    def test_update_with_threadlock(self, engine_with_threadlock, tables_with_lock):
        """Test UPDATE with ThreadLock."""
        with engine_with_threadlock.connect() as conn:
            # Insert first
            stmt = insert(UserSA).values(name="Eve", age=28)
            conn.execute(stmt)
            conn.commit()

            # Update
            stmt = update(UserSA).where(UserSA.name == "Eve").values(age=32)
            conn.execute(stmt)
            conn.commit()

            # Verify
            result = conn.execute(select(UserSA.age).where(UserSA.name == "Eve"))
            row = result.fetchone()
            assert row[0] == 32

    def test_delete_with_threadlock(self, engine_with_threadlock, tables_with_lock):
        """Test DELETE with ThreadLock."""
        from sqlalchemy import delete as delete_stmt

        with engine_with_threadlock.connect() as conn:
            # Insert
            stmt = insert(UserSA).values(name="Frank", email="frank@test.com")
            conn.execute(stmt)
            conn.commit()

            # Delete
            stmt = delete_stmt(UserSA).where(UserSA.name == "Frank")
            conn.execute(stmt)
            conn.commit()

            # Verify deleted
            result = conn.execute(select(UserSA).where(UserSA.name == "Frank"))
            row = result.fetchone()
            assert row is None

    def test_insert_select_with_threading_lock(self, engine_with_threading_lock):
        """Test INSERT and SELECT with threading.Lock."""
        # Setup tables
        Base.metadata.drop_all(engine_with_threading_lock)
        Base.metadata.create_all(engine_with_threading_lock)

        try:
            with engine_with_threading_lock.connect() as conn:
                stmt = insert(UserSA).values(name="Grace", email="grace@test.com", age=40)
                conn.execute(stmt)
                conn.commit()

                result = conn.execute(select(UserSA).where(UserSA.name == "Grace"))
                row = result.fetchone()
                assert row is not None
                assert row.name == "Grace"
        finally:
            Base.metadata.drop_all(engine_with_threading_lock)


class TestSyncThreadLockSQLAlchemyORMWithoutLock:
    """Test SQLAlchemy ORM operations without lock."""

    def test_session_add_commit(self, engine_without_lock, tables_without_lock):
        """Test Session.add() and commit without lock."""
        with Session(engine_without_lock) as session:
            user = UserSA(name="Henry", email="henry@test.com", age=45)
            session.add(user)
            session.commit()

            assert user.id is not None

    def test_session_query(self, engine_without_lock, tables_without_lock):
        """Test Session query without lock."""
        # Insert data
        with Session(engine_without_lock) as session:
            for name, email in [
                ("Ivy", "ivy@test.com"),
                ("Jack", "jack@test.com"),
            ]:
                session.add(UserSA(name=name, email=email))
            session.commit()

        # Query
        with Session(engine_without_lock) as session:
            users = session.query(UserSA).all()
            assert len(users) == 2

            ivy = session.query(UserSA).filter_by(name="Ivy").first()
            assert ivy is not None
            assert ivy.email == "ivy@test.com"

    def test_session_update(self, engine_without_lock, tables_without_lock):
        """Test Session update without lock."""
        with Session(engine_without_lock) as session:
            user = UserSA(name="Kate", email="kate@test.com", age=25)
            session.add(user)
            session.commit()

            # Update
            user.age = 30
            session.commit()

            # Verify
            updated = session.query(UserSA).filter_by(name="Kate").first()
            assert updated is not None
            assert updated.age == 30


class TestSyncThreadLockSQLAlchemyORMWithLock:
    """Test SQLAlchemy ORM operations with lock."""

    def test_session_add_commit_with_threadlock(self, engine_with_threadlock, tables_with_lock):
        """Test Session.add() and commit with ThreadLock."""
        with Session(engine_with_threadlock) as session:
            user = UserSA(name="Leo", email="leo@test.com", age=50)
            session.add(user)
            session.commit()

            assert user.id is not None

    def test_session_query_with_threadlock(self, engine_with_threadlock, tables_with_lock):
        """Test Session query with ThreadLock."""
        # Insert data
        with Session(engine_with_threadlock) as session:
            for name, email in [
                ("Mia", "mia@test.com"),
                ("Noah", "noah@test.com"),
            ]:
                session.add(UserSA(name=name, email=email))
            session.commit()

        # Query
        with Session(engine_with_threadlock) as session:
            users = session.query(UserSA).all()
            assert len(users) == 2

            mia = session.query(UserSA).filter_by(name="Mia").first()
            assert mia is not None
            assert mia.email == "mia@test.com"

    def test_session_update_with_threadlock(self, engine_with_threadlock, tables_with_lock):
        """Test Session update with ThreadLock."""
        with Session(engine_with_threadlock) as session:
            user = UserSA(name="Olivia", email="olivia@test.com", age=28)
            session.add(user)
            session.commit()

            # Update
            user.age = 35
            session.commit()

            # Verify
            updated = session.query(UserSA).filter_by(name="Olivia").first()
            assert updated is not None
            assert updated.age == 35

    def test_session_add_commit_with_threading_lock(self, engine_with_threading_lock):
        """Test Session.add() and commit with threading.Lock."""
        Base.metadata.drop_all(engine_with_threading_lock)
        Base.metadata.create_all(engine_with_threading_lock)

        try:
            with Session(engine_with_threading_lock) as session:
                user = UserSA(name="Paul", email="paul@test.com", age=55)
                session.add(user)
                session.commit()

                assert user.id is not None
        finally:
            Base.metadata.drop_all(engine_with_threading_lock)


class TestSyncThreadLockSQLAlchemyLockSuppressesWarnings:
    """Test that lock suppresses transaction warnings in SQLAlchemy."""

    def test_begin_warning_without_lock(self, engine_without_lock):
        """Test BEGIN SQL raises warning without lock.

        Note: rqlite v9 may or may not raise an error depending on state,
        but we verify that our client emits a warning about unsupported SQL.
        """
        with engine_without_lock.connect() as conn:
            with warnings.catch_warnings(record=True) as w:
                warnings.simplefilter("always")
                try:
                    conn.execute(text("BEGIN"))
                except Exception:
                    pass  # Error is OK, we just want to check the warning

                transaction_warnings = [
                    x
                    for x in w
                    if "BEGIN" in str(x.message) or "not supported" in str(x.message).lower()
                ]
                assert len(transaction_warnings) >= 1, "BEGIN should warn when no lock is provided"

    def test_begin_no_warning_with_lock(self, engine_with_threadlock):
        """Test BEGIN SQL does not raise warning with lock."""
        with engine_with_threadlock.connect() as conn:
            with warnings.catch_warnings(record=True) as w:
                warnings.simplefilter("always")
                try:
                    conn.execute(text("BEGIN"))
                except Exception:
                    pass  # Error is OK

                transaction_warnings = [
                    x
                    for x in w
                    if "BEGIN" in str(x.message) or "not supported" in str(x.message).lower()
                ]
                assert len(transaction_warnings) == 0, "BEGIN should not warn when lock is provided"

    def test_commit_warning_without_lock(self, engine_without_lock):
        """Test COMMIT SQL raises warning without lock."""
        with engine_without_lock.connect() as conn:
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
                assert len(transaction_warnings) >= 1, "COMMIT should warn when no lock is provided"

    def test_commit_no_warning_with_lock(self, engine_with_threadlock):
        """Test COMMIT SQL does not raise warning with lock."""
        with engine_with_threadlock.connect() as conn:
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
                assert len(transaction_warnings) == 0, (
                    "COMMIT should not warn when lock is provided"
                )


class TestSyncThreadLockSQLAlchemyFullWorkflowWithoutLock:
    """Test full CRUD workflow without lock."""

    def test_full_core_workflow(self, engine_without_lock):
        """Test complete Core CRUD lifecycle without lock."""
        # Setup
        Base.metadata.drop_all(engine_without_lock)
        Base.metadata.create_all(engine_without_lock)

        try:
            with engine_without_lock.connect() as conn:
                # INSERT MANY
                users = [
                    ("Quinn", "quinn@test.com", 30),
                    ("Ryan", "ryan@test.com", 35),
                    ("Sarah", "sarah@test.com", 40),
                ]
                for name, email, age in users:
                    stmt = insert(UserSA).values(name=name, email=email, age=age)
                    conn.execute(stmt)
                conn.commit()

                # SELECT ALL
                result = conn.execute(select(UserSA).order_by(UserSA.age))
                rows = result.fetchall()
                assert len(rows) == 3
                assert rows[0].name == "Quinn"

                # SELECT ONE
                result = conn.execute(select(UserSA).where(UserSA.name == "Ryan"))
                row = result.fetchone()
                assert row is not None
                assert row.age == 35

                # UPDATE
                stmt = update(UserSA).where(UserSA.name == "Quinn").values(age=32)
                conn.execute(stmt)
                conn.commit()

                # SELECT ONE (verify update)
                result = conn.execute(select(UserSA.age).where(UserSA.name == "Quinn"))
                row = result.fetchone()
                assert row[0] == 32

                # DELETE
                from sqlalchemy import delete

                stmt = delete(UserSA).where(UserSA.name == "Sarah")
                conn.execute(stmt)
                conn.commit()

                # SELECT MANY (final)
                result = conn.execute(select(UserSA))
                rows = result.fetchall()
                assert len(rows) == 2
        finally:
            Base.metadata.drop_all(engine_without_lock)


class TestSyncThreadLockSQLAlchemyFullWorkflowWithLock:
    """Test full CRUD workflow with lock."""

    def test_full_core_workflow_with_threadlock(self, engine_with_threadlock):
        """Test complete Core CRUD lifecycle with ThreadLock."""
        # Setup
        Base.metadata.drop_all(engine_with_threadlock)
        Base.metadata.create_all(engine_with_threadlock)

        try:
            with engine_with_threadlock.connect() as conn:
                # INSERT MANY
                users = [
                    ("Tom", "tom@test.com", 42),
                    ("Uma", "uma@test.com", 38),
                    ("Victor", "victor@test.com", 45),
                ]
                for name, email, age in users:
                    stmt = insert(UserSA).values(name=name, email=email, age=age)
                    conn.execute(stmt)
                conn.commit()

                # SELECT ALL
                result = conn.execute(select(UserSA).order_by(UserSA.age))
                rows = result.fetchall()
                assert len(rows) == 3
                assert rows[0].name == "Uma"

                # SELECT ONE
                result = conn.execute(select(UserSA).where(UserSA.name == "Victor"))
                row = result.fetchone()
                assert row is not None
                assert row.age == 45

                # UPDATE
                stmt = update(UserSA).where(UserSA.name == "Uma").values(age=40)
                conn.execute(stmt)
                conn.commit()

                # SELECT ONE (verify update)
                result = conn.execute(select(UserSA.age).where(UserSA.name == "Uma"))
                row = result.fetchone()
                assert row[0] == 40

                # DELETE
                from sqlalchemy import delete

                stmt = delete(UserSA).where(UserSA.name == "Tom")
                conn.execute(stmt)
                conn.commit()

                # SELECT MANY (final)
                result = conn.execute(select(UserSA))
                rows = result.fetchall()
                assert len(rows) == 2
        finally:
            Base.metadata.drop_all(engine_with_threadlock)

    def test_full_orm_workflow_with_threadlock(self, engine_with_threadlock):
        """Test complete ORM CRUD lifecycle with ThreadLock."""
        # Setup
        Base.metadata.drop_all(engine_with_threadlock)
        Base.metadata.create_all(engine_with_threadlock)

        try:
            with Session(engine_with_threadlock) as session:
                # CREATE: Insert multiple users
                new_users = [
                    UserSA(name="Wendy", email="wendy@test.com", age=28),
                    UserSA(name="Xavier", email="xavier@test.com", age=35),
                    UserSA(name="Yolanda", email="yolanda@test.com", age=42),
                ]
                session.add_all(new_users)
                session.commit()

                # SELECT MANY: Get all users ordered by age
                all_users = session.query(UserSA).order_by(UserSA.age.asc()).all()
                assert len(all_users) == 3
                assert all_users[0].name == "Wendy"

                # SELECT ONE: Get specific user
                xavier = session.query(UserSA).filter_by(name="Xavier").first()
                assert xavier is not None
                assert xavier.age == 35

                # UPDATE: Modify users
                wendy = session.query(UserSA).filter_by(name="Wendy").first()
                assert wendy is not None and wendy.age is not None
                wendy.age += 2
                session.commit()

                # SELECT ONE (verify update)
                wendy_updated = session.query(UserSA).filter_by(name="Wendy").first()
                assert wendy_updated is not None
                assert wendy_updated.age == 30

                # DELETE: Remove one user
                yolanda = session.query(UserSA).filter_by(name="Yolanda").first()
                session.delete(yolanda)
                session.commit()

                # SELECT MANY (final state)
                remaining = session.query(UserSA).all()
                assert len(remaining) == 2
        finally:
            Base.metadata.drop_all(engine_with_threadlock)


class TestSyncThreadLockSQLAlchemyEngineLockConfiguration:
    """Test engine lock configuration."""

    def test_connection_without_lock_has_none(self, engine_without_lock):
        """Test that connection created without lock has _lock=None."""
        with engine_without_lock.connect() as conn:
            # Get the underlying rqlite connection
            dbapi_conn = conn.connection
            rqlite_conn = dbapi_conn._conn
            assert rqlite_conn._lock is None

    def test_connection_with_threadlock_has_lock(self, engine_with_threadlock):
        """Test that connection created with ThreadLock has lock."""
        with engine_with_threadlock.connect() as conn:
            # Get the underlying rqlite connection
            dbapi_conn = conn.connection
            rqlite_conn = dbapi_conn._conn
            assert rqlite_conn._lock is not None
            assert isinstance(rqlite_conn._lock, ThreadLock)

    def test_connection_with_threading_lock_has_lock(self, engine_with_threading_lock):
        """Test that connection created with threading.Lock has lock."""
        with engine_with_threading_lock.connect() as conn:
            # Get the underlying rqlite connection
            dbapi_conn = conn.connection
            rqlite_conn = dbapi_conn._conn
            assert rqlite_conn._lock is not None
            assert isinstance(rqlite_conn._lock, threading.Lock)
