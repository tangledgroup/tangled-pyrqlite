"""Tests for SQLAlchemy dialect integration."""


import pytest
from sqlalchemy import (
    create_engine,
    select,
    text,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column

import rqlite


class Base(DeclarativeBase):
    """Base class for SQLAlchemy models."""


class User(Base):
    """User model for testing."""

    __tablename__ = "sa_users"

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(nullable=False)
    email: Mapped[str | None] = mapped_column(unique=True)
    age: Mapped[int | None] = mapped_column()


class Product(Base):
    """Product model for testing."""

    __tablename__ = "sa_products"

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(nullable=False)
    price: Mapped[int] = mapped_column()  # Store as cents
    quantity: Mapped[int] = mapped_column(default=0)


@pytest.fixture(scope="function")
def engine():
    """Create SQLAlchemy engine for rqlite."""
    return create_engine("rqlite://localhost:4001", echo=False)


@pytest.fixture(scope="function")
def tables(engine):
    """Create and cleanup test tables."""
    # Drop existing tables
    Base.metadata.drop_all(engine)

    # Create fresh tables
    Base.metadata.create_all(engine)

    yield

    # Cleanup
    Base.metadata.drop_all(engine)


class TestSQLAlchemyCore:
    """Test SQLAlchemy Core operations."""

    def test_create_tables(self, engine):
        """Test creating tables via SQLAlchemy."""
        Base.metadata.create_all(engine)

        # Verify tables exist
        with engine.connect() as conn:
            result = conn.execute(
                text("SELECT name FROM sqlite_master WHERE type='table'")
            )
            tables = [row[0] for row in result]
            assert "sa_users" in tables
            assert "sa_products" in tables

    def test_insert_select(self, engine, tables):
        """Test INSERT and SELECT via SQLAlchemy Core."""
        with engine.connect() as conn:
            # Insert
            from sqlalchemy import insert

            stmt = insert(User).values(name="Alice", email="alice@example.com", age=30)
            conn.execute(stmt)
            conn.commit()

            # Select
            result = conn.execute(select(User).where(User.name == "Alice"))
            row = result.fetchone()
            assert row is not None
            assert row.name == "Alice"
            assert row.email == "alice@example.com"
            assert row.age == 30

    def test_update(self, engine, tables):
        """Test UPDATE via SQLAlchemy Core."""
        from sqlalchemy import insert, update

        with engine.connect() as conn:
            # Insert first
            stmt = insert(User).values(name="Bob", age=25)
            conn.execute(stmt)
            conn.commit()

            # Update
            stmt = update(User).where(User.name == "Bob").values(age=30)
            conn.execute(stmt)
            conn.commit()

            # Verify
            result = conn.execute(select(User.age).where(User.name == "Bob"))
            row = result.fetchone()
            assert row[0] == 30

    def test_delete(self, engine, tables):
        """Test DELETE via SQLAlchemy Core."""
        from sqlalchemy import delete, insert

        with engine.connect() as conn:
            # Insert
            stmt = insert(User).values(name="Charlie", email="charlie@example.com")
            conn.execute(stmt)
            conn.commit()

            # Delete
            stmt = delete(User).where(User.name == "Charlie")
            conn.execute(stmt)
            conn.commit()

            # Verify deleted
            result = conn.execute(select(User).where(User.name == "Charlie"))
            row = result.fetchone()
            assert row is None


class TestSQLAlchemyORM:
    """Test SQLAlchemy ORM operations."""

    def test_session_add_commit(self, engine, tables):
        """Test adding and committing objects via Session."""
        with Session(engine) as session:
            user = User(name="David", email="david@example.com", age=35)
            session.add(user)
            session.commit()

            # Get ID after commit
            assert user.id is not None

    def test_session_query(self, engine, tables):
        """Test querying objects via Session."""
        # First insert some data
        with Session(engine) as session:
            for name, email in [
                ("Eve", "eve@example.com"),
                ("Frank", "frank@example.com"),
            ]:
                session.add(User(name=name, email=email))
            session.commit()

        # Now query
        with Session(engine) as session:
            users = session.query(User).all()
            assert len(users) == 2

            eve = session.query(User).filter_by(name="Eve").first()
            assert eve is not None
            assert eve.email == "eve@example.com"

    def test_session_relationships(self, engine):
        """Test that relationships work (using SQLite dialect base)."""
        # Create a simple relationship model
        class Order(Base):
            __tablename__ = "sa_orders"

            id: Mapped[int] = mapped_column(primary_key=True)
            user_id: Mapped[int | None] = mapped_column()
            product: Mapped[str | None] = mapped_column()

        Base.metadata.drop_all(engine)
        Base.metadata.create_all(engine)

        with Session(engine) as session:
            # Create user and order
            user = User(name="Grace", email="grace@example.com")
            session.add(user)
            session.flush()  # Get ID

            order = Order(user_id=user.id, product="Widget")
            session.add(order)
            session.commit()

            # Query order with user_id
            order_result = session.query(Order).first()
            assert order_result is not None
            assert order_result.user_id == user.id


class TestSQLAlchemyReflection:
    """Test SQLAlchemy reflection capabilities."""

    def test_has_table(self, engine, tables):
        """Test has_table method."""
        dialect = engine.dialect

        with engine.connect() as conn:
            assert dialect.has_table(conn, "sa_users") is True
            assert dialect.has_table(conn, "nonexistent") is False

    def test_get_columns(self, engine, tables):
        """Test get_columns method."""
        dialect = engine.dialect

        with engine.connect() as conn:
            columns = dialect.get_columns(conn, "sa_users")
            column_names = [c["name"] for c in columns]

            assert "id" in column_names
            assert "name" in column_names
            assert "email" in column_names
            assert "age" in column_names

    def test_get_pk_constraint(self, engine, tables):
        """Test get_pk_constraint method."""
        dialect = engine.dialect

        with engine.connect() as conn:
            pk = dialect.get_pk_constraint(conn, "sa_users")
            assert "id" in pk["constrained_columns"]


class TestSQLAlchemyConnectionURL:
    """Test connection URL parsing."""

    def test_basic_url(self):
        """Test basic connection URL."""
        engine = create_engine("rqlite://localhost:4001")
        assert engine.url.host == "localhost"
        assert engine.url.port == 4001

    def test_url_with_auth(self):
        """Test URL with authentication."""
        engine = create_engine("rqlite://user:pass@localhost:4001")
        assert engine.url.username == "user"
        assert engine.url.password == "pass"


class TestComplexORMWorkflow:
    """Test complex ORM workflow with multiple CRUD operations."""

    def test_full_orm_lifecycle(self, engine, tables):
        """Test complete lifecycle: create, select many, select few, select one, update, delete."""
        with Session(engine) as session:
            # CREATE: Insert multiple users
            new_users = [
                User(name="Emma", email="emma@test.com", age=28),
                User(name="Frank", email="frank@test.com", age=35),
                User(name="Grace", email="grace@test.com", age=42),
                User(name="Henry", email="henry@test.com", age=31),
                User(name="Ivy", email="ivy@test.com", age=26),
            ]
            session.add_all(new_users)
            session.commit()

            # SELECT MANY: Get all users ordered by age
            all_users = session.query(User).order_by(User.age.asc()).all()
            assert len(all_users) == 5
            assert all_users[0].name == "Ivy"  # Youngest
            assert all_users[-1].name == "Grace"  # Oldest

            # SELECT FEW: Filter users by age range
            filtered_users = session.query(User).filter(
                User.age >= 30, User.age <= 40
            ).all()
            assert len(filtered_users) == 2
            names = {u.name for u in filtered_users}
            assert names == {"Frank", "Henry"}

            # SELECT ONE: Get specific user by email
            grace = session.query(User).filter_by(email="grace@test.com").first()
            assert grace is not None
            assert grace.name == "Grace"
            assert grace.age == 42

            # UPDATE: Modify users
            emma = session.query(User).filter_by(name="Emma").first()
            frank = session.query(User).filter_by(name="Frank").first()
            emma.age += 2
            frank.age += 2
            session.commit()

            # SELECT ONE (verify update)
            emma_updated = session.query(User).filter_by(name="Emma").first()
            assert emma_updated.age == 30

            # DELETE: Remove users under 30
            young_users = session.query(User).filter(User.age < 30).all()
            for user in young_users:
                session.delete(user)
            session.commit()

            # SELECT MANY (final state)
            remaining = session.query(User).order_by(User.age.desc()).all()
            assert len(remaining) == 4

            # SELECT ONE (non-existent)
            ivy = session.query(User).filter_by(name="Ivy").first()
            assert ivy is None


class TestComplexDBAPIWorkflow:
    """Test complex DB-API 2.0 workflow with raw cursor operations."""

    def test_full_dbapi_lifecycle(self, engine):
        """Test complete lifecycle using raw DB-API 2.0 interface."""
        # Create a dedicated table for this test
        conn = rqlite.connect(host="localhost", port=4001)
        try:
            cursor = conn.cursor()

            # CREATE TABLE
            cursor.execute("""
                DROP TABLE IF EXISTS dbapi_test_products
            """)
            conn.commit()
            cursor.execute("""
                CREATE TABLE dbapi_test_products (
                    id INTEGER PRIMARY KEY,
                    name TEXT NOT NULL,
                    price INTEGER NOT NULL,
                    quantity INTEGER DEFAULT 0
                )
            """)
            conn.commit()

            # INSERT MANY: Add products
            products = [
                ("Laptop", 99999, 10),  # price in cents
                ("Mouse", 2999, 50),
                ("Keyboard", 7999, 30),
                ("Monitor", 29999, 15),
                ("Headphones", 14999, 25),
                ("Webcam", 8999, 20),
            ]
            cursor.executemany(
                "INSERT INTO dbapi_test_products (name, price, quantity) VALUES (?, ?, ?)",
                products,
            )
            conn.commit()

            # SELECT ALL: Fetch all products
            cursor.execute("SELECT * FROM dbapi_test_products ORDER BY price DESC")
            rows = cursor.fetchall()
            assert len(rows) == 6
            assert rows[0][1] == "Laptop"  # Most expensive

            # SELECT FEW: Filter by price range
            cursor.execute(
                "SELECT name, price FROM dbapi_test_products WHERE price BETWEEN 5000 AND 15000"
            )
            rows = cursor.fetchall()
            assert len(rows) == 3

            # SELECT ONE: Fetch single product
            cursor.execute(
                "SELECT name, price, quantity FROM dbapi_test_products WHERE name = ?",
                ("Laptop",),
            )
            row = cursor.fetchone()
            assert row is not None
            assert row[0] == "Laptop"
            assert row[1] == 99999

            # UPDATE: Increase prices by 10%
            cursor.execute(
                "UPDATE dbapi_test_products SET price = CAST(price * 1.10 AS INTEGER)"
            )
            conn.commit()

            # SELECT ONE (verify update)
            cursor.execute(
                "SELECT name, price FROM dbapi_test_products WHERE name = ?",
                ("Laptop",),
            )
            row = cursor.fetchone()
            assert row is not None
            # 99999 * 1.10 = 109998.9, truncated to 109998 by CAST
            assert row[1] == 109998

            # DELETE: Remove low stock items
            cursor.execute("DELETE FROM dbapi_test_products WHERE quantity < 20")
            conn.commit()

            # SELECT MANY (final state)
            cursor.execute(
                "SELECT name, price, quantity FROM dbapi_test_products ORDER BY price"
            )
            rows = cursor.fetchall()
            assert len(rows) == 4  # 6 - 2 deleted

            # SELECT ONE (deleted item)
            cursor.execute(
                "SELECT name FROM dbapi_test_products WHERE name = ?",
                ("Laptop",),
            )
            row = cursor.fetchone()
            assert row is None  # Laptop had qty=10, was deleted

            cursor.close()
        finally:
            conn.close()

    def test_no_spurious_warnings_with_empty_results(self, engine, tables):
        """Test that empty SELECT results don't trigger warnings.

        This is a regression test for the issue where fetchone() would warn
        even when a SELECT was executed but returned no rows.
        """
        conn = rqlite.connect(host="localhost", port=4001)
        try:
            cursor = conn.cursor()

            # Execute SELECT that returns no rows
            cursor.execute(
                "SELECT * FROM sa_users WHERE name = ?", ("NonExistentUser123",)
            )

            # fetchone() should return None without warning
            row = cursor.fetchone()
            assert row is None

            # fetchall() should return empty list without warning
            rows = cursor.fetchall()
            assert rows == []

            cursor.close()
        finally:
            conn.close()
