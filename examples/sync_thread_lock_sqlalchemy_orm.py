"""SQLAlchemy ORM examples for rqlite.

This example demonstrates using SQLAlchemy ORM with rqlite database.

Prerequisites:
    - rqlite server running on localhost:4001

    Start with Podman (recommended):
        podman run -d --name rqlite-test -p 4001:4001 docker.io/rqlite/rqlite

    Or with Docker:
        docker run -d --name rqlite-test -p 4001:4001 rqlite/rqlite

Usage:
    # Without lock (shows transaction warnings):
    uv run python -B examples/sync_thread_lock_sqlalchemy_orm.py

    # With lock (no transaction warnings):
    uv run python -B examples/sync_thread_lock_sqlalchemy_orm.py --with-lock
"""

from __future__ import annotations

import argparse
import functools
from collections.abc import Callable
from typing import Any

from sqlalchemy import create_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column

from rqlite import ThreadLock


def print_docstring(func: Callable) -> Callable:
    """Decorator that prints the function's docstring when called."""
    @functools.wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        if func.__doc__:
            print(f"\n{'─' * 60}")
            print(f"📝 {func.__name__}: {func.__doc__.strip()}")
            print("─" * 60)
        return func(*args, **kwargs)
    return wrapper


class Base(DeclarativeBase):
    """Base class for all models."""


class User(Base):
    """User model."""

    __tablename__ = "orm_users"

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(nullable=False)
    email: Mapped[str] = mapped_column(unique=True, nullable=False)
    age: Mapped[int | None] = mapped_column()


@print_docstring
def create_tables(use_lock: bool = False):
    """Create database tables."""
    lock = ThreadLock() if use_lock else None
    connect_args = {"lock": lock} if lock else {}
    engine = create_engine("rqlite://localhost:4001", echo=True, connect_args=connect_args)
    Base.metadata.drop_all(engine)  # Clean start
    Base.metadata.create_all(engine)
    print("✓ Tables created")


@print_docstring
def add_users(use_lock: bool = False):
    """Add users using SQLAlchemy ORM."""
    lock = ThreadLock() if use_lock else None
    connect_args = {"lock": lock} if lock else {}
    engine = create_engine("rqlite://localhost:4001", connect_args=connect_args)

    with Session(engine) as session:
        # Create new users
        alice = User(name="Alice", email="alice@example.com", age=30)
        bob = User(name="Bob", email="bob@example.com", age=25)
        charlie = User(name="Charlie", email="charlie@example.com", age=35)

        # Add to session
        session.add_all([alice, bob, charlie])

        # Commit transaction
        session.commit()

        print(f"✓ Added {session.query(User).count()} users")


@print_docstring
def query_users(use_lock: bool = False):
    """Query users using SQLAlchemy ORM."""
    lock = ThreadLock() if use_lock else None
    connect_args = {"lock": lock} if lock else {}
    engine = create_engine("rqlite://localhost:4001", connect_args=connect_args)

    with Session(engine) as session:
        # Get all users
        print("\nAll users:")
        for user in session.query(User).order_by(User.name).all():
            print(f"  {user.id}: {user.name} ({user.email}), age {user.age}")

        # Filter by name
        print("\nUser named Alice:")
        alice = session.query(User).filter_by(name="Alice").first()
        if alice:
            print(f"  Found: {alice.name} - {alice.email}")

        # Filter with conditions
        print("\nUsers over 30:")
        for user in session.query(User).filter(User.age > 30).all():
            print(f"  {user.name} (age {user.age})")


@print_docstring
def update_user(use_lock: bool = False):
    """Update a user."""
    lock = ThreadLock() if use_lock else None
    connect_args = {"lock": lock} if lock else {}
    engine = create_engine("rqlite://localhost:4001", connect_args=connect_args)

    with Session(engine) as session:
        # Find and update Bob's age
        bob = session.query(User).filter_by(name="Bob").first()
        if bob:
            bob.age = 30
            session.commit()
            print(f"✓ Updated {bob.name}'s age to {bob.age}")


@print_docstring
def delete_user(use_lock: bool = False):
    """Delete a user."""
    lock = ThreadLock() if use_lock else None
    connect_args = {"lock": lock} if lock else {}
    engine = create_engine("rqlite://localhost:4001", connect_args=connect_args)

    with Session(engine) as session:
        charlie = session.query(User).filter_by(name="Charlie").first()
        if charlie:
            session.delete(charlie)
            session.commit()
            print(f"✓ Deleted {charlie.name}")


@print_docstring
def bulk_operations(use_lock: bool = False):
    """Demonstrate bulk operations with transactions."""
    lock = ThreadLock() if use_lock else None
    connect_args = {"lock": lock} if lock else {}
    engine = create_engine("rqlite://localhost:4001", connect_args=connect_args)

    with Session(engine) as session:
        # Bulk insert - all statements queued until commit
        users = [
            User(name=f"User{i}", email=f"user{i}@example.com", age=20 + i)
            for i in range(5, 10)
        ]

        session.add_all(users)

        # All inserts are queued here - nothing sent to rqlite yet
        print(f"✓ Queued {len(users)} users for insert")

        # Commit sends everything atomically
        session.commit()
        print(f"✓ Committed {session.query(User).count()} total users")


@print_docstring
def complex_workflow(use_lock: bool = False):
    """Complex ORM workflow: create, select, update, delete with various query patterns."""
    lock = ThreadLock() if use_lock else None
    connect_args = {"lock": lock} if lock else {}
    engine = create_engine("rqlite://localhost:4001", connect_args=connect_args)

    with Session(engine) as session:
        # Step 1: CREATE - Insert multiple users
        print("\n[STEP 1] CREATE: Inserting 5 users")
        new_users = [
            User(name="Emma", email="emma@test.com", age=28),
            User(name="Frank", email="frank@test.com", age=35),
            User(name="Grace", email="grace@test.com", age=42),
            User(name="Henry", email="henry@test.com", age=31),
            User(name="Ivy", email="ivy@test.com", age=26),
        ]
        session.add_all(new_users)
        session.commit()
        print(f"✓ Inserted {len(new_users)} users")

        # Step 2: SELECT MANY - Get all users ordered by age
        print("\n[STEP 2] SELECT MANY: All users ordered by age")
        all_users = session.query(User).order_by(User.age.asc()).all()
        for user in all_users:
            print(f"  {user.id}: {user.name}, age {user.age}")
        print(f"✓ Retrieved {len(all_users)} users")

        # Step 3: SELECT FEW - Filter users by age range
        print("\n[STEP 3] SELECT FEW: Users aged 30-40")
        filtered_users = session.query(User).filter(
            User.age >= 30, User.age <= 40
        ).order_by(User.name).all()
        for user in filtered_users:
            print(f"  {user.name}: age {user.age}")
        print(f"✓ Found {len(filtered_users)} users in age range")

        # Step 4: SELECT ONE - Get specific user by email
        print("\n[STEP 4] SELECT ONE: Find Grace by email")
        grace = session.query(User).filter_by(email="grace@test.com").first()
        if grace:
            print(f"  Found: {grace.name} ({grace.email}), age {grace.age}")
            print("✓ Single user retrieved successfully")
        else:
            print("✗ User not found")

        # Step 5: UPDATE - Modify multiple users
        print("\n[STEP 5] UPDATE: Age up Emma and Frank by 2 years")
        emma = session.query(User).filter_by(name="Emma").first()
        frank = session.query(User).filter_by(name="Frank").first()
        if emma:
            emma.age += 2
            print(f"  Emma: {emma.age - 2} → {emma.age}")
        if frank:
            frank.age += 2
            print(f"  Frank: {frank.age - 2} → {frank.age}")
        session.commit()
        print("✓ Updates committed")

        # Step 6: SELECT ONE (verify update)
        print("\n[STEP 6] SELECT ONE: Verify Emma's updated age")
        emma_updated = session.query(User).filter_by(name="Emma").first()
        if emma_updated:
            print(f"  Emma is now {emma_updated.age} years old")
            print("✓ Update verified")

        # Step 7: DELETE - Remove users under 30
        print("\n[STEP 7] DELETE: Remove users under 30")
        young_users = session.query(User).filter(User.age < 30).all()
        deleted_names = [u.name for u in young_users]
        for user in young_users:
            session.delete(user)
        session.commit()
        print(f"  Deleted: {', '.join(deleted_names)}")
        print(f"✓ Removed {len(deleted_names)} users")

        # Step 8: SELECT MANY (final state)
        print("\n[STEP 8] SELECT MANY: Final user list")
        remaining = session.query(User).order_by(User.age.desc()).all()
        for user in remaining:
            print(f"  {user.name}: age {user.age}")
        print(f"✓ {len(remaining)} users remain in database")

        # Step 9: SELECT ONE (non-existent)
        print("\n[STEP 9] SELECT ONE: Query for deleted user (Ivy)")
        ivy = session.query(User).filter_by(name="Ivy").first()
        if ivy is None:
            print("  Ivy not found (successfully deleted)")
            print("✓ Non-existent query returns None as expected")

    print("\n✅ Complex ORM workflow completed successfully!")


def main():
    """Run all SQLAlchemy examples."""
    # Parse command line arguments
    parser = argparse.ArgumentParser(
        description="rqlite Sync ThreadLock SQLAlchemy ORM examples",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  uv run python -B examples/sync_thread_lock_sqlalchemy_orm.py              # Without lock (shows warnings)
  uv run python -B examples/sync_thread_lock_sqlalchemy_orm.py --with-lock  # With lock (no warnings)
        """
    )
    parser.add_argument(
        "--with-lock",
        action="store_true",
        help="Use ThreadLock to suppress transaction warnings"
    )
    args = parser.parse_args()

    use_lock = args.with_lock
    mode = "WITH LOCK" if use_lock else "WITHOUT LOCK"

    print("=" * 60)
    print(f"rqlite SQLAlchemy ORM Examples ({mode})")
    print("=" * 60)

    try:
        create_tables(use_lock=use_lock)
        add_users(use_lock=use_lock)
        query_users(use_lock=use_lock)
        update_user(use_lock=use_lock)
        bulk_operations(use_lock=use_lock)
        delete_user(use_lock=use_lock)
        query_users(use_lock=use_lock)
        complex_workflow(use_lock=use_lock)

        print("\n" + "=" * 60)
        print("All SQLAlchemy examples completed!")
        print("=" * 60)
    except Exception as e:
        print(f"\n✗ Error: {e}")
        print("Make sure rqlite is running on localhost:4001")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
