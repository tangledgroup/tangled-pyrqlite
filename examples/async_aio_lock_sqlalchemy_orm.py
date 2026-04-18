# ty: ignore[unresolved-attribute]
"""Async AioLock SQLAlchemy ORM examples for rqlite.

This example demonstrates using SQLAlchemy 2.0 async ORM with rqlite database.

Prerequisites:
    - rqlite server running on localhost:4001

    Start with Podman (recommended):
        podman run -d --name rqlite-test -p 4001:4001 docker.io/rqlite/rqlite

    Or with Docker:
        docker run -d --name rqlite-test -p 4001:4001 rqlite/rqlite

Usage:
    # Without lock (shows transaction warnings):
    uv run python -B examples/async_aio_lock_sqlalchemy_orm.py

    # With lock (no transaction warnings):
    uv run python -B examples/async_aio_lock_sqlalchemy_orm.py --with-lock
"""

import argparse
import asyncio
import functools
from collections.abc import Callable
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import (
    AsyncAttrs,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from rqlite import AioLock


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


class Base(AsyncAttrs, DeclarativeBase):
    """Base class for all models."""


class User(Base):
    """User model."""

    __tablename__ = "orm_users"

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(nullable=False)
    email: Mapped[str] = mapped_column(unique=True, nullable=False)
    age: Mapped[int | None] = mapped_column()


@print_docstring
async def create_tables(use_lock: bool = False):
    """Create database tables."""
    lock = AioLock() if use_lock else None
    connect_args = {"lock": lock} if use_lock else {}
    engine = create_async_engine(
        "rqlite+aiorqlite://localhost:4001", echo=True, connect_args=connect_args
    )

    async with engine.connect() as conn:
        await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all)

    await engine.dispose()
    print("✓ Tables created")


@print_docstring
async def add_users(use_lock: bool = False):
    """Add users using SQLAlchemy async ORM."""
    lock = AioLock() if use_lock else None
    connect_args = {"lock": lock} if use_lock else {}
    engine = create_async_engine("rqlite+aiorqlite://localhost:4001", connect_args=connect_args)
    session_local = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

    async with session_local() as session:
        # Create new users
        alice = User(name="Alice", email="alice@example.com", age=30)
        bob = User(name="Bob", email="bob@example.com", age=25)
        charlie = User(name="Charlie", email="charlie@example.com", age=35)

        # Add to session
        session.add_all([alice, bob, charlie])

        # Commit transaction
        await session.commit()

        result = await session.execute(select(User))
        count = len(result.scalars().all())
        print(f"✓ Added {count} users")

    await engine.dispose()


@print_docstring
async def query_users(use_lock: bool = False):
    """Query users using SQLAlchemy async ORM."""
    lock = AioLock() if use_lock else None
    connect_args = {"lock": lock} if use_lock else {}
    engine = create_async_engine("rqlite+aiorqlite://localhost:4001", connect_args=connect_args)
    session_local = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

    async with session_local() as session:
        # Get all users
        print("\nAll users:")
        result = await session.execute(select(User).order_by(User.name))
        for user in result.scalars().all():
            print(f"  {user.id}: {user.name} ({user.email}), age {user.age}")

        # Filter by name
        print("\nUser named Alice:")
        result = await session.execute(select(User).where(User.name == "Alice"))
        alice = result.scalar_one_or_none()
        if alice:
            print(f"  Found: {alice.name} - {alice.email}")

        # Filter with conditions
        print("\nUsers over 30:")
        result = await session.execute(select(User).where(User.age > 30))
        for user in result.scalars().all():
            print(f"  {user.name} (age {user.age})")

    await engine.dispose()


@print_docstring
async def update_user(use_lock: bool = False):
    """Update a user."""
    lock = AioLock() if use_lock else None
    connect_args = {"lock": lock} if use_lock else {}
    engine = create_async_engine("rqlite+aiorqlite://localhost:4001", connect_args=connect_args)
    session_local = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

    async with session_local() as session:
        # Find and update Bob's age
        result = await session.execute(select(User).where(User.name == "Bob"))
        bob = result.scalar_one_or_none()
        if bob:
            bob.age = 30
            await session.commit()
            print(f"✓ Updated {bob.name}'s age to {bob.age}")

    await engine.dispose()


@print_docstring
async def delete_user(use_lock: bool = False):
    """Delete a user."""
    lock = AioLock() if use_lock else None
    connect_args = {"lock": lock} if use_lock else {}
    engine = create_async_engine("rqlite+aiorqlite://localhost:4001", connect_args=connect_args)
    session_local = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

    async with session_local() as session:
        result = await session.execute(select(User).where(User.name == "Charlie"))
        charlie = result.scalar_one_or_none()
        if charlie:
            await session.delete(charlie)
            await session.commit()
            print(f"✓ Deleted {charlie.name}")

    await engine.dispose()


@print_docstring
async def bulk_operations(use_lock: bool = False):
    """Demonstrate bulk operations with transactions."""
    lock = AioLock() if use_lock else None
    connect_args = {"lock": lock} if use_lock else {}
    engine = create_async_engine("rqlite+aiorqlite://localhost:4001", connect_args=connect_args)
    session_local = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

    async with session_local() as session:
        # Bulk insert - all statements queued until commit
        users = [
            User(name=f"User{i}", email=f"user{i}@example.com", age=20 + i) for i in range(5, 10)
        ]

        session.add_all(users)

        # All inserts are queued here - nothing sent to rqlite yet
        print(f"✓ Queued {len(users)} users for insert")

        # Commit sends everything atomically
        await session.commit()

        result = await session.execute(select(User))
        count = len(result.scalars().all())
        print(f"✓ Committed {count} total users")

    await engine.dispose()


@print_docstring
async def complex_workflow(use_lock: bool = False):
    """Complex ORM workflow: create, select, update, delete with various query patterns."""
    lock = AioLock() if use_lock else None
    connect_args = {"lock": lock} if use_lock else {}
    engine = create_async_engine("rqlite+aiorqlite://localhost:4001", connect_args=connect_args)
    session_local = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

    async with session_local() as session:
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
        await session.commit()
        print(f"✓ Inserted {len(new_users)} users")

        # Step 2: SELECT MANY - Get all users ordered by age
        print("\n[STEP 2] SELECT MANY: All users ordered by age")
        result = await session.execute(select(User).order_by(User.age.asc()))
        all_users = result.scalars().all()
        for user in all_users:
            print(f"  {user.id}: {user.name}, age {user.age}")
        print(f"✓ Retrieved {len(all_users)} users")

        # Step 3: SELECT FEW - Filter users by age range
        print("\n[STEP 3] SELECT FEW: Users aged 30-40")
        result = await session.execute(
            select(User).where(User.age >= 30, User.age <= 40).order_by(User.name)
        )
        filtered_users = result.scalars().all()
        for user in filtered_users:
            print(f"  {user.name}: age {user.age}")
        print(f"✓ Found {len(filtered_users)} users in age range")

        # Step 4: SELECT ONE - Get specific user by email
        print("\n[STEP 4] SELECT ONE: Find Grace by email")
        result = await session.execute(select(User).where(User.email == "grace@test.com"))
        grace = result.scalar_one_or_none()
        if grace:
            print(f"  Found: {grace.name} ({grace.email}), age {grace.age}")
            print("✓ Single user retrieved successfully")
        else:
            print("✗ User not found")

        # Step 5: UPDATE - Modify multiple users
        print("\n[STEP 5] UPDATE: Age up Emma and Frank by 2 years")
        result = await session.execute(select(User).where(User.name == "Emma"))
        emma = result.scalar_one_or_none()
        result = await session.execute(select(User).where(User.name == "Frank"))
        frank = result.scalar_one_or_none()

        if emma and emma.age is not None:
            emma.age += 2
            print(f"  Emma: {emma.age - 2} → {emma.age}")
        if frank and frank.age is not None:
            frank.age += 2
            print(f"  Frank: {frank.age - 2} → {frank.age}")
        await session.commit()
        print("✓ Updates committed")

        # Step 6: SELECT ONE (verify update)
        print("\n[STEP 6] SELECT ONE: Verify Emma's updated age")
        result = await session.execute(select(User).where(User.name == "Emma"))
        emma_updated = result.scalar_one_or_none()
        if emma_updated:
            print(f"  Emma is now {emma_updated.age} years old")
            print("✓ Update verified")

        # Step 7: DELETE - Remove users under 30
        print("\n[STEP 7] DELETE: Remove users under 30")
        result = await session.execute(select(User).where(User.age < 30))
        young_users = result.scalars().all()
        deleted_names = [u.name for u in young_users]
        for user in young_users:
            await session.delete(user)
        await session.commit()
        print(f"  Deleted: {', '.join(deleted_names)}")
        print(f"✓ Removed {len(deleted_names)} users")

        # Step 8: SELECT MANY (final state)
        print("\n[STEP 8] SELECT MANY: Final user list")
        result = await session.execute(select(User).order_by(User.age.desc()))
        remaining = result.scalars().all()
        for user in remaining:
            print(f"  {user.name}: age {user.age}")
        print(f"✓ {len(remaining)} users remain in database")

        # Step 9: SELECT ONE (non-existent)
        print("\n[STEP 9] SELECT ONE: Query for deleted user (Ivy)")
        result = await session.execute(select(User).where(User.name == "Ivy"))
        ivy = result.scalar_one_or_none()
        if ivy is None:
            print("  Ivy not found (successfully deleted)")
            print("✓ Non-existent query returns None as expected")

    await engine.dispose()

    print("\n✅ Complex ORM workflow completed successfully!")


async def main():
    """Run all SQLAlchemy async examples."""
    # Parse command line arguments
    parser = argparse.ArgumentParser(
        description="rqlite async SQLAlchemy ORM examples",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  uv run python -B examples/async_aio_lock_sqlalchemy_orm.py              # Without lock (shows warnings)
  uv run python -B examples/async_aio_lock_sqlalchemy_orm.py --with-lock  # With lock (no warnings)
        """,
    )
    parser.add_argument(
        "--with-lock", action="store_true", help="Use AioLock to suppress transaction warnings"
    )
    args = parser.parse_args()

    use_lock = args.with_lock
    mode = "WITH LOCK" if use_lock else "WITHOUT LOCK"

    print("=" * 60)
    print(f"rqlite SQLAlchemy Async ORM Examples ({mode})")
    print("=" * 60)

    try:
        await create_tables(use_lock=use_lock)
        await add_users(use_lock=use_lock)
        await query_users(use_lock=use_lock)
        await update_user(use_lock=use_lock)
        await bulk_operations(use_lock=use_lock)
        await delete_user(use_lock=use_lock)
        await query_users(use_lock=use_lock)
        await complex_workflow(use_lock=use_lock)

        print("\n" + "=" * 60)
        print("All SQLAlchemy async examples completed!")
        print("=" * 60)
    except Exception as e:
        print(f"\n✗ Error: {e}")
        print("Make sure rqlite is running on localhost:4001")
        import traceback

        traceback.print_exc()


if __name__ == "__main__":
    asyncio.run(main())
