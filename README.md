# tangled-pyrqlite - python client

[![PyPI](https://img.shields.io/pypi/v/tangled-pyrqlite)](https://pypi.org/project/tangled-pyrqlite/)
[![PyPI Downloads](https://img.shields.io/pypi/dm/tangled-pyrqlite)](https://pypistats.org/packages/tangled-pyrqlite)
[![Supported Versions](https://img.shields.io/pypi/pyversions/tangled-pyrqlite)](https://pypi.org/project/tangled-pyrqlite)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](https://opensource.org/licenses/MIT)

A pure Python client for [rqlite](https://rqlite.io/) distributed SQLite clusters, providing:

- **DB-API 2.0** - Standard Python database API (PEP 249) — see [DB_API.md](./docs/DB_API.md)
- **SQLAlchemy dialect** - Full ORM support via SQLAlchemy 2.0 — see [SQLALCHEMY.md](./docs/SQLALCHEMY.md)
- **Parameterized queries** - Safe, SQL injection-proof query execution
- **Serializable transaction support** - Atomic batch operations using locking mechanism - bring your own distributed locking implementation using [REDIS_LOCK.md](./docs/REDIS_LOCK.md) or [VALKEY_LOCK.md](./docs/VALKEY_LOCK.md)

**NOTE**: Documentation and instructions are being actively written and improved.

## Installation

```bash
# Using uv (recommended)
uv add tangled-pyrqlite

# Using pip
pip install tangled-pyrqlite
```

## Quick Start

### Starting rqlite Server

Before using the client, start an rqlite server:

**Podman (recommended - no root required):**

```bash
podman rm -f rqlite-test
podman run -d --name rqlite-test -p 4001:4001 docker.io/rqlite/rqlite
```


## Locking

rqlite's transaction model differs from traditional databases.

By default, when using the rqlite library **without a lock**, you will receive a `UserWarning`:

```
UserWarning: Explicit BEGIN/COMMIT/ROLLBACK/SAVEPOINT SQL is not supported.
```

When you provide a lock, these warnings are suppressed, indicating that you're aware of the limitations and handling transactions appropriately.

For **simple locks** guide (local development, not recommended in production):
- `threading` sync lock - see [LOCK.md](./docs/LOCK.md).
- `asyncio` async lock - see [LOCK.md](./docs/LOCK.md).

For **distributed locks** guide (recommended):
- `redis` sync/async locks - see [REDIS_LOCK.md](./docs/REDIS_LOCK.md)
- `valkey` sync/async locks - see [VALKEY_LOCK.md](./docs/VALKEY_LOCK.md)

**IMPORTANT**: For true cross-process transaction serialization, use distributed locks backed by Redis or Valkey.


## Examples

Here we will use **Valkey** server and its locks for cross-process transaction serialization.

Install `tangled-pyrqlite` with the `valkey` extra:

```bash
uv add tangled-pyrqlite[valkey]
```

Start a Valkey server:

```bash
podman rm -f valkey-test
podman run -d --name valkey-test -p 6379:6379 docker.io/valkey/valkey:latest
```

**IMPORTANT**: never use context manager locks when working with rqlite client library like `with lock: ...` and `async with lock: ...`. Locks are used automatically by `engine` objects.

### Sync DB-API 2.0 with ValkeyLock

```python
import rqlite
from rqlite import ValkeyLock

# Create a distributed lock backed by Valkey
lock = ValkeyLock(name="readme_sync", timeout=10.0)
conn = rqlite.connect(host="localhost", port=4001, lock=lock)
cursor = conn.cursor()

try:
    # 1. CREATE TABLE IF NOT EXISTS
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS readme_products (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            price REAL NOT NULL,
            quantity INTEGER DEFAULT 0
        )
    """)
    conn.commit()
    print("✓ Table 'readme_products' ready")

    # 2. INSERT a product
    cursor.execute(
        "INSERT INTO readme_products (name, price, quantity) VALUES (?, ?, ?)",
        ("Widget A", 9.99, 100),
    )
    conn.commit()
    print("✓ Inserted: Widget A ($9.99, qty: 100)")

    # 3. SELECT — fetch by name
    cursor.execute(
        "SELECT name, price, quantity FROM readme_products WHERE name=?",
        ("Widget A",),
    )
    row = cursor.fetchone()
    print(f"✓ Found: {row[0]} (${row[1]:.2f}, qty: {row[2]})")

    # 4. UPDATE — adjust price and quantity
    cursor.execute(
        "SELECT price, quantity FROM readme_products WHERE name=?",
        ("Widget A",),
    )
    old_price, old_qty = cursor.fetchone()

    new_price, new_qty = 12.99, 85
    cursor.execute(
        "UPDATE readme_products SET price=?, quantity=? WHERE name=?",
        (new_price, new_qty, "Widget A"),
    )
    conn.commit()
    print(f"✓ Updated: Widget A (${old_price:.2f} → ${new_price:.2f}, qty: {old_qty} → {new_qty})")

    # 5. SELECT — verify update
    cursor.execute(
        "SELECT name, price, quantity FROM readme_products WHERE name=?",
        ("Widget A",),
    )
    row = cursor.fetchone()
    print(f"✓ Verified: {row[0]} (${row[1]:.2f}, qty: {row[2]})")

    # 6. DELETE the product
    cursor.execute("DELETE FROM readme_products WHERE name=?", ("Widget A",))
    conn.commit()
    print("✓ Deleted: Widget A")

    # 7. SELECT ALL — confirm table is empty
    cursor.execute("SELECT * FROM readme_products")
    rows = cursor.fetchall()
    print(f"✓ Table empty: {len(rows)} rows" if not rows else f"✗ Unexpected: {rows}")
finally:
    cursor.close()
    conn.close()
```

### Async DB-API 2.0 with AioValkeyLock

```python
import asyncio
import rqlite
from rqlite import AioValkeyLock

async def main():
    # Create an async distributed lock backed by Valkey
    lock = AioValkeyLock(name="readme_async", timeout=10.0)
    conn = rqlite.async_connect(host="localhost", port=4001, lock=lock)
    cursor = await conn.cursor()

    try:
        # 1. CREATE TABLE IF NOT EXISTS
        await cursor.execute("""
            CREATE TABLE IF NOT EXISTS readme_products (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                price REAL NOT NULL,
                quantity INTEGER DEFAULT 0
            )
        """)
        await conn.commit()
        print("✓ Table 'readme_products' ready")

        # 2. INSERT a product
        await cursor.execute(
            "INSERT INTO readme_products (name, price, quantity) VALUES (?, ?, ?)",
            ("Widget A", 9.99, 100),
        )
        await conn.commit()
        print("✓ Inserted: Widget A ($9.99, qty: 100)")

        # 3. SELECT — fetch by name
        await cursor.execute(
            "SELECT name, price, quantity FROM readme_products WHERE name=?",
            ("Widget A",),
        )
        row = cursor.fetchone()
        print(f"✓ Found: {row[0]} (${row[1]:.2f}, qty: {row[2]})")

        # 4. UPDATE — adjust price and quantity
        await cursor.execute(
            "SELECT price, quantity FROM readme_products WHERE name=?",
            ("Widget A",),
        )
        old_price, old_qty = cursor.fetchone()

        new_price, new_qty = 12.99, 85
        await cursor.execute(
            "UPDATE readme_products SET price=?, quantity=? WHERE name=?",
            (new_price, new_qty, "Widget A"),
        )
        await conn.commit()
        print(f"✓ Updated: Widget A (${old_price:.2f} → ${new_price:.2f}, qty: {old_qty} → {new_qty})")

        # 5. SELECT — verify update
        await cursor.execute(
            "SELECT name, price, quantity FROM readme_products WHERE name=?",
            ("Widget A",),
        )
        row = cursor.fetchone()
        print(f"✓ Verified: {row[0]} (${row[1]:.2f}, qty: {row[2]})")

        # 6. DELETE the product
        await cursor.execute("DELETE FROM readme_products WHERE name=?", ("Widget A",))
        await conn.commit()
        print("✓ Deleted: Widget A")

        # 7. SELECT ALL — confirm table is empty
        await cursor.execute("SELECT * FROM readme_products")
        rows = cursor.fetchall()
        print(f"✓ Table empty: {len(rows)} rows" if not rows else f"✗ Unexpected: {rows}")
    finally:
        await cursor.close()
        await conn.close()

asyncio.run(main())
```

### Sync SQLAlchemy ORM 2.0 with ValkeyLock

```python
from sqlalchemy import create_engine, text, select
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, Session
from rqlite import ValkeyLock

# Define the ORM model
class Base(DeclarativeBase): pass

class Product(Base):
    __tablename__ = "readme_products"
    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column()
    price: Mapped[float] = mapped_column()
    quantity: Mapped[int] = mapped_column()

# Create engine with Valkey distributed lock
lock = ValkeyLock(name="readme_sa_sync", timeout=10.0)

engine = create_engine(
    "rqlite://localhost:4001",
    connect_args={"lock": lock},
)

with Session(engine) as session:
    # 1. CREATE TABLE IF NOT EXISTS (raw SQL — model not yet in DB)
    session.execute(text("""
        CREATE TABLE IF NOT EXISTS readme_products (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            price REAL NOT NULL,
            quantity INTEGER DEFAULT 0
        )
    """))
    session.commit()
    print("✓ Table 'readme_products' ready")

    # 2. INSERT a product via ORM
    product = Product(name="Widget A", price=9.99, quantity=100)
    session.add(product)
    session.commit()
    print("✓ Inserted: Widget A ($9.99, qty: 100)")

    # 3. SELECT — fetch by name
    result = session.execute(select(Product).where(Product.name == "Widget A"))
    found = result.scalar_one_or_none()
    print(f"✓ Found: {found.name} (${found.price:.2f}, qty: {found.quantity})")

    # 4. UPDATE — adjust price and quantity
    old_price, old_qty = found.price, found.quantity
    found.price, found.quantity = 12.99, 85
    session.commit()
    print(f"✓ Updated: Widget A (${old_price:.2f} → ${found.price:.2f}, qty: {old_qty} → {found.quantity})")

    # 5. SELECT — verify update
    result = session.execute(select(Product).where(Product.name == "Widget A"))
    found = result.scalar_one_or_none()
    print(f"✓ Verified: {found.name} (${found.price:.2f}, qty: {found.quantity})")

    # 6. DELETE the product
    session.delete(found)
    session.commit()
    print("✓ Deleted: Widget A")

    # 7. SELECT ALL — confirm table is empty
    rows = session.execute(select(Product)).scalars().all()
    print(f"✓ Table empty: {len(rows)} rows" if not rows else f"✗ Unexpected: {rows}")
```

### Async SQLAlchemy ORM 2.0 with AioValkeyLock

```python
import asyncio
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import (
    AsyncAttrs,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from rqlite import AioValkeyLock

# Define the ORM model
class Base(AsyncAttrs, DeclarativeBase): pass

class Product(Base):
    __tablename__ = "readme_products"
    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column()
    price: Mapped[float] = mapped_column()
    quantity: Mapped[int] = mapped_column()

# Create async engine with AioValkey distributed lock
lock = AioValkeyLock(name="readme_sa_async", timeout=10.0)

engine = create_async_engine(
    "rqlite+aiorqlite://localhost:4001",
    connect_args={"lock": lock},
)

Session = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

async def main():
    async with Session() as session:
        # 1. CREATE TABLE IF NOT EXISTS (raw SQL — model not yet in DB)
        await session.execute(text("""
            CREATE TABLE IF NOT EXISTS readme_products (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                price REAL NOT NULL,
                quantity INTEGER DEFAULT 0
            )
        """))
        await session.commit()
        print("✓ Table 'readme_products' ready")

        # 2. INSERT a product via ORM
        product = Product(name="Widget A", price=9.99, quantity=100)
        session.add(product)
        await session.commit()
        print("✓ Inserted: Widget A ($9.99, qty: 100)")

        # 3. SELECT — fetch by name
        result = await session.execute(select(Product).where(Product.name == "Widget A"))
        found = result.scalar_one_or_none()
        print(f"✓ Found: {found.name} (${found.price:.2f}, qty: {found.quantity})")

        # 4. UPDATE — adjust price and quantity
        old_price, old_qty = found.price, found.quantity
        found.price, found.quantity = 12.99, 85
        await session.commit()
        print(f"✓ Updated: Widget A (${old_price:.2f} → ${found.price:.2f}, qty: {old_qty} → {found.quantity})")

        # 5. SELECT — verify update
        result = await session.execute(select(Product).where(Product.name == "Widget A"))
        found = result.scalar_one_or_none()
        print(f"✓ Verified: {found.name} (${found.price:.2f}, qty: {found.quantity})")

        # 6. DELETE the product
        await session.delete(found)
        await session.commit()
        print("✓ Deleted: Widget A")

        # 7. SELECT ALL — confirm table is empty
        rows = (await session.execute(select(Product))).scalars().all()
        print(f"✓ Table empty: {len(rows)} rows" if not rows else f"✗ Unexpected: {rows}")

    await engine.dispose()

asyncio.run(main())
```

For complete working examples, including running instructions and the full example file table, see [EXAMPLES.md](./docs/EXAMPLES.md).


## Miscellaneous

For development setup, testing, linting, project architecture overview, error handling and exception hierarchy, see [MISC.md](./docs/MISC.md).


## References

- [rqlite](https://rqlite.io) — Distributed SQLite database
- [Python DB-API 2.0 (PEP 249)](https://www.python.org/dev/peps/pep-0249/) — Python database API specification
- [SQLAlchemy Documentation](https://docs.sqlalchemy.org/) — Python SQL toolkit and ORM
- [Redis](https://github.com/redis/redis) — Redis in-memory data store (distributed locking)
- [Valkey](https://github.com/valkey-io/valkey) — Valkey in-memory data store (distributed locking, Redis-compatible)


## License

MIT License - see [LICENSE](./LICENSE) file for details.
