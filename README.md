# tangled-pyrqlite - python client

[![Downloads](https://img.shields.io/pypi/dm/tangled-pyrqlite)](https://pypistats.org/packages/tangled-pyrqlite)
[![Supported Versions](https://img.shields.io/pypi/pyversions/tangled-pyrqlite)](https://pypi.org/project/tangled-pyrqlite)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](https://opensource.org/licenses/MIT)

A pure Python client for [rqlite](https://rqlite.io/) distributed SQLite clusters, providing:

- **DB-API 2.0** - Standard Python database API (PEP 249)
- **SQLAlchemy dialect** - Full ORM support via SQLAlchemy 2.0
- **Transaction support** - Atomic batch operations with rqlite's transaction model
- **Parameterized queries** - Safe, SQL injection-proof query execution

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

**Docker:**
```bash
docker rm -f rqlite-test
docker run -d --name rqlite-test -p 4001:4001 rqlite/rqlite
```

### DB-API 2.0 Usage

```python
import rqlite
from rqlite import ReadConsistency, ThreadLock

# Basic connection (uses LINEARIZABLE consistency by default)
conn = rqlite.connect(host="localhost", port=4001)
cursor = conn.cursor()

# With custom read consistency and lock for transaction support
conn = rqlite.connect(
    host="localhost",
    port=4001,
    read_consistency=ReadConsistency.WEAK,  # or "weak" string
    lock=ThreadLock()  # Suppresses transaction warnings
)
cursor = conn.cursor()

# Create table
cursor.execute("""
    CREATE TABLE users (
        id INTEGER PRIMARY KEY,
        name TEXT NOT NULL,
        email TEXT UNIQUE
    )
""")
conn.commit()

# Insert with positional parameters (recommended)
cursor.execute(
    "INSERT INTO users (name, email) VALUES (?, ?)",
    ("Alice", "alice@example.com")
)

# Insert with named parameters (also supported)
cursor.execute(
    "INSERT INTO users (name, email) VALUES (:name, :email)",
    {"name": "Bob", "email": "bob@example.com"}
)
conn.commit()

# Query with positional parameters
cursor.execute("SELECT * FROM users WHERE name=?", ("Alice",))
row = cursor.fetchone()
print(row)  # (1, "Alice", "alice@example.com")

# Fetch all
cursor.execute("SELECT * FROM users")
for row in cursor:
    print(row)

# Close
cursor.close()
conn.close()

# Or use context managers
with rqlite.connect() as conn:
    with conn.cursor() as cursor:
        cursor.execute("SELECT * FROM users")
        for row in cursor:
            print(row)
```



### Parameter Binding

The client supports both parameter styles per DB-API 2.0 standard:

**Positional parameters (`?`) - Recommended:**
```python
cursor.execute("SELECT * FROM users WHERE id=? AND name=?", (42, "Alice"))
```

**Named parameters (`:name`) - Also supported:**
```python
cursor.execute(
    "SELECT * FROM users WHERE id=:id AND name=:name",
    {"id": 42, "name": "Alice"}
)
```

**Note for SQLAlchemy users:** SQLAlchemy automatically uses positional parameters (`?`) for all queries. The ORM and Core layers handle parameter binding before reaching the dialect, so you don't need to worry about parameter format when using SQLAlchemy.

### SQLAlchemy Usage

```python
from sqlalchemy import Integer, String, create_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, Session
from rqlite import ReadConsistency, ThreadLock


class Base(DeclarativeBase):
    pass


class User(Base):
    __tablename__ = "users"
    
    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(nullable=False)
    email: Mapped[str | None] = mapped_column(unique=True)


# Basic engine (uses LINEARIZABLE consistency by default)
engine = create_engine("rqlite://localhost:4001")

# With custom read consistency and lock via connect_args
engine = create_engine(
    "rqlite://localhost:4001",
    connect_args={
        "read_consistency": ReadConsistency.WEAK,  # or "weak" string
        "lock": ThreadLock()  # Suppresses transaction warnings
    }
)

# Or use URL query parameter for read_consistency only
engine = create_engine("rqlite://localhost:4001?read_consistency=weak")

# Create tables (echo=True shows SQL)
engine = create_engine("rqlite://localhost:4001", echo=True)
Base.metadata.create_all(engine)

# Use with Session
with Session(engine) as session:
    user = User(name="Charlie", email="charlie@example.com")
    session.add(user)
    session.commit()
    
    # Query
    user = session.query(User).filter_by(name="Charlie").first()
    print(user.name)  # Charlie
```

## Features

### Read Consistency Levels

rqlite supports multiple read consistency levels to balance between data freshness and performance. The client defaults to **LINEARIZABLE** for guaranteed fresh reads.

| Level | Speed | Freshness | Best For |
|-------|-------|-----------|----------|
| **LINEARIZABLE** (default) | Moderate | Guaranteed fresh | Critical reads requiring latest data |
| **WEAK** | Fast | Usually current (sub-second staleness possible) | General-purpose reads |
| **NONE** | Fastest | No guarantee | Read-only nodes, max performance |
| **STRONG** | Slow | Guaranteed fresh + applied | Testing only |
| **AUTO** | Varies | Varies | Mixed node type clusters |

**Usage:**
```python
import rqlite
from rqlite import ReadConsistency

# Use LINEARIZABLE (default) for guaranteed fresh reads
conn = rqlite.connect()

# Use WEAK for faster reads with possible sub-second staleness
# Supports both enum and string:
conn = rqlite.connect(read_consistency=ReadConsistency.WEAK)
conn = rqlite.connect(read_consistency="weak")

# Use NONE for read-only nodes or maximum performance
conn = rqlite.connect(read_consistency="none")
```

**SQLAlchemy:**
```python
from sqlalchemy import create_engine

# Via URL query parameter
engine = create_engine("rqlite://localhost:4001?read_consistency=weak")

# Via connect_args
from rqlite import ReadConsistency
engine = create_engine(
    "rqlite://localhost:4001",
    connect_args={"read_consistency": ReadConsistency.WEAK}
)
```

See [rqlite documentation](https://rqlite.io/docs/db_api/#read-consistency-levels) for detailed explanations of each consistency level.

### DB-API 2.0 Compliance

| Feature | Status | Notes |
|---------|--------|-------|
| `connect()` | ✅ | Returns Connection |
| `Connection.cursor()` | ✅ | Returns Cursor |
| `Connection.commit()` | ✅ | Queues then sends statements |
| `Connection.rollback()` | ✅ | Discards queued statements |
| `Connection.close()` | ✅ | Clears queue, closes resources |
| `Cursor.execute()` | ✅ | Supports positional and named params |
| `Cursor.executemany()` | ⚠️ | Executes sequentially |
| `Cursor.fetchall()` | ✅ | Returns all rows as tuples |
| `Cursor.fetchmany()` | ✅ | Respects `arraysize` |
| `Cursor.fetchone()` | ✅ | Returns single row or None |
| `Cursor.description` | ✅ | Column metadata after SELECT |
| `Cursor.rowcount` | ⚠️ | Only for write operations |
| `Cursor.lastrowid` | ✅ | Available after INSERT |

### SQLAlchemy Support

| Feature | Status | Notes |
|---------|--------|-------|
| Core SELECT/INSERT/UPDATE/DELETE | ✅ | Full support |
| ORM Models | ✅ | Full support |
| Relationships | ✅ | Via SQLite dialect |
| Sessions | ✅ | Standard SQLAlchemy sessions |
| Transactions | ⚠️ | Limited (see below) |
| Reflection | ⚠️ | Basic table/column introspection |

## Transaction Model

rqlite's transaction model differs from traditional databases:

### How It Works

1. **Queue-based**: Statements are queued locally until `commit()` is called
2. **Atomic batch**: All queued statements sent in single HTTP request with `?transaction=true`
3. **All-or-nothing**: Either all statements succeed or none do

### Important Limitations

⚠️ **Cannot use SELECT results within transactions**

```python
# This does NOT work as expected:
cursor.execute("SELECT MAX(id) FROM users")
max_id = cursor.fetchone()[0]  # Returns None! Results not available yet
cursor.execute("INSERT INTO users (id, name) VALUES (?, ?)", (max_id + 1, "New"))
conn.commit()  # SELECT results only available AFTER commit
```

**Workaround**: Execute SELECT outside transaction:

```python
# Get max ID first (outside transaction)
cursor.execute("SELECT COALESCE(MAX(id), 0) FROM users")
max_id = cursor.fetchone()[0]
conn.commit()  # Commit the SELECT

# Then insert in new transaction
cursor.execute("INSERT INTO users (id, name) VALUES (?, ?)", (max_id + 1, "New"))
conn.commit()
```

### Other Limitations

- ❌ No savepoints
- ⚠️ Explicit `BEGIN`/`COMMIT`/`ROLLBACK` SQL is ignored (use Python API)
- ❌ No transaction isolation levels
- ⚠️ `rowcount` not available for SELECT statements

## Connection URLs

### DB-API 2.0

```python
# Basic connection (uses LINEARIZABLE consistency by default)
conn = rqlite.connect(host="localhost", port=4001)

# With authentication
conn = rqlite.connect(
    host="localhost",
    port=4001,
    username="admin",
    password="secret"
)

# Custom timeout
conn = rqlite.connect(host="localhost", port=4001, timeout=60.0)

# Custom read consistency (enum or string)
conn = rqlite.connect(host="localhost", port=4001, read_consistency="weak")

# Or using the enum:
from rqlite import ReadConsistency
conn = rqlite.connect(
    host="localhost",
    port=4001,
    read_consistency=ReadConsistency.WEAK
)

# With lock for transaction support (suppresses warnings)
from rqlite import ThreadLock
conn = rqlite.connect(
    host="localhost",
    port=4001,
    lock=ThreadLock()
)

# Combining read_consistency and lock
conn = rqlite.connect(
    host="localhost",
    port=4001,
    read_consistency=ReadConsistency.WEAK,
    lock=ThreadLock()
)
```

### SQLAlchemy

**Note:** For SQLAlchemy, custom parameters like `read_consistency` and `lock` must be passed via `connect_args` dictionary, not directly to `create_engine()`. This is because SQLAlchemy validates kwargs before passing them to the dialect.

```python
# Basic (uses LINEARIZABLE consistency by default)
engine = create_engine("rqlite://localhost:4001")

# With authentication
engine = create_engine("rqlite://admin:secret@localhost:4001")

# Enable SQL echo for debugging
engine = create_engine("rqlite://localhost:4001", echo=True)

# Custom read consistency via URL query parameter
engine = create_engine("rqlite://localhost:4001?read_consistency=weak")

# Custom read consistency via connect_args
from rqlite import ReadConsistency
engine = create_engine(
    "rqlite://localhost:4001",
    connect_args={"read_consistency": ReadConsistency.WEAK}
)

# With lock for transaction support (via connect_args)
from rqlite import ThreadLock
engine = create_engine(
    "rqlite://localhost:4001",
    connect_args={"lock": ThreadLock()}
)

# Both read_consistency and lock together
engine = create_engine(
    "rqlite://localhost:4001",
    connect_args={
        "read_consistency": ReadConsistency.WEAK,
        "lock": ThreadLock()
    }
)
```

## Error Handling

```python
import rqlite

try:
    conn = rqlite.connect()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM nonexistent_table")
except rqlite.ProgrammingError as e:
    print(f"SQL error: {e}")
except rqlite.OperationalError as e:
    print(f"Connection error: {e}")
except rqlite.DatabaseError as e:
    print(f"Database error: {e}")
```

### Exception Hierarchy

- `Error` - Base exception
  - `InterfaceError` - Interface-related errors
  - `DatabaseError` - Database errors
    - `DataError` - Data-related errors
    - `OperationalError` - Connection/operation errors
    - `IntegrityError` - Constraint violations
    - `InternalError` - Internal database errors
    - `ProgrammingError` - SQL syntax errors
    - `NotSupportedError` - Unsupported operations

## Examples

See the `examples/` directory for complete working examples.

### Running Sync Examples

**Without lock (shows transaction warnings):**
```bash
# DB-API 2.0 examples with warnings
uv run python -B examples/basic_usage.py

# SQLAlchemy ORM examples with warnings
uv run python -B examples/sqlalchemy_orm.py
```

**With lock (no transaction warnings):**
```bash
# DB-API 2.0 examples without warnings
uv run python -B examples/basic_usage.py --with-lock

# SQLAlchemy ORM examples without warnings
uv run python -B examples/sqlalchemy_orm.py --with-lock
```

The `-B` flag disables byte-code generation for cleaner output.

### Example Files

- `basic_usage.py` - DB-API 2.0 CRUD operations
- `sqlalchemy_orm.py` - SQLAlchemy ORM usage

### Locking Mechanism

The examples demonstrate the optional locking mechanism for transaction support:

- **Without lock**: Shows warnings about explicit BEGIN/COMMIT/ROLLBACK SQL not being supported
- **With lock (`--with-lock`)**: Uses `ThreadLock` to suppress warnings, allowing explicit transaction SQL

For more details, see the [Locking Mechanism](#locking-mechanism) section below.

## Locking Mechanism

rqlite provides an optional locking mechanism to support transactions and suppress warnings about explicit transaction SQL commands (BEGIN/COMMIT/ROLLBACK/SAVEPOINT).

### Why Use Locks?

By default, when using the rqlite library **without a lock**, you will receive a `UserWarning`:

```
UserWarning: Explicit BEGIN/COMMIT/ROLLBACK/SAVEPOINT SQL is not supported.
```

This warning is **expected behavior** and indicates that:
- You are aware of rqlite's transaction model (queue-based, atomic batch)
- You understand that explicit transaction SQL commands are not supported in the traditional sense
- You are using the Python API (`commit()`, `rollback()`) for transaction control

This is **fine** if you understand how rqlite transactions work. However, if you need **true ACID compliance** with proper isolation guarantees, it is **recommended to use a lock** (e.g., `ThreadLock()`). The lock:
- Suppresses the warning
- Indicates intentional handling of transaction limitations
- Provides thread-safety for concurrent operations

When you provide a lock, these warnings are suppressed, indicating that you're aware of the limitations and handling transactions appropriately.

### Available Lock Classes

1. **`ThreadLock`** (recommended) - Thread-safe wrapper around `threading.Lock`
2. **`threading.Lock`** - Use directly (satisfies `LockProtocol`)
3. **Custom locks** - Any class implementing `LockProtocol`

### Usage Examples

**DB-API 2.0 with ThreadLock:**
```python
import rqlite
from rqlite import ThreadLock

# Connect with lock to suppress warnings
conn = rqlite.connect(lock=ThreadLock())
cursor = conn.cursor()

# No warning about explicit transaction SQL
cursor.execute("BEGIN")
cursor.execute("INSERT INTO users (name) VALUES ('Alice')")
cursor.execute("COMMIT")
```

**Using threading.Lock directly:**
```python
import rqlite
import threading

conn = rqlite.connect(lock=threading.Lock())
# Same behavior as ThreadLock
```

**SQLAlchemy with lock:**
```python
from sqlalchemy import create_engine
from rqlite import ThreadLock

engine = create_engine(
    "rqlite://localhost:4001",
    connect_args={"lock": ThreadLock()}
)
# No warnings when using explicit transaction SQL
```

**Custom lock implementation:**
```python
import rqlite
from rqlite import LockProtocol

class MyLock:
    """Custom lock satisfying LockProtocol."""
    def __init__(self): pass
    def acquire(self, blocking=True, timeout=-1): return True
    def release(self): pass
    def __enter__(self): return self
    def __exit__(self, *args): pass

conn = rqlite.connect(lock=MyLock())
```

### LockProtocol

Any lock implementation must satisfy the `LockProtocol`:

```python
from typing import Protocol

class LockProtocol(Protocol):
    def __init__(self) -> None: ...
    def acquire(self, blocking: bool = ..., timeout: float = ...) -> bool: ...
    def release(self) -> None: ...
    def __enter__(self) -> "LockProtocol": ...
    def __exit__(
        self,
        exc_type: type[Exception] | None,
        exc_val: Exception | None,
        exc_tb: object,
    ) -> None: ...
```

### Abstract Lock Class

The `rqlite.Lock` class is abstract and should NOT be instantiated directly. Use `ThreadLock` or `threading.Lock` instead:

```python
from rqlite import Lock

# ❌ Don't do this - raises NotImplementedError
lock = Lock()

# ✅ Do this instead
from rqlite import ThreadLock
lock = ThreadLock()
```

## Development

### Setup

```bash
cd rqlite
uv sync
```

### Run Tests

```bash
# Start rqlite first
podman run -d --name rqlite-test -p 4001:4001 docker.io/rqlite/rqlite

# Run tests
pytest -v

# With coverage
pytest --cov=rqlite --cov-report=term-missing
```

### Linting & Type Checking

```bash
uv run ruff check .
uv run ty check
```

## Architecture

```
rqlite/
├── __init__.py           # Package init, exports
├── connection.py         # Connection class (DB-API 2.0)
├── cursor.py             # Cursor class (DB-API 2.0)
├── types.py              # Type helpers
├── exceptions.py         # Exception classes
└── sqlalchemy/           # SQLAlchemy dialect
    ├── __init__.py       # Dialect exports
    └── dialect.py        # RQLiteDialect implementation
```

## References

- [rqlite Documentation](https://rqlite.io/docs/)
- [rqlite HTTP API](https://rqlite.io/docs/api/)
- [Python DB-API 2.0 (PEP 249)](https://www.python.org/dev/peps/pep-0249/)
- [SQLAlchemy Documentation](https://docs.sqlalchemy.org/)

## License

MIT License - see LICENSE file for details.
