# DB-API 2.0 Usage

## Connection URLs

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
```

```python
from rqlite import ThreadLock

# With lock for transaction support (suppresses warnings)
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

## Basic Connection and CRUD

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

## Parameter Binding

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

## DB-API 2.0 Compliance

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
