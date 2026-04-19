# Development

## Setup

```bash
uv sync
```


## Run Tests

```bash
# Start fresh rqlite first
podman rm -f rqlite-test
podman run -d --name rqlite-test -p 4001:4001 docker.io/rqlite/rqlite

# Run tests
pytest -v

# With coverage
pytest --cov=rqlite --cov-report=term-missing
```


## Linting & Type Checking

```bash
uv run ruff check .
uv run ty check
```


## Transaction Model

rqlite's transaction model differs from traditional databases:

### How It Works

1. **Queue-based**: Statements are queued locally until `commit()` is called
2. **Atomic batch**: All queued statements sent in single HTTP request with `?transaction=true`
3. **All-or-nothing**: Either all statements succeed or none do

### Important Limitations

- ❌ No savepoints
- ⚠️ Explicit `BEGIN`/`COMMIT`/`ROLLBACK` SQL is ignored (use Python API)
- ❌ No native transaction isolation levels
- ⚠️ `rowcount` not available for SELECT statements


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


## Read Consistency Levels

For details on read consistency levels and usage, see [READ_CONSISTENCY.md](./READ_CONSISTENCY.md).


## Architecture

```
rqlite/
├── __init__.py              # Package init, exports
├── connection.py            # Connection class (DB-API 2.0 sync)
├── cursor.py                # Cursor class (DB-API 2.0 sync)
├── types.py                 # Type helpers & sync locks (ThreadLock, LockProtocol)
├── exceptions.py            # Exception classes
├── async_connection.py      # Async Connection class
├── async_cursor.py          # Async Cursor class
├── async_types.py           # Async locks (AioLock, AsyncLockProtocol)
├── redis_lock.py            # Redis distributed lock (sync, RedisLock)
├── async_redis_lock.py      # Async Redis distributed lock (AioRedisLock)
├── valkey_lock.py           # Valkey distributed lock (sync, ValkeyLock)
├── async_valkey_lock.py     # Async Valkey distributed lock (AioValkeyLock)
└── sqlalchemy/              # SQLAlchemy dialect
    ├── __init__.py          # Dialect exports
    ├── dialect.py           # RQLiteDialect implementation (sync)
    └── async_dialect.py     # AioRQLiteDialect implementation (async)
```
