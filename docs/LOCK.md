# Locking Mechanism

rqlite provides an optional locking mechanism to support transactions and suppress warnings about explicit transaction SQL commands (BEGIN/COMMIT/ROLLBACK/SAVEPOINT).

## Why Use Locks?

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

## Available Lock Classes

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
    def __exit__(
        self,
        exc_type: type[Exception] | None,
        exc_val: Exception | None,
        exc_tb: object,
    ) -> None: pass

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

# ❌ Don't do this
lock = Lock()   # raises NotImplementedError

# ✅ Do this instead
from rqlite import ThreadLock

lock = ThreadLock()
```

> **Important:** `ThreadLock` and `AioLock` are only practical during local development. They provide in-process synchronization but do NOT offer cross-process or distributed transaction serialization. This holds even if a single thread/process is used to interact with an rqlite server/cluster — for any deployment scenario, Redis or Valkey locks are recommended. If you can start from the beginning, we advise using either Redis or Valkey locks. Never mix sync and async locks.

## Distributed Locks (Cross-Process)

For true cross-process transaction serialization, use distributed locks backed by Redis or Valkey:

- **Redis locks** — see [REDIS_LOCK.md](./REDIS_LOCK.md)
- **Valkey locks** — see [VALKEY_LOCK.md](./VALKEY_LOCK.md)

Both are drop-in replacements for `ThreadLock`/`AioLock` and provide real ACID isolation across processes.

### Available Lock Classes (Summary)

| Lock | Sync/Async | Scope | Use case |
|------|-----------|-------|----------|
| `ThreadLock` | sync | In-process threads | Single process, thread-safe transactions |
| `threading.Lock` | sync | In-process threads | Direct stdlib lock |
| **`RedisLock`** | **sync** | **Cross-process (distributed)** | Multi-process, ACID isolation |
| `AioLock` | async | In-process tasks | Single process, async-safe transactions |
| **`AioRedisLock`** | **async** | **Cross-process (distributed)** | Multi-process async, ACID isolation |
| **`ValkeyLock`** | **sync** | **Cross-process (distributed)** | Multi-process, ACID isolation (Valkey) |
| `AioLock` | async | In-process tasks | Single process, async-safe transactions |
| **`AioValkeyLock`** | **async** | **Cross-process (distributed)** | Multi-process async, ACID isolation (Valkey) |

For full examples see: `examples/sync_redis_lock_basic_usage.py`, `examples/async_redis_lock_basic_usage.py`,
`examples/sync_valkey_lock_basic_usage.py`, `examples/async_valkey_lock_basic_usage.py`,
`examples/sync_redis_lock_distributed_transfer.py`.
