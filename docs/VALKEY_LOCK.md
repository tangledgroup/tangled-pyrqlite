# Valkey Distributed Lock

Valkey is a Redis-compatible in-memory data store, maintained by the Linux Foundation. It works with the same lock classes using the `valkey` extra instead of `redis`.

Install with the `valkey` extra:

```bash
uv add tangled-pyrqlite[valkey]
```

Start a Valkey server:

```bash
podman rm -f valkey-test
podman run -d --name valkey-test -p 6379:6379 docker.io/valkey/valkey:latest
```

## Sync DB-API 2.0 with ValkeyLock

```python
import rqlite
from rqlite import ValkeyLock

# Connect with Valkey distributed lock
lock = ValkeyLock(name="transfer", timeout=10.0)
conn = rqlite.connect(host="localhost", port=4001, lock=lock)
cursor = conn.cursor()

cursor.execute("SELECT balance FROM accounts WHERE id=?")
balance = cursor.fetchone()[0]
cursor.execute("UPDATE accounts SET balance=? WHERE id=?", (balance - 100, 1))
conn.commit()
```

## Async DB-API 2.0 with AioValkeyLock

```python
import asyncio
import rqlite
from rqlite import AioValkeyLock

async def transfer():
    lock = AioValkeyLock(name="transfer", timeout=10.0)
    conn = rqlite.async_connect(lock=lock)
    cursor = await conn.cursor()

    await cursor.execute("SELECT balance FROM accounts WHERE id=?")
    balance = cursor.fetchone()[0]
    await cursor.execute(
        "UPDATE accounts SET balance=? WHERE id=?",
        (balance - 100, 1),
    )
    await conn.commit()

    await cursor.close()
    await conn.close()

asyncio.run(transfer())
```

## SQLAlchemy with ValkeyLock

```python
from sqlalchemy import create_engine
from rqlite import ValkeyLock

lock = ValkeyLock(name="sa_lock", timeout=10.0)

engine = create_engine(
    "rqlite://localhost:4001",
    connect_args={"lock": lock}
)
```
