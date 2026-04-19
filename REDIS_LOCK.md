# Redis Distributed Lock

For **true cross-process transaction serialization**, use the built-in Redis-backed locks.
Install with the `redis` extra:

```bash
uv add tangled-pyrqlite[redis]
```

Start a Redis server:

```bash
podman rm -f redis-test
podman run -d --name redis-test -p 6379:6379 docker.io/redis
```

## Sync DB-API 2.0 with RedisLock

```python
import rqlite
from rqlite import RedisLock

# Connect with Redis distributed lock
lock = RedisLock(name="transfer", timeout=10.0)
conn = rqlite.connect(host="localhost", port=4001, lock=lock)
cursor = conn.cursor()

cursor.execute("SELECT balance FROM accounts WHERE id=?")
balance = cursor.fetchone()[0]
cursor.execute("UPDATE accounts SET balance=? WHERE id=?", (balance - 100, 1))
conn.commit()
```

## Async DB-API 2.0 with AioRedisLock

```python
import asyncio
import rqlite
from rqlite import AioRedisLock

async def transfer():
    lock = AioRedisLock(name="transfer", timeout=10.0)
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

## SQLAlchemy with RedisLock

```python
from sqlalchemy import create_engine
from rqlite import RedisLock

lock = RedisLock(name="sa_lock", timeout=10.0)

engine = create_engine(
    "rqlite://localhost:4001",
    connect_args={"lock": lock}
)
```
