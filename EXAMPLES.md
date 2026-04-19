# Examples

See the `examples/` directory for complete working examples.

## Running Sync Examples

**Without lock (shows transaction warnings):**
```bash
# Sync ThreadLock DB-API 2.0 examples with warnings
uv run python -B examples/sync_thread_lock_basic_usage.py

# Sync ThreadLock SQLAlchemy ORM examples with warnings
uv run python -B examples/sync_thread_lock_sqlalchemy_orm.py
```

**With lock (no transaction warnings):**
```bash
# Sync ThreadLock DB-API 2.0 examples without warnings
uv run python -B examples/sync_thread_lock_basic_usage.py --with-lock

# Sync ThreadLock SQLAlchemy ORM examples without warnings
uv run python -B examples/sync_thread_lock_sqlalchemy_orm.py --with-lock
```

The `-B` flag disables byte-code generation for cleaner output.

## Example Files

| File | Description |
|------|-------------|
| `sync_thread_lock_basic_usage.py` | Sync DB-API 2.0 CRUD with ThreadLock — [see DB_API.md](./DB_API.md) |
| `async_aio_lock_basic_usage.py` | Async DB-API 2.0 examples with AioLock — [see DB_API.md](./DB_API.md) |
| `sync_thread_lock_sqlalchemy_orm.py` | Sync SQLAlchemy ORM usage — [see SQLALCHEMY.md](./SQLALCHEMY.md) |
| `async_aio_lock_sqlalchemy_orm.py` | Async SQLAlchemy ORM usage — [see SQLALCHEMY.md](./SQLALCHEMY.md) |
| `sync_redis_lock_basic_usage.py` | Sync Redis distributed lock examples — [see REDIS_LOCK.md](./REDIS_LOCK.md) |
| `async_redis_lock_basic_usage.py` | Async Redis distributed lock examples — [see REDIS_LOCK.md](./REDIS_LOCK.md) |
| `sync_valkey_lock_basic_usage.py` | Sync Valkey distributed lock examples — [see VALKEY_LOCK.md](./VALKEY_LOCK.md) |
| `async_valkey_lock_basic_usage.py` | Async Valkey distributed lock examples — [see VALKEY_LOCK.md](./VALKEY_LOCK.md) |
| `sync_redis_lock_distributed_transfer.py` | Cross-process bank transfer demo (proves Redis lock works) — [see REDIS_LOCK.md](./REDIS_LOCK.md) |

The examples demonstrate the optional locking mechanism for transaction support:

- **Without lock**: Shows warnings about explicit BEGIN/COMMIT/ROLLBACK SQL not being supported
- **With lock (`--with-lock`)**: Uses `ThreadLock` to suppress warnings, allowing explicit transaction SQL

For more details on locks, see [LOCK.md](./LOCK.md).
