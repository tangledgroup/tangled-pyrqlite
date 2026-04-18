# Changelog

## v0.1.4 - 2026-04-18

### Changed

- **Renamed all test files** to strict naming convention: `test_{sync|async}_{thread_lock|aio_lock|redis_lock|valkey_lock}_<scenario>.py`. Every test class and method name includes the full lock type prefix (e.g., `TestSyncThreadLockUnit`, `test_sync_thread_lock_creation`).
- **Renamed all example files** to strict naming convention: `{sync|async}_{thread_lock|aio_lock|redis_lock|valkey_lock}_<scenario>.py` (e.g., `sync_thread_lock_basic_usage.py`, `async_aio_lock_sqlalchemy_orm.py`). All docstrings, argparse descriptions, and print titles updated.
- **Split sync DB-API tests** into dedicated files: `test_sync_thread_lock_dbapi.py` (connection), `test_sync_thread_lock_dbapi_cursor.py` (cursor), `test_sync_thread_lock_dbapi_read_consistency.py` (consistency levels).
- **Split sync SQLAlchemy tests** into dedicated files: `test_sync_thread_lock_sqlalchemy.py` (lock-specific), `test_sync_thread_lock_sqlalchemy_dialect.py` (dialect/ORM/Core).

### Added

- **New async DB-API test files**: `test_async_aio_lock_dbapi.py` (connection, CRUD, read consistency), `test_async_aio_lock_dbapi_cursor.py` (cursor operations: fetch, execute, executemany), `test_async_aio_lock_sqlalchemy.py` (async SQLAlchemy Core + ORM).

### Deleted

- Removed `tests/test_lock.py` — content merged into `tests/test_sync_thread_lock.py`.

## v0.1.3 - 2026-04-18

### Added

- **Valkey distributed locks** (`ValkeyLock` sync, `AioValkeyLock` async) — wraps valkey-py's `valkey.lock.Lock` and `valkey.asyncio.lock.Lock` for cross-process transaction serialization. Requires optional `[valkey]` extra.
- Sync and async usage examples for Valkey locks (`examples/sync_valkey_lock_basic_usage.py`, `examples/async_valkey_lock_basic_usage.py`).

## v0.1.2 - 2026-04-18

### Added

- **Redis distributed locks** (`RedisLock` sync, `AioRedisLock` async) — wraps redis-py's `redis.lock.Lock` and `redis.asyncio.lock.Lock` for cross-process transaction serialization. Requires optional `[redis]` extra.
- Distributed serialization tests demonstrating correct final balance under concurrent access with Redis lock vs race conditions without it.

### Fixed

- Documentation: removed spurious `await` from `rqlite.async_connect()` in README and module docstrings (`async_connect` returns `AsyncConnection` directly, not a coroutine).

## v0.1.1 - 2026-04-18

### Changed

- Clarified transaction support as serializable with a locking mechanism.

### Fixed

- Documentation: added blank lines before imports in code examples (PEP 8).
- Documentation: modernized `__exit__` type annotations.
- Documentation: updated test setup to use podman directly with clean state.

### Added

- `[project.urls]` metadata (Repository, Issues, Changelog) to `pyproject.toml`.


## v0.1.0 - 2026-04-17

### Added

- DB-API 2.0 compliant synchronous client for rqlite distributed SQLite clusters.
- Async DB-API 2.0 client using aiohttp.
- SQLAlchemy dialect integration (sync and async) registered as `rqlite` and `rqlite.aiorqlite`.
- Read consistency level support (`LINEARIZABLE`, `WEAK`, `NONE`, `STRONG`, `AUTO`).
- Thread-safe locking with `ThreadLock` for sync and `AioLock`/`AsyncLock` for async connections.
- Full DB-API 2.0 type system (STRING, BINARY, NUMBER, DATETIME, ROWID) with value adaptation.
- Standard DB-API 2.0 exception hierarchy (Error, DatabaseError, OperationalError, etc.).
