# Changelog

## v0.1.8 - 2026-07-24

### Added

- **Redis/Valkey cluster support** — four new cluster client factory modules (`redis_cluster.py`, `async_redis_cluster.py`, `valkey_cluster.py`, `async_valkey_cluster.py`) that auto-detect cluster mode via `INFO cluster` and return the appropriate client type (`Redis`/`RedisCluster`, `Valkey`/`ValkeyCluster`).
- **`cluster` parameter on distributed locks** — `RedisLock`, `AioRedisLock`, `ValkeyLock`, `AioValkeyLock` now accept `cluster: bool | None` (True = force cluster, False = force standalone, None = auto-detect). Client creation delegated to the cluster factory helpers.
- **`close()` method on all lock classes** — properly shuts down the underlying Redis/Valkey client to avoid resource leaks.
- **Cluster-backed DB-API test suites** — `test_dbapi_sync_redis_cluster.py`, `test_dbapi_async_redis_cluster.py`, `test_dbapi_sync_valkey_cluster.py`, `test_dbapi_async_valkey_cluster.py`.
- **Cluster-backed SQLAlchemy test suites** — `test_sqlalchemy_sync_redis_cluster.py`, `test_sqlalchemy_async_redis_cluster.py`, `test_sqlalchemy_sync_valkey_cluster.py`, `test_sqlalchemy_async_valkey_cluster.py`.
- **Comprehensive Valkey lock test suites** — full coverage for all lock mode combinations (sync/async, thread lock, aio lock, redis lock, valkey lock) across DB-API and SQLAlchemy interfaces.

### Changed

- **Lock client lifecycle** — `_get_client()` in all four lock classes now uses cluster factory helpers instead of inline constructors; client reference stored on `self._client` for lifecycle management.
- **Type annotations** — lock classes now include `RedisCluster`/`ValkeyCluster` union types for `_client` and `_get_client()` return types.
- **Test fixtures** — `conftest.py` expanded with 24 new table names for cluster-backed SQLAlchemy and DB-API tests.

## v0.1.7 - 2026-06-10

### Added

- **BLOB column type support** — full round-trip for `Binary`/`LargeBinary`/`BLOB` columns between rqlite and Python. Uses `blob_array` query parameter so BLOB data is returned as integer arrays (unambiguous) and decoded back to Python `bytes`. Covers sync DB-API, async DB-API, sync SQLAlchemy ORM, and async SQLAlchemy ORM.
- **DB-API 2.0 `Binary()` constructor** — exposed on both sync (`rqlite.Binary`) and async (`AioRQLiteDBAPI.Binary`) modules, matching `sqlite3.Binary` behavior. Required by SQLAlchemy's `LargeBinary.bind_processor()`.
- **BLOB test suite** — `tests/test_blob_dbapi.py` (22 tests) and `tests/test_blob_sqlalchemy.py` (14 tests) covering insert, select, update, empty BLOB, large BLOB (>1KB), multiple rows, and async variants.
- **BLOB examples** — `examples/blob_basic_usage.py` (raw DB-API) and `examples/blob_sqlalchemy_orm.py` (SQLAlchemy ORM).

### Changed

- **Updated dependencies** for rqlite 10.2.0 compatibility.
- **`adapt_value(bytes)` serialization** — changed from hex string (`value.hex()`) to JSON integer array (`list(value)`) matching rqlite's BLOB parameter format.
- **Cursor read path** — SELECT queries now include `blob_array=true` and `_parse_result()` decodes BLOB integer arrays back to Python `bytes` for both sync and async cursors.

### Fixed

- **`AttributeError: 'AioRQLiteDBAPI' object has no attribute 'Binary'`** — SQLAlchemy's `LargeBinary` type calls `dialect.dbapi.Binary`; added the missing constructor to both dialects.
- **BLOB data stored as TEXT** — bytes were serialized as hex strings which rqlite interpreted as TEXT; now sent as integer arrays so rqlite stores them as actual BLOBs.
- **BLOB read ambiguity** — default base64 encoding made it impossible to distinguish text from blob; `blob_array` parameter resolves this.

## v0.1.6 - 2026-04-19

### Added

- **Valkey lock CRUD examples in README** — four self-contained, copy-paste runnable code blocks under `## Examples` demonstrating the full lifecycle (CREATE TABLE IF NOT EXISTS → INSERT → SELECT → UPDATE → SELECT → DELETE → SELECT ALL) using `ValkeyLock`, `AioValkeyLock`, sync SQLAlchemy ORM, and async SQLAlchemy ORM.

## v0.1.5 - 2026-04-18

### Fixed

- **Distributed lock cross-process demo** (`examples/sync_redis_lock_distributed_transfer.py`): Fixed incorrect result collection that masked correct lock serialization. Each worker process no longer reads a stale DB snapshot after its loop; final balance is now read once from the database after all processes complete, correctly verifying data integrity under Redis/Valkey distributed locks.

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
