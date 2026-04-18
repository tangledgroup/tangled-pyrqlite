# Changelog

## v0.1.3 - 2026-04-18

### Added

- **Valkey distributed locks** (`ValkeyLock` sync, `AioValkeyLock` async) — wraps valkey-py's `valkey.lock.Lock` and `valkey.asyncio.lock.Lock` for cross-process transaction serialization. Requires optional `[valkey]` extra.
- Sync and async usage examples for Valkey locks (`examples/valkey_lock_sync.py`, `examples/valkey_lock_async.py`).

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
