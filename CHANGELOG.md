# Changelog

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
