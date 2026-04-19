# tangled-pyrqlite - python client

[![PyPI](https://img.shields.io/pypi/v/tangled-pyrqlite)](https://pypi.org/project/tangled-pyrqlite/)
[![PyPI Downloads](https://img.shields.io/pypi/dm/tangled-pyrqlite)](https://pypistats.org/packages/tangled-pyrqlite)
[![Supported Versions](https://img.shields.io/pypi/pyversions/tangled-pyrqlite)](https://pypi.org/project/tangled-pyrqlite)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](https://opensource.org/licenses/MIT)

A pure Python client for [rqlite](https://rqlite.io/) distributed SQLite clusters, providing:

- **DB-API 2.0** - Standard Python database API (PEP 249) — see [DB_API.md](./docs/DB_API.md)
- **SQLAlchemy dialect** - Full ORM support via SQLAlchemy 2.0 — see [SQLALCHEMY.md](./docs/SQLALCHEMY.md)
- **Parameterized queries** - Safe, SQL injection-proof query execution
- **Serializable transaction support** - Atomic batch operations using locking mechanism - bring your own distributed locking implementation using [REDIS_LOCK.md](./docs/REDIS_LOCK.md) or [VALKEY_LOCK.md](./docs/VALKEY_LOCK.md)

**NOTE**: Documentation and instructions are being actively written and improved.

## Installation

```bash
# Using uv (recommended)
uv add tangled-pyrqlite

# Using pip
pip install tangled-pyrqlite
```

## Quick Start

### Starting rqlite Server

Before using the client, start an rqlite server:

**Podman (recommended - no root required):**

```bash
podman rm -f rqlite-test
podman run -d --name rqlite-test -p 4001:4001 docker.io/rqlite/rqlite
```


## Locking

rqlite's transaction model differs from traditional databases.

By default, when using the rqlite library **without a lock**, you will receive a `UserWarning`:

```
UserWarning: Explicit BEGIN/COMMIT/ROLLBACK/SAVEPOINT SQL is not supported.
```

When you provide a lock, these warnings are suppressed, indicating that you're aware of the limitations and handling transactions appropriately.

For **simple locks** guide (local development, not recommended in production):
- `threading` sync lock - see [LOCK.md](./docs/LOCK.md).
- `asyncio` async lock - see [LOCK.md](./docs/LOCK.md).

For **distributed locks** guide (recommended):
- `redis` sync/async locks - see [REDIS_LOCK.md](./docs/REDIS_LOCK.md)
- `valkey` sync/async locks - see [VALKEY_LOCK.md](./docs/VALKEY_LOCK.md)

**IMPORTANT**: For true cross-process transaction serialization, use distributed locks backed by Redis or Valkey.


## Examples

Here we will use **Valkey** server and its locks for cross-process transaction serialization.

Install `tangled-pyrqlite` with the `valkey` extra:

```bash
uv add tangled-pyrqlite[valkey]
```

Start a Valkey server:

```bash
podman rm -f valkey-test
podman run -d --name valkey-test -p 6379:6379 docker.io/valkey/valkey:latest
```

## Sync DB-API 2.0 with ValkeyLock
...

## Async DB-API 2.0 with AioValkeyLock
...

## Sync SQLAlchemy ORM 2.0 with ValkeyLock
...

## Async SQLAlchemy ORM 2.0 with AioValkeyLock
...

For complete working examples, including running instructions and the full example file table, see [EXAMPLES.md](./docs/EXAMPLES.md).


## Miscellaneous

For development setup, testing, linting, project architecture overview, error handling and exception hierarchy, see [MISC.md](./docs/MISC.md).


## References

- [rqlite](https://rqlite.io) — Distributed SQLite database
- [Python DB-API 2.0 (PEP 249)](https://www.python.org/dev/peps/pep-0249/) — Python database API specification
- [SQLAlchemy Documentation](https://docs.sqlalchemy.org/) — Python SQL toolkit and ORM
- [Redis](https://github.com/redis/redis) — Redis in-memory data store (distributed locking)
- [Valkey](https://github.com/valkey-io/valkey) — Valkey in-memory data store (distributed locking, Redis-compatible)


## License

MIT License - see [LICENSE](./LICENSE) file for details.
