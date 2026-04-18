# Plan: Separate Sync (ThreadLock) and Async (AioLock) Lock Tests & Examples

## Goal Achieved

All test and example files renamed to strict naming convention. Every filename clearly indicates:
1. **Sync vs async** (`sync_` / `async_`)
2. **Lock type** (`thread_lock`, `aio_lock`, `redis_lock`, `valkey_lock`)
3. **Scenario** (`basic_usage`, `sqlalchemy_orm`, `distributed_transfer`, etc.)

## Changes Made

### Tests (13 files)

#### Renamed from old names:

| Old Name | New Name | Content |
|----------|----------|---------|
| `test_lock.py` | **deleted** → merged into `test_sync_thread_lock.py` | ThreadLock unit + integration |
| `test_connection.py` | `tests/test_sync_thread_lock_dbapi.py` | Sync DB-API connection tests |
| `test_cursor.py` | `tests/test_sync_thread_lock_dbapi_cursor.py` | Sync DB-API cursor tests |
| `test_read_consistency.py` | `tests/test_sync_thread_lock_dbapi_read_consistency.py` | Sync DB-API read consistency |
| `test_sqlalchemy.py` | `tests/test_sync_thread_lock_sqlalchemy_dialect.py` | Sync SQLAlchemy Core + ORM |
| `test_redis_lock.py` | `tests/test_sync_redis_lock.py` | Sync RedisLock tests |
| `test_valkey_lock.py` | `tests/test_sync_valkey_lock.py` | Sync ValkeyLock tests |

#### New files (async DB-API + async SQLAlchemy):

| New Name | Content |
|----------|---------|
| `tests/test_async_aio_lock_dbapi.py` | Async AioLock connection, CRUD, read consistency |
| `tests/test_async_aio_lock_dbapi_cursor.py` | Async AioLock cursor operations (fetch, execute, etc.) |
| `tests/test_async_aio_lock_sqlalchemy.py` | Async SQLAlchemy Core + ORM with AioLock |

#### Already correct (renamed inner classes only):

| File | Inner class rename |
|------|-------------------|
| `test_async_redis_lock.py` | `TestAioRedisLock*` → `TestAsyncRedisLock*` |
| `test_async_valkey_lock.py` | `TestAioValkeyLock*` → `TestAsyncValkeyLock*` |

### Examples (9 files)

| Old Name | New Name |
|----------|----------|
| `basic_usage.py` | `sync_thread_lock_basic_usage.py` |
| `async_basic_usage.py` | `async_aio_lock_basic_usage.py` |
| `sqlalchemy_orm.py` | `sync_thread_lock_sqlalchemy_orm.py` |
| `async_sqlalchemy_orm.py` | `async_aio_lock_sqlalchemy_orm.py` |
| `redis_lock_sync.py` | `sync_redis_lock_basic_usage.py` |
| `redis_lock_async.py` | `async_redis_lock_basic_usage.py` |
| `valkey_lock_sync.py` | `sync_valkey_lock_basic_usage.py` |
| `valkey_lock_async.py` | `async_valkey_lock_basic_usage.py` |
| `distributed_transfer.py` | `sync_redis_lock_distributed_transfer.py` |

### Naming Convention (strict)

Every filename matches one of:
- `test_sync_thread_lock_<scenario>.py`
- `test_async_aio_lock_<scenario>.py`
- `test_sync_redis_lock_<scenario>.py`
- `test_async_redis_lock_<scenario>.py`
- `test_sync_valkey_lock_<scenario>.py`
- `test_async_valkey_lock_<scenario>.py`

Every test class name starts with:
- `TestSyncThreadLock*` — ThreadLock tests
- `TestAsyncAioLock*` — AioLock tests
- `TestSyncRedisLock*` — RedisLock tests
- `TestAsyncRedisLock*` — AioRedisLock tests
- `TestSyncValkeyLock*` — ValkeyLock tests
- `TestAsyncValkeyLock*` — AioValkeyLock tests

Every test method name starts with the same prefix:
- `test_sync_thread_lock_*`
- `test_async_aio_lock_*`
- `test_sync_redis_lock_*`
- `test_async_redis_lock_*`
- `test_sync_valkey_lock_*`
- `test_async_valkey_lock_*`

### Verification

- ✅ All 13 test files pass Python syntax check (`py_compile`)
- ✅ All 9 example files pass Python syntax check (`py_compile`)
- ✅ No old filenames remain (`test_lock.py`, `basic_usage.py`, etc. deleted/renamed)
- ✅ No mixed sync/async in any file
- ✅ All class names include lock type prefix
- ✅ All method names include lock type prefix
- ✅ Docstrings, argparse descriptions, print titles updated
