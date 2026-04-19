# Plan: Valkey Lock Examples for README.md

## Goal

Add four self-contained example subsections under `## Examples` in `README.md` demonstrating the full CRUD lifecycle (create table if not exists → insert → select → update → select → delete → select all) using each of the four lock patterns with Valkey.

## Design Decisions

### Common Scenario
All four examples use the **same domain**: a `products` table with `id`, `name`, `price`, `quantity` fields.

### CRUD Operations per Example
1. CREATE TABLE IF NOT EXISTS — ensure table exists (idempotent)
2. INSERT — add product "Widget A", price 9.99, qty 100
3. SELECT — fetch by name (single row)
4. UPDATE — adjust quantity (100→85) and price (9.99→12.99)
5. SELECT — verify the update
6. DELETE — remove the product
7. SELECT ALL — confirm empty table

### Lock Pattern Differences

| Aspect | Sync DB-API + ValkeyLock | Async DB-API + AioValkeyLock | Sync SQLAlchemy ORM + ValkeyLock | Async SQLAlchemy ORM + AioValkeyLock |
|---|---|---|---|---|
| Lock class | `ValkeyLock` | `AioValkeyLock` | `ValkeyLock` | `AioValkeyLock` |
| Connection | `rqlite.connect()` | `async_connect()` | `create_engine()` + `Session()` | `create_async_engine()` + `AsyncSession()` |
| Lock in connect | `lock=lock` arg | `lock=lock` arg | `connect_args={"lock": lock}` | `connect_args={"lock": lock}` |
| Execute | `cursor.execute(sql, params)` | `await cursor.execute()` | ORM model operations | Async ORM + `select()` |
| Commit | `conn.commit()` | `await conn.commit()` | `session.commit()` | `await session.commit()` |
| Lock context | `with lock:` around read-modify-write | `async with lock:` | N/A (lock wraps entire engine) | N/A (lock wraps entire engine) |

## Key Technical Details

### Sync DB-API 2.0 + ValkeyLock
- `rqlite.connect(host, port, lock=lock)`
- `cursor.execute()`, `conn.commit()`
- `with lock:` around the read-modify-write UPDATE step

### Async DB-API 2.0 + AioValkeyLock
- `rqlite.async_connect(host, port, lock=lock)`
- `await cursor.execute()`, `await conn.commit()`
- `async with lock:` around the read-modify-write UPDATE step
- Wrapped in `async def main():` + `asyncio.run(main())`

### Sync SQLAlchemy ORM + ValkeyLock
- `create_engine("rqlite://...", connect_args={"lock": lock})`
- Raw `text()` SQL for CREATE TABLE (model not yet in DB)
- ORM `session.add()`, `session.execute(select(...))`, `session.delete()`

### Async SQLAlchemy ORM + AioValkeyLock
- `create_async_engine("rqlite+aiorqlite://...", connect_args={"lock": lock})`
- `async_sessionmaker` for session factory
- `await session.execute()`, `AsyncAttrs` mixin on base class

## Validation Criteria
1. Syntactically correct Python (passes `ast.parse`)
2. Follows existing code style in examples/ directory
3. Uses consistent table name (`readme_products`) to avoid collisions
4. Self-contained — no external dependencies beyond imports
5. Shows all 7 CRUD steps clearly
6. Properly closes/cleans up resources
