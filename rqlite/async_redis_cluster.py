"""Asynchronous Redis cluster detection and client factory.

Provides async helpers to detect whether a Redis node is running in cluster
mode and to create the appropriate async client.

Usage:
    >>> import asyncio
    >>> from rqlite.async_redis_cluster import is_cluster_mode_async, create_redis_client_async
    >>>
    >>> async def main():
    ...     client = await create_redis_client_async("localhost", 6379)
    ...     print(await client.ping())
    >>> asyncio.run(main())
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    pass

try:
    import redis  # noqa: PLC0417
    import redis.asyncio  # noqa: PLC0417
    from redis.asyncio.cluster import RedisCluster  # noqa: PLC0417
    from redis.exceptions import ConnectionError, ResponseError  # noqa: PLC0417
except ImportError as exc:
    raise ImportError(
        "redis package is required for async cluster support. "
        "Install it with: uv add tangled-pyrqlite[redis]"
    ) from exc


async def is_cluster_mode_async(host: str, port: int = 6379, **kwargs: Any) -> bool:
    """Return True if the node is running with *cluster-enabled yes*.

    Probes ``INFO cluster`` on a short-lived async client.

    Args:
        host: Redis hostname.
        port: Redis port.
        **kwargs: Extra keyword arguments forwarded to ``redis.asyncio.Redis()``.

    Returns:
        True if the server reports cluster mode, False otherwise (including
        on connection errors).
    """
    client = redis.asyncio.Redis(host=host, port=port, **kwargs)
    try:
        info = await client.info("cluster")
        return info.get("cluster_enabled") == "1" or info.get("cluster_enabled") == 1
    except (ResponseError, ConnectionError):
        return False
    finally:
        await client.aclose()


async def create_redis_client_async(
    host: str,
    port: int = 6379,
    *,
    cluster: bool | None = None,
    **kwargs: Any,
) -> redis.asyncio.Redis[Any] | RedisCluster:
    """Create an async Redis client, auto-detecting cluster mode when *cluster* is None.

    Args:
        host: Redis hostname (seed node for clusters).
        port: Redis port (seed node port for clusters).
        cluster: Force cluster mode (True), standalone mode (False), or auto-
                 detect (None, default).
        **kwargs: Extra keyword arguments forwarded to the client constructor
                  (e.g. ``password``, ``decode_responses``, ``socket_timeout``).

    Returns:
        A ``redis.asyncio.cluster.RedisCluster`` instance when in cluster
        mode (initialised), or a ``redis.asyncio.Redis`` for standalone.
    """
    if cluster is None:
        cluster = await is_cluster_mode_async(host, port, **kwargs)

    if cluster:
        client = RedisCluster(
            host=host,
            port=port,
            **kwargs,
        )
        await client.initialize()
        return client

    return redis.asyncio.Redis(
        host=host,
        port=port,
        **kwargs,
    )
