"""Synchronous Redis cluster detection and client factory.

Provides helpers to detect whether a Redis node is running in cluster mode
and to create the appropriate client (standalone ``redis.Redis`` or
``redis.cluster.RedisCluster``).

Usage:
    >>> from rqlite.redis_cluster import is_cluster_mode, create_redis_client
    >>> client = create_redis_client("localhost", 6379)
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    pass

try:
    import redis  # noqa: PLC0417
    from redis.cluster import RedisCluster  # noqa: PLC0417
    from redis.exceptions import ConnectionError, ResponseError  # noqa: PLC0417
except ImportError as exc:
    raise ImportError(
        "redis package is required for cluster support. "
        "Install it with: uv add tangled-pyrqlite[redis]"
    ) from exc


def is_cluster_mode(host: str, port: int = 6379, **kwargs: Any) -> bool:
    """Return True if the node is running with *cluster-enabled yes*.

    Probes ``INFO cluster`` on a short-lived standalone client and checks
    the ``cluster_enabled`` field.

    Args:
        host: Redis hostname.
        port: Redis port.
        **kwargs: Extra keyword arguments forwarded to ``redis.Redis()``.

    Returns:
        True if the server reports cluster mode, False otherwise (including
        on connection errors).
    """
    client: redis.Redis[Any] = redis.Redis(host=host, port=port, **kwargs)
    try:
        info: dict[str, object] = client.info("cluster")  # type: ignore[assignment]
        return info.get("cluster_enabled") == "1" or info.get("cluster_enabled") == 1
    except (ResponseError, ConnectionError):
        return False
    finally:
        client.close()


def create_redis_client(
    host: str,
    port: int = 6379,
    *,
    cluster: bool | None = None,
    **kwargs: Any,
) -> redis.Redis[Any] | RedisCluster:
    """Create a Redis client, auto-detecting cluster mode when *cluster* is None.

    Args:
        host: Redis hostname (seed node for clusters).
        port: Redis port (seed node port for clusters).
        cluster: Force cluster mode (True), standalone mode (False), or auto-
                 detect (None, default).
        **kwargs: Extra keyword arguments forwarded to the client constructor
                  (e.g. ``password``, ``decode_responses``, ``socket_timeout``).

    Returns:
        A ``redis.cluster.RedisCluster`` instance when in cluster mode,
        or a ``redis.Redis`` instance for standalone.
    """
    if cluster is None:
        cluster = is_cluster_mode(host, port, **kwargs)

    if cluster:
        return RedisCluster(
            host=host,
            port=port,
            **kwargs,
        )

    return redis.Redis(
        host=host,
        port=port,
        **kwargs,
    )
