"""Asynchronous Valkey cluster detection and client factory.

Provides async helpers to detect whether a Valkey node is running in cluster
mode and to create the appropriate async client.

Usage:
    >>> import asyncio
    >>> from rqlite.async_valkey_cluster import is_cluster_mode_async, create_valkey_client_async
    >>>
    >>> async def main():
    ...     if await is_cluster_mode_async("localhost", 6379):
    ...         client = await create_valkey_client_async("localhost", 6379)
    ...     print(await client.ping())
    >>> asyncio.run(main())
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    pass

try:
    import valkey  # noqa: PLC0417
    import valkey.asyncio  # noqa: PLC0417
    from valkey.asyncio.cluster import ValkeyCluster  # noqa: PLC0417
    from valkey.exceptions import ConnectionError, ResponseError  # noqa: PLC0417
except ImportError as exc:
    raise ImportError(
        "valkey package is required for async cluster support. "
        "Install it with: uv add tangled-pyrqlite[valkey]"
    ) from exc


async def is_cluster_mode_async(host: str, port: int = 6379, **kwargs: Any) -> bool:
    """Return True if the node is running with *cluster-enabled yes*.

    Probes ``INFO cluster`` on a short-lived async client.

    Args:
        host: Valkey hostname.
        port: Valkey port.
        **kwargs: Extra keyword arguments forwarded to ``valkey.asyncio.Valkey()``.

    Returns:
        True if the server reports cluster mode, False otherwise (including
        on connection errors).
    """
    client = valkey.asyncio.Valkey(host=host, port=port, **kwargs)
    try:
        info = await client.info("cluster")
        return info.get("cluster_enabled") == "1" or info.get("cluster_enabled") == 1
    except (ResponseError, ConnectionError):
        return False
    finally:
        await client.aclose()


async def create_valkey_client_async(
    host: str,
    port: int = 6379,
    *,
    cluster: bool | None = None,
    **kwargs: Any,
) -> valkey.asyncio.Valkey[Any] | ValkeyCluster:
    """Create an async Valkey client, auto-detecting cluster mode when *cluster* is None.

    Args:
        host: Valkey hostname (seed node for clusters).
        port: Valkey port (seed node port for clusters).
        cluster: Force cluster mode (True), standalone mode (False), or auto-
                 detect (None, default).
        **kwargs: Extra keyword arguments forwarded to the client constructor
                  (e.g. ``password``, ``decode_responses``, ``socket_timeout``).

    Returns:
        A ``valkey.asyncio.cluster.ValkeyCluster`` instance when in cluster
        mode (initialised), or a ``valkey.asyncio.Valkey`` for standalone.
    """
    if cluster is None:
        cluster = await is_cluster_mode_async(host, port, **kwargs)

    if cluster:
        client = ValkeyCluster(
            host=host,
            port=port,
            **kwargs,
        )
        await client.initialize()
        return client

    return valkey.asyncio.Valkey(
        host=host,
        port=port,
        **kwargs,
    )
