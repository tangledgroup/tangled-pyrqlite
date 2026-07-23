"""Synchronous Valkey cluster detection and client factory.

Provides helpers to detect whether a Valkey node is running in cluster mode
and to create the appropriate client (standalone ``valkey.Redis`` or
``valkey.cluster.ValkeyCluster``).

Usage:
    >>> from rqlite.valkey_cluster import is_cluster_mode, create_valkey_client
    >>> if is_cluster_mode("localhost", 6379):
    ...     client = create_valkey_client("localhost", 6379)  # ValkeyCluster
    ... else:
    ...     client = create_valkey_client("localhost", 6379)  # valkey.Redis
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    pass

try:
    import valkey  # noqa: PLC0417
    from valkey.cluster import ValkeyCluster  # noqa: PLC0417
    from valkey.exceptions import ConnectionError, ResponseError  # noqa: PLC0417
except ImportError as exc:
    raise ImportError(
        "valkey package is required for cluster support. "
        "Install it with: uv add tangled-pyrqlite[valkey]"
    ) from exc


def is_cluster_mode(host: str, port: int = 6379, **kwargs: Any) -> bool:
    """Return True if the node is running with *cluster-enabled yes*.

    Probes ``INFO cluster`` on a short-lived standalone client and checks
    the ``cluster_enabled`` field.

    Args:
        host: Valkey hostname.
        port: Valkey port.
        **kwargs: Extra keyword arguments forwarded to ``valkey.Redis()``.

    Returns:
        True if the server reports cluster mode, False otherwise (including
        on connection errors).
    """
    client: valkey.Redis[Any] = valkey.Redis(host=host, port=port, **kwargs)
    try:
        info: dict[str, object] = client.info("cluster")  # type: ignore[assignment]
        return info.get("cluster_enabled") == "1" or info.get("cluster_enabled") == 1
    except (ResponseError, ConnectionError):
        return False
    finally:
        client.close()


def create_valkey_client(
    host: str,
    port: int = 6379,
    *,
    cluster: bool | None = None,
    **kwargs: Any,
) -> valkey.Redis[Any] | ValkeyCluster:
    """Create a Valkey client, auto-detecting cluster mode when *cluster* is None.

    Args:
        host: Valkey hostname (seed node for clusters).
        port: Valkey port (seed node port for clusters).
        cluster: Force cluster mode (True), standalone mode (False), or auto-
                 detect (None, default).
        **kwargs: Extra keyword arguments forwarded to the client constructor
                  (e.g. ``password``, ``decode_responses``, ``socket_timeout``).

    Returns:
        A ``valkey.cluster.ValkeyCluster`` instance when in cluster mode,
        or a ``valkey.Redis`` instance for standalone.
    """
    if cluster is None:
        cluster = is_cluster_mode(host, port, **kwargs)

    if cluster:
        return ValkeyCluster(
            host=host,
            port=port,
            **kwargs,
        )

    return valkey.Redis(
        host=host,
        port=port,
        **kwargs,
    )
