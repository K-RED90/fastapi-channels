import asyncio
import uuid
from collections.abc import AsyncIterator
from typing import TYPE_CHECKING, Any

from fastapi import WebSocket

from core.backends.memory import MemoryBackend
from core.connections.state import Connection
from core.exceptions import ConnectionError
from core.typed import ConnectionState

if TYPE_CHECKING:
    from core.backends.base import BaseBackend


class ConnectionRegistry:
    """Registry for tracking active WebSocket connections.

    ConnectionRegistry maintains an in-memory registry of active WebSocket connections
    and coordinates with the backend for distributed state management. It provides
    fast local lookups while ensuring consistency with distributed backends.

    Key features:
    - Local connection state tracking
    - User-to-connection mapping
    - Group membership management
    - Connection limits enforcement
    - Backend synchronization

    The registry serves as the single source of truth for connection state
    within each server instance, while the backend handles cross-server visibility.

    Parameters
    ----------
    max_connections : int, optional
        Maximum total connections allowed. Default: 10000
    heartbeat_timeout : int, optional
        Heartbeat timeout in seconds. Default: 60
    backend : BaseBackend, optional
        Backend for distributed state. Default: MemoryBackend()

    Examples
    --------
    Basic registry setup:

    >>> registry = ConnectionRegistry(
    ...     max_connections=1000,
    ...     heartbeat_timeout=30,
    ...     backend=RedisBackend()
    ... )

    Registering connections:

    >>> connection = await registry.register(
    ...     websocket=ws,
    ...     user_id="alice",
    ...     metadata={"ip": "192.168.1.1"}
    ... )
    >>> print(connection.channel_name)  # ws.alice.1640995200000.a1b2c3d4

    Notes
    -----
    Connection limits are enforced atomically using backend.registry_add_connection_if_under_limit(),
    which prevents race conditions in distributed deployments. The Redis backend uses a Lua script
    for true atomicity across all server instances, while the Memory backend uses asyncio locks
    for thread-safety within a single process.

    """

    def __init__(
        self,
        max_connections: int = 10000,
        heartbeat_timeout: int = 60,
        backend: "BaseBackend | None" = None,
    ):
        self.connections: dict[str, Connection] = {}
        self.max_connections = max_connections
        self.heartbeat_timeout = heartbeat_timeout
        self._lock = asyncio.Lock()
        self.backend = backend or MemoryBackend()

    async def register(
        self,
        websocket: WebSocket,
        connection_id: str | None = None,
        user_id: str | None = None,
        metadata: dict[str, Any] | None = None,
        heartbeat_timeout: int | None = None,
    ) -> Connection:
        """Register new WebSocket connection in registry.

        Parameters
        ----------
        websocket : WebSocket
            FastAPI WebSocket instance
        connection_id : str | None, optional
            Custom connection identifier. Default: None (auto-generated)
        user_id : str | None, optional
            User identifier for authenticated connections. Default: None
        metadata : dict[str, Any] | None, optional
            Connection metadata. Default: None
        heartbeat_timeout : int | None, optional
            Custom heartbeat timeout. Default: None (use registry default)

        Returns
        -------
        Connection
            New Connection object

        Raises
        ------
        ConnectionError
            If connection limit exceeded or ID already exists

        Notes
        -----
        Thread-safe operation using asyncio lock.
        Registers connection in both local registry and backend.
        Generates UUID if no connection_id provided.

        """
        conn_id = connection_id or str(uuid.uuid4())

        async with self._lock:
            if conn_id in self.connections:
                raise ConnectionError(f"Connection {conn_id} already exists")

            connection = Connection(
                websocket=websocket,
                channel_name=conn_id,
                user_id=user_id or "",
                metadata=metadata or {},
                heartbeat_timeout=heartbeat_timeout or self.heartbeat_timeout,
            )
            connection.state = ConnectionState.CONNECTED

        # Atomically check limit and register in backend
        # This prevents race conditions in distributed deployments
        added = await self.backend.registry_add_connection_if_under_limit(
            connection_id=conn_id,
            user_id=connection.user_id,
            metadata=connection.metadata,
            groups=connection.groups,
            heartbeat_timeout=connection.heartbeat_timeout,
            max_connections=self.max_connections,
        )

        if not added:
            raise ConnectionError("Maximum connections reached")

        async with self._lock:
            self.connections[conn_id] = connection

        return connection

    async def unregister(self, connection_id: str) -> None:
        """Remove connection from registry.

        Parameters
        ----------
        connection_id : str
            Connection identifier to remove

        Notes
        -----
        Thread-safe operation.
        Removes from both local registry and backend.
        No-op if connection doesn't exist.

        """
        async with self._lock:
            connection = self.connections.pop(connection_id, None)

        if connection is not None:
            await self.backend.registry_remove_connection(
                connection_id=connection_id, user_id=connection.user_id
            )

    def get(self, connection_id: str) -> Connection | None:
        """Get connection by ID from local registry.

        Parameters
        ----------
        connection_id : str
            Connection identifier to lookup

        Returns
        -------
        Connection | None
            Connection object if found, None otherwise

        Notes
        -----
        Returns only locally registered connections.
        Use backend methods for cross-server lookups.

        """
        return self.connections.get(connection_id)

    def get_all(self) -> list[Connection]:
        """Get all locally registered connections.

        Returns
        -------
        list[Connection]
            List of all active Connection objects in local registry

        Notes
        -----
        Returns only connections registered on this server.
        Use backend.registry_count_connections() for global count.

        Consider using iter_connections() for large registries to avoid
        loading all connections into memory at once.

        """
        return list(self.connections.values())

    async def iter_connections(self, batch_size: int = 100) -> AsyncIterator[tuple[Connection, ...]]:
        """Stream connections in batches to avoid loading all into memory.

        Parameters
        ----------
        batch_size : int, optional
            Number of connections per batch. Default: 100

        Yields
        ------
        tuple[Connection, ...]
            Batch of Connection objects

        Notes
        -----
        Uses iterator-based approach to yield connections in batches,
        allowing memory-efficient processing of large registries.
        Each batch is processed before the next is yielded.

        Examples
        --------
        Processing connections in batches:

        >>> async for batch in registry.iter_connections(batch_size=50):
        ...     tasks = [process_conn(conn) for conn in batch]
        ...     await asyncio.gather(*tasks)

        """
        from core.utils import batch_items

        for batch in batch_items(self.connections.values(), batch_size):
            yield batch

    async def count(self) -> int:
        """Get total connection count across all servers.

        Returns
        -------
        int
            Total active connections in distributed deployment

        Notes
        -----
        Delegates to backend for accurate distributed count.
        Includes connections from all server instances.

        """
        return await self.backend.registry_count_connections()

    async def add_to_group(self, connection_id: str, group: str) -> None:
        """Add connection to group in local registry and backend.

        Parameters
        ----------
        connection_id : str
            Connection identifier to add to group
        group : str
            Group name to join

        Notes
        -----
        Updates both local connection state and backend registry.
        Ensures cross-server visibility of group membership.

        """
        if connection := self.get(connection_id):
            connection.groups.add(group)
            # Persist groups to backend for cross-server visibility and cleanup.
            await self.backend.registry_update_groups(
                connection_id=connection_id, groups=connection.groups
            )

    async def remove_from_group(self, connection_id: str, group: str) -> None:
        """Remove connection from group in local registry and backend.

        Parameters
        ----------
        connection_id : str
            Connection identifier to remove from group
        group : str
            Group name to leave

        Notes
        -----
        Updates both local connection state and backend registry.
        Ensures consistent group membership across servers.

        """
        if connection := self.get(connection_id):
            connection.groups.discard(group)
            # Persist groups to backend for cross-server visibility and cleanup.
            await self.backend.registry_update_groups(
                connection_id=connection_id, groups=connection.groups
            )

    async def user_channels(self, user_id: str) -> set[str]:
        """Get all connection channel names for a user across all servers.

        Parameters
        ----------
        user_id : str
            User identifier to query

        Returns
        -------
        set[str]
            Set of channel names for the user

        Notes
        -----
        Includes connections from all server instances.
        Returns empty set for anonymous users (empty user_id).

        """
        if not user_id:
            return set()

        return await self.backend.registry_get_user_connections(user_id)

    async def user_channel_count(self, user_id: str) -> int:
        """Get number of active connections for a user.

        Parameters
        ----------
        user_id : str
            User identifier to query

        Returns
        -------
        int
            Number of active connections for the user

        Notes
        -----
        Counts connections across all server instances.
        Returns 0 for anonymous users.

        """
        if not user_id:
            return 0

        channels = await self.user_channels(user_id)
        return len(channels)
