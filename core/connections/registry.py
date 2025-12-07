import asyncio
import uuid
from typing import TYPE_CHECKING, Any

from fastapi import WebSocket

from core.backends.memory import MemoryBackend
from core.connections.state import Connection
from core.exceptions import ConnectionError
from core.typed import ConnectionState

if TYPE_CHECKING:
    from core.typed import BackendProtocol


class ConnectionRegistry:
    def __init__(
        self,
        max_connections: int = 10000,
        heartbeat_interval: int = 30,
        heartbeat_timeout: int = 60,
        backend: "BackendProtocol | None" = None,
    ):
        self.connections: dict[str, Connection] = {}
        self.max_connections = max_connections
        self.heartbeat_interval = heartbeat_interval
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
        async with self._lock:
            # Use backend count when available so limits consider all servers.
            total_connections = await self.count()
            if total_connections >= self.max_connections:
                raise ConnectionError("Maximum connections reached")

            conn_id = connection_id or str(uuid.uuid4())

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

            self.connections[conn_id] = connection

        # Persist to backend
        await self.backend.registry_add_connection(
            connection_id=conn_id,
            user_id=connection.user_id,
            metadata=connection.metadata,
            groups=connection.groups,
            heartbeat_timeout=connection.heartbeat_timeout,
        )

        return connection

    async def unregister(self, connection_id: str) -> None:
        async with self._lock:
            connection = self.connections.pop(connection_id, None)

        # Remove from backend
        if connection is not None:
            await self.backend.registry_remove_connection(
                connection_id=connection_id, user_id=connection.user_id
            )

    def get(self, connection_id: str) -> Connection | None:
        # WebSocket references only exist locally; only return local connections.
        return self.connections.get(connection_id)

    def get_all(self) -> list[Connection]:
        return list(self.connections.values())

    async def count(self) -> int:
        return await self.backend.registry_count_connections()

    async def add_to_group(self, connection_id: str, group: str) -> None:
        if connection := self.get(connection_id):
            connection.groups.add(group)
            # Persist groups to backend for cross-server visibility and cleanup.
            await self.backend.registry_update_groups(
                connection_id=connection_id, groups=connection.groups
            )

    async def remove_from_group(self, connection_id: str, group: str) -> None:
        if connection := self.get(connection_id):
            connection.groups.discard(group)
            # Persist groups to backend for cross-server visibility and cleanup.
            await self.backend.registry_update_groups(
                connection_id=connection_id, groups=connection.groups
            )

    def get_by_group(self, group: str) -> list[Connection]:
        return [conn for conn in self.connections.values() if group in conn.groups]

    async def user_channels(self, user_id: str) -> set[str]:
        """Return all channel names for a user across servers."""
        if not user_id:
            return set()

        return await self.backend.registry_get_user_connections(user_id)

    async def user_channel_count(self, user_id: str) -> int:
        if not user_id:
            return 0

        channels = await self.user_channels(user_id)
        return len(channels)
