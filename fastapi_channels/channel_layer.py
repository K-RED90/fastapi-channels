"""Channel Layer implementation for FastAPI Channel.

Provides a Django Channels-like singleton pattern for managing WebSocket connections,
enabling external event handling and unified configuration.
"""

from typing import TYPE_CHECKING, Any

from fastapi import WebSocket

from fastapi_channels.backends import MemoryBackend, RedisBackend
from fastapi_channels.config import WSConfig
from fastapi_channels.connections import Connection, ConnectionManager, ConnectionRegistry

if TYPE_CHECKING:
    from fastapi_channels.backends import BaseBackend


_channel_layer_instance: "ChannelLayer | None" = None


class ChannelLayer:
    """Unified channel layer for WebSocket connection management.

    ChannelLayer consolidates config, Backend, Registry, and Manager into a single
    class, providing a Django Channels-like API for managing WebSocket connections
    and enabling external event handling (e.g., SQLAlchemy events, Celery tasks).

    The ChannelLayer follows a singleton pattern, allowing access from anywhere
    in the application via `get_channel_layer()`.

    Key features:
    - Unified configuration management
    - Automatic backend selection (Memory/Redis)
    - Connection lifecycle management
    - Group messaging and broadcasting
    - External event support

    Parameters
    ----------
    config : WSConfig | None, optional
        Application config. If None, creates config from environment variables.
        Default: None

    Examples
    --------
    Initialize in application startup (automatically sets itself as singleton):

    >>> channel_layer = ChannelLayer(config=WSConfig(BACKEND_TYPE="redis"))
    >>> await channel_layer.start()

    Use from external code (e.g., SQLAlchemy event):

    >>> from fastapi_channels import get_channel_layer
    >>> channel_layer = get_channel_layer()
    >>> await channel_layer.send_to_group("room:general", {"type": "update", "data": {...}})

    Notes
    -----
    Only one ChannelLayer instance should exist per application.
    Creating a ChannelLayer instance automatically sets it as the global singleton.
    Use `get_channel_layer()` to access the singleton instance from anywhere.
    Background tasks must be started with `start()` and stopped with `stop()`.

    """

    def __init__(self, config: WSConfig | None = None):
        """Initialize ChannelLayer with config.

        Parameters
        ----------
        config : WSConfig | None, optional
            Application config. If None, creates from environment variables.
            Default: None

        """
        self._config = config or WSConfig()
        self._backend: BaseBackend | None = None
        self._registry: ConnectionRegistry | None = None
        self._manager: ConnectionManager | None = None
        self._initialized = False

        global _channel_layer_instance
        if _channel_layer_instance is None:
            _channel_layer_instance = self

    def _initialize(self) -> None:
        """Initialize backend, registry, and manager components.

        This method is called lazily on first use to ensure all components
        are created with the correct config.

        """
        if self._initialized:
            return

        if self._config.BACKEND_TYPE == "redis":
            self._backend = RedisBackend(
                redis_url=self._config.REDIS_URL,
                registry_expiry=self._config.REDIS_REGISTRY_EXPIRY,
                group_expiry=self._config.REDIS_GROUP_EXPIRY,
            )
        else:
            self._backend = MemoryBackend()

        self._registry = ConnectionRegistry(
            backend=self._backend,
            max_connections=self._config.MAX_TOTAL_CONNECTIONS,
            heartbeat_timeout=self._config.WS_HEARTBEAT_TIMEOUT,
        )

        self._manager = ConnectionManager(
            registry=self._registry,
            max_connections_per_client=self._config.MAX_CONNECTIONS_PER_CLIENT,
            heartbeat_interval=self._config.WS_HEARTBEAT_INTERVAL,
            server_instance_id=self._config.SERVER_INSTANCE_ID,
        )

        self._initialized = True

    @property
    def config(self) -> WSConfig:
        return self._config

    @property
    def backend(self) -> "BaseBackend":
        self._initialize()
        assert self._backend is not None
        return self._backend

    @property
    def registry(self) -> ConnectionRegistry:
        self._initialize()
        assert self._registry is not None
        return self._registry

    @property
    def manager(self) -> ConnectionManager:
        self._initialize()
        assert self._manager is not None
        return self._manager

    async def connect(
        self,
        websocket: WebSocket,
        user_id: str | None = None,
        connection_id: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> Connection:
        """Establish new WebSocket connection.

        Parameters
        ----------
        websocket : WebSocket
            FastAPI WebSocket instance
        user_id : str | None, optional
            User identifier for authenticated connections. Default: None
        connection_id : str | None, optional
            Custom connection identifier. Default: None (auto-generated)
        metadata : dict[str, Any] | None, optional
            Additional connection metadata. Default: None

        Returns
        -------
        Connection
            Connection object representing the established connection

        """
        return await self.manager.connect(
            websocket=websocket,
            user_id=user_id,
            connection_id=connection_id,
            metadata=metadata,
        )

    async def disconnect(self, connection_id: str, code: int = 1000) -> None:
        """Disconnect a WebSocket connection.

        Parameters
        ----------
        connection_id : str
            Connection identifier to disconnect
        code : int, optional
            WebSocket close code. Default: 1000

        """
        await self.manager.disconnect(connection_id, code=code)

    async def send_personal(self, connection_id: str, message: dict[str, Any]) -> None:
        """Send message to specific connection.

        Parameters
        ----------
        connection_id : str
            Target connection identifier
        message : dict[str, Any]
            Message payload to send

        """
        await self.manager.send_personal(connection_id, message)

    async def send_to_group(self, group: str, message: dict[str, Any]) -> None:
        """Send message to all connections in a group.

        Parameters
        ----------
        group : str
            Target group name
        message : dict[str, Any]
            Message payload to send

        Examples
        --------
        Send update to all users in a room:

        >>> await channel_layer.send_to_group("room:general", {
        ...     "type": "room_update",
        ...     "data": {"message": "New user joined"}
        ... })

        """
        await self.manager.send_group(group, message)

    async def send_group_except(
        self, group: str, message: dict[str, Any], exclude_connection_id: str
    ) -> None:
        """Send message to group excluding specific connection.

        Parameters
        ----------
        group : str
            Target group name
        message : dict[str, Any]
            Message payload to send
        exclude_connection_id : str
            Connection ID to exclude from message delivery

        Notes
        -----
        Useful for echo prevention in chat applications.

        """
        await self.manager.send_group_except(group, message, exclude_connection_id)

    async def broadcast(self, message: dict[str, Any]) -> None:
        """Send message to all active connections.

        Parameters
        ----------
        message : dict[str, Any]
            Message payload to broadcast

        Examples
        --------
        Broadcast server announcement:

        >>> await channel_layer.broadcast({
        ...     "type": "announcement",
        ...     "data": {"message": "Server maintenance in 5 minutes"}
        ... })

        """
        await self.manager.broadcast(message)

    async def send_to_user(self, user_id: str, message: dict[str, Any]) -> int:
        """Send message to all connections of a user.

        Parameters
        ----------
        user_id : str
            Target user identifier
        message : dict[str, Any]
            Message payload to send

        Returns
        -------
        int
            Number of connections the message was sent to

        """
        return await self.manager.send_to_user(user_id, message)

    async def join_group(self, connection_id: str, group: str) -> None:
        """Add connection to a messaging group.

        Parameters
        ----------
        connection_id : str
            Connection identifier
        group : str
            Group name to join

        """
        await self.manager.join_group(connection_id, group)

    async def leave_group(self, connection_id: str, group: str) -> None:
        """Remove connection from a messaging group.

        Parameters
        ----------
        connection_id : str
            Connection identifier
        group : str
            Group name to leave

        """
        await self.manager.leave_group(connection_id, group)

    async def start(self) -> None:
        """Start background tasks.

        Should be called during application startup (e.g., in FastAPI lifespan).

        Notes
        -----
        Idempotent - safe to call multiple times.

        """
        await self.manager.start_tasks()

    async def stop(self, timeout: float = 10.0) -> None:
        """Stop background tasks and cleanup.

        Should be called during application shutdown (e.g., in FastAPI lifespan).

        Parameters
        ----------
        timeout : float, optional
            Maximum seconds to wait for graceful shutdown. Default: 10.0

        """
        await self.manager.stop_tasks(timeout=timeout)


def get_channel_layer() -> ChannelLayer:
    """Get the global ChannelLayer singleton instance.

    Returns the singleton ChannelLayer instance, creating it if necessary
    using default config (loaded from environment variables).

    Returns
    -------
    ChannelLayer
        The global ChannelLayer singleton instance

    Raises
    ------
    RuntimeError
        If ChannelLayer has not been initialized and cannot be auto-created
        (should not happen with default config)

    Examples
    --------
    Access from external code:

    >>> from fastapi_channels import get_channel_layer
    >>> channel_layer = get_channel_layer()
    >>> await channel_layer.send_to_group("room:general", {"type": "update"})

    Notes
    -----
    The singleton is created on first access if not already initialized.
    To initialize with custom config, create a ChannelLayer instance first:
    `ChannelLayer(config=...)` - this automatically sets itself as the singleton.

    """
    global _channel_layer_instance

    if _channel_layer_instance is None:
        _channel_layer_instance = ChannelLayer()

    return _channel_layer_instance
