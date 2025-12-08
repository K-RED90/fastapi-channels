import asyncio
import uuid
from abc import ABC, abstractmethod
from typing import Any


class BaseBackend(ABC):
    """Abstract base class defining the protocol for WebSocket channel layer backends.

    This interface provides the core functionality needed for WebSocket communication
    including direct channel messaging, group messaging, and connection registry management.
    Implementations can be in-memory (single-server) or distributed (Redis-based).

    The backend handles:
    - Direct channel-to-channel messaging
    - Group-based messaging (one-to-many)
    - Connection registry for tracking active connections
    - Message persistence and delivery guarantees

    Notes
    -----
    All methods are async to support both local and remote backend implementations.
    Connection registry methods enable cross-server visibility in distributed deployments.

    """

    @abstractmethod
    async def publish(self, channel: str, message: dict[str, Any]) -> None:
        """Publish a message to a specific channel.

        Parameters
        ----------
        channel : str
            Target channel name for message delivery
        message : dict[str, Any]
            Message payload to deliver

        Notes
        -----
        This method should be non-blocking and return immediately.
        Message delivery is handled asynchronously by the backend.

        """

    @abstractmethod
    async def subscribe(self, channel: str) -> None:
        """Subscribe to receive messages from a channel.

        Parameters
        ----------
        channel : str
            Channel name to subscribe to

        Notes
        -----
        After subscribing, messages sent to this channel will be
        deliverable via the receive() method.

        """

    @abstractmethod
    async def unsubscribe(self, channel: str) -> None:
        """Unsubscribe from a channel.

        Parameters
        ----------
        channel : str
            Channel name to unsubscribe from

        Notes
        -----
        After unsubscribing, messages to this channel will no longer
        be delivered to this backend instance.

        """

    @abstractmethod
    async def group_add(self, group: str, channel: str) -> None:
        """Add a channel to a messaging group.

        Parameters
        ----------
        group : str
            Group name to add channel to
        channel : str
            Channel name to add to group

        Notes
        -----
        Channels in a group receive messages sent to the group.
        A channel can belong to multiple groups simultaneously.

        """

    @abstractmethod
    async def group_discard(self, group: str, channel: str) -> None:
        """Remove a channel from a messaging group.

        Parameters
        ----------
        group : str
            Group name to remove channel from
        channel : str
            Channel name to remove from group

        Notes
        -----
        Channel will no longer receive group messages after removal.

        """

    @abstractmethod
    async def group_send(self, group: str, message: dict[str, Any]) -> None:
        """Send a message to all channels in a group.

        Parameters
        ----------
        group : str
            Target group name
        message : dict[str, Any]
            Message payload to deliver

        Notes
        -----
        This is equivalent to publishing to each channel in the group.
        Failed deliveries should be logged but not raise exceptions.

        """

    @abstractmethod
    async def cleanup(self) -> None:
        """Clean up backend resources and connections.

        This method should:
        - Close any open connections (Redis, etc.)
        - Cancel background tasks
        - Release system resources

        Notes
        -----
        Called during application shutdown to ensure clean resource cleanup.

        """

    @abstractmethod
    async def receive(self, channel: str, timeout: float | None = None) -> dict[str, Any] | None:
        """Receive the next message from a channel.

        Parameters
        ----------
        channel : str
            Channel name to receive from
        timeout : float | None, optional
            Maximum time to wait for a message in seconds. Default: None (wait indefinitely)

        Returns
        -------
        dict[str, Any] | None
            Next message from channel, or None if timeout exceeded

        Notes
        -----
        This is typically a blocking operation that waits for messages.
        Should return None on timeout rather than raising an exception.

        """

    @abstractmethod
    async def group_channels(self, group: str) -> set[str]:
        """Get all channels currently in a group.

        Parameters
        ----------
        group : str
            Group name to query

        Returns
        -------
        set[str]
            Set of channel names in the group

        Notes
        -----
        Returns a snapshot of group membership at the time of the call.

        """

    @abstractmethod
    async def flush(self) -> None:
        """Clear all backend state and data.

        This method should:
        - Remove all channels and groups
        - Clear all pending messages
        - Reset registry state

        Notes
        -----
        Used primarily for testing and development.
        Should not be used in production environments.

        """

    async def new_channel(self, prefix: str = "channel") -> str:
        """Generate a unique channel name.

        Parameters
        ----------
        prefix : str, optional
            Prefix for the channel name. Default: "channel"

        Returns
        -------
        str
            Unique channel name with timestamp and random suffix

        Examples
        --------
        >>> await backend.new_channel()
        'channel.1640995200000.a1b2c3d4'
        >>> await backend.new_channel("ws.user")
        'ws.user.1640995200000.e5f6g7h8'

        """
        return f"{prefix}.{int(asyncio.get_event_loop().time() * 1000)}.{uuid.uuid4().hex[:8]}"

    @abstractmethod
    async def registry_add_connection(
        self,
        connection_id: str,
        user_id: str | None,
        metadata: dict[str, Any],
        groups: set[str],
        heartbeat_timeout: float,
    ) -> None:
        """Add a WebSocket connection to the registry.

        Parameters
        ----------
        connection_id : str
            Unique identifier for the connection
        user_id : str | None
            User identifier if authenticated, None for anonymous
        metadata : dict[str, Any]
            Additional connection metadata (IP, user agent, etc.)
        groups : set[str]
            Initial groups the connection belongs to
        heartbeat_timeout : float
            Heartbeat timeout in seconds

        Notes
        -----
        This enables cross-server visibility of connections in distributed deployments.
        The registry tracks active connections for group messaging and user targeting.

        """

    @abstractmethod
    async def registry_remove_connection(self, connection_id: str, user_id: str | None) -> None:
        """Remove a connection from the registry.

        Parameters
        ----------
        connection_id : str
            Connection identifier to remove
        user_id : str | None
            User identifier for cleanup of user-connection mappings

        Notes
        -----
        Called when connections disconnect or timeout.
        Ensures registry stays consistent with active connections.

        """

    @abstractmethod
    async def registry_update_groups(self, connection_id: str, groups: set[str]) -> None:
        """Update the groups a connection belongs to.

        Parameters
        ----------
        connection_id : str
            Connection identifier to update
        groups : set[str]
            New set of groups for the connection

        Notes
        -----
        Used when connections join/leave groups dynamically.
        Maintains consistency between local state and registry.

        """

    @abstractmethod
    async def registry_get_connection_groups(self, connection_id: str) -> set[str]:
        """Get all groups a connection belongs to.

        Parameters
        ----------
        connection_id : str
            Connection identifier to query

        Returns
        -------
        set[str]
            Set of group names the connection belongs to

        Notes
        -----
        Used for cleanup when connections disconnect.
        Returns empty set if connection not found.

        """

    @abstractmethod
    async def registry_count_connections(self) -> int:
        """Get total number of active connections across all servers.

        Returns
        -------
        int
            Total connection count

        Notes
        -----
        Includes connections from all servers in distributed deployments.
        Used for monitoring and connection limits.

        """

    @abstractmethod
    async def registry_get_user_connections(self, user_id: str) -> set[str]:
        """Get all connection IDs for a specific user.

        Parameters
        ----------
        user_id : str
            User identifier to query

        Returns
        -------
        set[str]
            Set of connection IDs for the user

        Notes
        -----
        Enables sending messages to all connections of a user.
        Returns empty set if user has no connections.

        """

    async def registry_add_connection_if_under_limit(
        self,
        connection_id: str,
        user_id: str | None,
        metadata: dict[str, Any],
        groups: set[str],
        heartbeat_timeout: float,
        max_connections: int,
    ) -> bool:
        """Atomically check connection limit and add connection if under limit.

        Parameters
        ----------
        connection_id : str
            Unique identifier for the connection
        user_id : str | None
            User identifier if authenticated, None for anonymous
        metadata : dict[str, Any]
            Additional connection metadata (IP, user agent, etc.)
        groups : set[str]
            Initial groups the connection belongs to
        heartbeat_timeout : float
            Heartbeat timeout in seconds
        max_connections : int
            Maximum total connections allowed

        Returns
        -------
        bool
            True if connection was added, False if limit would be exceeded

        Notes
        -----
        This method atomically checks the connection count and adds the connection
        only if under the limit. Prevents race conditions in distributed deployments.
        Default implementation uses non-atomic check-then-act (for backwards compatibility).
        Subclasses should override with atomic implementation.

        """
        # Default non-atomic implementation for backwards compatibility
        count = await self.registry_count_connections()
        if count >= max_connections:
            return False
        await self.registry_add_connection(
            connection_id, user_id, metadata, groups, heartbeat_timeout
        )
        return True

    @abstractmethod
    def registry_get_prefix(self) -> str:
        """Get the registry key prefix used by this backend.

        Returns
        -------
        str
            Registry key prefix (e.g., "memory:registry:" or "ws:registry:")

        Notes
        -----
        Used for debugging and monitoring registry operations.

        """

    def supports_broadcast_channel(self) -> bool:
        """Check if backend supports global broadcast channel.

        Returns
        -------
        bool
            True if backend supports "__broadcast__" channel, False otherwise

        Notes
        -----
        Broadcast channel allows sending messages to all connections
        without explicitly managing groups. Only Redis backend supports this.

        """
        return False
