import asyncio
from collections import defaultdict
from typing import Any

from core.utils import run_with_concurrency_limit

from .base import BaseBackend


class MemoryBackend(BaseBackend):
    """In-memory channel layer backend for single-server WebSocket deployments.

    This backend stores all channel, group, and registry data in memory,
    making it suitable for single-server applications where persistence
    across restarts is not required. It provides fast, synchronous operations
    but data is lost when the process terminates.

    The backend maintains:
    - Channel subscriptions and message queues
    - Group membership mappings
    - Connection registry with user mappings
    - Thread-safe operations using asyncio locks

    Parameters
    ----------
    None - uses default in-memory storage

    Examples
    --------
    Basic usage for single-server chat application:

    >>> backend = MemoryBackend()
    >>> await backend.subscribe("room.general")
    >>> await backend.publish("room.general", {"type": "message", "text": "hello"})
    >>> message = await backend.receive("room.general")
    >>> print(message["text"])
    hello

    Notes
    -----
    Not suitable for multi-server deployments due to lack of cross-server communication.
    All data is lost on process restart - use Redis backend for persistence.

    """

    def __init__(self):
        self.groups: dict[str, set[str]] = {}
        self.subscriptions: dict[str, set[str]] = {}
        self._lock = asyncio.Lock()
        self.channels: dict[str, asyncio.Queue] = {}
        self.listeners: dict[str, set[asyncio.Queue]] = defaultdict(set)
        self._registry_connections: set[str] = set()
        self._registry_connection_data: dict[str, dict[str, Any]] = {}
        self._registry_user_connections: dict[str, set[str]] = defaultdict(set)

    async def publish(self, channel: str, message: dict[str, Any]) -> None:
        """Publish message to all subscribers of a channel.

        Parameters
        ----------
        channel : str
            Target channel name
        message : dict[str, Any]
            Message payload to deliver

        Notes
        -----
        Message is delivered asynchronously to all channel subscribers.
        If channel has no subscribers, message is silently dropped.

        """
        if channel in self.listeners:
            for queue in self.listeners[channel].copy():
                await queue.put(message)

    async def subscribe(self, channel: str) -> None:
        """Subscribe to receive messages from a channel.

        Parameters
        ----------
        channel : str
            Channel name to subscribe to

        Notes
        -----
        Creates a message queue for the channel if it doesn't exist.
        Multiple subscriptions to the same channel share the same queue.

        """
        if channel not in self.channels:
            self.channels[channel] = asyncio.Queue()

        queue = self.channels[channel]
        self.listeners[channel].add(queue)

    async def unsubscribe(self, channel: str) -> None:
        """Unsubscribe from a channel.

        Parameters
        ----------
        channel : str
            Channel name to unsubscribe from

        Notes
        -----
        Removes the channel's message queue and all listeners.
        Channel becomes unavailable for messaging after unsubscribe.

        """
        if channel in self.channels:
            queue = self.channels[channel]
            self.listeners[channel].discard(queue)
            del self.channels[channel]

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
        Thread-safe operation using asyncio lock.
        Creates group if it doesn't exist.

        """
        async with self._lock:
            if group not in self.groups:
                self.groups[group] = set()
            self.groups[group].add(channel)

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
        Thread-safe operation. Removes empty groups automatically.

        """
        async with self._lock:
            if group in self.groups:
                self.groups[group].discard(channel)
                if not self.groups[group]:
                    del self.groups[group]

    def _get_group_channels(self, group: str) -> set[str]:
        return self.groups.get(group, set()).copy()

    async def group_channels(self, group: str) -> set[str]:
        """Return thread-safe copy of channels in a group.

        Parameters
        ----------
        group : str
            Group name to query

        Returns
        -------
        set[str]
            Set of channel names in the group

        """
        async with self._lock:
            return self._get_group_channels(group)

    async def group_send(self, group: str, message: dict[str, Any]) -> None:
        """Send message to all channels in a group.

        Parameters
        ----------
        group : str
            Target group name
        message : dict[str, Any]
            Message payload to deliver

        Notes
        -----
        Publishes message to each channel in the group concurrently.
        Logs warnings for any failed deliveries but doesn't raise exceptions.

        """
        channels = self._get_group_channels(group)
        if not channels:
            return

        tasks = [self.publish(channel, message) for channel in channels]
        results = await run_with_concurrency_limit(
            tasks, max_concurrent=100, return_exceptions=True
        )

        # Log any exceptions that occurred during publishing
        failed_channels = []
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                failed_channels.append(list(channels)[i])

        if failed_channels:
            import logging

            logger = logging.getLogger(__name__)
            logger.warning(
                "Failed to publish message to %d/%d channels in group %s",
                len(failed_channels),
                len(channels),
                group,
                extra={
                    "group": group,
                    "total_channels": len(channels),
                    "failed_channels": failed_channels,
                    "component": "memory_backend.group_send",
                },
            )

    async def get_message(
        self, channel: str, timeout: float | None = None
    ) -> dict[str, Any] | None:
        """Get next message from channel with optional timeout.

        Parameters
        ----------
        channel : str
            Channel name to receive from
        timeout : float | None, optional
            Maximum wait time in seconds. Default: None (wait indefinitely)

        Returns
        -------
        dict[str, Any] | None
            Next message from channel, or None if timeout exceeded

        Raises
        ------
        asyncio.TimeoutError
            If timeout is exceeded (handled internally)

        """
        if channel not in self.channels:
            return None

        return await asyncio.wait_for(self.channels[channel].get(), timeout=timeout)

    async def receive(self, channel: str, timeout: float | None = None) -> dict[str, Any] | None:
        """Receive next message from channel (alias for get_message).

        Parameters
        ----------
        channel : str
            Channel name to receive from
        timeout : float | None, optional
            Maximum wait time in seconds. Default: None

        Returns
        -------
        dict[str, Any] | None
            Next message from channel, or None if timeout exceeded

        """
        return await self.get_message(channel, timeout)

    async def cleanup(self) -> None:
        """Clean up all in-memory resources and connections.

        Clears all channels, listeners, groups, and registry data.
        Should be called during application shutdown.
        """
        self.channels.clear()
        self.listeners.clear()
        await self.flush()

    async def flush(self) -> None:
        """Clear all in-memory backend state.

        Removes all groups, subscriptions, and registry data.
        Used primarily for testing and development.
        """
        async with self._lock:
            self.groups.clear()
            self.subscriptions.clear()

    async def registry_add_connection(
        self,
        connection_id: str,
        user_id: str | None,
        metadata: dict[str, Any],
        groups: set[str],
        heartbeat_timeout: float,
    ) -> None:
        """Add WebSocket connection to in-memory registry.

        Parameters
        ----------
        connection_id : str
            Unique connection identifier
        user_id : str | None
            User identifier if authenticated
        metadata : dict[str, Any]
            Connection metadata (IP, user agent, etc.)
        groups : set[str]
            Initial groups the connection belongs to
        heartbeat_timeout : float
            Heartbeat timeout in seconds

        Notes
        -----
        Thread-safe operation. Maintains bidirectional user-connection mappings.

        """
        async with self._lock:
            self._registry_connections.add(connection_id)
            self._registry_connection_data[connection_id] = {
                "user_id": user_id,
                "metadata": metadata,
                "groups": groups.copy(),
                "heartbeat_timeout": heartbeat_timeout,
            }
            if user_id:
                self._registry_user_connections[user_id].add(connection_id)

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
        Atomic operation using asyncio lock prevents race conditions.
        Since MemoryBackend is single-server only, this provides thread-safety
        within a single process.

        """
        async with self._lock:
            if len(self._registry_connections) >= max_connections:
                return False

            self._registry_connections.add(connection_id)
            self._registry_connection_data[connection_id] = {
                "user_id": user_id,
                "metadata": metadata,
                "groups": groups.copy(),
                "heartbeat_timeout": heartbeat_timeout,
            }
            if user_id:
                self._registry_user_connections[user_id].add(connection_id)

        return True

    async def registry_remove_connection(self, connection_id: str, user_id: str | None) -> None:
        """Remove connection from in-memory registry.

        Parameters
        ----------
        connection_id : str
            Connection identifier to remove
        user_id : str | None
            User identifier for cleanup

        Notes
        -----
        Thread-safe operation. Cleans up user-connection mappings.

        """
        async with self._lock:
            self._registry_connections.discard(connection_id)
            self._registry_connection_data.pop(connection_id, None)
            if user_id and user_id in self._registry_user_connections:
                self._registry_user_connections[user_id].discard(connection_id)
                if not self._registry_user_connections[user_id]:
                    del self._registry_user_connections[user_id]

    async def registry_update_groups(self, connection_id: str, groups: set[str]) -> None:
        """Update groups for a connection in registry.

        Parameters
        ----------
        connection_id : str
            Connection identifier to update
        groups : set[str]
            New set of groups for the connection

        Notes
        -----
        Thread-safe operation for dynamic group membership changes.

        """
        async with self._lock:
            if connection_id in self._registry_connection_data:
                self._registry_connection_data[connection_id]["groups"] = groups.copy()

    async def registry_get_connection_groups(self, connection_id: str) -> set[str]:
        """Get groups for a connection from registry.

        Parameters
        ----------
        connection_id : str
            Connection identifier to query

        Returns
        -------
        set[str]
            Set of group names the connection belongs to

        """
        async with self._lock:
            if connection_id in self._registry_connection_data:
                return self._registry_connection_data[connection_id]["groups"].copy()
            return set()

    async def registry_count_connections(self) -> int:
        """Count total connections in registry.

        Returns
        -------
        int
            Number of active connections

        """
        async with self._lock:
            return len(self._registry_connections)

    async def registry_get_user_connections(self, user_id: str) -> set[str]:
        """Get all connection IDs for a user.

        Parameters
        ----------
        user_id : str
            User identifier to query

        Returns
        -------
        set[str]
            Set of connection IDs for the user

        """
        async with self._lock:
            return self._registry_user_connections.get(user_id, set()).copy()

    def registry_get_prefix(self) -> str:
        """Get registry key prefix for this backend.

        Returns
        -------
        str
            Registry prefix: "memory:registry:"

        """
        return "memory:registry:"

    def supports_broadcast_channel(self) -> bool:
        """Check if backend supports broadcast channel.

        Returns
        -------
        bool
            False - MemoryBackend does not support global broadcast

        Notes
        -----
        Memory backend is single-server only, no broadcast capability.
        Use Redis backend for broadcast functionality.

        """
        return False
