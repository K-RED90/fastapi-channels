import asyncio
from typing import Any

from core.typed import BackendProtocol


class BaseBackend(BackendProtocol):
    """
    Base backend with common functionality

    Attributes:
        groups: Dictionary of groups and their channels
        subscriptions: Dictionary of subscriptions and their channels
        _lock: Lock for thread safety

    Methods:
        group_add: Add a channel to a group
        group_discard: Remove a channel from a group
        _get_group_channels: Get the channels in a group

    Example:
        >>> backend = BaseBackend()
        >>> backend.group_add("group1", "channel1")
        >>> backend.group_discard("group1", "channel1")
        >>> backend._get_group_channels("group1")
        ["channel1"]
    """

    def __init__(self):
        self.groups: dict[str, set[str]] = {}
        self.subscriptions: dict[str, set[str]] = {}
        self._lock = asyncio.Lock()
        self._channel_counter: int = 0

    async def group_add(self, group: str, channel: str) -> None:
        async with self._lock:
            if group not in self.groups:
                self.groups[group] = set()
            self.groups[group].add(channel)

    async def group_discard(self, group: str, channel: str) -> None:
        async with self._lock:
            if group in self.groups:
                self.groups[group].discard(channel)
                if not self.groups[group]:
                    del self.groups[group]

    def _get_group_channels(self, group: str) -> set[str]:
        return self.groups.get(group, set()).copy()

    async def group_channels(self, group: str) -> set[str]:
        """Return copy of channels in a group."""
        async with self._lock:
            return self._get_group_channels(group)

    async def receive(self, channel: str, timeout: float | None = None):
        """Receive next message for a channel (override in subclasses)."""
        raise NotImplementedError

    async def flush(self) -> None:
        """Clear all in-memory tracking."""
        async with self._lock:
            self.groups.clear()
            self.subscriptions.clear()

    async def new_channel(self, prefix: str = "channel") -> str:
        """Generate a unique channel name."""
        async with self._lock:
            self._channel_counter += 1
            return f"{prefix}.{int(asyncio.get_event_loop().time() * 1000)}.{self._channel_counter}"

    async def registry_add_connection(
        self,
        connection_id: str,
        user_id: str | None,
        metadata: dict[str, Any],
        groups: set[str],
        heartbeat_timeout: float,
    ) -> None:
        """Add connection to registry with metadata (override in subclasses)."""
        raise NotImplementedError

    async def registry_remove_connection(self, connection_id: str, user_id: str | None) -> None:
        """Remove connection from registry (override in subclasses)."""
        raise NotImplementedError

    async def registry_update_groups(self, connection_id: str, groups: set[str]) -> None:
        """Update groups for a connection (override in subclasses)."""
        raise NotImplementedError

    async def registry_get_connection_groups(self, connection_id: str) -> set[str]:
        """Get groups for a connection (override in subclasses)."""
        raise NotImplementedError

    async def registry_count_connections(self) -> int:
        """Count total connections in registry (override in subclasses)."""
        raise NotImplementedError

    async def registry_get_user_connections(self, user_id: str) -> set[str]:
        """Get all connection IDs for a user (override in subclasses)."""
        raise NotImplementedError

    def registry_get_prefix(self) -> str:
        """Get prefix for registry keys (override in subclasses)."""
        raise NotImplementedError

    def supports_broadcast_channel(self) -> bool:
        """Check if backend supports broadcast channel."""
        return False
