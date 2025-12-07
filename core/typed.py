import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class ConnectionState(Enum):
    CONNECTING = "connecting"
    CONNECTED = "connected"
    DISCONNECTING = "disconnecting"
    DISCONNECTED = "disconnected"


class MessagePriority(Enum):
    HIGH = "high"
    NORMAL = "normal"
    LOW = "low"


@dataclass
class Message:
    type: str
    data: Any
    sender_id: str | None = None
    group: str | None = None
    metadata: dict[str, Any] | None = None
    priority: MessagePriority = MessagePriority.NORMAL
    ttl_seconds: float | None = None
    created_at: float = field(default_factory=time.time)

    def is_expired(self) -> bool:
        """Return True if message TTL has elapsed."""
        if self.ttl_seconds is None:
            return False
        return (time.time() - self.created_at) > self.ttl_seconds

    def to_dict(self) -> dict[str, Any]:
        return {
            "type": self.type,
            "data": self.data,
            "sender_id": self.sender_id,
            "group": self.group,
            "metadata": self.metadata,
            "priority": self.priority.value
            if isinstance(self.priority, MessagePriority)
            else self.priority,
            "ttl_seconds": self.ttl_seconds,
            "created_at": self.created_at,
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "Message":
        priority_value = payload.get("priority", MessagePriority.NORMAL.value)
        priority = (
            priority_value
            if isinstance(priority_value, MessagePriority)
            else MessagePriority(priority_value)
            if priority_value in MessagePriority._value2member_map_
            else MessagePriority.NORMAL
        )

        return cls(
            type=payload.get("type", "message"),
            data=payload.get("data"),
            sender_id=payload.get("sender_id"),
            group=payload.get("group"),
            metadata=payload.get("metadata"),
            priority=priority,
            ttl_seconds=payload.get("ttl_seconds"),
            created_at=payload.get("created_at", time.time()),
        )


class BackendProtocol(ABC):
    """Protocol for channel layer backends"""

    @abstractmethod
    async def publish(self, channel: str, message: dict[str, Any]) -> None:
        """Publish a message to a channel"""

    @abstractmethod
    async def subscribe(self, channel: str) -> None:
        """Subscribe to a channel"""

    @abstractmethod
    async def unsubscribe(self, channel: str) -> None:
        """Unsubscribe from a channel"""

    @abstractmethod
    async def group_add(self, group: str, channel: str) -> None:
        """Add a channel to a group"""

    @abstractmethod
    async def group_discard(self, group: str, channel: str) -> None:
        """Remove a channel from a group"""

    @abstractmethod
    async def group_send(self, group: str, message: dict[str, Any]) -> None:
        """Send a message to all channels in a group"""

    @abstractmethod
    async def cleanup(self) -> None:
        """Cleanup resources"""

    @abstractmethod
    async def receive(self, channel: str, timeout: float | None = None):
        """Receive next message from a channel."""

    @abstractmethod
    async def group_channels(self, group: str) -> set[str]:
        """Return channels in a group."""

    @abstractmethod
    async def flush(self) -> None:
        """Clear backend state."""

    @abstractmethod
    async def new_channel(self, prefix: str = "channel") -> str:
        """Generate unique channel name."""

    @abstractmethod
    async def registry_add_connection(
        self,
        connection_id: str,
        user_id: str | None,
        metadata: dict[str, Any],
        groups: set[str],
        heartbeat_timeout: float,
    ) -> None:
        """Add connection to registry with metadata."""

    @abstractmethod
    async def registry_remove_connection(self, connection_id: str, user_id: str | None) -> None:
        """Remove connection from registry."""

    @abstractmethod
    async def registry_update_groups(self, connection_id: str, groups: set[str]) -> None:
        """Update groups for a connection."""

    @abstractmethod
    async def registry_get_connection_groups(self, connection_id: str) -> set[str]:
        """Get groups for a connection."""

    @abstractmethod
    async def registry_count_connections(self) -> int:
        """Count total connections in registry."""

    @abstractmethod
    async def registry_get_user_connections(self, user_id: str) -> set[str]:
        """Get all connection IDs for a user."""

    @abstractmethod
    def registry_get_prefix(self) -> str:
        """Get prefix for registry keys."""

    def supports_broadcast_channel(self) -> bool:
        """Check if backend supports broadcast channel."""
        return False


class ConsumerProtocol(ABC):
    """Protocol for WebSocket consumers"""

    @abstractmethod
    async def connect(self) -> None:
        """Handle new connection"""

    @abstractmethod
    async def disconnect(self, code: int) -> None:
        """Handle disconnection"""

    @abstractmethod
    async def receive(self, message: Message) -> None:
        """Handle received message"""

    @abstractmethod
    async def send(self, message: Message) -> None:
        """Send message to client"""
