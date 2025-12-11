import base64
import time
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


@dataclass(slots=True)
class Message:
    """Represents a WebSocket message with metadata and delivery options.

    The Message class encapsulates all information needed for WebSocket communication,
    including message type, payload data, routing information, and delivery controls.

    Parameters
    ----------
    type : str
        Message type identifier (e.g., "chat_message", "user_join", "ping")
    data : Any
        Message payload data (can be any serializable type)
    sender_id : str | None, optional
        ID of the connection that sent the message. Default: None
    group : str | None, optional
        Group name for group messaging. Default: None
    metadata : dict[str, Any] | None, optional
        Additional metadata for message processing. Default: None
    priority : MessagePriority, optional
        Message processing priority. Default: MessagePriority.NORMAL
    ttl_seconds : float | None, optional
        Time-to-live in seconds. Message expires after this time. Default: None
    created_at : float, optional
        Unix timestamp when message was created. Default: current time

    Examples
    --------
    Create a simple chat message:

    >>> message = Message(
    ...     type="chat_message",
    ...     data={"text": "Hello world!", "user": "alice"},
    ...     sender_id="conn_123"
    ... )

    Create a group message with TTL:

    >>> group_msg = Message(
    ...     type="announcement",
    ...     data={"text": "Server maintenance in 5 minutes"},
    ...     group="admin_channel",
    ...     priority=MessagePriority.HIGH,
    ...     ttl_seconds=300.0  # 5 minutes
    ... )

    """

    type: str
    data: Any
    sender_id: str | None = None
    group: str | None = None
    metadata: dict[str, Any] | None = None
    priority: MessagePriority = MessagePriority.NORMAL
    ttl_seconds: float | None = None
    created_at: float = field(default_factory=time.time)
    binary_data: bytes | None = None

    def __post_init__(self) -> None:
        """Validate that data and binary_data are mutually exclusive."""
        if self.data is not None and self.binary_data is not None:
            raise ValueError("Message cannot have both 'data' and 'binary_data' set")

    def is_expired(self) -> bool:
        """Check if message has exceeded its time-to-live.

        Returns
        -------
        bool
            True if TTL has elapsed, False otherwise or if no TTL set

        Examples
        --------
        >>> msg = Message(type="test", data="hello", ttl_seconds=60.0)
        >>> msg.is_expired()  # False (just created)
        False
        >>> # After 60+ seconds...
        >>> msg.is_expired()  # True
        True

        """
        if self.ttl_seconds is None:
            return False
        return (time.time() - self.created_at) > self.ttl_seconds

    def to_dict(self) -> dict[str, Any]:
        """Convert message to dictionary representation for serialization.

        Returns
        -------
        dict[str, Any]
            Dictionary containing all message fields. Binary data is base64-encoded.

        Examples
        --------
        >>> msg = Message(type="test", data="hello")
        >>> msg.to_dict()
        {'type': 'test', 'data': 'hello', 'sender_id': None, ...}

        """
        result = {
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
        if self.binary_data is not None:
            result["binary_data"] = base64.b64encode(self.binary_data).decode("utf-8")
        return result

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "Message":
        """Create Message instance from dictionary representation.

        Parameters
        ----------
        payload : dict[str, Any]
            Dictionary containing message fields

        Returns
        -------
        Message
            New Message instance

        Examples
        --------
        >>> data = {"type": "chat", "data": "hello", "priority": "high"}
        >>> msg = Message.from_dict(data)
        >>> msg.priority
        <MessagePriority.HIGH: 'high'>

        """
        priority_value = payload.get("priority", MessagePriority.NORMAL.value)
        priority = (
            priority_value
            if isinstance(priority_value, MessagePriority)
            else MessagePriority(priority_value)
            if priority_value in MessagePriority._value2member_map_
            else MessagePriority.NORMAL
        )

        binary_data = None
        if "binary_data" in payload:
            binary_data = base64.b64decode(payload["binary_data"])

        return cls(
            type=payload.get("type", "message"),
            data=payload.get("data"),
            sender_id=payload.get("sender_id"),
            group=payload.get("group"),
            metadata=payload.get("metadata"),
            priority=priority,
            ttl_seconds=payload.get("ttl_seconds"),
            created_at=payload.get("created_at", time.time()),
            binary_data=binary_data,
        )
