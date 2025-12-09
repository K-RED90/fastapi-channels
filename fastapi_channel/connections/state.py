from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any

from fastapi import WebSocket
from fastapi.websockets import WebSocketState

from core.connections.heartbeat import HeartbeatMonitor
from core.typed import ConnectionState


def _now() -> datetime:
    return datetime.now(UTC)


@dataclass(init=True)
class Connection:
    """Represents an active WebSocket connection with metadata and state.

    The Connection class encapsulates all state and metadata for an active
    WebSocket connection, including timing information, group memberships,
    message statistics, and heartbeat monitoring.

    Connection lifecycle:
    1. Created during WebSocket acceptance
    2. State transitions: CONNECTING -> CONNECTED -> DISCONNECTING -> DISCONNECTED
    3. Tracks activity, heartbeats, and message statistics
    4. Manages group memberships for broadcast messaging

    Parameters
    ----------
    websocket : WebSocket
        FastAPI WebSocket instance for communication
    channel_name : str
        Unique channel identifier for this connection
    user_id : str | None, optional
        User identifier if authenticated. Default: "" (anonymous)
    connected_at : datetime, optional
        Connection establishment timestamp. Default: current time
    last_activity : datetime, optional
        Last message activity timestamp. Default: current time
    last_heartbeat : datetime, optional
        Last heartbeat response timestamp. Default: current time
    groups : set[str], optional
        Set of groups this connection belongs to. Default: empty set
    metadata : dict[str, Any], optional
        Additional connection metadata. Default: empty dict
    message_count : int, optional
        Total messages sent to this connection. Default: 0
    bytes_sent : int, optional
        Total bytes sent to this connection. Default: 0
    bytes_received : int, optional
        Total bytes received from this connection. Default: 0
    heartbeat_timeout : float, optional
        Heartbeat timeout in seconds. Default: 60.0
    heartbeat : HeartbeatMonitor, optional
        Heartbeat monitoring instance. Default: new HeartbeatMonitor
    state : ConnectionState, optional
        Current connection state. Default: CONNECTED

    Examples
    --------
    Creating a connection for authenticated user:

    >>> connection = Connection(
    ...     websocket=ws,
    ...     channel_name="ws.alice.1640995200000.a1b2c3d4",
    ...     user_id="alice",
    ...     metadata={"ip": "192.168.1.100", "device": "mobile"},
    ...     heartbeat_timeout=30.0
    ... )

    Notes
    -----
    All datetime fields use UTC timezone for consistency.
    Group memberships are tracked locally and synchronized with backend.
    Heartbeat monitoring helps detect dead connections.

    """

    websocket: WebSocket
    channel_name: str
    user_id: str | None = ""
    connected_at: datetime = field(default_factory=_now)
    last_activity: datetime = field(default_factory=_now)
    last_heartbeat: datetime = field(default_factory=_now)
    groups: set[str] = field(default_factory=set)
    metadata: dict[str, Any] = field(default_factory=dict)
    message_count: int = 0
    bytes_sent: int = 0
    bytes_received: int = 0
    heartbeat_timeout: float = 60.0
    heartbeat: HeartbeatMonitor = field(default_factory=HeartbeatMonitor)
    state: ConnectionState = ConnectionState.CONNECTED

    @property
    def now(self) -> datetime:
        return _now()

    @property
    def is_alive(self) -> bool:
        """Check if connection is alive and healthy.

        Returns
        -------
        bool
            True if connection is responsive, False if dead or unhealthy

        Notes
        -----
        Checks WebSocket state, heartbeat health, and timeout.
        Used by heartbeat loop to detect dead connections.

        """
        if self.websocket.client_state != WebSocketState.CONNECTED:
            return False
        if self.heartbeat and not self.heartbeat.is_alive():
            return False
        timeout = timedelta(seconds=self.heartbeat_timeout)
        return (self.now - self.last_heartbeat) < timeout

    @property
    def idle_time(self) -> float:
        """Get seconds since last activity.

        Returns
        -------
        float
            Seconds elapsed since last message activity

        Notes
        -----
        Measures time since last message sent or received.
        Used for connection cleanup and monitoring.

        """
        return (self.now - self.last_activity).total_seconds()

    @property
    def connection_duration(self) -> float:
        """Get total connection lifetime in seconds.

        Returns
        -------
        float
            Seconds elapsed since connection was established

        Notes
        -----
        Measures total time connection has been active.
        Used for statistics and connection monitoring.

        """
        return (self.now - self.connected_at).total_seconds()

    def update_activity(self) -> None:
        """Update last activity timestamp to current time.

        Notes
        -----
        Called when sending or receiving messages.
        Resets idle time counter.

        """
        self.last_activity = self.now

    def update_heartbeat(self) -> None:
        """Update heartbeat timestamp and record pong response.

        Notes
        -----
        Called when receiving pong responses from client.
        Updates both heartbeat and activity timestamps.
        Records pong in heartbeat monitor.

        """
        self.last_heartbeat = self.now
        self.heartbeat.record_pong()
        self.update_activity()

    def to_dict(self) -> dict[str, Any]:
        """Convert connection to dictionary representation.

        Returns
        -------
        dict[str, Any]
            Dictionary containing connection information and statistics

        Notes
        -----
        Used for serialization and API responses.
        Includes computed fields like duration and idle time.
        Safe for JSON serialization.

        """
        return {
            "channel_name": self.channel_name,
            "user_id": self.user_id,
            "connected_at": self.connected_at.isoformat(),
            "connection_duration": self.connection_duration,
            "idle_time": self.idle_time,
            "groups": list(self.groups),
            "message_count": self.message_count,
            "bytes_sent": self.bytes_sent,
            "bytes_received": self.bytes_received,
        }
