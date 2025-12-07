from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any

from fastapi import WebSocket
from fastapi.websockets import WebSocketState

from core.connections.heartbeat import HeartbeatMonitor
from core.typed import ConnectionState


def _now() -> datetime:
    return datetime.now(UTC)


@dataclass
class Connection:
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
    ip_address: str = ""
    user_agent: str = ""
    heartbeat_timeout: float = 60.0
    heartbeat: HeartbeatMonitor = field(default_factory=HeartbeatMonitor)
    state: ConnectionState = ConnectionState.CONNECTED

    @property
    def now(self) -> datetime:
        return _now()

    @property
    def is_alive(self) -> bool:
        if self.websocket.client_state != WebSocketState.CONNECTED:
            return False
        if self.heartbeat and not self.heartbeat.is_alive():
            return False
        timeout = timedelta(seconds=self.heartbeat_timeout)
        return (self.now - self.last_heartbeat) < timeout

    @property
    def idle_time(self) -> float:
        return (self.now - self.last_activity).total_seconds()

    @property
    def connection_duration(self) -> float:
        return (self.now - self.connected_at).total_seconds()

    def update_activity(self) -> None:
        self.last_activity = self.now

    def update_heartbeat(self) -> None:
        """Refresh heartbeat and activity timestamps."""
        self.last_heartbeat = self.now
        self.heartbeat.record_pong()
        self.update_activity()

    def to_dict(self) -> dict[str, Any]:
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
            "ip_address": self.ip_address,
            "user_agent": self.user_agent,
        }
