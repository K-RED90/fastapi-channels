import uuid
from typing import Literal

from pydantic import field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class WSConfig(BaseSettings):
    """Application configuration settings for the WebSocket chat system.

    This class manages all configurable parameters for the WebSocket backend,
    including backend type selection, Redis configuration, heartbeat settings,
    and connection limits. Settings can be overridden via environment variables.

    Parameters
    ----------
    BACKEND_TYPE : Literal["memory", "redis"]
        Backend storage type. "memory" for single-server deployments,
        "redis" for multi-server or persistent deployments. Default: "memory"

    REDIS_URL : str
        Redis connection URL when using Redis backend. Default: "redis://localhost:6379/0"

    REDIS_CHANNEL_PREFIX : str
        Prefix for Redis pub/sub channels. Default: "ws:"

    REDIS_REGISTRY_EXPIRY : int | None
        TTL in seconds for registry keys (connections, users) in Redis.
        None means no expiry. Default: 3600

    REDIS_GROUP_EXPIRY : int | None
        TTL in seconds for group keys in Redis. None means no expiry. Default: 3600

    WS_HEARTBEAT_INTERVAL : int
        Interval in seconds between heartbeat pings. Default: 30

    WS_HEARTBEAT_TIMEOUT : int
        Timeout in seconds before considering a connection dead. Default: 60

    WS_MAX_MESSAGE_SIZE : int
        Maximum message size in bytes. Default: 10MB (10*1024*1024)

    WS_RECONNECT_MAX_ATTEMPTS : int
        Maximum number of reconnection attempts. Default: 5

    WS_RECONNECT_DELAY : int
        Delay in seconds between reconnection attempts. Default: 5

    MAX_CONNECTIONS_PER_CLIENT : int
        Maximum concurrent connections per user. Default: 1000

    MAX_TOTAL_CONNECTIONS : int
        Maximum total connections across all users. Default: 200000

    MAX_TOTAL_GROUPS : int
        Maximum total groups/channels across all rooms. Default: 5000000

    SERVER_INSTANCE_ID : str | None
        Unique identifier for this server instance in distributed deployments.
        If not set, auto-generated on startup. Default: None (auto-generated)

    LOG_LEVEL : Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
        Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL). Default: "INFO"

    Examples
    --------
    Configure for Redis backend with custom settings:

    >>> settings = Settings(
    ...     BACKEND_TYPE="redis",
    ...     REDIS_URL="redis://my-redis:6379/0",
    ...     MAX_CONNECTIONS_PER_CLIENT=10,
    ...     LOG_LEVEL="DEBUG"
    ... )

    Notes
    -----
    Settings are loaded from environment variables with case-sensitive matching.
    Use a .env file in the project root for local development.

    """

    BACKEND_TYPE: Literal["memory", "redis"] = "memory"
    REDIS_URL: str = "redis://localhost:6379/0"
    REDIS_CHANNEL_PREFIX: str = "ws:"
    REDIS_REGISTRY_EXPIRY: int | None = 3600  # TTL in seconds for registry keys
    REDIS_GROUP_EXPIRY: int | None = 3600  # TTL in seconds for group keys

    WS_HEARTBEAT_INTERVAL: int = 30  # seconds
    WS_HEARTBEAT_TIMEOUT: int = 60  # seconds
    WS_MAX_MESSAGE_SIZE: int = 10 * 1024 * 1024  # 10MB
    WS_RECONNECT_MAX_ATTEMPTS: int = 5
    WS_RECONNECT_DELAY: int = 5  # seconds

    MAX_CONNECTIONS_PER_CLIENT: int = 1000
    MAX_TOTAL_CONNECTIONS: int = 200000
    MAX_TOTAL_GROUPS: int = 5000000

    SERVER_INSTANCE_ID: str | None = None

    LOG_LEVEL: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"] = "INFO"

    model_config = SettingsConfigDict(env_file=".env", case_sensitive=True)

    @field_validator("SERVER_INSTANCE_ID", mode="before")
    @classmethod
    def set_instance_id(cls, v: str | None) -> str:
        return v or f"server-{uuid.uuid4().hex[:12]}"
