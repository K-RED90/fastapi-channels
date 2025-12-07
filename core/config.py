from typing import Literal

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    BACKEND_TYPE: Literal["memory", "redis"] = "memory"
    REDIS_URL: str = "redis://localhost:6379/0"
    REDIS_CHANNEL_PREFIX: str = "ws:"

    WS_HEARTBEAT_INTERVAL: int = 30  # seconds
    WS_HEARTBEAT_TIMEOUT: int = 60  # seconds
    WS_MAX_MESSAGE_SIZE: int = 1024 * 1024  # 1MB
    WS_RECONNECT_MAX_ATTEMPTS: int = 5
    WS_RECONNECT_DELAY: int = 5  # seconds

    MAX_CONNECTIONS_PER_CLIENT: int = 5
    MAX_TOTAL_CONNECTIONS: int = 10000

    LOG_LEVEL: str = "INFO"

    model_config = SettingsConfigDict(env_file=".env", case_sensitive=True)
