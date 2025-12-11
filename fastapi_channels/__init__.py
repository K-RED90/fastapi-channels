"""FastAPI Channels - WebSocket connection management for FastAPI applications."""

from fastapi_channels.config import WSConfig
from fastapi_channels.connections import Connection, ConnectionManager, ConnectionRegistry
from fastapi_channels.connections.manager import get_manager
from fastapi_channels.consumer import BaseConsumer
from fastapi_channels.exceptions import BaseError

__all__ = [
    "BaseConsumer",
    "BaseError",
    "Connection",
    "ConnectionManager",
    "ConnectionRegistry",
    "WSConfig",
    "get_manager",
]
