"""FastAPI Channels - WebSocket connection management for FastAPI applications."""

from fastapi_channels.channel_layer import ChannelLayer, get_channel_layer
from fastapi_channels.config import WSConfig
from fastapi_channels.connections import Connection, ConnectionManager, ConnectionRegistry
from fastapi_channels.consumer import BaseConsumer
from fastapi_channels.exceptions import BaseError

__all__ = [
    "BaseConsumer",
    "BaseError",
    "ChannelLayer",
    "Connection",
    "ConnectionManager",
    "ConnectionRegistry",
    "WSConfig",
    "get_channel_layer",
]
