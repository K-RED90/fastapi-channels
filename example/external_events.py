"""Utilities for WebSocket functionality testing.

This module provides utility functions for testing WebSocket functionality
"""

import asyncio
from typing import Any

from fastapi_channels import get_channel_layer


async def send_broadcast_announcement(message: str) -> None:
    """Send broadcast announcement to all connected users.

    Parameters
    ----------
    message : str
        Announcement message to broadcast

    """
    channel_layer = get_channel_layer()

    await channel_layer.broadcast(
        message={
            "type": "announcement",
            "message": message,
            "timestamp": asyncio.get_event_loop().time(),
        }
    )


async def send_test_message_to_room(room_name: str, user_id: str, text: str) -> None:
    """Send a test message to a specific room.

    Parameters
    ----------
    room_name : str
        Room name to send message to
    user_id : str
        User ID of the sender
    text : str
        Message text

    """
    channel_layer = get_channel_layer()

    await channel_layer.send_to_group(
        group=room_name,
        message={
            "type": "chat_message",
            "user_id": user_id,
            "username": f"TestUser_{user_id[:8]}",
            "room": room_name,
            "text": text,
            "timestamp": asyncio.get_event_loop().time(),
        },
    )


async def send_test_notification_to_user(user_id: str, message: str) -> None:
    """Send a test notification to a specific user.

    Parameters
    ----------
    user_id : str
        User ID to send notification to
    message : str
        Notification message

    """
    channel_layer = get_channel_layer()

    await channel_layer.send_to_user(
        user_id=user_id,
        message={
            "type": "notification",
            "message": message,
            "timestamp": asyncio.get_event_loop().time(),
        },
    )


async def send_test_group_message(group_name: str, data: dict[str, Any]) -> None:
    """Send a test message to a group.

    Parameters
    ----------
    group_name : str
        Group name to send message to
    data : dict[str, Any]
        Message data to send

    """
    channel_layer = get_channel_layer()

    await channel_layer.send_to_group(
        group=group_name,
        message={
            "type": "test_group_message",
            "data": data,
            "group": group_name,
            "timestamp": asyncio.get_event_loop().time(),
        },
    )


def get_channel_layer_status() -> dict[str, Any]:
    """Get current channel layer status information.

    Returns
    -------
    dict[str, Any]
        Status information including connection counts

    """
    channel_layer = get_channel_layer()

    return {
        "status": "active",
        "backend_type": channel_layer.config.BACKEND_TYPE,
        "timestamp": asyncio.get_event_loop().time(),
    }
