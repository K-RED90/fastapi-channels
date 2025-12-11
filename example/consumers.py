import asyncio
import time
import base64
from typing import TYPE_CHECKING, Any

from fastapi_channel.consumer.base import BaseConsumer
from fastapi_channel.typed import Message

if TYPE_CHECKING:
    from fastapi_channel.channel_layer import ChannelLayer
    from fastapi_channel.connections.manager import ConnectionManager
    from fastapi_channel.connections.state import Connection
    from fastapi_channel.middleware.base import Middleware


class ChatConsumer(BaseConsumer):
    """Example chat consumer implementation with full chat features"""

    def __init__(
        self,
        connection: "Connection",
        manager: "ConnectionManager | None" = None,
        channel_layer: "ChannelLayer | None" = None,
        middleware_stack: "Middleware | None" = None,
        database: Any = None,
        *args,
        **kwargs,
    ):
        super().__init__(
            connection,
            manager=manager,
            channel_layer=channel_layer,
            middleware_stack=middleware_stack,
        )
        # Typing indicators stored in-memory per consumer instance
        # NOTE: For production multi-server deployments, consider using Redis backend
        # to store typing indicators for cross-server synchronization
        self.typing_users: dict[str, list[str]] = {}  # room_name -> typing user_ids
        self.max_message_history = 100
        self.max_message_length = 100000  # Support up to 100k characters
        self.max_room_name_length = 50
        self.max_file_size = 10 * 1024 * 1024  # 10MB file size limit
        self.database = (
            database  # In-memory SQLite database for persistent message and user/room storage
        )

    async def connect(self) -> None:
        """Handle connection"""
        await super().connect()

        connection_id = self.connection.channel_name
        user_id = self.connection.user_id or connection_id
        username = self.connection.user_id or f"User_{connection_id[:8]}"

        if self.database:
            user = self.database.get_user(user_id)
            if user is None:
                self.database.create_user(
                    user_id=user_id,
                    username=username,
                    status="online",
                    joined_at=time.time(),
                    last_seen=time.time(),
                )
            else:
                self.database.update_user(user_id, status="online", last_seen=time.time())

        await self.send_json(
            {
                "type": "welcome",
                "message": f"Welcome to the chat, {username}! Create or join a room to start chatting.",
                "user_id": user_id,
                "username": username,
                "timestamp": self.timestamp,
            }
        )

    async def on_disconnect(self, code: int) -> None:
        connection_id = self.connection.channel_name
        user_id = self.connection.user_id or connection_id

        username = connection_id
        if self.database:
            user = self.database.get_user(user_id)
            if user:
                username = user["username"]
                self.database.update_user(user_id, status="offline", last_seen=time.time())
            else:
                username = connection_id

        for group in self.connection.groups.copy():
            await self.leave_group(group)
            if self.database:
                self.database.remove_user_from_room(user_id, group)
                self.database.decrement_room_user_count(group)

            await self.send_to_group(
                group,
                {
                    "type": "user_left",
                    "user_id": user_id,
                    "username": username,
                    "timestamp": self.timestamp,
                },
            )

    async def receive(self, message: Message) -> None:
        """Handle received messages"""
        msg_type = message.type

        try:
            if msg_type == "chat_message":
                await self.handle_chat_message(message)
            elif msg_type == "join_room":
                await self.handle_join_room(message)
            elif msg_type == "leave_room":
                await self.handle_leave_room(message)
            elif msg_type == "private_message":
                await self.handle_private_message(message)
            elif msg_type == "create_room":
                await self.handle_create_room(message)
            elif msg_type == "list_rooms":
                await self.handle_list_rooms(message)
            elif msg_type == "get_room_users":
                await self.handle_get_room_users(message)
            elif msg_type == "get_message_history":
                await self.handle_get_message_history(message)
            elif msg_type == "typing_start":
                await self.handle_typing_start(message)
            elif msg_type == "typing_stop":
                await self.handle_typing_stop(message)
            elif msg_type == "file_message":
                await self.handle_file_message(message)
            elif msg_type == "ping":
                await self.handle_ping(message)
            else:
                await self.send_error(f"Unknown message type: {msg_type}")
        except Exception as e:
            await self.send_error(f"Error processing message: {e!s}", msg_type)

    async def handle_chat_message(self, message: Message) -> None:
        """Handle chat message"""
        room = message.data.get("room")
        if not room:
            await self.send_error("Room name is required")
            return
        text = message.data.get("text", "").strip()

        if not text:
            await self.send_error("Message text is required")
            return

        if len(text) > self.max_message_length:
            await self.send_error(f"Message too long (max {self.max_message_length} characters)")
            return

        if room not in self.connection.groups:
            await self.send_error(f"Not in room: {room}")
            return

        connection_id = self.connection.channel_name
        user_id = self.connection.user_id or connection_id

        username = connection_id
        if self.database:
            user = self.database.get_user(user_id)
            if user:
                username = user["username"]

        timestamp = time.time()

        chat_message = {
            "type": "chat_message",
            "user_id": user_id,
            "username": username,
            "room": room,
            "text": text,
            "timestamp": timestamp,
        }

        if self.database:
            self.database.save_message(
                room=room, user_id=user_id, username=username, text=text, timestamp=timestamp
            )

        await self.send_to_group(room, chat_message)

    async def handle_join_room(self, message: Message) -> None:
        """Handle room join request"""
        room = message.data.get("room", "").strip()

        if not room:
            await self.send_error("Room name is required")
            return

        if len(room) > self.max_room_name_length:
            await self.send_error(
                f"Room name too long (max {self.max_room_name_length} characters)"
            )
            return

        if room in self.connection.groups:
            await self.send_error(f"Already in room: {room}")
            return

        if self.database:
            room_info = self.database.get_room(room)
            if room_info is None:
                # Room doesn't exist - automatically create it as a public room
                # This provides better UX: joining a non-existent room creates it
                connection_id = self.connection.channel_name
                user_id = self.connection.user_id or connection_id

                # Get username from database
                username = connection_id
                user = self.database.get_user(user_id)
                if user:
                    username = user["username"]
                else:
                    # Create user if doesn't exist
                    self.database.create_user(
                        user_id=user_id,
                        username=username,
                        status="online",
                        joined_at=time.time(),
                        last_seen=time.time(),
                    )

                # Create the room
                self.database.create_room(
                    room_name=room,
                    created_by=user_id,
                    creator_username=username,
                    is_public=True,
                    created_at=time.time(),
                )
        else:
            await self.send_error("Database not available")
            return

        connection_id = self.connection.channel_name
        user_id = self.connection.user_id or connection_id

        # Get username from database
        username = connection_id
        if self.database:
            user = self.database.get_user(user_id)
            if user:
                username = user["username"]

        # Join the room
        await self.join_group(room)
        if self.database:
            self.database.add_user_to_room(user_id, room)
            self.database.increment_room_user_count(room)

        await self.send_json({"type": "room_joined", "room": room, "timestamp": self.timestamp})

        await self.send_room_info(room)

        # Notify others in room
        await self.send_to_group(
            room,
            {
                "type": "user_joined_room",
                "user_id": user_id,
                "username": username,
                "room": room,
                "timestamp": self.timestamp,
            },
        )

    async def handle_leave_room(self, message: Message) -> None:
        """Handle room leave request"""
        room = message.data.get("room", "").strip()

        if not room or room not in self.connection.groups:
            await self.send_error("Not in that room")
            return

        connection_id = self.connection.channel_name
        user_id = self.connection.user_id or connection_id

        # Get username from database
        username = connection_id
        if self.database:
            user = self.database.get_user(user_id)
            if user:
                username = user["username"]

        # Leave the room
        await self.leave_group(room)
        if self.database:
            self.database.remove_user_from_room(user_id, room)
            self.database.decrement_room_user_count(room)

        # Send confirmation
        await self.send_json({"type": "room_left", "room": room, "timestamp": self.timestamp})

        # Notify others in room
        await self.send_to_group(
            room,
            {
                "type": "user_left_room",
                "user_id": user_id,
                "username": username,
                "room": room,
                "timestamp": self.timestamp,
            },
        )

    async def handle_private_message(self, message: Message) -> None:
        """Handle private message to another user"""
        target_user_id = message.data.get("target_user_id", "").strip()
        text = message.data.get("text", "").strip()

        if not target_user_id:
            await self.send_error("Target user ID is required")
            return

        if not text:
            await self.send_error("Message text is required")
            return

        if len(text) > self.max_message_length:
            await self.send_error(f"Message too long (max {self.max_message_length} characters)")
            return

        # Check if target user exists in database
        if self.database:
            target_user = self.database.get_user(target_user_id)
            if target_user is None:
                await self.send_error("User not found")
                return
            target_username = target_user["username"]
        else:
            await self.send_error("Database not available")
            return

        sender_user_id = self.connection.user_id or self.connection.channel_name

        # Get sender username from database
        sender_username = self.connection.channel_name
        if self.database:
            sender_user = self.database.get_user(sender_user_id)
            if sender_user:
                sender_username = sender_user["username"]
        timestamp = time.time()

        # Create private message
        private_message = {
            "type": "private_message",
            "from_user_id": sender_user_id,
            "from_username": sender_username,
            "to_user_id": target_user_id,
            "to_username": target_username,
            "text": text,
            "timestamp": timestamp,
        }

        # Send to target user (if they're online)
        try:
            await self.manager.send_to_user(target_user_id, private_message)
        except Exception:
            await self.send_error("Failed to send private message")

        # Send confirmation to sender
        await self.send_json(
            {
                "type": "private_message_sent",
                "to_user_id": target_user_id,
                "to_username": target_username,
                "text": text,
                "timestamp": timestamp,
            }
        )

    async def handle_create_room(self, message: Message) -> None:
        """Handle room creation"""
        room_name = message.data.get("room_name", "").strip()
        is_public = message.data.get("is_public", True)

        if not room_name:
            await self.send_error("Room name is required")
            return

        if len(room_name) > self.max_room_name_length:
            await self.send_error(
                f"Room name too long (max {self.max_room_name_length} characters)"
            )
            return

        # Check if room already exists
        if self.database:
            existing_room = self.database.get_room(room_name)
            if existing_room is not None:
                # Room already exists - just join it instead of erroring
                # This provides better UX: creating an existing room just joins it
                if room_name in self.connection.groups:
                    await self.send_error(f"Already in room: {room_name}")
                    return

                # Join the existing room
                connection_id = self.connection.channel_name
                user_id = self.connection.user_id or connection_id

                # Get username from database
                username = connection_id
                user = self.database.get_user(user_id)
                if user:
                    username = user["username"]

                # Join the room
                await self.join_group(room_name)
                if self.database:
                    self.database.add_user_to_room(user_id, room_name)
                    self.database.increment_room_user_count(room_name)

                await self.send_json(
                    {"type": "room_joined", "room": room_name, "timestamp": self.timestamp}
                )
                await self.send_room_info(room_name)

                # Notify others in room
                await self.send_to_group(
                    room_name,
                    {
                        "type": "user_joined_room",
                        "user_id": user_id,
                        "username": username,
                        "room": room_name,
                        "timestamp": self.timestamp,
                    },
                )
                return
        else:
            await self.send_error("Database not available")
            return

        connection_id = self.connection.channel_name
        user_id = self.connection.user_id or connection_id

        # Get username from database
        username = connection_id
        if self.database:
            user = self.database.get_user(user_id)
            if user:
                username = user["username"]
            else:
                # Create user if doesn't exist
                self.database.create_user(
                    user_id=user_id,
                    username=username,
                    status="online",
                    joined_at=time.time(),
                    last_seen=time.time(),
                )

        # Create room in database
        if self.database:
            self.database.create_room(
                room_name=room_name,
                created_by=user_id,
                creator_username=username,
                is_public=is_public,
                created_at=time.time(),
            )

        # Automatically join the room after creating it
        await self.join_group(room_name)
        if self.database:
            self.database.add_user_to_room(user_id, room_name)
            self.database.increment_room_user_count(room_name)

        # Send room created message
        await self.send_json(
            {
                "type": "room_created",
                "room": room_name,
                "is_public": is_public,
                "timestamp": self.timestamp,
            }
        )

        # Send room joined confirmation (so frontend knows user is now in the room)
        await self.send_json(
            {"type": "room_joined", "room": room_name, "timestamp": self.timestamp}
        )

        # Send room info
        await self.send_room_info(room_name)

    async def handle_list_rooms(self, message: Message) -> None:
        """Handle request to list available rooms"""
        if not self.database:
            await self.send_error("Database not available")
            return

        rooms_list = []
        all_rooms = self.database.list_rooms()
        for room_info in all_rooms:
            # Show public rooms or rooms the user is in
            if room_info["is_public"] or room_info["name"] in self.connection.groups:
                rooms_list.append(
                    {
                        "name": room_info["name"],
                        "user_count": room_info["user_count"],
                        "created_by": room_info.get("creator_username", "system"),
                        "is_public": room_info["is_public"],
                    }
                )

        await self.send_json(
            {"type": "rooms_list", "rooms": rooms_list, "timestamp": self.timestamp}
        )

    async def handle_get_room_users(self, message: Message) -> None:
        """Handle request to get users in a room"""
        room = message.data.get("room")

        if not room:
            await self.send_error("Room name is required")
            return

        if not self.database:
            await self.send_error("Database not available")
            return

        # Check if room exists
        room_info = self.database.get_room(room)
        if room_info is None:
            await self.send_error("Room not found")
            return

        # Get users in the room from database
        users_in_room = self.database.get_room_users(room)

        await self.send_json(
            {
                "type": "room_users",
                "room": room,
                "users": users_in_room,
                "timestamp": self.timestamp,
            }
        )

    async def handle_get_message_history(self, message: Message) -> None:
        """Handle request to get message history for a room"""
        room = message.data.get("room")
        limit = min(int(message.data.get("limit", 50)), self.max_message_history)

        if not room:
            await self.send_error("Room name is required")
            return

        if room not in self.connection.groups:
            await self.send_error("Not in that room")
            return

        # Get messages from database
        if not self.database:
            await self.send_error("Database not available")
            return

        history = self.database.get_recent_messages(room, limit=limit)

        await self.send_json(
            {
                "type": "message_history",
                "room": room,
                "messages": history,
                "timestamp": self.timestamp,
            }
        )

    async def handle_typing_start(self, message: Message) -> None:
        """Handle typing start indicator"""
        room = message.data.get("room")

        if not room or room not in self.connection.groups:
            return  # Silently ignore invalid requests

        connection_id = self.connection.channel_name
        user_id = self.connection.user_id or connection_id

        # Get username from database
        username = connection_id
        if self.database:
            user = self.database.get_user(user_id)
            if user:
                username = user["username"]

        if room not in self.typing_users:
            self.typing_users[room] = []

        if user_id not in self.typing_users[room]:
            self.typing_users[room].append(user_id)

            # Notify others in room (exclude sender)
            await self.send_to_group_except_sender(
                room,
                {
                    "type": "user_typing_start",
                    "user_id": user_id,
                    "username": username,
                    "room": room,
                    "timestamp": self.timestamp,
                },
            )

    async def handle_typing_stop(self, message: Message) -> None:
        """Handle typing stop indicator"""
        room = message.data.get("room")

        if not room or room not in self.connection.groups:
            return  # Silently ignore invalid requests

        connection_id = self.connection.channel_name
        user_id = self.connection.user_id or connection_id

        # Get username from database
        username = connection_id
        if self.database:
            user = self.database.get_user(user_id)
            if user:
                username = user["username"]

        if room in self.typing_users and user_id in self.typing_users[room]:
            self.typing_users[room].remove(user_id)

            # Notify others in room (exclude sender)
            await self.send_to_group_except_sender(
                room,
                {
                    "type": "user_typing_stop",
                    "user_id": user_id,
                    "username": username,
                    "room": room,
                    "timestamp": self.timestamp,
                },
            )

    async def handle_ping(self, message: Message) -> None:
        """Handle ping message"""
        await self.send_json({"type": "pong", "timestamp": self.timestamp})

    async def handle_file_message(self, message: Message) -> None:
        """Handle file message"""
        room = message.data.get("room")
        filename = message.data.get("filename")
        mime_type = message.data.get("mime_type")
        file_size = message.data.get("file_size")
        file_data_b64 = message.data.get("file_data")

        if not room or not filename or not mime_type or file_size is None or not file_data_b64:
            await self.send_error("File message incomplete")
            return

        if len(filename) > 255:  # Reasonable filename length limit
            await self.send_error("Filename too long")
            return

        if file_size > self.max_file_size:
            await self.send_error(f"File too large (max {self.max_file_size} bytes)")
            return

        if room not in self.connection.groups:
            await self.send_error(f"Not in room: {room}")
            return

        try:
            file_data = await asyncio.to_thread(base64.b64decode, file_data_b64)
            if len(file_data) != file_size:
                await self.send_error("File data size mismatch")
                return
        except Exception:
            await self.send_error("Invalid file data")
            return

        connection_id = self.connection.channel_name
        user_id = self.connection.user_id or connection_id
        username = connection_id
        if self.database:
            user = self.database.get_user(user_id)
            if user:
                username = user["username"]

        timestamp = time.time()
        file_message = {
            "type": "file_message",
            "user_id": user_id,
            "username": username,
            "room": room,
            "filename": filename,
            "mime_type": mime_type,
            "file_size": file_size,
            "file_data": file_data_b64,  # Keep base64 for transmission
            "timestamp": timestamp,
        }

        # Store in database
        if self.database:
            text_content = f"[File: {filename}]"
            self.database.save_message(
                room=room,
                user_id=user_id,
                username=username,
                text=text_content,
                timestamp=timestamp,
                message_type="file",
                filename=filename,
                mime_type=mime_type,
                file_size=file_size,
                file_data=file_data_b64
            )

        await self.send_to_group(room, file_message)

    # Helper methods

    async def send_error(self, error_message: str, message_type: str = "error") -> None:
        """Send error message"""
        await self.send_json(
            {
                "type": "error",
                "message": error_message,
                "message_type": message_type,
                "timestamp": self.timestamp,
            }
        )

    async def send_room_info(self, room: str) -> None:
        """Send room information to user"""
        if not self.database:
            return

        room_info = self.database.get_room(room)
        if room_info is None:
            return

        await self.send_json(
            {
                "type": "room_info",
                "room": room,
                "user_count": room_info["user_count"],
                "created_by": room_info.get("creator_username", "system"),
                "is_public": room_info["is_public"],
                "timestamp": self.timestamp,
            }
        )

    async def send_to_group_except_sender(self, group: str, message: dict[str, Any]) -> None:
        """Send message to all users in group except the sender"""
        await self.manager.send_group_except(
            group, message, exclude_connection_id=self.connection.channel_name
        )

    @property
    def timestamp(self) -> float:
        return time.time()
