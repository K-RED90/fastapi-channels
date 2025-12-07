import time
from typing import Any

from core.consumer.base import BaseConsumer
from core.typed import Message


class ChatConsumer(BaseConsumer):
    """Example chat consumer implementation with full chat features"""

    def __init__(self, *args, database=None, **kwargs):
        super().__init__(*args, **kwargs)
        # In-memory storage for chat features
        self.users: dict[str, dict[str, Any]] = {}  # user_id -> user_info
        self.rooms: dict[str, dict[str, Any]] = {}  # room_name -> room_info
        self.typing_users: dict[str, list[str]] = {}  # room_name -> typing user_ids
        self.max_message_history = 100
        self.max_message_length = 1000
        self.max_room_name_length = 50
        self.database = database  # SQLite database for persistent message storage

    async def connect(self) -> None:
        """Handle connection"""
        await super().connect()

        # Register user
        user_id = self.connection.channel_name
        username = self.connection.user_id or f"User_{user_id[:8]}"
        self.users[user_id] = {
            "username": username,
            "status": "online",
            "joined_at": time.time(),
            "last_seen": time.time(),
            "rooms": set(),
        }

        # Send welcome message with user info
        await self.send_json(
            {
                "type": "welcome",
                "message": f"Welcome to the chat, {username}! Create or join a room to start chatting.",
                "user_id": user_id,
                "username": username,
                "timestamp": time.time(),
            }
        )

    async def disconnect(self, code: int) -> None:
        """Handle disconnection"""
        user_id = self.connection.channel_name
        username = self.users.get(user_id, {}).get("username", user_id)

        # Update user status
        if user_id in self.users:
            self.users[user_id]["status"] = "offline"
            self.users[user_id]["last_seen"] = time.time()

        # Notify others in all groups and clean up room counts
        for group in self.groups.copy():
            # Update room user count
            if group in self.rooms:
                self.rooms[group]["user_count"] = max(0, self.rooms[group]["user_count"] - 1)

            await self.send_to_group(
                group,
                {
                    "type": "user_left",
                    "user_id": user_id,
                    "username": username,
                    "timestamp": time.time(),
                },
            )

        await super().disconnect(code)

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

        # Validate input
        if not text:
            await self.send_error("Message text is required")
            return

        if len(text) > self.max_message_length:
            await self.send_error(f"Message too long (max {self.max_message_length} characters)")
            return

        if room not in self.groups:
            await self.send_error(f"Not in room: {room}")
            return

        user_id = self.connection.channel_name
        username = self.users.get(user_id, {}).get("username", user_id)
        timestamp = time.time()

        # Create message object
        chat_message = {
            "type": "chat_message",
            "user_id": user_id,
            "username": username,
            "room": room,
            "text": text,
            "timestamp": timestamp,
        }

        # Save to database
        if self.database:
            self.database.save_message(
                room=room, user_id=user_id, username=username, text=text, timestamp=timestamp
            )

        # Broadcast to room
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

        if room in self.groups:
            await self.send_error(f"Already in room: {room}")
            return

        if room not in self.rooms:
            await self.send_error(f"Room does not exist: {room}")
            return

        user_id = self.connection.channel_name
        username = self.users.get(user_id, {}).get("username", user_id)

        # Join the room
        await self.join_group(room)
        self.users[user_id]["rooms"].add(room)
        self.rooms[room]["user_count"] += 1

        # Send confirmation
        await self.send_json({"type": "room_joined", "room": room, "timestamp": time.time()})

        # Send room info
        await self.send_room_info(room)

        # Notify others in room
        await self.send_to_group(
            room,
            {
                "type": "user_joined_room",
                "user_id": user_id,
                "username": username,
                "room": room,
                "timestamp": time.time(),
            },
        )

    async def handle_leave_room(self, message: Message) -> None:
        """Handle room leave request"""
        room = message.data.get("room", "").strip()

        if not room or room not in self.groups:
            await self.send_error("Not in that room")
            return

        user_id = self.connection.channel_name
        username = self.users.get(user_id, {}).get("username", user_id)

        # Leave the room
        await self.leave_group(room)
        if user_id in self.users:
            self.users[user_id]["rooms"].discard(room)
        if room in self.rooms:
            self.rooms[room]["user_count"] = max(0, self.rooms[room]["user_count"] - 1)

        # Send confirmation
        await self.send_json({"type": "room_left", "room": room, "timestamp": time.time()})

        # Notify others in room
        await self.send_to_group(
            room,
            {
                "type": "user_left_room",
                "user_id": user_id,
                "username": username,
                "room": room,
                "timestamp": time.time(),
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

        if target_user_id not in self.users:
            await self.send_error("User not found")
            return

        sender_id = self.connection.channel_name
        sender_username = self.users.get(sender_id, {}).get("username", sender_id)
        target_username = self.users.get(target_user_id, {}).get("username", target_user_id)
        timestamp = time.time()

        # Create private message
        private_message = {
            "type": "private_message",
            "from_user_id": sender_id,
            "from_username": sender_username,
            "to_user_id": target_user_id,
            "to_username": target_username,
            "text": text,
            "timestamp": timestamp,
        }

        # Send to target user (if they're online)
        try:
            await self.manager.send_personal(target_user_id, private_message)
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

        if room_name in self.rooms:
            await self.send_error("Room already exists")
            return

        user_id = self.connection.channel_name
        username = self.users.get(user_id, {}).get("username", user_id)

        # Create room
        self.rooms[room_name] = {
            "name": room_name,
            "created_at": time.time(),
            "created_by": user_id,
            "creator_username": username,
            "is_public": is_public,
            "user_count": 0,
        }

        # Automatically join the room after creating it
        await self.join_group(room_name)
        self.users[user_id]["rooms"].add(room_name)
        self.rooms[room_name]["user_count"] += 1

        # Send room created message
        await self.send_json(
            {
                "type": "room_created",
                "room": room_name,
                "is_public": is_public,
                "timestamp": time.time(),
            }
        )

        # Send room joined confirmation (so frontend knows user is now in the room)
        await self.send_json({"type": "room_joined", "room": room_name, "timestamp": time.time()})

        # Send room info
        await self.send_room_info(room_name)

    async def handle_list_rooms(self, message: Message) -> None:
        """Handle request to list available rooms"""
        rooms_list = []
        for room_name, room_info in self.rooms.items():
            if room_info["is_public"] or room_name in self.groups:
                rooms_list.append(
                    {
                        "name": room_name,
                        "user_count": room_info["user_count"],
                        "created_by": room_info.get("creator_username", "system"),
                        "is_public": room_info["is_public"],
                    }
                )

        await self.send_json({"type": "rooms_list", "rooms": rooms_list, "timestamp": time.time()})

    async def handle_get_room_users(self, message: Message) -> None:
        """Handle request to get users in a room"""
        room = message.data.get("room")

        if not room:
            await self.send_error("Room name is required")
            return

        if room not in self.rooms:
            await self.send_error("Room not found")
            return

        # Get users in the room (this is a simplified version)
        # In a real implementation, you'd track users per room
        users_in_room = []
        for user_id, user_info in self.users.items():
            if room in user_info.get("rooms", set()):
                users_in_room.append(
                    {
                        "user_id": user_id,
                        "username": user_info["username"],
                        "status": user_info["status"],
                    }
                )

        await self.send_json(
            {"type": "room_users", "room": room, "users": users_in_room, "timestamp": time.time()}
        )

    async def handle_get_message_history(self, message: Message) -> None:
        """Handle request to get message history for a room"""
        room = message.data.get("room")
        limit = min(int(message.data.get("limit", 50)), self.max_message_history)

        if not room:
            await self.send_error("Room name is required")
            return

        if room not in self.groups:
            await self.send_error("Not in that room")
            return

        # Get messages from database
        if not self.database:
            await self.send_error("Database not available")
            return

        history = self.database.get_recent_messages(room, limit=limit)

        await self.send_json(
            {"type": "message_history", "room": room, "messages": history, "timestamp": time.time()}
        )

    async def handle_typing_start(self, message: Message) -> None:
        """Handle typing start indicator"""
        room = message.data.get("room")

        if not room or room not in self.groups:
            return  # Silently ignore invalid requests

        user_id = self.connection.channel_name
        username = self.users.get(user_id, {}).get("username", user_id)

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
                    "timestamp": time.time(),
                },
            )

    async def handle_typing_stop(self, message: Message) -> None:
        """Handle typing stop indicator"""
        room = message.data.get("room")

        if not room or room not in self.groups:
            return  # Silently ignore invalid requests

        user_id = self.connection.channel_name
        username = self.users.get(user_id, {}).get("username", user_id)

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
                    "timestamp": time.time(),
                },
            )

    async def handle_ping(self, message: Message) -> None:
        """Handle ping message"""
        await self.send_json({"type": "pong", "timestamp": time.time()})

    # Helper methods

    async def send_error(self, error_message: str, message_type: str = "error") -> None:
        """Send error message"""
        await self.send_json(
            {
                "type": "error",
                "message": error_message,
                "message_type": message_type,
                "timestamp": time.time(),
            }
        )

    async def send_room_info(self, room: str) -> None:
        """Send room information to user"""
        if room not in self.rooms:
            return

        room_info = self.rooms[room]
        await self.send_json(
            {
                "type": "room_info",
                "room": room,
                "user_count": room_info["user_count"],
                "created_by": room_info.get("creator_username", "system"),
                "is_public": room_info["is_public"],
                "timestamp": time.time(),
            }
        )

    async def send_to_group_except_sender(self, group: str, message: dict[str, Any]) -> None:
        """Send message to all users in group except the sender"""
        await self.manager.send_group_except(
            group, message, exclude_connection_id=self.connection.channel_name
        )
