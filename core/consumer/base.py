import json
from abc import abstractmethod
from typing import Any

from core.backends.base import BackendProtocol
from core.connections.manager import ConnectionManager
from core.connections.state import Connection
from core.exceptions import (
    AuthenticationError,
    MessageError,
    ValidationError,
    create_error_context,
)
from core.middleware.base import Middleware
from core.typed import ConsumerProtocol, Message, MessagePriority


class BaseConsumer(ConsumerProtocol):
    def __init__(
        self,
        connection: Connection,
        manager: ConnectionManager,
        backend: BackendProtocol,
        middleware_stack: Middleware | None = None,
    ):
        self.connection = connection
        self.manager = manager
        self.backend = backend
        self.groups: set[str] = set()
        self.middleware_stack = middleware_stack

    @abstractmethod
    async def connect(self) -> None:
        pass

    @abstractmethod
    async def disconnect(self, code: int) -> None:
        pass

    @abstractmethod
    async def receive(self, message: Message) -> None:
        pass

    async def send(self, message: Message) -> None:
        await self.connection.websocket.send_json(message.to_dict())

    async def send_json(self, data: dict[str, Any]) -> None:
        message = Message(type=data.get("type") or "message", data=data)
        await self.send(message)

    async def join_group(self, group: str) -> None:
        await self.manager.join_group(self.connection.channel_name, group)
        self.groups.add(group)

    async def leave_group(self, group: str) -> None:
        await self.manager.leave_group(self.connection.channel_name, group)
        self.groups.discard(group)

    async def send_to_group(self, group: str, message: dict[str, Any] | Message) -> None:
        payload = message.to_dict() if isinstance(message, Message) else message
        await self.manager.send_group(group, payload)

    async def handle_message(self, raw_message: str) -> None:
        try:
            data = json.loads(raw_message)
            message_type = data.get("type", "message")

            if message_type == "pong":
                self.connection.update_heartbeat()
                return

            priority_value = data.get("priority", MessagePriority.NORMAL.value)
            priority = (
                MessagePriority(priority_value)
                if priority_value in MessagePriority._value2member_map_
                else MessagePriority.NORMAL
            )
            message = Message(
                type=message_type,
                data=data.get("data"),
                sender_id=self.connection.channel_name,
                metadata=data.get("metadata"),
                ttl_seconds=data.get("ttl_seconds"),
                priority=priority,
            )

            self.connection.bytes_received += len(raw_message.encode())
            self.connection.update_activity()

            if self.middleware_stack:
                message = await self.middleware_stack(message, self.connection, self)
                if not message:
                    return

            await self.receive(message)

        except json.JSONDecodeError as e:
            context = create_error_context(
                user_id=self.connection.user_id,
                connection_id=self.connection.channel_name,
                component="consumer",
            )
            raise ValidationError(
                message="Invalid JSON format",
                error_code="INVALID_JSON",
                context=context,
                details={"parse_error": str(e)},
            ) from e

        except (AuthenticationError, ValidationError, MessageError) as e:
            error_response = e.to_response()
            await self.connection.websocket.send_json(error_response.to_dict())
