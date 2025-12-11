import json
from abc import abstractmethod
from typing import TYPE_CHECKING, Any

from fastapi_channels.connections import Connection, ConnectionManager
from fastapi_channels.exceptions import BaseError, ValidationError, create_error_context
from fastapi_channels.middleware import Middleware
from fastapi_channels.typed import Message, MessagePriority


class BaseConsumer:
    """Abstract base class for WebSocket consumer implementations.

    BaseConsumer provides the core functionality for handling WebSocket connections,
    processing incoming messages, and managing group memberships. It serves as the
    foundation for application-specific WebSocket consumers.

    The consumer lifecycle:
    1. connect() - Called when WebSocket connection is established
    2. receive() - Called for each incoming message
    3. disconnect() - Called when connection is closed

    Key features:
    - Automatic message parsing and validation
    - Middleware support for message processing
    - Group management for broadcast messaging
    - Error handling with structured error responses
    - Connection state tracking

    Parameters
    ----------
    connection : Connection
        WebSocket connection state and metadata
    manager : ConnectionManager
        Manager for connection lifecycle and messaging. Required.
    middleware_stack : Middleware | None, optional
        Message processing middleware chain. Default: None

    Examples
    --------
    Basic consumer implementation:

    >>> class ChatConsumer(BaseConsumer):
    ...     async def connect(self) -> None:
    ...         await self.join_group("general")
    ...         await self.send_json({"type": "welcome"})
    ...
    ...     async def receive(self, message: Message) -> None:
    ...         if message.type == "chat":
    ...             await self.send_to_group("general", message)
    ...
    ...     async def on_disconnect(self, code: int) -> None:
    ...         await self.leave_group("general")

    Notes
    -----
    Subclasses must implement connect(), on_disconnect(), and receive() methods.
    Messages are automatically parsed from JSON and validated.
    Heartbeat messages ("pong") are handled automatically.
    The disconnect() method is provided by BaseConsumer and handles both
    consumer cleanup (via on_disconnect()) and connection manager disconnection.

    """

    def __init__(
        self,
        connection: Connection,
        manager: ConnectionManager,
        middleware_stack: Middleware | None = None,
    ):
        if manager is None:
            raise ValueError("'manager' must be provided")

        self.connection = connection
        self.manager = manager
        self.middleware_stack = middleware_stack

    @abstractmethod
    async def connect(self) -> None:
        """Called when WebSocket connection is established.

        Implement this method to perform connection setup tasks such as:
        - Authentication checks
        - Initial group subscriptions
        - Sending welcome messages
        - Setting up connection-specific state

        Raises
        ------
        AuthenticationError
            If connection authentication fails
        ValidationError
            If connection setup validation fails

        """

    async def disconnect(self, code: int = 1000) -> None:
        """Disconnect the WebSocket connection and perform cleanup.

        This method handles both consumer-specific cleanup and connection manager
        disconnection. Subclasses should override `on_disconnect()` for custom cleanup.

        Parameters
        ----------
        code : int, optional
            WebSocket close code (e.g., 1000 for normal closure). Default: 1000

        Notes
        -----
        This method:
        1. Calls `on_disconnect()` for consumer-specific cleanup
        2. Disconnects from the connection manager (handles WebSocket closure, group cleanup, etc.)

        Subclasses should override `on_disconnect()` instead of `disconnect()` to add
        custom cleanup logic.

        """
        await self.on_disconnect(code)
        await self.manager.disconnect(self.connection.channel_name, code=code)

    @abstractmethod
    async def on_disconnect(self, code: int) -> None:
        """Hook for consumer-specific cleanup when connection is closed.

        Parameters
        ----------
        code : int
            WebSocket close code (e.g., 1000 for normal closure)

        Override this method to perform cleanup tasks such as:
        - Leaving subscribed groups
        - Cleaning up connection-specific resources
        - Logging disconnection events
        - Updating database state

        Notes
        -----
        This method is called before the connection manager disconnects the WebSocket.
        The manager will handle group cleanup automatically, but you can perform
        additional cleanup here if needed.

        """

    @abstractmethod
    async def receive(self, message: Message) -> None:
        """Process incoming WebSocket message.

        Parameters
        ----------
        message : Message
            Parsed and validated message from client

        This method is called for each message received after:
        - JSON parsing and validation
        - Middleware processing
        - Message priority handling

        Implement message type routing and business logic here.

        Raises
        ------
        MessageError
            If message processing fails
        ValidationError
            If message content is invalid

        """

    async def send(self, message: Message) -> None:
        """Send a Message object to the connected WebSocket client.

        Parameters
        ----------
        message : Message
            Message to send to client

        Notes
        -----
        Routes to appropriate WebSocket send method based on message content:
        - If binary_data is set: uses send_bytes()
        - If data is set: uses send_json()
        Updates connection statistics (message count, bytes sent).

        """
        if message.binary_data is not None:
            await self.connection.websocket.send_bytes(message.binary_data)
            self.connection.message_count += 1
            self.connection.bytes_sent += len(message.binary_data)
        else:
            await self.connection.websocket.send_json(message.to_dict())
            payload_bytes = json.dumps(message.to_dict()).encode()
            self.connection.message_count += 1
            self.connection.bytes_sent += len(payload_bytes)
        self.connection.update_activity()

    async def send_text(self, data: str) -> None:
        """Send plain text message to the connected client.

        Parameters
        ----------
        data : str
            Plain text data to send

        Notes
        -----
        Sends directly as text, no Message wrapper.
        Updates connection statistics (message count, bytes sent).

        """
        await self.connection.websocket.send_text(data)
        self.connection.message_count += 1
        self.connection.bytes_sent += len(data.encode())
        self.connection.update_activity()

    async def send_json(self, data: dict[str, Any] | list) -> None:
        """Send JSON data to the connected client.

        Parameters
        ----------
        data : dict[str, Any] | list
            JSON-serializable data to send (dict or list)

        Notes
        -----
        Sends directly as JSON, no Message wrapper, no string conversion.
        Updates connection statistics (message count, bytes sent).

        """
        await self.connection.websocket.send_json(data)
        payload_bytes = json.dumps(data).encode()
        self.connection.message_count += 1
        self.connection.bytes_sent += len(payload_bytes)
        self.connection.update_activity()

    async def send_bytes(self, data: bytes) -> None:
        """Send binary data to the connected client.

        Parameters
        ----------
        data : bytes
            Binary data to send

        Notes
        -----
        Sends directly as bytes, no Message wrapper.
        Updates connection statistics (message count, bytes sent).

        """
        await self.connection.websocket.send_bytes(data)
        self.connection.message_count += 1
        self.connection.bytes_sent += len(data)
        self.connection.update_activity()

    async def join_group(self, group: str) -> None:
        """Add connection to a messaging group.

        Parameters
        ----------
        group : str
            Group name to join

        Notes
        -----
        Updates manager which updates connection.groups (source of truth).
        Enables receiving group messages.

        """
        await self.manager.join_group(self.connection.channel_name, group)

    async def leave_group(self, group: str) -> None:
        """Remove connection from a messaging group.

        Parameters
        ----------
        group : str
            Group name to leave

        Notes
        -----
        Updates manager which updates connection.groups (source of truth).
        Stops receiving group messages.

        """
        await self.manager.leave_group(self.connection.channel_name, group)

    async def send_to_group(self, group: str, message: dict[str, Any] | Message) -> None:
        """Send message to all connections in a group.

        Parameters
        ----------
        group : str
            Target group name
        message : dict[str, Any] | Message
            Message to send to group

        Notes
        -----
        Automatically serializes Message objects.
        Sends to all connections in group except sender.

        """
        payload = message.to_dict() if isinstance(message, Message) else message
        await self.manager.send_group(group, payload)

    async def handle_message(
        self,
        json_str: str | None = None,
        binary: bytes | None = None,
    ) -> None:
        """Process incoming WebSocket message.

        Main entry point for incoming WebSocket messages. Handles JSON string and binary messages.

        Parameters
        ----------
        json_str : str | None, optional
            JSON message as string (will be parsed). Default: None
        binary : bytes | None, optional
            Binary message. Default: None

        Raises
        ------
        ValidationError
            If no parameter provided, multiple parameters provided, or JSON parsing fails
        AuthenticationError
            If message requires authentication
        MessageError
            If message processing fails

        Notes
        -----
        Exactly one parameter must be provided.
        JSON strings are always parsed - message type is determined from parsed JSON.
        Handles heartbeat ("pong") messages automatically.
        Tracks message statistics (bytes received, activity).

        """
        provided = sum(1 for param in [json_str, binary] if param is not None)
        if provided != 1:
            context = create_error_context(
                user_id=self.connection.user_id,
                connection_id=self.connection.channel_name,
                component="consumer",
            )
            raise ValidationError(
                message="Exactly one of 'json_str' or 'binary' must be provided",
                error_code="INVALID_MESSAGE_PARAMS",
                context=context,
            )

        if binary is not None:
            message = Message(
                type="binary",
                data=None,
                binary_data=binary,
                sender_id=self.connection.channel_name,
            )
            self.connection.bytes_received += len(binary)
            self.connection.update_activity()

            if self.middleware_stack:
                message = await self.middleware_stack(message, self.connection, self)
                if not message:
                    return

            await self.receive(message)
            return

        if json_str is not None:
            context = create_error_context(
                user_id=self.connection.user_id,
                connection_id=self.connection.channel_name,
                component="consumer",
            )

            try:
                json_data = json.loads(json_str)
            except json.JSONDecodeError as e:
                raise ValidationError(
                    message="Invalid JSON format",
                    error_code="INVALID_JSON",
                    context=context,
                    details={"parse_error": str(e)},
                ) from e

            if not isinstance(json_data, dict):
                raise ValidationError(
                    message="JSON message must be an object with a 'type' field",
                    error_code="INVALID_MESSAGE_FORMAT",
                    context=context,
                    details={"received_type": type(json_data).__name__},
                )

            if "type" not in json_data:
                raise ValidationError(
                    message="JSON message missing required 'type'",
                    error_code="MISSING_MESSAGE_TYPE",
                    context=context,
                    details={"payload_keys": list(json_data.keys())},
                )

            message_type = json_data["type"]
            if message_type == "pong":
                self.connection.update_heartbeat()
                return

            priority_value = json_data.get("priority", MessagePriority.NORMAL.value)
            priority = (
                MessagePriority(priority_value)
                if priority_value in MessagePriority._value2member_map_
                else MessagePriority.NORMAL
            )

            message = Message(
                type=message_type,
                data=json_data.get("data"),
                sender_id=self.connection.channel_name,
                metadata=json_data.get("metadata"),
                ttl_seconds=json_data.get("ttl_seconds"),
                priority=priority,
            )

            self.connection.bytes_received += len(json_str.encode())
            self.connection.update_activity()

            if self.middleware_stack:
                message = await self.middleware_stack(message, self.connection, self)
                if not message:
                    return

            await self.receive(message)
            return

    async def handle_error(self, error: BaseError, as_text: bool = False) -> None:
        """Handle error and send error response to client.

        Parameters
        ----------
        error : BaseError
            Error to handle
        as_text : bool, optional
            If True, send error as plain text. If False, send as JSON. Default: False

        """
        error_response = error.to_response()
        if as_text:
            await self.send_text(str(error_response.message))
        else:
            await self.send_json(error_response.to_dict())
