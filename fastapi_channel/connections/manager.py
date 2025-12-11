import asyncio
import base64
import contextlib
import json
import logging
import time
from typing import TYPE_CHECKING, Any

from fastapi import WebSocket
from fastapi.websockets import WebSocketState

from fastapi_channel.connections.registry import ConnectionRegistry
from fastapi_channel.connections.state import Connection
from fastapi_channel.exceptions import ConnectionError, create_error_context
from fastapi_channel.typed import ConnectionState
from fastapi_channel.utils import run_with_concurrency_limit

if TYPE_CHECKING:
    from fastapi_channel.backends import BaseBackend


class ConnectionManager:
    """Central manager for WebSocket connection lifecycle and messaging.

    ConnectionManager orchestrates WebSocket connections across the application,
    providing high-level APIs for connection management, group messaging, and
    broadcast communication. It coordinates between the connection registry,
    channel layer backend, and individual WebSocket connections.

    Key responsibilities:
    - Connection establishment and teardown
    - Group membership management
    - Message routing (personal, group, broadcast)
    - Heartbeat monitoring and cleanup
    - Connection limits enforcement
    - Background task coordination

    The manager runs several background tasks:
    - Heartbeat loop: Sends ping messages and monitors connection health
    - Statistics loop: Periodic statistics logging
    - Broadcast loop: Handles global broadcast messages (Redis backend only)

    Parameters
    ----------
    registry : ConnectionRegistry
        Registry for tracking active connections
    max_connections_per_client : int, optional
        Maximum connections per user. Default: 10000
    heartbeat_interval : int, optional
        Heartbeat ping interval in seconds. Default: 30
    server_instance_id : str | None, optional
        Unique identifier for this server instance. Auto-generated if not provided.
        Used for distributed deployment tracking and debugging. Default: None

    Examples
    --------
    Basic setup with memory backend:

    >>> registry = ConnectionRegistry(backend=MemoryBackend())
    >>> manager = ConnectionManager(
    ...     registry=registry,
    ...     max_connections_per_client=5,
    ...     heartbeat_interval=30
    ... )
    >>> await manager.start_tasks()  # Start background tasks

    Managing connections:

    >>> connection = await manager.connect(websocket, user_id="alice")
    >>> await manager.send_personal(connection.channel_name, {"hello": "world"})
    >>> await manager.disconnect(connection.channel_name)

    Notes
    -----
    Background tasks must be started with start_tasks() and stopped with stop_tasks().
    Connection limits are enforced per user to prevent abuse.
    Broadcast functionality requires Redis backend support.
    Server instance ID is automatically added to connection metadata.

    """

    logger = logging.getLogger(__name__)

    def __init__(
        self,
        registry: ConnectionRegistry,
        max_connections_per_client: int = 10000,
        heartbeat_interval: int = 30,
        server_instance_id: str | None = None,
        log_stats: bool = True,
        enable_heartbeat: bool = True,
    ):
        self._registry = registry
        self._receiver_tasks: dict[str, asyncio.Task] = {}
        self._heartbeat_task: asyncio.Task | None = None
        self._stats_task: asyncio.Task | None = None
        self._broadcast_task: asyncio.Task | None = None
        self._running = False
        self.max_connections_per_client = max_connections_per_client
        self.heartbeat_interval = heartbeat_interval
        self._broadcast_channel = "__broadcast__"

        self.server_instance_id = server_instance_id or self._generate_instance_id()
        self.log_stats = log_stats
        self.enable_heartbeat = enable_heartbeat

    @staticmethod
    def _generate_instance_id() -> str:
        import uuid

        return f"server-{uuid.uuid4().hex[:12]}"

    @property
    def backend(self) -> "BaseBackend":
        return self.registry.backend

    @property
    def registry(self) -> ConnectionRegistry:
        return self._registry

    async def connect(
        self,
        websocket: WebSocket,
        user_id: str | None = None,
        connection_id: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> Connection:
        """Establish new WebSocket connection.

        Parameters
        ----------
        websocket : WebSocket
            FastAPI WebSocket instance
        user_id : str | None, optional
            User identifier for authenticated connections. Default: None
        connection_id : str | None, optional
            Custom connection identifier. Default: None (auto-generated)
        metadata : dict[str, Any] | None, optional
            Additional connection metadata. Default: None

        Returns
        -------
        Connection
            Connection object representing the established connection

        Raises
        ------
        ConnectionError
            If connection limit exceeded or registration fails

        Notes
        -----
        Performs connection limit checks, registers connection in backend,
        subscribes to personal channel, and starts message receiver task.

        """
        if user_id:
            current = await self.registry.user_channel_count(user_id)
            if current >= self.max_connections_per_client:
                context = create_error_context(
                    user_id=user_id,
                    component="connection_manager",
                    current_connections=current,
                    max_connections=self.max_connections_per_client,
                )
                raise ConnectionError(
                    f"User {user_id} exceeded connection limit ({self.max_connections_per_client})",
                    error_code="CONNECTION_LIMIT_EXCEEDED",
                    context=context,
                )

        await websocket.accept()

        channel_name = await self.backend.new_channel(prefix=f"ws.{user_id}" if user_id else "ws")

        # Add server instance ID to connection metadata for distributed tracking
        connection_metadata = metadata.copy() if metadata else {}
        connection_metadata["server_instance_id"] = self.server_instance_id

        connection = await self.registry.register(
            websocket=websocket,
            connection_id=channel_name if connection_id is None else connection_id,
            user_id=user_id,
            metadata=connection_metadata,
            heartbeat_timeout=self.registry.heartbeat_timeout,
        )

        await self.registry.backend.subscribe(connection.channel_name)

        receiver_task = asyncio.create_task(self._receive_loop(connection.channel_name))
        self._receiver_tasks[connection.channel_name] = receiver_task

        return connection

    async def _safe_send_json(self, connection: Connection, message: dict[str, Any]) -> bool:
        if connection.state != ConnectionState.CONNECTED:
            return False
        if connection.websocket.client_state != WebSocketState.CONNECTED:
            return False

        try:
            await connection.websocket.send_json(message)
            payload_bytes = json.dumps(message).encode()
            connection.message_count += 1
            connection.bytes_sent += len(payload_bytes)
            connection.update_activity()
            return True
        except RuntimeError:
            return False
        except Exception:
            return False

    async def _safe_send_text(self, connection: Connection, text: str) -> bool:
        """Safely send text message to connection.

        Parameters
        ----------
        connection : Connection
            Target connection
        text : str
            Text message to send

        Returns
        -------
        bool
            True if sent successfully, False otherwise

        """
        if connection.state != ConnectionState.CONNECTED:
            return False
        if connection.websocket.client_state != WebSocketState.CONNECTED:
            return False

        try:
            await connection.websocket.send_text(text)
            connection.message_count += 1
            connection.bytes_sent += len(text.encode())
            connection.update_activity()
            return True
        except RuntimeError:
            return False
        except Exception:
            return False

    async def _safe_send_bytes(self, connection: Connection, data: bytes) -> bool:
        """Safely send binary message to connection.

        Parameters
        ----------
        connection : Connection
            Target connection
        data : bytes
            Binary data to send

        Returns
        -------
        bool
            True if sent successfully, False otherwise

        """
        if connection.state != ConnectionState.CONNECTED:
            return False
        if connection.websocket.client_state != WebSocketState.CONNECTED:
            return False

        try:
            await connection.websocket.send_bytes(data)
            connection.message_count += 1
            connection.bytes_sent += len(data)
            connection.update_activity()
            return True
        except RuntimeError:
            return False
        except Exception:
            return False

    async def _safe_send_message(self, connection: Connection, message: dict[str, Any]) -> bool:
        """Safely send message to connection, routing based on content type.

        Parameters
        ----------
        connection : Connection
            Target connection
        message : dict[str, Any]
            Message payload. If contains 'binary_data' (base64-encoded), sends as bytes.
            Otherwise sends as JSON.

        Returns
        -------
        bool
            True if sent successfully, False otherwise

        """
        if "binary_data" in message:
            binary_data = await asyncio.to_thread(base64.b64decode, message["binary_data"])
            return await self._safe_send_bytes(connection, binary_data)
        else:
            return await self._safe_send_json(connection, message)

    async def _safe_close_websocket(self, connection: Connection, code: int = 1000) -> None:
        if connection.websocket.client_state == WebSocketState.CONNECTED:
            try:
                await connection.websocket.close(code=code)
            except RuntimeError:
                pass

    async def disconnect(self, connection_id: str, code: int = 1000) -> None:
        """Close WebSocket connection and perform cleanup.

        Parameters
        ----------
        connection_id : str
            Connection identifier to disconnect
        code : int, optional
            WebSocket close code. Default: 1000 (normal closure)

        Notes
        -----
        Idempotent operation - safe to call multiple times.
        Performs graceful cleanup: cancels receiver task, leaves groups,
        unsubscribes from backend, closes WebSocket, unregisters connection.

        """
        connection = self.registry.get(connection_id)
        if not connection:
            return

        if connection.state in (ConnectionState.DISCONNECTING, ConnectionState.DISCONNECTED):
            return

        connection.state = ConnectionState.DISCONNECTING

        if connection_id in self._receiver_tasks:
            self._receiver_tasks[connection_id].cancel()
            del self._receiver_tasks[connection_id]

        groups_to_leave = connection.groups.copy()
        if not groups_to_leave:
            groups_to_leave = await self.backend.registry_get_connection_groups(connection_id)

        for group in groups_to_leave:
            await self.leave_group(connection_id, group)

        await self.backend.unsubscribe(connection_id)

        await self._safe_close_websocket(connection, code=code)

        await self.registry.unregister(connection_id)
        connection.state = ConnectionState.DISCONNECTED

    async def send_personal(self, connection_id: str, message: dict[str, Any]) -> None:
        """Send message to specific connection.

        Parameters
        ----------
        connection_id : str
            Target connection identifier
        message : dict[str, Any]
            Message payload to send

        Notes
        -----
        Updates connection statistics (message count, bytes sent).
        Message delivery handled by backend publish mechanism.

        """
        await self.backend.publish(connection_id, message)
        if conn := self.registry.get(connection_id):
            payload_bytes = json.dumps(message).encode()
            conn.message_count += 1
            conn.bytes_sent += len(payload_bytes)

    async def send_group(self, group: str, message: dict[str, Any]) -> None:
        """Send message to all connections in a group.

        Parameters
        ----------
        group : str
            Target group name
        message : dict[str, Any]
            Message payload to send

        Notes
        -----
        Uses backend group_send for efficient multi-connection delivery.

        """
        await self.backend.group_send(group, message)

    async def send_group_except(
        self, group: str, message: dict[str, Any], exclude_connection_id: str
    ) -> None:
        """Send message to group excluding specific connection.

        Parameters
        ----------
        group : str
            Target group name
        message : dict[str, Any]
            Message payload to send
        exclude_connection_id : str
            Connection ID to exclude from message delivery

        Notes
        -----
        Useful for echo prevention in chat applications.
        Uses backend group_send for efficient multi-connection delivery across all servers.
        Avoids loading all connections into memory by using backend streaming.

        """
        await self.backend.group_send(group, message, exclude_channel=exclude_connection_id)

    async def broadcast(self, message: dict[str, Any]) -> None:
        """Send message to all active connections.

        Parameters
        ----------
        message : dict[str, Any]
            Message payload to broadcast

        Notes
        -----
        Uses broadcast channel if backend supports it (Redis only),
        otherwise iterates through all connections in registry.

        """
        if self.backend.supports_broadcast_channel():
            await self.backend.publish(self._broadcast_channel, message)
        else:
            tasks = [self._safe_send_json(conn, message) for conn in self.registry.get_all()]
            await run_with_concurrency_limit(tasks, max_concurrent=100, return_exceptions=True)

    async def join_group(self, connection_id: str, group: str) -> None:
        """Add connection to a messaging group.

        Parameters
        ----------
        connection_id : str
            Connection identifier to add to group
        group : str
            Group name to join

        Notes
        -----
        Updates both backend group membership and local registry.
        Enables connection to receive group messages.

        """
        await self.backend.group_add(group, connection_id)
        await self.registry.add_to_group(connection_id, group)

    async def leave_group(self, connection_id: str, group: str) -> None:
        """Remove connection from a messaging group.

        Parameters
        ----------
        connection_id : str
            Connection identifier to remove from group
        group : str
            Group name to leave

        Notes
        -----
        Updates both backend group membership and local registry.
        Stops connection from receiving group messages.

        """
        await self.backend.group_discard(group, connection_id)
        await self.registry.remove_from_group(connection_id, group)

    async def get_user_connections(self, user_id: str) -> list[Connection]:
        """Get all active connections for a user.

        Parameters
        ----------
        user_id : str
            User identifier to query

        Returns
        -------
        list[Connection]
            List of active Connection objects for the user

        Notes
        -----
        Returns only connections that exist in local registry.
        May not include connections from other servers in distributed setup.

        """
        channels = await self.registry.user_channels(user_id)
        results: list[Connection] = []
        for ch in channels:
            conn = self.registry.get(ch)
            if conn:
                results.append(conn)
        return results

    async def send_to_user(self, user_id: str, message: dict[str, Any]) -> int:
        """Send message to all connections of a user.

        Parameters
        ----------
        user_id : str
            Target user identifier
        message : dict[str, Any]
            Message payload to send

        Returns
        -------
        int
            Number of connections the message was sent to

        Notes
        -----
        Sends message to all active connections for the user.
        Returns count of successful sends.

        """
        count = 0
        channels = await self.registry.user_channels(user_id)
        for channel in channels:
            await self.send_personal(channel, message)
            count += 1
        return count

    async def _receive_loop(self, channel_name: str) -> None:
        connection = self.registry.get(channel_name)
        if not connection:
            return

        while connection.state == ConnectionState.CONNECTED:
            try:
                message = await self.backend.receive(channel_name, timeout=1)
                if not message:
                    continue

                if not await self._safe_send_message(connection, message):
                    # Send failed, likely connection closed
                    break
            except asyncio.CancelledError:
                break
            except TimeoutError:
                continue
            except Exception:
                # Only disconnect if not already disconnecting
                if connection.state == ConnectionState.CONNECTED:
                    await self.disconnect(channel_name, code=1011)
                break

    async def start_tasks(self) -> None:
        """Start background maintenance tasks.

        Starts heartbeat, statistics, and broadcast tasks.
        Should be called during application startup.

        Notes
        -----
        Idempotent - safe to call multiple times.
        Broadcast task only started if backend supports broadcast channel.

        """
        if self._running:
            return
        self._running = True

        connection_stats = await self.backend.cleanup_stale_connections(self.server_instance_id)
        group_stats = await self.backend.cleanup_orphaned_group_members()
        self.logger.info(
            "Redis cleanup completed: connections_removed=%s user_mappings_cleaned=%s "
            "orphaned_members_removed=%s empty_groups_removed=%s",
            connection_stats["connections_removed"],
            connection_stats["user_mappings_cleaned"],
            group_stats["orphaned_members_removed"],
            group_stats["empty_groups_removed"],
        )

        if self.enable_heartbeat:
            self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())

        if self.log_stats:
            self._stats_task = asyncio.create_task(self._stats_loop())

        if self.backend.supports_broadcast_channel():
            try:
                await self.backend.subscribe(self._broadcast_channel)
                self._broadcast_task = asyncio.create_task(self._broadcast_loop())
            except Exception:
                self._broadcast_task = None
        else:
            self._broadcast_task = None

    async def stop_tasks(self, timeout: float = 10.0) -> None:
        """Stop all background maintenance tasks with graceful shutdown.

        Performs graceful shutdown by:
        1. Signaling all background tasks to stop
        2. Waiting for in-flight operations to complete (with timeout)
        3. Closing all active connections gracefully
        4. Cleaning up receiver tasks
        5. Cleaning up backend resources

        Parameters
        ----------
        timeout : float, optional
            Maximum seconds to wait for graceful shutdown. Default: 10.0

        Notes
        -----
        After timeout, remaining tasks are forcefully cancelled.
        This ensures the shutdown completes within a bounded time.

        """
        self._running = False

        background_tasks = [
            task
            for task in [self._heartbeat_task, self._stats_task, self._broadcast_task]
            if task is not None
        ]

        for task in background_tasks:
            task.cancel()

        if background_tasks:
            _, pending = await asyncio.wait(
                background_tasks,
                timeout=timeout / 3,
                return_when=asyncio.ALL_COMPLETED,
            )
            # Force cancel any remaining
            for task in pending:
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await task

        disconnect_tasks: list[asyncio.Task] = []
        async for batch in self.registry.iter_connections(batch_size=50):
            for conn in batch:
                task = asyncio.create_task(
                    self.disconnect(conn.channel_name, code=1001)  # 1001 = going away
                )
                disconnect_tasks.append(task)

        if disconnect_tasks:
            _, pending = await asyncio.wait(
                disconnect_tasks,
                timeout=timeout / 3,
                return_when=asyncio.ALL_COMPLETED,
            )
            for task in pending:
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await task

        receiver_tasks = list(self._receiver_tasks.values())
        for task in receiver_tasks:
            task.cancel()

        if receiver_tasks:
            _, pending = await asyncio.wait(
                receiver_tasks,
                timeout=timeout / 3,
                return_when=asyncio.ALL_COMPLETED,
            )
            for task in pending:
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await task

        self._receiver_tasks.clear()

        try:
            await self.backend.cleanup()
        except Exception:
            self.logger.exception("Failed to cleanup backend")
        finally:
            self.logger.info("Backend cleanup completed")

    async def _heartbeat_loop(self) -> None:
        while self._running:
            try:
                await asyncio.sleep(self.heartbeat_interval)
                dead: list[str] = []

                async def check_connection(conn: Connection) -> str | None:
                    """Check single connection health and send ping."""
                    if not conn.is_alive:
                        return conn.channel_name

                    if not await self._safe_send_json(
                        conn, {"type": "ping", "timestamp": time.time()}
                    ):
                        return conn.channel_name
                    conn.heartbeat.record_ping()
                    conn.heartbeat.increment_missed()
                    try:
                        await self.backend.registry_refresh_ttl(conn.channel_name, conn.user_id)
                    except Exception:
                        self.logger.warning(
                            "Failed to refresh TTL for connection %s", conn.channel_name
                        )  # Best effort - don't fail heartbeat if TTL refresh fails
                    return None

                async for batch in self.registry.iter_connections(batch_size=100):
                    tasks = [check_connection(conn) for conn in batch]
                    results = await run_with_concurrency_limit(
                        tasks, max_concurrent=50, return_exceptions=True
                    )
                    for result in results:
                        if isinstance(result, str):
                            dead.append(result)

                disconnect_tasks = [self.disconnect(channel, code=1011) for channel in dead]
                if disconnect_tasks:
                    await run_with_concurrency_limit(
                        disconnect_tasks, max_concurrent=50, return_exceptions=True
                    )
            except asyncio.CancelledError:
                break

    async def _stats_loop(self) -> None:
        while self._running:
            try:
                await asyncio.sleep(60)
                total_connections = 0
                total_msgs = 0
                total_bytes = 0
                unique_users: set[str] = set()

                # Stream connections in batches for memory efficiency
                async for batch in self.registry.iter_connections(batch_size=100):
                    total_connections += len(batch)
                    for c in batch:
                        total_msgs += c.message_count
                        total_bytes += c.bytes_sent + c.bytes_received
                        if c.user_id:
                            unique_users.add(c.user_id)

                self.logger.info(
                    "[ws] stats connections=%s users=%s messages=%s bytes=%s",
                    total_connections,
                    len(unique_users),
                    total_msgs,
                    total_bytes,
                )
            except asyncio.CancelledError:
                break
            except Exception:
                await asyncio.sleep(60)

    async def _broadcast_loop(self) -> None:
        while self._running:
            try:
                message = await self.backend.receive(self._broadcast_channel, timeout=1)
                if not message:
                    continue

                broadcast_message: dict[str, Any] = message
                dead: list[str] = []

                async def send_to_connection(conn: Connection) -> str | None:
                    """Send message to single connection, return channel_name if failed."""
                    if not await self._safe_send_message(conn, broadcast_message):
                        if conn.state == ConnectionState.CONNECTED:
                            return conn.channel_name
                    return None

                async for batch in self.registry.iter_connections(batch_size=100):
                    tasks = [send_to_connection(conn) for conn in batch]
                    results = await run_with_concurrency_limit(
                        tasks, max_concurrent=50, return_exceptions=True
                    )
                    for result in results:
                        if isinstance(result, str):
                            dead.append(result)

                disconnect_tasks = [self.disconnect(channel, code=1011) for channel in dead]
                if disconnect_tasks:
                    await run_with_concurrency_limit(
                        disconnect_tasks, max_concurrent=50, return_exceptions=True
                    )
            except asyncio.CancelledError:
                break
            except TimeoutError:
                # Timeout is expected - just continue the loop
                continue
            except Exception:
                await asyncio.sleep(1)
