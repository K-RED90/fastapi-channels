import asyncio
import contextlib
import json
import time
from typing import Any

from fastapi import WebSocket
from fastapi.websockets import WebSocketState

from core.backends.base import BackendProtocol
from core.connections.registry import Connection, ConnectionRegistry
from core.exceptions import ConnectionError
from core.typed import ConnectionState


class ConnectionManager:
    def __init__(
        self,
        backend: BackendProtocol,
        registry: ConnectionRegistry,
        max_connections_per_client: int = 10000,
        heartbeat_interval: int = 30,
        heartbeat_timeout: int = 60,
    ):
        self.backend = backend
        self.registry = registry
        self._receiver_tasks: dict[str, asyncio.Task] = {}
        self._heartbeat_task: asyncio.Task | None = None
        self._cleanup_task: asyncio.Task | None = None
        self._broadcast_task: asyncio.Task | None = None
        self._running = False
        self.max_connections_per_client = max_connections_per_client
        self.heartbeat_interval = heartbeat_interval
        self.heartbeat_timeout = heartbeat_timeout
        self._broadcast_channel = "__broadcast__"

    async def connect(
        self,
        websocket: WebSocket,
        user_id: str | None = None,
        connection_id: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> Connection:
        await websocket.accept()

        if user_id:
            current = await self.registry.user_channel_count(user_id)
            if current >= self.max_connections_per_client:
                from core.exceptions import create_error_context

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

        channel_name = await self.backend.new_channel(prefix=f"ws.{user_id}" if user_id else "ws")

        connection = await self.registry.register(
            websocket=websocket,
            connection_id=channel_name if connection_id is None else connection_id,
            user_id=user_id,
            metadata=metadata,
            heartbeat_timeout=self.heartbeat_timeout,
        )

        await self.backend.subscribe(connection.channel_name)

        receiver_task = asyncio.create_task(self._receive_loop(connection.channel_name))
        self._receiver_tasks[connection.channel_name] = receiver_task

        return connection

    async def _safe_send_json(self, connection: Connection, message: dict[str, Any]) -> bool:
        """Safely send JSON message to websocket. Returns True if sent, False otherwise."""
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
            # WebSocket already closed
            return False
        except Exception:
            return False

    async def _safe_close_websocket(self, connection: Connection, code: int = 1000) -> None:
        if connection.websocket.client_state == WebSocketState.CONNECTED:
            try:
                await connection.websocket.close(code=code)
            except RuntimeError:
                pass

    async def disconnect(self, connection_id: str, code: int = 1000) -> None:
        connection = self.registry.get(connection_id)
        if not connection:
            return

        # Idempotent: if already disconnecting or disconnected, return early
        if connection.state in (ConnectionState.DISCONNECTING, ConnectionState.DISCONNECTED):
            return

        connection.state = ConnectionState.DISCONNECTING

        if connection_id in self._receiver_tasks:
            self._receiver_tasks[connection_id].cancel()
            del self._receiver_tasks[connection_id]

        # Leave all groups - use local groups if available, otherwise query backend
        groups_to_leave = connection.groups.copy()
        if not groups_to_leave:
            # If local groups are empty, try to restore from backend
            groups_to_leave = await self.backend.registry_get_connection_groups(connection_id)

        for group in groups_to_leave:
            await self.leave_group(connection_id, group)

        await self.backend.unsubscribe(connection_id)

        await self._safe_close_websocket(connection, code=code)

        await self.registry.unregister(connection_id)
        connection.state = ConnectionState.DISCONNECTED

    async def send_personal(self, connection_id: str, message: dict[str, Any]) -> None:
        await self.backend.publish(connection_id, message)
        if conn := self.registry.get(connection_id):
            payload_bytes = json.dumps(message).encode()
            conn.message_count += 1
            conn.bytes_sent += len(payload_bytes)

    async def send_group(self, group: str, message: dict[str, Any]) -> None:
        await self.backend.group_send(group, message)

    async def send_group_except(
        self, group: str, message: dict[str, Any], exclude_connection_id: str
    ) -> None:
        connections = self.registry.get_by_group(group)
        tasks = [
            self._safe_send_json(conn, message)
            for conn in connections
            if conn.channel_name != exclude_connection_id
        ]
        await asyncio.gather(*tasks, return_exceptions=True)

    async def broadcast(self, message: dict[str, Any]) -> None:
        if self.backend.supports_broadcast_channel():
            await self.backend.publish(self._broadcast_channel, message)
        else:
            tasks = [self._safe_send_json(conn, message) for conn in self.registry.get_all()]
            await asyncio.gather(*tasks, return_exceptions=True)

    async def join_group(self, connection_id: str, group: str) -> None:
        await self.backend.group_add(group, connection_id)
        await self.registry.add_to_group(connection_id, group)

    async def leave_group(self, connection_id: str, group: str) -> None:
        await self.backend.group_discard(group, connection_id)
        await self.registry.remove_from_group(connection_id, group)

    async def get_user_connections(self, user_id: str) -> list[Connection]:
        channels = await self.registry.user_channels(user_id)
        results: list[Connection] = []
        for ch in channels:
            conn = self.registry.get(ch)
            if conn:
                results.append(conn)
        return results

    async def send_to_user(self, user_id: str, message: dict[str, Any]) -> int:
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

                if not await self._safe_send_json(connection, message):
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
        if self._running:
            return
        self._running = True
        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())
        self._cleanup_task = asyncio.create_task(self._cleanup_loop())
        if self.backend.supports_broadcast_channel():
            try:
                await self.backend.subscribe(self._broadcast_channel)
                self._broadcast_task = asyncio.create_task(self._broadcast_loop())
            except Exception:
                self._broadcast_task = None
        else:
            self._broadcast_task = None

    async def stop_tasks(self) -> None:
        self._running = False
        for task in [self._heartbeat_task, self._cleanup_task, self._broadcast_task]:
            if task:
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await task

    async def _heartbeat_loop(self) -> None:
        while self._running:
            try:
                await asyncio.sleep(self.heartbeat_interval)
                dead: list[str] = []

                for conn in self.registry.get_all():
                    if not conn.is_alive:
                        dead.append(conn.channel_name)
                        continue

                    if not await self._safe_send_json(
                        conn, {"type": "ping", "timestamp": time.time()}
                    ):
                        dead.append(conn.channel_name)
                    else:
                        conn.heartbeat.record_ping()
                        conn.heartbeat.increment_missed()

                for channel in dead:
                    await self.disconnect(channel, code=1011)
            except asyncio.CancelledError:
                break

    async def _cleanup_loop(self) -> None:
        while self._running:
            try:
                await asyncio.sleep(60)
                connections = self.registry.get_all()
                total_msgs = sum(c.message_count for c in connections)
                total_bytes = sum(c.bytes_sent + c.bytes_received for c in connections)
                unique_users = {c.user_id for c in connections if c.user_id}
                print(
                    f"[ws] stats connections={len(connections)} "
                    f"users={len(unique_users)} "
                    f"messages={total_msgs} bytes={total_bytes}"
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

                for conn in self.registry.get_all():
                    if not await self._safe_send_json(conn, message):
                        # Send failed, disconnect if still connected
                        if conn.state == ConnectionState.CONNECTED:
                            await self.disconnect(conn.channel_name, code=1011)
            except asyncio.CancelledError:
                break
            except TimeoutError:
                # Timeout is expected - just continue the loop
                continue
            except Exception:
                await asyncio.sleep(1)
