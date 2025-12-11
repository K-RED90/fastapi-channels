import asyncio
import json
import time
import uuid

import pytest
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.testclient import TestClient
from redis.asyncio import Redis

from example.consumers import ChatConsumer
from example.database import ChatDatabase
from fastapi_channels.backends import MemoryBackend, RedisBackend
from fastapi_channels.config import Settings
from fastapi_channels.connections import ConnectionManager, ConnectionRegistry
from fastapi_channels.middleware import LoggingMiddleware, ValidationMiddleware


class TestWebSocketIntegration:
    """Integration tests for WebSocket chat functionality"""

    @pytest.fixture
    def setup_app(self):
        """Setup FastAPI app with WebSocket endpoint"""
        settings = Settings(
            MAX_TOTAL_CONNECTIONS=200000,
            MAX_CONNECTIONS_PER_CLIENT=1000,
            WS_MAX_MESSAGE_SIZE=10 * 1024 * 1024,
        )

        # Initialize components
        backend = MemoryBackend()
        registry = ConnectionRegistry(backend=backend)
        manager = ConnectionManager(registry)

        # Middleware stack
        middleware_stack = ValidationMiddleware(settings.WS_MAX_MESSAGE_SIZE) | LoggingMiddleware()

        # Database for persistence
        database = ChatDatabase()

        # Global state for consumers
        consumers = {}

        app = FastAPI()

        @app.websocket("/ws/{user_id}")
        async def websocket_endpoint(websocket: WebSocket, user_id: str):
            # Create connection (TestClient handles accept automatically)
            connection = await manager.connect(websocket=websocket, user_id=user_id)

            # Create consumer
            consumer = ChatConsumer(
                connection=connection,
                manager=manager,
                middleware_stack=middleware_stack,
                database=database,
            )
            consumers[connection.channel_name] = consumer

            try:
                # Handle connection
                await consumer.connect()

                # Message handling loop
                while True:
                    message = await websocket.receive()
                    if "text" in message:
                        json_str = message["text"]
                        await consumer.handle_message(json_str=json_str)
                    elif "bytes" in message:
                        binary = message["bytes"]
                        await consumer.handle_message(binary=binary)
                    else:
                        continue

            except WebSocketDisconnect:
                await consumer.disconnect(1000)
            except Exception as e:
                print(f"Error: {e}")
                await consumer.disconnect(1011)

        return app, manager, consumers, backend, database

    @pytest.mark.asyncio
    async def test_full_chat_flow(self, setup_app):
        """Test complete chat flow with multiple users"""
        app, manager, consumers, backend, database = setup_app

        # Create test client
        client = TestClient(app)

        # Connect first user
        with client.websocket_connect("/ws/user1") as ws1:
            # Receive welcome message
            welcome = json.loads(ws1.receive_text())
            assert welcome["type"] == "welcome"
            assert "user1" in welcome["data"]["message"]

            # Connect second user
            with client.websocket_connect("/ws/user2") as ws2:
                # Receive welcome for user2
                welcome2 = json.loads(ws2.receive_text())
                assert welcome2["type"] == "welcome"
                assert "user2" in welcome2["data"]["message"]

                # Both users join the lobby room
                ws1.send_text(json.dumps({"type": "join_room", "data": {"room": "lobby"}}))
                join_response1 = json.loads(ws1.receive_text())
                assert join_response1["type"] == "room_joined"
                assert join_response1["data"]["room"] == "lobby"
                # Skip room_info message
                room_info = json.loads(ws1.receive_text())
                assert room_info["type"] == "room_info"

                ws2.send_text(json.dumps({"type": "join_room", "data": {"room": "lobby"}}))
                join_response2 = json.loads(ws2.receive_text())
                assert join_response2["type"] == "room_joined"
                assert join_response2["data"]["room"] == "lobby"
                # Skip room_info message
                room_info = json.loads(ws2.receive_text())
                assert room_info["type"] == "room_info"

                # User1 sends a chat message
                ws1.send_text(
                    json.dumps(
                        {
                            "type": "chat_message",
                            "data": {"text": "Hello everyone!", "room": "lobby"},
                        }
                    )
                )

                # User1 receives their own message back (may receive user_joined_room first)
                own_message = json.loads(ws1.receive_text())
                while own_message["type"] == "user_joined_room":
                    own_message = json.loads(ws1.receive_text())  # Skip user joined notifications
                assert own_message["type"] == "chat_message"
                assert own_message["text"] == "Hello everyone!"
                assert own_message["username"] == "user1"

                # User2 should receive the message (may receive room_info or user_joined_room first)
                message = json.loads(ws2.receive_text())
                while message["type"] in ["room_info", "user_joined_room"]:
                    message = json.loads(ws2.receive_text())  # Skip notifications
                assert message["type"] == "chat_message"
                assert message["text"] == "Hello everyone!"
                assert message["username"] == "user1"

                # User2 replies
                ws2.send_text(
                    json.dumps(
                        {"type": "chat_message", "data": {"text": "Hi user1!", "room": "lobby"}}
                    )
                )

                # User2 receives their own message back
                own_reply = json.loads(ws2.receive_text())
                assert own_reply["type"] == "chat_message"
                assert own_reply["text"] == "Hi user1!"
                assert own_reply["username"] == "user2"

                # User1 receives the reply (may receive user_joined_room from gaming room first)
                reply = json.loads(ws1.receive_text())
                while reply["type"] == "user_joined_room":
                    reply = json.loads(ws1.receive_text())  # Skip user joined notifications
                assert reply["type"] == "chat_message"
                assert reply["text"] == "Hi user1!"
                assert reply["username"] == "user2"

                # User1 creates a new room
                ws1.send_text(
                    json.dumps(
                        {"type": "create_room", "data": {"room_name": "gaming", "is_public": True}}
                    )
                )

                # User1 receives confirmation
                create_response = json.loads(ws1.receive_text())
                assert create_response["type"] == "room_created"
                assert create_response["data"]["room"] == "gaming"

                # User2 joins the room
                ws2.send_text(json.dumps({"type": "join_room", "data": {"room": "gaming"}}))

                # User2 receives confirmation
                join_response = json.loads(ws2.receive_text())
                assert join_response["type"] == "room_joined"
                assert join_response["data"]["room"] == "gaming"
                # Skip room_info message
                room_info = json.loads(ws2.receive_text())
                assert room_info["type"] == "room_info"

                # User1 sends message in gaming room
                ws1.send_text(
                    json.dumps(
                        {"type": "chat_message", "data": {"text": "Let's play!", "room": "gaming"}}
                    )
                )

                # User2 receives the message (may receive user_joined_room first)
                game_message = json.loads(ws2.receive_text())
                if game_message["type"] == "user_joined_room":
                    game_message = json.loads(ws2.receive_text())  # Skip user joined notification
                assert game_message["type"] == "chat_message"
                assert game_message["text"] == "Let's play!"
                assert game_message["room"] == "gaming"

    @pytest.mark.asyncio
    async def test_private_messaging(self, setup_app):
        """Test private messaging between users"""
        app, manager, consumers, backend, database = setup_app
        client = TestClient(app)

        with client.websocket_connect("/ws/user1") as ws1:
            # Skip welcome
            ws1.receive_text()

            with client.websocket_connect("/ws/user2") as ws2:
                # Skip welcome
                ws2.receive_text()

                # User1 sends private message to user2
                ws1.send_text(
                    json.dumps(
                        {
                            "type": "private_message",
                            "data": {"target_user_id": "user2", "text": "Secret message!"},
                        }
                    )
                )

                # User2 receives private message
                private_msg = json.loads(ws2.receive_text())
                assert private_msg["type"] == "private_message"
                assert private_msg["text"] == "Secret message!"
                assert private_msg["from_username"] == "user1"

    @pytest.mark.asyncio
    async def test_room_management(self, setup_app):
        """Test room creation and listing"""
        app, manager, consumers, backend, database = setup_app
        client = TestClient(app)

        with client.websocket_connect("/ws/user1") as ws1:
            # Skip welcome
            ws1.receive_text()

            # List rooms (should see lobby)
            ws1.send_text(json.dumps({"type": "list_rooms", "data": {}}))

            rooms_response = json.loads(ws1.receive_text())
            assert rooms_response["type"] == "rooms_list"
            # Initially there may be no rooms

            # Create a room
            ws1.send_text(
                json.dumps(
                    {"type": "create_room", "data": {"room_name": "test_room", "is_public": True}}
                )
            )

            # Receive confirmation
            create_response = json.loads(ws1.receive_text())
            assert create_response["type"] == "room_created"
            assert create_response["data"]["room"] == "test_room"

            # Skip room_joined and room_info messages
            room_joined = json.loads(ws1.receive_text())
            assert room_joined["type"] == "room_joined"
            room_info = json.loads(ws1.receive_text())
            assert room_info["type"] == "room_info"

            # List rooms again
            ws1.send_text(json.dumps({"type": "list_rooms", "data": {}}))

            rooms_response2 = json.loads(ws1.receive_text())
            assert rooms_response2["type"] == "rooms_list"
            room_names = [r["name"] for r in rooms_response2["data"]["rooms"]]
            assert "test_room" in room_names

    @pytest.mark.asyncio
    async def test_typing_indicators(self, setup_app):
        """Test typing indicators"""
        app, manager, consumers, backend, database = setup_app
        client = TestClient(app)

        with client.websocket_connect("/ws/user1") as ws1:
            # Skip welcome
            ws1.receive_text()

            with client.websocket_connect("/ws/user2") as ws2:
                # Skip welcome
                ws2.receive_text()

                # Both users join lobby
                ws1.send_text(json.dumps({"type": "join_room", "data": {"room": "lobby"}}))
                join1 = json.loads(ws1.receive_text())  # join confirmation
                assert join1["type"] == "room_joined"
                room_info1 = json.loads(ws1.receive_text())  # room info
                assert room_info1["type"] == "room_info"

                ws2.send_text(json.dumps({"type": "join_room", "data": {"room": "lobby"}}))
                join2 = json.loads(ws2.receive_text())  # join confirmation
                assert join2["type"] == "room_joined"
                room_info2 = json.loads(ws2.receive_text())  # room info
                assert room_info2["type"] == "room_info"
                # Skip user_joined_room notification from user1 joining
                user_joined = json.loads(ws2.receive_text())
                assert user_joined["type"] == "user_joined_room"

                # User1 starts typing
                ws1.send_text(json.dumps({"type": "typing_start", "data": {"room": "lobby"}}))

                # User2 should receive typing start (may need to skip user_joined_room from earlier)
                typing_start = json.loads(ws2.receive_text())
                if typing_start["type"] == "user_joined_room":
                    typing_start = json.loads(ws2.receive_text())
                assert typing_start["type"] == "user_typing_start"
                assert typing_start["username"] == "user1"

                # User1 stops typing
                ws1.send_text(json.dumps({"type": "typing_stop", "data": {"room": "lobby"}}))

                # User2 should receive typing stop
                typing_stop = json.loads(ws2.receive_text())
                assert typing_stop["type"] == "user_typing_stop"
                assert typing_stop["username"] == "user1"

    @pytest.mark.asyncio
    async def test_message_history(self, setup_app):
        """Test message history retrieval"""
        app, manager, consumers, backend, database = setup_app
        client = TestClient(app)

        with client.websocket_connect("/ws/user1") as ws1:
            # Skip welcome
            ws1.receive_text()

            # Join lobby first
            ws1.send_text(json.dumps({"type": "join_room", "data": {"room": "lobby"}}))
            join_resp = json.loads(ws1.receive_text())  # join confirmation
            assert join_resp["type"] == "room_joined"
            room_info = json.loads(ws1.receive_text())  # room info
            assert room_info["type"] == "room_info"

            # Send a few messages
            for i in range(3):
                ws1.send_text(
                    json.dumps(
                        {"type": "chat_message", "data": {"text": f"Message {i}", "room": "lobby"}}
                    )
                )
                # Consume the echoed message (may be preceded by user_joined_room)
                echoed = json.loads(ws1.receive_text())
                if echoed["type"] == "user_joined_room":
                    echoed = json.loads(ws1.receive_text())
                assert echoed["type"] == "chat_message"
                assert echoed["text"] == f"Message {i}"

            # Get message history
            ws1.send_text(
                json.dumps({"type": "get_message_history", "data": {"room": "lobby", "limit": 10}})
            )

            history_response = json.loads(ws1.receive_text())
            assert history_response["type"] == "message_history"
            assert len(history_response["data"]["messages"]) == 3
            assert history_response["data"]["messages"][0]["text"] == "Message 0"

    @pytest.mark.asyncio
    async def test_error_handling(self, setup_app):
        """Test error handling for invalid operations"""
        app, manager, consumers, backend, database = setup_app
        client = TestClient(app)

        with client.websocket_connect("/ws/user1") as ws1:
            # Skip welcome
            ws1.receive_text()

            # Send empty message
            ws1.send_text(
                json.dumps({"type": "chat_message", "data": {"text": "", "room": "lobby"}})
            )

            # Should receive error
            error_response = json.loads(ws1.receive_text())
            assert error_response["type"] == "error"
            assert "required" in error_response["data"]["message"]

            # Try to join room with name too long
            long_room_name = "a" * 60  # max is 50
            ws1.send_text(json.dumps({"type": "join_room", "data": {"room": long_room_name}}))

            # Should receive error
            error_response2 = json.loads(ws1.receive_text())
            assert error_response2["type"] == "error"
            assert "too long" in error_response2["data"]["message"]

    @pytest.mark.asyncio
    async def test_ping_pong(self, setup_app):
        """Test ping/pong functionality"""
        app, manager, consumers, backend, database = setup_app
        client = TestClient(app)

        with client.websocket_connect("/ws/user1") as ws1:
            # Skip welcome
            ws1.receive_text()

            # Send ping
            ws1.send_text(json.dumps({"type": "ping", "data": {}}))

            # Should receive pong
            pong_response = json.loads(ws1.receive_text())
            assert pong_response["type"] == "pong"

    @pytest.mark.asyncio
    async def test_connection_limits(self, setup_app):
        """Test connection limits with high concurrent connections"""
        app, manager, consumers, backend, database = setup_app
        client = TestClient(app)

        # Test that we can create connections up to the limit per user
        connections = []

        # Create connections for user1 up to the per-client limit
        for i in range(min(100, manager.max_connections_per_client)):  # Test with 100 connections
            try:
                ws = client.websocket_connect(f"/ws/user1_{i}")
                connections.append(ws)
                # Skip welcome message
                json.loads(ws.receive_text())
            except Exception:
                break

        # Verify we have connections
        assert len(connections) > 0

        # Clean up connections
        for ws in connections:
            try:
                ws.close()
            except:
                pass  # Ignore cleanup errors in test

    @pytest.mark.asyncio
    async def test_high_load_message_handling(self, setup_app):
        """Test message handling under high load conditions"""
        app, manager, consumers, backend, database = setup_app
        client = TestClient(app)

        # Test with larger messages (up to 1MB)
        large_message = "x" * (1024 * 1024)  # 1MB message

        with client.websocket_connect("/ws/user1") as ws1:
            # Skip welcome
            ws1.receive_text()

            # Join room
            ws1.send_text(json.dumps({"type": "join_room", "data": {"room": "test_room"}}))
            join_resp = json.loads(ws1.receive_text())
            assert join_resp["type"] == "room_joined"

            # Skip room_info
            json.loads(ws1.receive_text())

            # Send large message (within limits)
            ws1.send_text(
                json.dumps(
                    {
                        "type": "chat_message",
                        "data": {
                            "text": "x" * 100000,
                            "room": "test_room",
                        },  # 100k character message
                    }
                )
            )

            # Should receive the message back
            response = json.loads(ws1.receive_text())
            while response["type"] not in ["chat_message", "error"]:
                response = json.loads(ws1.receive_text())

            # Should not be an error
            assert response["type"] != "error"

    @pytest.mark.asyncio
    async def test_message_length_limits(self, setup_app):
        """Test message length limits"""
        app, manager, consumers, backend, database = setup_app
        client = TestClient(app)

        with client.websocket_connect("/ws/user1") as ws1:
            # Skip welcome
            ws1.receive_text()

            # Join room
            ws1.send_text(json.dumps({"type": "join_room", "data": {"room": "test_room"}}))
            join_resp = json.loads(ws1.receive_text())
            assert join_resp["type"] == "room_joined"
            json.loads(ws1.receive_text())  # Skip room_info

            # Test valid long message (50k characters)
            long_message = "x" * 50000
            ws1.send_text(
                json.dumps(
                    {"type": "chat_message", "data": {"text": long_message, "room": "test_room"}}
                )
            )

            # Should receive the message back (may be preceded by user_joined_room)
            response = json.loads(ws1.receive_text())
            while response["type"] == "user_joined_room":
                response = json.loads(ws1.receive_text())
            assert response["type"] == "chat_message"
            expect_message_length = 50000
            assert len(response["text"]) == expect_message_length

            # Test oversized message (150k characters - over 100k limit)
            oversized_message = "x" * 150000
            ws1.send_text(
                json.dumps(
                    {
                        "type": "chat_message",
                        "data": {"text": oversized_message, "room": "test_room"},
                    }
                )
            )

            # Should receive error
            error_response = json.loads(ws1.receive_text())
            assert error_response["type"] == "error"
            expect_error_message = "too long"
            assert expect_error_message in error_response["data"]["message"].lower()

    @pytest.mark.asyncio
    async def test_group_limits(self, setup_app):
        """Test group/channel limits with large number of rooms"""
        app, *_ = setup_app
        client = TestClient(app)

        with client.websocket_connect("/ws/user1") as ws1:
            # Skip welcome
            ws1.receive_text()

            # Create multiple rooms to test group limits
            created_rooms = []

            # Create up to 1000 rooms to test scalability (well under 3M+ limit but enough for testing)
            for i in range(100):
                room_name = f"test_room_{i}"
                ws1.send_text(
                    json.dumps(
                        {"type": "create_room", "data": {"room_name": room_name, "is_public": True}}
                    )
                )

                # Receive confirmation
                response = json.loads(ws1.receive_text())
                assert response["type"] == "room_created"
                created_rooms.append(room_name)

                # Skip room_joined and room_info messages
                json.loads(ws1.receive_text())  # room_joined
                json.loads(ws1.receive_text())  # room_info

            # List rooms to verify they were created
            ws1.send_text(json.dumps({"type": "list_rooms", "data": {}}))
            rooms_response = json.loads(ws1.receive_text())
            assert rooms_response["type"] == "rooms_list"

            room_names = [r["name"] for r in rooms_response["data"]["rooms"]]
            for room in created_rooms:
                assert room in room_names

    @pytest.mark.asyncio
    async def test_large_group_operations(self, setup_app):
        """Test operations on large groups with many channels"""
        app, manager, consumers, backend, database = setup_app
        client = TestClient(app)

        # Test group operations with many channels
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        try:
            # Add many channels to a group
            group_name = "large_test_group"
            channels = [f"channel_{i}" for i in range(1000)]  # Test with 1000 channels

            for channel in channels:
                await backend.group_add(group_name, channel)

            # Verify all channels are in the group
            group_channels = await backend.group_channels(group_name)
            assert len(group_channels) == len(channels)
            for channel in channels:
                assert channel in group_channels

            # Test removing channels
            for i in range(0, len(channels), 2):  # Remove every other channel
                await backend.group_discard(group_name, channels[i])

            # Verify remaining channels
            remaining_channels = await backend.group_channels(group_name)
            assert len(remaining_channels) == len(channels) // 2

        finally:
            loop.close()

    @pytest.mark.asyncio
    async def test_high_frequency_messaging(self, setup_app):
        """Test high-frequency message sending and processing"""
        app, manager, consumers, backend, database = setup_app
        client = TestClient(app)

        with client.websocket_connect("/ws/user1") as ws1:
            # Skip welcome
            ws1.receive_text()

            # Join room
            ws1.send_text(json.dumps({"type": "join_room", "data": {"room": "speed_test_room"}}))
            json.loads(ws1.receive_text())  # room_joined
            json.loads(ws1.receive_text())  # room_info

            # Send many messages quickly
            message_count = 100
            start_time = time.time()

            for i in range(message_count):
                ws1.send_text(
                    json.dumps(
                        {
                            "type": "chat_message",
                            "data": {"text": f"Message {i}", "room": "speed_test_room"},
                        }
                    )
                )

            # Receive all messages back
            received_count = 0
            for _ in range(message_count):
                try:
                    response = json.loads(ws1.receive_text())
                    if response["type"] == "chat_message":
                        received_count += 1
                except:
                    break

            end_time = time.time()
            total_time = end_time - start_time

            # Should handle high-frequency messaging reasonably well
            assert received_count >= message_count * 0.8  # At least 80% success rate
            messages_per_second = received_count / total_time
            assert messages_per_second > 10  # At least 10 messages per second


@pytest.mark.asyncio
async def test_startup_cleanup_removes_stale_registry_entries():
    """Startup should remove stale Redis registry entries without failing."""
    redis_url = "redis://localhost:6379/0"
    ping_client = Redis.from_url(redis_url)
    try:
        await ping_client.ping()  # type: ignore
    except Exception:
        await ping_client.close()
        pytest.skip("Redis server not available for Redis integration test")
    await ping_client.close()

    backend = RedisBackend(
        redis_url=redis_url,
        channel_prefix=f"test:ws:{uuid.uuid4().hex}:",
        registry_expiry=1,
        group_expiry=1,
    )
    await backend.connect()

    manager: ConnectionManager | None = None

    try:
        # Seed stale connection data belonging to an old server
        await backend.registry_add_connection(
            connection_id="stale-conn",
            user_id="user-stale",
            metadata={"server_instance_id": "old-server"},
            groups=set(),
            heartbeat_timeout=30,
        )

        await asyncio.sleep(1.1)  # Allow TTL to expire

        registry = ConnectionRegistry(backend=backend, heartbeat_timeout=30)
        manager = ConnectionManager(
            registry=registry,
            heartbeat_interval=1,
            server_instance_id="new-server",
        )

        await manager.start_tasks()

        assert await backend.registry_count_connections() == 0
    finally:
        if manager:
            await manager.stop_tasks()
        await backend.flush()
        await backend.cleanup()


@pytest.mark.asyncio
async def test_startup_cleanup_handles_failures_gracefully(monkeypatch):
    """Cleanup errors during startup should not prevent tasks from starting."""
    redis_url = "redis://localhost:6379/0"
    ping_client = Redis.from_url(redis_url)
    try:
        await ping_client.ping()  # type: ignore
    except Exception:
        await ping_client.close()
        pytest.skip("Redis server not available for Redis integration test")
    await ping_client.close()

    backend = RedisBackend(
        redis_url=redis_url,
        channel_prefix=f"test:ws:{uuid.uuid4().hex}:",
        registry_expiry=1,
        group_expiry=1,
    )
    await backend.connect()

    registry = ConnectionRegistry(backend=backend, heartbeat_timeout=30)
    manager = ConnectionManager(
        registry=registry, heartbeat_interval=1, server_instance_id="srv-error"
    )

    async def _raise_stale(*args, **kwargs):
        raise RuntimeError("cleanup stale failed")

    async def _raise_groups(*args, **kwargs):
        raise RuntimeError("cleanup groups failed")

    monkeypatch.setattr(backend, "cleanup_stale_connections", _raise_stale)
    monkeypatch.setattr(backend, "cleanup_orphaned_group_members", _raise_groups)

    try:
        await manager.start_tasks()
        # Manager should still mark as running and start heartbeat despite cleanup failures
        assert manager._running is True
        assert manager._heartbeat_task is not None
    finally:
        await manager.stop_tasks()
        await backend.flush()
        await backend.cleanup()


@pytest.mark.asyncio
async def test_cleanup_performance(monkeypatch):
    """Cleanup should handle many stale connections within a reasonable time."""
    redis_url = "redis://localhost:6379/0"
    ping_client = Redis.from_url(redis_url)
    try:
        await ping_client.ping()  # type: ignore
    except Exception:
        await ping_client.close()
        pytest.skip("Redis server not available for Redis integration test")
    await ping_client.close()

    backend = RedisBackend(
        redis_url=redis_url,
        channel_prefix=f"test:ws:{uuid.uuid4().hex}:",
        registry_expiry=1,
        group_expiry=1,
    )
    await backend.connect()

    try:
        redis_client = await backend.redis
        assert redis_client is not None
        connections_key = f"{backend.channel_prefix}registry:connections"

        total = 1200
        pipe = redis_client.pipeline()
        for idx in range(total):
            conn_id = f"conn-{idx}"
            conn_key = f"{backend.channel_prefix}registry:connection:{conn_id}"
            pipe.sadd(connections_key, conn_id)
            pipe.hset(
                conn_key,
                mapping={
                    "user_id": "",
                    "metadata": json.dumps({"server_instance_id": "old-server"}),
                    "heartbeat_timeout": "30",
                    "groups": json.dumps([]),
                },
            )
        await pipe.execute()

        # Do not set TTL to force cleanup to treat missing TTL as stale when expiry configured
        await asyncio.sleep(1.05)

        start = time.monotonic()
        stats = await backend.cleanup_stale_connections(server_instance_id="srv-perf")
        duration = time.monotonic() - start

        assert duration < 10
        assert stats["connections_removed"] == total
        assert await backend.registry_count_connections() == 0
    finally:
        await backend.flush()
        await backend.cleanup()


if __name__ == "__main__":
    pytest.main([__file__])
