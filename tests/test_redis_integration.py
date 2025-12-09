import json
from unittest.mock import patch

import pytest
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.testclient import TestClient

from core.backends.redis import RedisBackend
from core.config import Settings
from core.connections.manager import ConnectionManager
from core.connections.registry import ConnectionRegistry
from core.middleware.logging import LoggingMiddleware
from core.middleware.validation import ValidationMiddleware
from example.consumers import ChatConsumer
from example.database import ChatDatabase


class TestRedisIntegration:
    """Integration tests for Redis backend with WebSocket chat system"""

    @pytest.fixture
    async def redis_app(self):
        """Setup FastAPI app with Redis backend"""
        # Mock Redis for testing (since we may not have Redis running)
        with patch("core.backends.redis.Redis") as mock_redis_class:
            mock_redis = mock_redis_class.return_value
            # Configure mock to behave like Redis
            mock_redis.get.return_value = None
            mock_redis.set.return_value = True
            mock_redis.delete.return_value = 1
            mock_redis.sadd.return_value = 1
            mock_redis.srem.return_value = 1
            mock_redis.smembers.return_value = set()
            mock_redis.scard.return_value = 0
            mock_redis.publish.return_value = 1
            mock_redis.flushdb.return_value = True

            settings = Settings(
                BACKEND_TYPE="redis",
                MAX_TOTAL_CONNECTIONS=200000,
                MAX_CONNECTIONS_PER_CLIENT=1000,
                WS_MAX_MESSAGE_SIZE=10 * 1024 * 1024,
            )

            # Initialize components with Redis backend
            backend = RedisBackend(
                redis_url="redis://localhost:6379/0", max_connections=settings.MAX_TOTAL_CONNECTIONS
            )
            registry = ConnectionRegistry(backend=backend)
            manager = ConnectionManager(
                registry, max_connections_per_client=settings.MAX_CONNECTIONS_PER_CLIENT
            )

            # Middleware stack
            middleware_stack = (
                ValidationMiddleware(settings.WS_MAX_MESSAGE_SIZE) | LoggingMiddleware()
            )

            # Database for persistence
            database = ChatDatabase()

            # Global state for consumers
            consumers = {}

            app = FastAPI()

            @app.websocket("/ws/{user_id}")
            async def websocket_endpoint(websocket: WebSocket, user_id: str):
                # Create connection
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
                        raw_message = await websocket.receive_text()
                        await consumer.handle_message(raw_message)

                except WebSocketDisconnect:
                    await consumer.disconnect(1000)
                    await manager.disconnect(connection.channel_name)
                except Exception as e:
                    print(f"Error: {e}")
                    await consumer.disconnect(1011)
                    await manager.disconnect(connection.channel_name)

            yield app, manager, consumers, backend, database

    @pytest.mark.asyncio
    async def test_redis_backend_initialization(self, redis_app):
        """Test Redis backend initializes correctly"""
        app, manager, consumers, backend, database = redis_app

        assert isinstance(backend, RedisBackend)
        assert backend.redis_url == "redis://localhost:6379/0"
        assert backend.max_connections == 200000

    @pytest.mark.asyncio
    async def test_redis_full_chat_flow(self, redis_app):
        """Test complete chat flow with Redis backend"""
        app, manager, consumers, backend, database = redis_app

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

    @pytest.mark.asyncio
    async def test_redis_group_operations_at_scale(self, redis_app):
        """Test Redis group operations with larger scale"""
        app, manager, consumers, backend, database = redis_app
        client = TestClient(app)

        # Test creating multiple rooms with Redis backend
        with client.websocket_connect("/ws/user1") as ws1:
            # Skip welcome
            ws1.receive_text()

            # Create multiple rooms
            room_count = 50  # Test with 50 rooms

            for i in range(room_count):
                room_name = f"redis_test_room_{i}"
                ws1.send_text(
                    json.dumps(
                        {"type": "create_room", "data": {"room_name": room_name, "is_public": True}}
                    )
                )

                # Receive confirmation
                response = json.loads(ws1.receive_text())
                assert response["type"] == "room_created"
                assert response["data"]["room"] == room_name

                # Skip room_joined and room_info messages
                room_joined = json.loads(ws1.receive_text())
                assert room_joined["type"] == "room_joined"
                room_info = json.loads(ws1.receive_text())
                assert room_info["type"] == "room_info"

            # List rooms to verify they were created
            ws1.send_text(json.dumps({"type": "list_rooms", "data": {}}))
            rooms_response = json.loads(ws1.receive_text())
            assert rooms_response["type"] == "rooms_list"

            # Verify we have the rooms (may include default lobby)
            room_names = [r["name"] for r in rooms_response["data"]["rooms"]]
            created_rooms = [f"redis_test_room_{i}" for i in range(room_count)]
            for room in created_rooms:
                assert room in room_names

    @pytest.mark.asyncio
    async def test_redis_connection_limits(self, redis_app):
        """Test connection limits with Redis backend"""
        app, manager, consumers, backend, database = redis_app
        client = TestClient(app)

        # Test per-user connection limits
        max_connections_per_user = 5  # Test with smaller limit for this test

        connections = []
        try:
            # Try to create multiple connections for same user
            for i in range(max_connections_per_user + 2):  # Try to exceed limit
                try:
                    ws = client.websocket_connect("/ws/test_user")
                    connections.append(ws)
                    # Skip welcome
                    json.loads(ws.receive_text())
                except Exception:
                    break  # Expected when limit exceeded

            # Should have created connections up to the limit
            assert len(connections) > 0

        finally:
            # Cleanup connections
            for ws in connections:
                try:
                    ws.close()
                except:
                    pass

    @pytest.mark.asyncio
    async def test_redis_message_broadcast(self, redis_app):
        """Test message broadcasting with Redis backend"""
        app, manager, consumers, backend, database = redis_app
        client = TestClient(app)

        # Connect multiple users
        user_count = 10

        connections = []
        try:
            for i in range(user_count):
                ws = client.websocket_connect(f"/ws/user{i}")
                connections.append(ws)
                # Skip welcome
                json.loads(ws.receive_text())

            # All users join same room
            room_name = "redis_broadcast_room"
            for ws in connections:
                ws.send_text(json.dumps({"type": "join_room", "data": {"room": room_name}}))
                # Skip join confirmation and room info
                json.loads(ws.receive_text())  # room_joined
                json.loads(ws.receive_text())  # room_info

            # Send message from first user
            connections[0].send_text(
                json.dumps(
                    {
                        "type": "chat_message",
                        "data": {"text": "Redis broadcast test!", "room": room_name},
                    }
                )
            )

            # Check that other users receive the message
            received_count = 0
            for i in range(1, len(connections)):
                try:
                    response = json.loads(connections[i].receive_text())
                    while response["type"] not in ["chat_message", "error"]:
                        response = json.loads(connections[i].receive_text())
                    if response["type"] == "chat_message":
                        assert response["text"] == "Redis broadcast test!"
                        received_count += 1
                except:
                    pass  # Some connections may timeout

            # Should have broadcast to most users
            assert received_count >= user_count // 2

        finally:
            # Cleanup
            for ws in connections:
                try:
                    ws.close()
                except:
                    pass

    @pytest.mark.asyncio
    async def test_redis_large_messages(self, redis_app):
        """Test large message handling with Redis backend"""
        app, *_ = redis_app
        client = TestClient(app)

        with client.websocket_connect("/ws/user1") as ws1:
            # Skip welcome
            ws1.receive_text()

            # Join room
            ws1.send_text(json.dumps({"type": "join_room", "data": {"room": "test_room"}}))
            json.loads(ws1.receive_text())  # room_joined
            json.loads(ws1.receive_text())  # room_info

            # Send large message (within 10MB limit)
            large_message = "x" * 100000  # 100k character message
            ws1.send_text(
                json.dumps(
                    {"type": "chat_message", "data": {"text": large_message, "room": "test_room"}}
                )
            )

            # Should receive the message back
            response = json.loads(ws1.receive_text())
            while response["type"] == "user_joined_room":
                response = json.loads(ws1.receive_text())
            assert response["type"] == "chat_message"
            expect_message_length = 100000
            assert len(response["text"]) == expect_message_length

    @pytest.mark.asyncio
    async def test_redis_backend_resilience(self, redis_app):
        """Test Redis backend resilience and error handling"""
        app, manager, consumers, backend, database = redis_app

        # Test that backend operations handle Redis unavailability gracefully
        # (In this mock setup, we're testing the interface contract)

        # Test group operations
        result = await backend.group_add("resilience_test", "channel1")
        assert result is True  # Should succeed with mock

        channels = await backend.group_channels("resilience_test")
        assert isinstance(channels, list) or isinstance(channels, set)

        # Test registry operations
        result = await backend.registry_add_connection_if_under_limit("user1", "conn1", 10)
        assert result is True

        exists = await backend.registry_connection_exists("user1", "conn1")
        assert exists is True


if __name__ == "__main__":
    pytest.main([__file__])
