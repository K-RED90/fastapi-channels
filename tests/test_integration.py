import json

import pytest
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.testclient import TestClient

from core.backends.memory import MemoryBackend
from core.config import Settings
from core.connections.manager import ConnectionManager
from core.connections.registry import ConnectionRegistry
from core.middleware.logging import LoggingMiddleware
from core.middleware.validation import ValidationMiddleware
from example.consumers import ChatConsumer


class TestWebSocketIntegration:
    """Integration tests for WebSocket chat functionality"""

    @pytest.fixture
    async def setup_app(self):
        """Setup FastAPI app with WebSocket endpoint"""
        settings = Settings()

        # Initialize components
        backend = MemoryBackend()
        registry = ConnectionRegistry()
        manager = ConnectionManager(backend, registry)

        # Middleware stack
        middleware = [ValidationMiddleware(settings.WS_MAX_MESSAGE_SIZE), LoggingMiddleware()]

        # Global state for consumers
        consumers = {}

        app = FastAPI()

        @app.websocket("/ws/{user_id}")
        async def websocket_endpoint(websocket: WebSocket, user_id: str):
            await websocket.accept()

            # Create connection
            connection = await manager.connect(websocket=websocket, user_id=user_id)

            # Create consumer
            consumer = ChatConsumer(
                connection=connection,
                manager=manager,
                backend=backend,
                middleware_stacks=middleware,
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

        # Start manager tasks
        await manager.start_tasks()

        yield app, manager, consumers

        # Cleanup
        await manager.stop_tasks()

    @pytest.mark.asyncio
    async def test_full_chat_flow(self, setup_app):
        """Test complete chat flow with multiple users"""

        app, manager, consumers = setup_app

        # Create test client
        client = TestClient(app)

        # Connect first user
        with client.websocket_connect("/ws/user1") as ws1:
            # Receive welcome message
            welcome = json.loads(ws1.receive_text())
            assert welcome["type"] == "welcome"
            assert "user1" in welcome["message"]

            # Connect second user
            with client.websocket_connect("/ws/user2") as ws2:
                # Receive welcome for user2
                welcome2 = json.loads(ws2.receive_text())
                assert welcome2["type"] == "welcome"

                # User1 sends a chat message
                ws1.send_text(
                    json.dumps(
                        {
                            "type": "chat_message",
                            "data": {"text": "Hello everyone!", "room": "lobby"},
                        }
                    )
                )

                # User1 should not receive their own message back
                # User2 should receive the message
                message = json.loads(ws2.receive_text())
                assert message["type"] == "chat_message"
                assert message["text"] == "Hello everyone!"
                assert message["username"] == "user1"

                # User2 replies
                ws2.send_text(
                    json.dumps(
                        {"type": "chat_message", "data": {"text": "Hi user1!", "room": "lobby"}}
                    )
                )

                # User1 receives the reply
                reply = json.loads(ws1.receive_text())
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
                assert create_response["room"] == "gaming"

                # User2 joins the room
                ws2.send_text(json.dumps({"type": "join_room", "data": {"room": "gaming"}}))

                # User2 receives confirmation
                join_response = json.loads(ws2.receive_text())
                assert join_response["type"] == "room_joined"
                assert join_response["room"] == "gaming"

                # User1 sends message in gaming room
                ws1.send_text(
                    json.dumps(
                        {"type": "chat_message", "data": {"text": "Let's play!", "room": "gaming"}}
                    )
                )

                # User2 receives the message
                game_message = json.loads(ws2.receive_text())
                assert game_message["type"] == "chat_message"
                assert game_message["text"] == "Let's play!"
                assert game_message["room"] == "gaming"

    @pytest.mark.asyncio
    async def test_private_messaging(self, setup_app):
        """Test private messaging between users"""

        app, manager, consumers = setup_app
        client = TestClient(app)

        with client.websocket_connect("/ws/user1") as ws1:
            # Skip welcome
            ws1.receive_text()

            with client.websocket_connect("/ws/user2") as ws2:
                # Skip welcome
                ws2.receive_text()

                # Get connection IDs from consumers
                user1_conn_id = None
                user2_conn_id = None
                for conn_id, consumer in consumers.items():
                    if consumer.connection.user_id == "user1":
                        user1_conn_id = conn_id
                    elif consumer.connection.user_id == "user2":
                        user2_conn_id = conn_id

                # User1 sends private message to user2
                ws1.send_text(
                    json.dumps(
                        {
                            "type": "private_message",
                            "data": {"target_user_id": user2_conn_id, "text": "Secret message!"},
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

        app, manager, consumers = setup_app
        client = TestClient(app)

        with client.websocket_connect("/ws/user1") as ws1:
            # Skip welcome
            ws1.receive_text()

            # List rooms (should see lobby)
            ws1.send_text(json.dumps({"type": "list_rooms", "data": {}}))

            rooms_response = json.loads(ws1.receive_text())
            assert rooms_response["type"] == "rooms_list"
            assert len(rooms_response["rooms"]) >= 1  # At least lobby

            # Create a room
            ws1.send_text(
                json.dumps(
                    {"type": "create_room", "data": {"room_name": "test_room", "is_public": True}}
                )
            )

            # Receive confirmation
            create_response = json.loads(ws1.receive_text())
            assert create_response["type"] == "room_created"

            # List rooms again
            ws1.send_text(json.dumps({"type": "list_rooms", "data": {}}))

            rooms_response2 = json.loads(ws1.receive_text())
            assert rooms_response2["type"] == "rooms_list"
            room_names = [r["name"] for r in rooms_response2["rooms"]]
            assert "test_room" in room_names

    @pytest.mark.asyncio
    async def test_typing_indicators(self, setup_app):
        """Test typing indicators"""

        app, manager, consumers = setup_app
        client = TestClient(app)

        with client.websocket_connect("/ws/user1") as ws1:
            # Skip welcome
            ws1.receive_text()

            with client.websocket_connect("/ws/user2") as ws2:
                # Skip welcome
                ws2.receive_text()

                # User1 starts typing
                ws1.send_text(json.dumps({"type": "typing_start", "data": {"room": "lobby"}}))

                # User2 should receive typing start
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

        app, manager, consumers = setup_app
        client = TestClient(app)

        with client.websocket_connect("/ws/user1") as ws1:
            # Skip welcome
            ws1.receive_text()

            # Send a few messages
            for i in range(3):
                ws1.send_text(
                    json.dumps(
                        {"type": "chat_message", "data": {"text": f"Message {i}", "room": "lobby"}}
                    )
                )
                # Skip the echoed message (if any)
                try:
                    ws1.receive_text()
                except:
                    pass

            # Get message history
            ws1.send_text(
                json.dumps({"type": "get_message_history", "data": {"room": "lobby", "limit": 10}})
            )

            history_response = json.loads(ws1.receive_text())
            assert history_response["type"] == "message_history"
            assert len(history_response["messages"]) == 3
            assert history_response["messages"][0]["text"] == "Message 0"

    @pytest.mark.asyncio
    async def test_error_handling(self, setup_app):
        """Test error handling for invalid operations"""

        app, manager, consumers = setup_app
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
            assert "required" in error_response["message"]

            # Try to join non-existent room
            ws1.send_text(json.dumps({"type": "join_room", "data": {"room": "nonexistent_room"}}))

            # Should receive error
            error_response2 = json.loads(ws1.receive_text())
            assert error_response2["type"] == "error"
            assert "not found" in error_response2["message"]

    @pytest.mark.asyncio
    async def test_ping_pong(self, setup_app):
        """Test ping/pong functionality"""

        app, manager, consumers = setup_app
        client = TestClient(app)

        with client.websocket_connect("/ws/user1") as ws1:
            # Skip welcome
            ws1.receive_text()

            # Send ping
            ws1.send_text(json.dumps({"type": "ping", "data": {}}))

            # Should receive pong
            pong_response = json.loads(ws1.receive_text())
            assert pong_response["type"] == "pong"


if __name__ == "__main__":
    pytest.main([__file__])
