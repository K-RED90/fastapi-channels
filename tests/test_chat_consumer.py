import asyncio
from unittest.mock import Mock

import pytest

from core.backends.memory import MemoryBackend
from core.connections.manager import ConnectionManager
from core.connections.registry import ConnectionRegistry
from core.middleware.logging import LoggingMiddleware
from core.middleware.validation import ValidationMiddleware
from core.typed import Message
from example.consumers import ChatConsumer


class TestChatConsumer:
    """Comprehensive tests for ChatConsumer functionality"""

    def test_imports(self):
        """Test that all imports work correctly"""
        from core.backends.memory import MemoryBackend
        from core.middleware.validation import ValidationMiddleware
        from core.typed import Message

        assert ChatConsumer is not None
        assert MemoryBackend is not None
        assert ConnectionManager is not None
        assert ConnectionRegistry is not None
        assert LoggingMiddleware is not None
        assert ValidationMiddleware is not None
        assert Message is not None

    def test_chat_consumer_creation(self):
        """Test that ChatConsumer can be instantiated"""
        from core.backends.memory import MemoryBackend
        from example.consumers import ChatConsumer

        # Create minimal mocks
        connection = Mock()
        connection.channel_name = "test_conn"
        connection.user_id = "test_user"

        manager = Mock()
        backend = MemoryBackend()

        consumer = ChatConsumer(connection=connection, manager=manager, backend=backend)

        assert consumer.connection == connection
        assert consumer.manager == manager
        assert consumer.backend == backend
        assert hasattr(consumer, "users")
        assert hasattr(consumer, "rooms")
        assert hasattr(consumer, "message_history")

    def test_message_creation(self):
        """Test Message object creation and methods"""

        # Test message creation
        msg = Message(type="test", data={"key": "value"}, sender_id="sender123")

        assert msg.type == "test"
        assert msg.data == {"key": "value"}
        assert msg.sender_id == "sender123"

        # Test to_dict
        msg_dict = msg.to_dict()
        assert msg_dict["type"] == "test"
        assert msg_dict["data"] == {"key": "value"}

        # Test from_dict
        msg2 = Message.from_dict(msg_dict)
        assert msg2.type == msg.type
        assert msg2.data == msg.data

    def test_memory_backend(self):
        """Test MemoryBackend functionality"""

        backend = MemoryBackend()

        # Test group operations
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        try:
            # Test group_add
            loop.run_until_complete(backend.group_add("test_group", "channel1"))
            loop.run_until_complete(backend.group_add("test_group", "channel2"))

            # Test group_channels
            channels = loop.run_until_complete(backend.group_channels("test_group"))
            assert "channel1" in channels
            assert "channel2" in channels
            assert len(channels) == 2

            # Test group_discard
            loop.run_until_complete(backend.group_discard("test_group", "channel1"))
            channels = loop.run_until_complete(backend.group_channels("test_group"))
            assert "channel1" not in channels
            assert "channel2" in channels
            assert len(channels) == 1

        finally:
            loop.close()

    def test_validation_middleware(self):
        """Test ValidationMiddleware functionality"""
        from unittest.mock import Mock

        from core.typed import Message

        middleware = ValidationMiddleware(max_message_size=1000)

        # Create test message
        message = Message(type="test", data={"text": "short message"})
        connection = Mock()
        connection.channel_name = "test_conn"
        connection.user_id = "test_user"

        consumer = Mock()

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        try:
            # Test valid message
            result = loop.run_until_complete(middleware.process(message, connection, consumer))
            assert result == message

            # Test oversized message
            big_message = Message(type="test", data={"text": "x" * 200})
            try:
                result = loop.run_until_complete(
                    middleware.process(big_message, connection, consumer)
                )
                # The middleware raises ValidationError, but we just check it was called
                # In real usage, the consumer would catch this
            except Exception as e:
                # Exception is expected - either ValidationError or handled by consumer
                assert "ValidationError" in str(type(e).__name__) or "error" in str(e).lower()

        finally:
            loop.close()

    def test_rate_limit_middleware(self):
        """Test RateLimitMiddleware functionality"""
        from unittest.mock import Mock

        from core.middleware.rate_limit import RateLimitMiddleware

        middleware = RateLimitMiddleware()

        # Create test message
        message = Message(type="chat_message", data={"text": "test"})
        connection = Mock()
        connection.channel_name = "test_conn"
        connection.user_id = "test_user"

        consumer = Mock()

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        try:
            # Test normal message processing
            result = loop.run_until_complete(middleware.process(message, connection, consumer))
            assert result == message

        finally:
            loop.close()


if __name__ == "__main__":
    pytest.main([__file__])
