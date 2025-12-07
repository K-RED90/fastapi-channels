# AgentCore

A comprehensive WebSocket chat system built with FastAPI and asyncio.

## Features

### Core Chat Features
- **Real-time messaging** - Send and receive messages instantly
- **Room-based chat** - Create and join chat rooms
- **Private messaging** - Direct messages between users
- **User presence** - Online/offline status tracking
- **Typing indicators** - Show when users are typing
- **Message history** - Retrieve previous messages
- **Room management** - Create, join, and leave rooms

### Technical Features
- **Multiple backends** - Memory and Redis backends for different deployment scenarios
- **Middleware system** - Validation, logging, and rate limiting
- **Connection management** - Automatic heartbeat monitoring and cleanup
- **Scalable architecture** - Support for multiple servers with Redis backend
- **Comprehensive testing** - Full test coverage with integration tests

## Quick Start

### Installation

```bash
# Install dependencies
pip install -e .

# Or with uv
uv sync
```

### Basic Usage

```python
from fastapi import FastAPI, WebSocket
from core.backends.memory import MemoryBackend
from core.connections.manager import ConnectionManager
from core.connections.registry import ConnectionRegistry
from example.consumers import ChatConsumer

app = FastAPI()

# Initialize components
backend = MemoryBackend()
registry = ConnectionRegistry()
manager = ConnectionManager(backend, registry)

@app.websocket("/ws/{user_id}")
async def chat_endpoint(websocket: WebSocket, user_id: str):
    # Accept connection
    connection = await manager.connect(websocket, user_id)

    # Create consumer
    consumer = ChatConsumer(connection, manager, backend)

    try:
        await consumer.connect()
        while True:
            raw_message = await websocket.receive_text()
            await consumer.handle_message(raw_message)
    except Exception:
        await consumer.disconnect(1000)
        await manager.disconnect(connection.channel_name)
```

### Running Tests

```bash
# Run all tests
python run_tests.py

# Or with pytest directly
pytest tests/ -v

# Run specific test file
pytest tests/test_chat_consumer.py -v
```

## WebSocket Message Types

### Chat Messages
```json
{
  "type": "chat_message",
  "data": {
    "text": "Hello world!",
    "room": "lobby"
  }
}
```

### Room Management
```json
// Join a room
{
  "type": "join_room",
  "data": {
    "room": "gaming"
  }
}

// Leave a room
{
  "type": "leave_room",
  "data": {
    "room": "gaming"
  }
}

// Create a room
{
  "type": "create_room",
  "data": {
    "room_name": "new_room",
    "is_public": true
  }
}

// List rooms
{
  "type": "list_rooms",
  "data": {}
}
```

### Private Messages
```json
{
  "type": "private_message",
  "data": {
    "target_user_id": "user123",
    "text": "Secret message!"
  }
}
```

### Typing Indicators
```json
// Start typing
{
  "type": "typing_start",
  "data": {
    "room": "lobby"
  }
}

// Stop typing
{
  "type": "typing_stop",
  "data": {
    "room": "lobby"
  }
}
```

### Other Features
```json
// Get message history
{
  "type": "get_message_history",
  "data": {
    "room": "lobby",
    "limit": 50
  }
}

// Get users in room
{
  "type": "get_room_users",
  "data": {
    "room": "lobby"
  }
}

// Ping/Pong
{
  "type": "ping",
  "data": {}
}
```

## Response Messages

### Successful Operations
```json
{
  "type": "chat_message",
  "user_id": "conn123",
  "username": "user1",
  "room": "lobby",
  "text": "Hello!",
  "timestamp": 1234567890.123
}
```

### Error Responses
```json
{
  "type": "error",
  "message": "Message text is required",
  "message_type": "chat_message",
  "timestamp": 1234567890.123
}
```

## Architecture

### Components

- **Consumers** - Handle WebSocket message processing and business logic
- **Backends** - Handle message routing and persistence (Memory/Redis)
- **Middleware** - Process messages through validation, logging, rate limiting
- **Connection Manager** - Manage WebSocket connections and groups
- **Registry** - Track connections, users, and groups across servers

### Backends

#### Memory Backend
- Perfect for single-server deployments
- Fast, in-memory message routing
- No persistence across restarts

#### Redis Backend
- Multi-server deployments
- Persistent message routing
- Pub/Sub for cross-server communication

### Middleware

- **ValidationMiddleware** - Message size limits, format validation
- **LoggingMiddleware** - Request/response logging
- **RateLimitMiddleware** - Prevent spam with token bucket algorithm

## Configuration

Environment variables:

```bash
# Backend selection
BACKEND_TYPE=memory  # or redis

# Redis settings (if using redis backend)
REDIS_URL=redis://localhost:6379/0
REDIS_CHANNEL_PREFIX=ws:

# Connection limits
MAX_CONNECTIONS_PER_CLIENT=5
MAX_TOTAL_CONNECTIONS=10000

# WebSocket settings
WS_HEARTBEAT_INTERVAL=30
WS_HEARTBEAT_TIMEOUT=60
WS_MAX_MESSAGE_SIZE=1048576  # 1MB

# Rate limiting
RATE_LIMIT_MESSAGES=100  # per window
RATE_LIMIT_WINDOW=60     # seconds
RATE_LIMIT_BURST=150     # burst capacity
```

## Development

### Project Structure
```
agentcore/
├── core/
│   ├── backends/          # Message routing backends
│   ├── connections/       # Connection management
│   ├── consumer/          # Base consumer classes
│   ├── middleware/        # Message processing middleware
│   ├── serializers/       # Message serialization
│   └── typed.py          # Type definitions
├── example/
│   └── consumers.py      # Example chat consumer
├── tests/                # Comprehensive test suite
└── main.py              # Entry point
```

### Contributing

1. Fork the repository
2. Create a feature branch
3. Add tests for new functionality
4. Ensure all tests pass
5. Submit a pull request

## License

MIT License - see LICENSE file for details.