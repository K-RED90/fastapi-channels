"""FastAPI WebSocket Chat Application Example

A complete example application demonstrating the websocket chat system
with an embedded HTML frontend.
"""

import logging
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse

from core.backends.redis import RedisBackend
from core.config import Settings
from core.connections.manager import ConnectionManager
from core.connections.registry import ConnectionRegistry
from core.middleware.logging import LoggingMiddleware
from core.middleware.validation import ValidationMiddleware
from example.consumers import ChatConsumer
from example.database import ChatDatabase

settings = Settings()

logging.basicConfig(
    level=getattr(logging, settings.LOG_LEVEL.upper(), logging.INFO),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
backend = RedisBackend(
    redis_url=settings.REDIS_URL,
    registry_expiry=settings.REDIS_REGISTRY_EXPIRY,
    group_expiry=settings.REDIS_GROUP_EXPIRY,
)
registry = ConnectionRegistry(
    backend=backend,
    max_connections=settings.MAX_TOTAL_CONNECTIONS,
    heartbeat_timeout=settings.WS_HEARTBEAT_TIMEOUT,
)
manager = ConnectionManager(
    registry=registry,
    max_connections_per_client=settings.MAX_CONNECTIONS_PER_CLIENT,
    heartbeat_interval=settings.WS_HEARTBEAT_INTERVAL,
)
middleware = ValidationMiddleware(settings.WS_MAX_MESSAGE_SIZE) >> LoggingMiddleware()

db = ChatDatabase()

template_path = os.path.join(os.path.dirname(__file__), "template.html")
with open(template_path, encoding="utf-8") as f:
    HTML_TEMPLATE = f.read()


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup and shutdown handlers"""
    await manager.start_tasks()
    yield
    await manager.stop_tasks()
    db.close()


app = FastAPI(title="WebSocket Chat Example", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.websocket("/ws/{user_id}")
async def websocket_endpoint(websocket: WebSocket, user_id: str):
    """WebSocket endpoint for chat connections"""
    connection = await manager.connect(websocket=websocket, user_id=user_id)

    consumer = ChatConsumer(
        connection=connection,
        manager=manager,
        backend=backend,
        middleware_stack=middleware,
        database=db,
    )

    try:
        await consumer.connect()

        while True:
            raw_message = await websocket.receive_text()
            await consumer.handle_message(raw_message)

    except WebSocketDisconnect:
        await consumer.disconnect(1000)
        await manager.disconnect(connection.channel_name)
    except Exception as e:
        print(f"WebSocket error: {e}")
        await consumer.disconnect(1011)
        await manager.disconnect(connection.channel_name)


@app.get("/api/rooms/{room_name}/messages")
async def get_room_messages(room_name: str, limit: int = 50):
    """REST API endpoint to get message history for a room"""
    if limit < 1 or limit > 1000:
        raise HTTPException(status_code=400, detail="Limit must be between 1 and 1000")

    messages = db.get_recent_messages(room_name, limit=limit)
    return {"room": room_name, "messages": messages, "count": len(messages)}


@app.get("/", response_class=HTMLResponse)
async def get_frontend():
    return HTML_TEMPLATE


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
