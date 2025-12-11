"""FastAPI WebSocket Chat Application Example

A complete example application demonstrating the websocket chat system
with an embedded HTML frontend.
"""

import logging
import os
from contextlib import asynccontextmanager
from typing import Any

from fastapi import Body, FastAPI, HTTPException, Query, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from redis.asyncio import Redis

from example.consumers import ChatConsumer
from example.database import ChatDatabase
from example.external_events import (
    get_channel_layer_status,
    send_broadcast_announcement,
    send_test_group_message,
    send_test_message_to_room,
    send_test_notification_to_user,
)
from fastapi_channels import ConnectionManager
from fastapi_channels.config import WSConfig
from fastapi_channels.exceptions import BaseError
from fastapi_channels.middleware import LoggingMiddleware, RateLimitMiddleware, ValidationMiddleware

ws_config = WSConfig(BACKEND_TYPE="redis")

logging.basicConfig(
    level=getattr(logging, ws_config.LOG_LEVEL.upper(), logging.INFO),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)

manager = ConnectionManager(ws_config=ws_config)
middleware = (
    ValidationMiddleware(ws_config.WS_MAX_MESSAGE_SIZE)
    >> LoggingMiddleware()
    >> RateLimitMiddleware(
        messages_per_window=3,
        window_seconds=60,
        redis=Redis.from_url(ws_config.REDIS_URL, encoding="utf-8", decode_responses=True),
        excluded_message_types={
            "ping",
            "pong",
            "welcome",
            "room_users",
            "message_history",
            "typing_start",
            "typing_stop",
            "list_rooms",
        },
    )
)

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
    connection = await manager.connect(websocket=websocket, user_id=user_id)

    consumer = ChatConsumer(
        connection=connection,
        manager=manager,
        middleware_stack=middleware,
        database=db,
    )

    try:
        await consumer.connect()

        while True:
            message = await websocket.receive()
            if "text" in message:
                json_str = message["text"]
                await consumer.handle_message(json_str=json_str)
            elif "bytes" in message:
                binary = message["bytes"]
                await consumer.handle_message(binary=binary)
            else:
                # Unknown message type, skip
                continue

    except WebSocketDisconnect:
        await consumer.disconnect(1000)
    except BaseError as e:
        if e.should_disconnect():
            code = e.ws_code
            await consumer.disconnect(code)
        else:
            await consumer.handle_error(e)
    except Exception:
        await consumer.disconnect(1011)


@app.get("/api/rooms/{room_name}/messages")
async def get_room_messages(room_name: str, limit: int = 50):
    """REST API endpoint to get message history for a room"""
    if limit < 1 or limit > 1000:
        raise HTTPException(status_code=400, detail="Limit must be between 1 and 1000")

    messages = db.get_recent_messages(room_name, limit=limit)
    return {"room": room_name, "messages": messages, "count": len(messages)}


@app.post("/api/announce", summary="Test endpoint to send a broadcast announcement.")
async def test_broadcast_announcement(message: str):
    await send_broadcast_announcement(message)
    return {"status": "sent", "message": message}


@app.post("/api/room/{room_name}/message")
async def test_room_message(
    room_name: str, user_id: str = Query(min_length=1), text: str = Query(min_length=1)
):
    await send_test_message_to_room(room_name, user_id, text)
    return {
        "status": "sent",
        "room": room_name.strip(),
        "user_id": user_id.strip(),
        "text": text.strip(),
    }


@app.post(
    "/api/user/{user_id}/notification",
    summary="Test endpoint to send a notification to a specific user.",
)
async def test_user_notification(user_id: str, message: str = Query(min_length=1)):
    await send_test_notification_to_user(user_id, message)
    return {"status": "sent", "user_id": user_id.strip(), "message": message.strip()}


@app.post("/api/group/{group_name}/message", summary="Test endpoint to send a message to a group.")
async def test_group_message(
    group_name: str, data: dict[str, Any] = Body(..., example={"message": "Hello, world!"})
):
    await send_test_group_message(group_name, data)
    return {"status": "sent", "group": group_name.strip(), "data": data}


@app.get("/api/status", summary="Test endpoint to get channel layer status.")
async def test_channel_status():
    return get_channel_layer_status()


@app.get("/", response_class=HTMLResponse)
async def get_frontend():
    return HTML_TEMPLATE


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
