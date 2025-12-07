import asyncio
import json
from collections import defaultdict
from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING, Any, Optional

import redis.asyncio as aioredis
from redis.asyncio import Redis
from redis.asyncio.client import PubSub

from core.backends.base import BaseBackend
from core.serializers import JSONSerializer

if TYPE_CHECKING:
    from core.serializers import BaseSerializer


class RedisBackend(BaseBackend):
    def __init__(
        self,
        redis_url: str,
        channel_prefix: str = "ws:",
        serializer: Optional["BaseSerializer"] = None,
    ):
        super().__init__()
        self.redis_url = redis_url
        self.channel_prefix = channel_prefix
        self.serializer = serializer or JSONSerializer()
        self.redis: Redis | None = None
        self.pubsub: PubSub | None = None
        self.subscribed_channels: set[str] = set()
        self._subscriptions: dict[str, asyncio.Queue] = {}
        self._listener_task: asyncio.Task | None = None
        self._message_handlers: dict[str, set[Callable[[Any], Awaitable[None]]]] = defaultdict(set)

    async def connect(self) -> None:
        self.redis = aioredis.from_url(
            self.redis_url,
            encoding=None if self.serializer.binary else "utf-8",
            decode_responses=not self.serializer.binary,
        )
        self.pubsub = self.redis.pubsub()

    async def publish(self, channel: str, message: dict[str, Any]) -> None:
        if not self.redis:
            await self.connect()
        assert self.redis is not None

        full_channel = f"{self.channel_prefix}{channel}"
        serialized = self.serializer.dumps(message)
        await self.redis.publish(full_channel, serialized)

    async def subscribe(self, channel: str) -> None:
        if not self.pubsub:
            await self.connect()
        assert self.pubsub is not None

        full_channel = f"{self.channel_prefix}{channel}"
        await self.pubsub.subscribe(full_channel)
        self.subscribed_channels.add(channel)
        if channel not in self._subscriptions:
            self._subscriptions[channel] = asyncio.Queue()

        if not self._listener_task or self._listener_task.done():
            self._listener_task = asyncio.create_task(self._listen())

    async def unsubscribe(self, channel: str) -> None:
        if not self.pubsub:
            return

        full_channel = f"{self.channel_prefix}{channel}"
        await self.pubsub.unsubscribe(full_channel)
        self.subscribed_channels.discard(channel)

    async def group_send(self, group: str, message: dict[str, Any]) -> None:
        if not self.redis:
            await self.connect()
        assert self.redis is not None

        group_key = f"{self.channel_prefix}group:{group}"
        redis_client: Any = self.redis
        channels = await redis_client.smembers(group_key)

        tasks = [self.publish(channel, message) for channel in channels]
        await asyncio.gather(*tasks, return_exceptions=True)

    async def group_add(self, group: str, channel: str) -> None:
        """Add channel to group in Redis (no local storage)."""
        if not self.redis:
            await self.connect()
        assert self.redis is not None

        group_key = f"{self.channel_prefix}group:{group}"
        redis_client: Any = self.redis
        await redis_client.sadd(group_key, channel)

    async def group_discard(self, group: str, channel: str) -> None:
        """Remove channel from group in Redis (no local storage)."""
        if not self.redis:
            return
        assert self.redis is not None

        group_key = f"{self.channel_prefix}group:{group}"
        redis_client: Any = self.redis
        await redis_client.srem(group_key, channel)

    async def group_channels(self, group: str) -> set[str]:
        """Return channels for a group from Redis."""
        if not self.redis:
            await self.connect()
        assert self.redis is not None

        group_key = f"{self.channel_prefix}group:{group}"
        redis_client: Any = self.redis
        members = await redis_client.smembers(group_key)
        return {m.decode() if isinstance(m, (bytes, bytearray)) else m for m in members}

    async def _listen(self) -> None:
        if not self.pubsub:
            return

        async for message in self.pubsub.listen():
            if message["type"] == "message":
                channel = message["channel"].replace(self.channel_prefix, "")
                data = self.serializer.loads(message["data"])

                if channel in self._subscriptions:
                    try:
                        self._subscriptions[channel].put_nowait(data)
                    except asyncio.QueueFull:
                        pass

                if channel in self._message_handlers:
                    for handler in self._message_handlers[channel].copy():
                        await handler(data)

    def add_message_handler(self, channel: str, handler: Callable) -> None:
        self._message_handlers[channel].add(handler)

    def remove_message_handler(self, channel: str, handler: Callable) -> None:
        self._message_handlers[channel].discard(handler)

    async def receive(self, channel: str, timeout: float | None = None) -> dict[str, Any] | None:
        """Receive next message for a channel via subscription."""
        if channel not in self._subscriptions:
            await self.subscribe(channel)

        queue = self._subscriptions[channel]
        try:
            return await asyncio.wait_for(queue.get(), timeout=timeout)
        except TimeoutError:
            return None

    async def flush(self) -> None:
        """Clear redis keys and local subscription tracking."""
        if self.redis:
            pattern = f"{self.channel_prefix}*"
            async for key in self.redis.scan_iter(match=pattern):
                await self.redis.delete(key)
        self._subscriptions.clear()

    async def cleanup(self) -> None:
        if self._listener_task:
            self._listener_task.cancel()
            await self._listener_task

        if self.pubsub:
            await self.pubsub.close()

        if self.redis:
            await self.redis.close()

    def _registry_key(self, *parts: str) -> str:
        """Generate a registry key."""
        return f"{self.channel_prefix}registry:{':'.join(parts)}"

    async def registry_add_connection(
        self,
        connection_id: str,
        user_id: str | None,
        metadata: dict[str, Any],
        groups: set[str],
        heartbeat_timeout: float,
    ) -> None:
        """Add connection to registry with metadata."""
        if not self.redis:
            await self.connect()
        assert self.redis is not None

        redis_client: Any = self.redis
        await redis_client.sadd(self._registry_key("connections"), connection_id)
        await redis_client.hset(
            self._registry_key("connection", connection_id),
            mapping={
                "user_id": user_id or "",
                "metadata": json.dumps(metadata),
                "heartbeat_timeout": str(heartbeat_timeout),
                "groups": json.dumps(list(groups)),
            },
        )
        if user_id:
            await redis_client.sadd(self._registry_key("user", user_id), connection_id)

    async def registry_remove_connection(self, connection_id: str, user_id: str | None) -> None:
        """Remove connection from registry."""
        if not self.redis:
            return
        assert self.redis is not None

        redis_client: Any = self.redis
        await redis_client.srem(self._registry_key("connections"), connection_id)
        await redis_client.delete(self._registry_key("connection", connection_id))
        if user_id:
            await redis_client.srem(self._registry_key("user", user_id), connection_id)

    async def registry_update_groups(self, connection_id: str, groups: set[str]) -> None:
        if not self.redis:
            await self.connect()
        assert self.redis is not None

        redis_client: Any = self.redis
        await redis_client.hset(
            self._registry_key("connection", connection_id),
            "groups",
            json.dumps(list(groups)),
        )

    async def registry_get_connection_groups(self, connection_id: str) -> set[str]:
        """Get groups for a connection."""
        if not self.redis:
            await self.connect()
        assert self.redis is not None

        redis_client: Any = self.redis
        groups_data = await redis_client.hget(
            self._registry_key("connection", connection_id), "groups"
        )
        if groups_data:
            try:
                return set(json.loads(groups_data))
            except (json.JSONDecodeError, TypeError):
                pass
        return set()

    async def registry_count_connections(self) -> int:
        """Count total connections in registry."""
        if not self.redis:
            await self.connect()
        assert self.redis is not None

        redis_client: Any = self.redis
        return int(await redis_client.scard(self._registry_key("connections")))

    async def registry_get_user_connections(self, user_id: str) -> set[str]:
        if not self.redis:
            await self.connect()
        assert self.redis is not None

        redis_client: Any = self.redis
        members = await redis_client.smembers(self._registry_key("user", user_id))
        return {m.decode() if isinstance(m, (bytes, bytearray)) else str(m) for m in members}

    def registry_get_prefix(self) -> str:
        return f"{self.channel_prefix}registry:"

    def supports_broadcast_channel(self) -> bool:
        """Check if backend supports broadcast channel."""
        return True
