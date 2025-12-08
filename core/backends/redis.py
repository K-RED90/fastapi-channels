import asyncio
import json
from collections import defaultdict
from typing import TYPE_CHECKING, Any, Optional

import redis.asyncio as aioredis
from redis.asyncio import Redis
from redis.asyncio.client import PubSub

from core.serializers import JSONSerializer

from .base import BaseBackend

if TYPE_CHECKING:
    from core.serializers import BaseSerializer


class RedisBackend(BaseBackend):
    """Redis-based channel layer backend for distributed WebSocket deployments.

    This backend uses Redis for message passing and state storage, enabling
    WebSocket communication across multiple server instances. It provides
    persistent storage, cross-server group messaging, and broadcast capabilities.

    Key features:
    - Distributed messaging across multiple servers
    - Persistent group memberships and registry data
    - Configurable TTL for registry keys
    - Broadcast channel support for global messaging
    - JSON/binary message serialization

    Parameters
    ----------
    redis_url : str
        Redis connection URL (e.g., "redis://localhost:6379/0")
    channel_prefix : str, optional
        Prefix for Redis pub/sub channels. Default: "ws:"
    serializer : BaseSerializer, optional
        Message serializer for encoding/decoding. Default: JSONSerializer()
    registry_expiry : int, optional
        TTL in seconds for registry keys. Default: None (no expiry)
    group_expiry : int, optional
        TTL in seconds for group keys. Default: None (no expiry)

    Examples
    --------
    Basic setup for distributed chat application:

    >>> backend = RedisBackend(
    ...     redis_url="redis://localhost:6379/0",
    ...     channel_prefix="chat:",
    ...     registry_expiry=3600,  # 1 hour TTL
    ...     group_expiry=86400    # 24 hour TTL
    ... )
    >>> await backend.connect()
    >>> await backend.publish("room.general", {"text": "hello"})

    Notes
    -----
    Requires Redis server for operation. Connection is established lazily
    on first operation. Registry expiry helps prevent stale connection data
    in long-running distributed deployments.

    """

    def __init__(
        self,
        redis_url: str,
        channel_prefix: str = "ws:",
        serializer: Optional["BaseSerializer"] = None,
        registry_expiry: int | None = None,
        group_expiry: int | None = None,
    ):
        self.redis_url = redis_url
        self.channel_prefix = channel_prefix
        self.serializer = serializer or JSONSerializer()
        self.registry_expiry = registry_expiry
        self.group_expiry = group_expiry
        self.redis: Redis | None = None
        self.pubsub: PubSub | None = None
        self._listener_task: asyncio.Task | None = None
        self._pending_receives: dict[str, list[asyncio.Future]] = defaultdict(list)

    async def connect(self) -> None:
        """Establish connection to Redis server.

        Creates Redis client and PubSub instances with appropriate
        encoding settings based on serializer configuration.

        Raises
        ------
        redis.ConnectionError
            If unable to connect to Redis server

        """
        self.redis = aioredis.from_url(
            self.redis_url,
            encoding=None if self.serializer.binary else "utf-8",
            decode_responses=not self.serializer.binary,
        )
        self.pubsub = self.redis.pubsub()

    async def publish(self, channel: str, message: dict[str, Any]) -> None:
        """Publish message to Redis pub/sub channel.

        Parameters
        ----------
        channel : str
            Target channel name (will be prefixed)
        message : dict[str, Any]
            Message payload to publish

        Notes
        -----
        Message is serialized using configured serializer before publishing.
        Connects to Redis automatically if not already connected.

        """
        if not self.redis:
            await self.connect()
        assert self.redis is not None

        full_channel = f"{self.channel_prefix}{channel}"
        serialized = self.serializer.dumps(message)
        await self.redis.publish(full_channel, serialized)

    async def subscribe(self, channel: str) -> None:
        """Subscribe to Redis pub/sub channel.

        Parameters
        ----------
        channel : str
            Channel name to subscribe to

        Notes
        -----
        Starts message listener task if not already running.
        Channel name is prefixed automatically.

        """
        if not self.pubsub:
            await self.connect()
        assert self.pubsub is not None

        full_channel = f"{self.channel_prefix}{channel}"
        await self.pubsub.subscribe(full_channel)

        if not self._listener_task or self._listener_task.done():
            self._listener_task = asyncio.create_task(self._listen())

    async def unsubscribe(self, channel: str) -> None:
        """Unsubscribe from Redis pub/sub channel.

        Parameters
        ----------
        channel : str
            Channel name to unsubscribe from

        Notes
        -----
        Channel name is prefixed automatically.
        No-op if not currently subscribed.

        """
        if not self.pubsub:
            return

        full_channel = f"{self.channel_prefix}{channel}"
        await self.pubsub.unsubscribe(full_channel)

    async def group_send(self, group: str, message: dict[str, Any]) -> None:
        """Send message to all channels in Redis group.

        Parameters
        ----------
        group : str
            Target group name
        message : dict[str, Any]
            Message payload to deliver

        Notes
        -----
        Retrieves group members from Redis and publishes to each channel.
        Handles exceptions gracefully - failed publishes are ignored.

        """
        if not self.redis:
            await self.connect()
        assert self.redis is not None

        group_key = f"{self.channel_prefix}group:{group}"
        redis_client: Any = self.redis
        channels = await redis_client.smembers(group_key)

        tasks = [self.publish(channel, message) for channel in channels]
        await asyncio.gather(*tasks, return_exceptions=True)

    async def group_add(self, group: str, channel: str) -> None:
        """Add channel to Redis group with optional TTL.

        Parameters
        ----------
        group : str
            Group name to add channel to
        channel : str
            Channel name to add to group

        Notes
        -----
        Uses Redis pipeline for atomic add + expire operations when TTL configured.
        Group key gets TTL on first channel addition.

        """
        if not self.redis:
            await self.connect()
        assert self.redis is not None

        group_key = f"{self.channel_prefix}group:{group}"
        redis_client: Any = self.redis

        if self.group_expiry is not None:
            pipe = redis_client.pipeline()
            pipe.sadd(group_key, self.group_expiry)
            pipe.expire(group_key, self.group_expiry)
            await pipe.execute()
        else:
            await redis_client.sadd(group_key, channel)

    async def group_discard(self, group: str, channel: str) -> None:
        """Remove channel from Redis group.

        Parameters
        ----------
        group : str
            Group name to remove channel from
        channel : str
            Channel name to remove from group

        Notes
        -----
        No-op if channel is not in group.
        Group key remains in Redis even when empty.

        """
        if not self.redis:
            return
        assert self.redis is not None

        group_key = f"{self.channel_prefix}group:{group}"
        redis_client: Any = self.redis
        await redis_client.srem(group_key, channel)

    async def group_channels(self, group: str) -> set[str]:
        """Get all channels in Redis group.

        Parameters
        ----------
        group : str
            Group name to query

        Returns
        -------
        Set[str]
            Set of channel names in the group

        Notes
        -----
        Handles both string and bytes responses from Redis.
        Returns empty set if group doesn't exist.

        """
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

                # Deliver to pending receive() calls
                if self._pending_receives.get(channel):
                    # Pop the first pending future and set its result
                    future = self._pending_receives[channel].pop(0)
                    if not future.done():
                        future.set_result(data)

    async def receive(self, channel: str, timeout: float | None = None) -> dict[str, Any] | None:
        """Receive next message from Redis pub/sub channel.

        Parameters
        ----------
        channel : str
            Channel name to receive from
        timeout : float | None, optional
            Maximum wait time in seconds. Default: None (wait indefinitely)

        Returns
        -------
        Dict[str, Any] | None
            Next message from channel, or None if timeout exceeded

        Notes
        -----
        Creates future and waits for message delivery via _listen task.
        Automatically subscribes to channel if not already subscribed.

        """
        await self.subscribe(channel)

        future: asyncio.Future = asyncio.Future()
        self._pending_receives[channel].append(future)

        try:
            if timeout is not None:
                return await asyncio.wait_for(future, timeout=timeout)
            return await future
        except TimeoutError:
            if future in self._pending_receives[channel]:
                self._pending_receives[channel].remove(future)
            return None
        except Exception:
            if future in self._pending_receives[channel]:
                self._pending_receives[channel].remove(future)
            raise

    async def flush(self) -> None:
        """Clear all Redis keys matching channel prefix.

        Removes all channels, groups, and registry data from Redis.
        Cancels all pending receive operations.

        Notes
        -----
        Uses SCAN to find and delete all matching keys.
        Used primarily for testing and development.

        """
        if self.redis:
            pattern = f"{self.channel_prefix}*"
            async for key in self.redis.scan_iter(match=pattern):
                await self.redis.delete(key)
        # Cancel any pending receives
        for channel_futures in self._pending_receives.values():
            for future in channel_futures:
                if not future.done():
                    future.cancel()
        self._pending_receives.clear()

    async def cleanup(self) -> None:
        """Clean up Redis connections and background tasks.

        Cancels listener task, closes pubsub connection, and closes Redis client.
        Should be called during application shutdown.
        """
        if self._listener_task:
            self._listener_task.cancel()
            await self._listener_task

        if self.pubsub:
            await self.pubsub.close()

        if self.redis:
            await self.redis.close()

    def _registry_key(self, *parts: str) -> str:
        return f"{self.channel_prefix}registry:{':'.join(parts)}"

    async def registry_add_connection(
        self,
        connection_id: str,
        user_id: str | None,
        metadata: dict[str, Any],
        groups: set[str],
        heartbeat_timeout: float,
    ) -> None:
        """Add connection to Redis-based distributed registry.

        Parameters
        ----------
        connection_id : str
            Unique connection identifier
        user_id : str | None
            User identifier if authenticated
        metadata : dict[str, Any]
            Connection metadata (IP, user agent, etc.)
        groups : set[str]
            Initial groups the connection belongs to
        heartbeat_timeout : float
            Heartbeat timeout in seconds

        Notes
        -----
        Stores connection data in Redis hash with JSON serialization.
        Applies TTL to registry keys if configured.
        Maintains bidirectional user-connection mappings.

        """
        if not self.redis:
            await self.connect()
        assert self.redis is not None

        redis_client: Any = self.redis
        await redis_client.sadd(self._registry_key("connections"), connection_id)
        connection_key = self._registry_key("connection", connection_id)
        await redis_client.hset(
            connection_key,
            mapping={
                "user_id": user_id or "",
                "metadata": json.dumps(metadata),
                "heartbeat_timeout": str(heartbeat_timeout),
                "groups": json.dumps(list(groups)),
            },
        )
        if user_id:
            await redis_client.sadd(self._registry_key("user", user_id), connection_id)

        # Apply TTL if configured
        if self.registry_expiry is not None:
            await redis_client.expire(self._registry_key("connections"), self.registry_expiry)
            await redis_client.expire(connection_key, self.registry_expiry)
            if user_id:
                await redis_client.expire(self._registry_key("user", user_id), self.registry_expiry)

    async def registry_add_connection_if_under_limit(
        self,
        connection_id: str,
        user_id: str | None,
        metadata: dict[str, Any],
        groups: set[str],
        heartbeat_timeout: float,
        max_connections: int,
    ) -> bool:
        """Atomically check connection limit and add connection if under limit.

        Uses a Lua script to ensure atomicity across distributed servers.

        Parameters
        ----------
        connection_id : str
            Unique identifier for the connection
        user_id : str | None
            User identifier if authenticated, None for anonymous
        metadata : dict[str, Any]
            Additional connection metadata (IP, user agent, etc.)
        groups : set[str]
            Initial groups the connection belongs to
        heartbeat_timeout : float
            Heartbeat timeout in seconds
        max_connections : int
            Maximum total connections allowed

        Returns
        -------
        bool
            True if connection was added, False if limit would be exceeded

        Notes
        -----
        Atomic operation prevents race conditions in distributed deployments.
        Uses Redis Lua script for true atomicity across all server instances.

        """
        if not self.redis:
            await self.connect()
        assert self.redis is not None

        redis_client: Any = self.redis
        connections_key = self._registry_key("connections")
        connection_key = self._registry_key("connection", connection_id)
        user_key = self._registry_key("user", user_id) if user_id else None

        # Lua script for atomic check-and-add
        # Handles optional user_key by checking if it's provided in KEYS
        lua_script = """
        local connections_key = KEYS[1]
        local connection_key = KEYS[2]
        local user_key = #KEYS >= 3 and KEYS[3] or nil
        local max_connections = tonumber(ARGV[1])
        local connection_id = ARGV[2]
        local user_id = ARGV[3]
        local metadata = ARGV[4]
        local heartbeat_timeout = ARGV[5]
        local groups = ARGV[6]
        local registry_expiry = ARGV[7]

        -- Check current count
        local current_count = redis.call('SCARD', connections_key)
        if current_count >= max_connections then
            return 0
        end

        -- Add connection atomically
        redis.call('SADD', connections_key, connection_id)
        redis.call('HSET', connection_key,
            'user_id', user_id,
            'metadata', metadata,
            'heartbeat_timeout', heartbeat_timeout,
            'groups', groups
        )

        -- Add to user set if user_key and user_id provided
        if user_key and user_id ~= '' then
            redis.call('SADD', user_key, connection_id)
        end

        -- Apply TTL if configured
        if registry_expiry and registry_expiry ~= '' then
            local expiry = tonumber(registry_expiry)
            redis.call('EXPIRE', connections_key, expiry)
            redis.call('EXPIRE', connection_key, expiry)
            if user_key then
                redis.call('EXPIRE', user_key, expiry)
            end
        end

        return 1
        """

        keys = [connections_key, connection_key]
        if user_key:
            keys.append(user_key)

        args = [
            str(max_connections),
            connection_id,
            user_id or "",
            json.dumps(metadata),
            str(heartbeat_timeout),
            json.dumps(list(groups)),
            str(self.registry_expiry) if self.registry_expiry is not None else "",
        ]

        result = await redis_client.eval(lua_script, len(keys), *keys, *args)
        return bool(result)

    async def registry_remove_connection(self, connection_id: str, user_id: str | None) -> None:
        """Remove connection from Redis registry.

        Parameters
        ----------
        connection_id : str
            Connection identifier to remove
        user_id : str | None
            User identifier for cleanup

        Notes
        -----
        Removes connection from all registry sets and deletes connection hash.
        Cleans up user-connection mappings.

        """
        if not self.redis:
            return
        assert self.redis is not None

        redis_client: Any = self.redis
        await redis_client.srem(self._registry_key("connections"), connection_id)
        await redis_client.delete(self._registry_key("connection", connection_id))
        if user_id:
            await redis_client.srem(self._registry_key("user", user_id), connection_id)

    async def registry_update_groups(self, connection_id: str, groups: set[str]) -> None:
        """Update groups for connection in Redis registry.

        Parameters
        ----------
        connection_id : str
            Connection identifier to update
        groups : Set[str]
            New set of groups for the connection

        Notes
        -----
        Updates groups field in connection hash.
        Refreshes TTL if configured.

        """
        if not self.redis:
            await self.connect()
        assert self.redis is not None

        redis_client: Any = self.redis
        connection_key = self._registry_key("connection", connection_id)
        await redis_client.hset(
            connection_key,
            "groups",
            json.dumps(list(groups)),
        )
        # Refresh TTL if configured
        if self.registry_expiry is not None:
            await redis_client.expire(connection_key, self.registry_expiry)

    async def registry_get_connection_groups(self, connection_id: str) -> set[str]:
        """Get groups for a connection from Redis registry.

        Parameters
        ----------
        connection_id : str
            Connection identifier to query

        Returns
        -------
        set[str]
            Set of group names the connection belongs to

        Notes
        -----
        Deserializes groups from JSON stored in Redis hash.
        Returns empty set if connection not found or JSON invalid.

        """
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
        """Count total connections across all servers from Redis registry.

        Returns
        -------
        int
            Total connection count across distributed deployment

        Notes
        -----
        Uses Redis SCARD to count connections set members.
        Includes connections from all server instances.

        """
        if not self.redis:
            await self.connect()
        assert self.redis is not None

        redis_client: Any = self.redis
        return int(await redis_client.scard(self._registry_key("connections")))

    async def registry_get_user_connections(self, user_id: str) -> set[str]:
        """Get all connection IDs for a user from Redis registry.

        Parameters
        ----------
        user_id : str
            User identifier to query

        Returns
        -------
        set[str]
            Set of connection IDs for the user

        Notes
        -----
        Handles both string and bytes responses from Redis.
        Returns empty set if user has no connections.

        """
        if not self.redis:
            await self.connect()
        assert self.redis is not None

        redis_client: Any = self.redis
        members = await redis_client.smembers(self._registry_key("user", user_id))
        return {m.decode() if isinstance(m, (bytes, bytearray)) else str(m) for m in members}

    def registry_get_prefix(self) -> str:
        """Get registry key prefix for this Redis backend.

        Returns
        -------
        str
            Registry prefix including channel prefix

        Examples
        --------
        >>> backend.registry_get_prefix()
        'ws:registry:'

        """
        return f"{self.channel_prefix}registry:"

    def supports_broadcast_channel(self) -> bool:
        """Check if backend supports broadcast channel.

        Returns
        -------
        bool
            True - RedisBackend supports global broadcast via "__broadcast__" channel

        Notes
        -----
        Broadcast channel allows sending messages to all connections
        without explicitly managing groups. Requires Redis pub/sub.

        """
        return True
