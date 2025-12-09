import asyncio
import json
from collections import defaultdict
from collections.abc import AsyncIterator, Awaitable
from typing import TYPE_CHECKING, Any, Optional, TypeVar

from redis.asyncio import Redis
from redis.asyncio.client import PubSub
from redis.asyncio.connection import ConnectionPool
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import TimeoutError as RedisTimeoutError

from core.serializers import JSONSerializer
from core.utils import with_retry

from .base import BaseBackend, CleanupStats, OrphanedGroupMembersStats

if TYPE_CHECKING:
    from core.serializers import BaseSerializer


T = TypeVar("T")


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
    - Connection pooling for high concurrency
    - Retry logic with exponential backoff for reliability

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
    max_connections : int, optional
        Maximum connections in the connection pool. Default: 100
    socket_timeout : float, optional
        Socket read/write timeout in seconds. Default: 5.0
    socket_keepalive : bool, optional
        Enable TCP keepalive. Default: True
    retry_on_timeout : bool, optional
        Retry operations on timeout. Default: True
    health_check_interval : int, optional
        Connection health check interval in seconds. Default: 30
    retry_max_attempts : int, optional
        Maximum retry attempts for failed operations. Default: 3
    retry_base_delay : float, optional
        Base delay between retries in seconds. Default: 0.1
    retry_max_delay : float, optional
        Maximum delay between retries in seconds. Default: 2.0

    Examples
    --------
    Basic setup for distributed chat application:

    >>> backend = RedisBackend(
    ...     redis_url="redis://localhost:6379/0",
    ...     channel_prefix="chat:",
    ...     registry_expiry=3600,  # 1 hour TTL
    ...     group_expiry=86400,   # 24 hour TTL
    ...     max_connections=200,  # Connection pool size
    ... )
    >>> await backend.connect()
    >>> await backend.publish("room.general", {"text": "hello"})

    Notes
    -----
    Requires Redis server for operation. Connection is established lazily
    on first operation. Registry expiry helps prevent stale connection data
    in long-running distributed deployments. Connection pooling improves
    performance under high concurrency.

    """

    def __init__(
        self,
        redis_url: str,
        channel_prefix: str = "ws:",
        serializer: Optional["BaseSerializer"] = None,
        registry_expiry: int | None = None,
        group_expiry: int | None = None,
        max_connections: int = 100,
        socket_timeout: float = 5.0,
        socket_keepalive: bool = True,
        retry_on_timeout: bool = True,
        health_check_interval: int = 30,
        retry_max_attempts: int = 3,
        retry_base_delay: float = 0.1,
        retry_max_delay: float = 2.0,
    ):
        self.redis_url = redis_url
        self.channel_prefix = channel_prefix
        self.serializer = serializer or JSONSerializer()
        self.registry_expiry = registry_expiry
        self.group_expiry = group_expiry
        self.max_connections = max_connections
        self.socket_timeout = socket_timeout
        self.socket_keepalive = socket_keepalive
        self.retry_on_timeout = retry_on_timeout
        self.health_check_interval = health_check_interval
        self.retry_max_attempts = retry_max_attempts
        self.retry_base_delay = retry_base_delay
        self.retry_max_delay = retry_max_delay
        self.redis: Redis | None = None
        self.pubsub: PubSub | None = None
        self._listener_task: asyncio.Task | None = None
        self._pending_receives: dict[str, list[asyncio.Future]] = defaultdict(list)
        self._connection_pool: ConnectionPool | None = None

    async def _with_optional_timeout(self, coro: Awaitable[T], timeout: float | None) -> T:
        if timeout is None:
            return await coro
        return await asyncio.wait_for(coro, timeout=timeout)

    async def connect(self) -> None:
        """Establish connection to Redis server with connection pooling.

        Creates Redis client with connection pool and PubSub instances.
        Connection pool provides better resource management for high
        concurrency scenarios.

        Raises
        ------
        redis.ConnectionError
            If unable to connect to Redis server

        Notes
        -----
        Connection pool settings:
        - max_connections: Maximum pool size (default: 100)
        - socket_timeout: Socket read/write timeout (default: 5.0s)
        - socket_keepalive: Enable TCP keepalive (default: True)
        - retry_on_timeout: Retry operations on timeout (default: True)
        - health_check_interval: Connection health check interval (default: 30s)

        """
        self._connection_pool = ConnectionPool.from_url(
            self.redis_url,
            max_connections=self.max_connections,
            socket_timeout=self.socket_timeout,
            socket_keepalive=self.socket_keepalive,
            retry_on_timeout=self.retry_on_timeout,
            health_check_interval=self.health_check_interval,
            encoding=None if self.serializer.binary else "utf-8",
            decode_responses=not self.serializer.binary,
        )

        self.redis = Redis(connection_pool=self._connection_pool)
        self.pubsub = self.redis.pubsub()

    def _get_retry_decorator(self):
        return with_retry(
            max_retries=self.retry_max_attempts,
            base_delay=self.retry_base_delay,
            max_delay=self.retry_max_delay,
            exponential_base=2.0,
            jitter=True,
            exceptions=(RedisConnectionError, RedisTimeoutError, OSError),
        )

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
        Uses retry logic with exponential backoff for reliability.

        """
        if not self.redis:
            await self.connect()
        assert self.redis is not None

        full_channel = f"{self.channel_prefix}{channel}"
        serialized = self.serializer.dumps(message)

        @self._get_retry_decorator()
        async def _publish() -> None:
            assert self.redis is not None
            await self.redis.publish(full_channel, serialized)

        await _publish()

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
        Uses retry logic with exponential backoff for reliability.

        """
        if not self.pubsub:
            await self.connect()
        assert self.pubsub is not None

        full_channel = f"{self.channel_prefix}{channel}"

        @self._get_retry_decorator()
        async def _subscribe() -> None:
            assert self.pubsub is not None
            await self.pubsub.subscribe(full_channel)

        await _subscribe()

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

    async def group_send(
        self, group: str, message: dict[str, Any], exclude_channel: str | None = None
    ) -> None:
        """Send message to all channels in Redis group.

        Parameters
        ----------
        group : str
            Target group name
        message : dict[str, Any]
            Message payload to deliver
        exclude_channel : str | None, optional
            Channel to exclude from delivery. Default: None

        Notes
        -----
        Uses streaming with SSCAN to avoid loading all members into memory.
        Uses Redis pipeline for efficient batch publishing within each batch.
        Handles exceptions gracefully - failed publishes are ignored.
        If exclude_channel is provided, that channel will not receive the message.

        """
        if not self.redis:
            await self.connect()
        assert self.redis is not None

        serialized = self.serializer.dumps(message)
        redis_client: Any = self.redis

        # Stream group members in batches and use pipeline for each batch
        async for batch in self.group_channels_stream(group, batch_size=100):
            # Filter out excluded channel if specified
            filtered_batch = (
                [ch for ch in batch if ch != exclude_channel] if exclude_channel else batch
            )
            if not filtered_batch:
                continue

            pipe = redis_client.pipeline()
            for channel in filtered_batch:
                full_channel = f"{self.channel_prefix}{channel}"
                pipe.publish(full_channel, serialized)
            try:
                await pipe.execute()
            except Exception:
                # Fallback to individual publishes if pipeline fails
                for channel in filtered_batch:
                    try:
                        await self.publish(channel, message)
                    except Exception:
                        pass

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
            pipe.sadd(group_key, channel)
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

        Consider using group_channels_stream() for large groups to avoid
        loading all members into memory at once.

        """
        if not self.redis:
            await self.connect()
        assert self.redis is not None

        group_key = f"{self.channel_prefix}group:{group}"
        redis_client: Any = self.redis
        members = await redis_client.smembers(group_key)
        return {m.decode() if isinstance(m, (bytes, bytearray)) else m for m in members}

    async def group_channels_stream(
        self, group: str, batch_size: int = 100
    ) -> AsyncIterator[list[str]]:
        """Stream group members using SSCAN to avoid loading all into memory.

        Parameters
        ----------
        group : str
            Group name to query
        batch_size : int, optional
            Approximate number of members per batch. Default: 100

        Yields
        ------
        list[str]
            Batch of channel names in the group

        Notes
        -----
        Uses Redis SSCAN for cursor-based iteration, which is memory-efficient
        for large groups. The batch_size is a hint to Redis and actual batch
        sizes may vary.

        Examples
        --------
        Processing group members in batches:

        >>> async for batch in backend.group_channels_stream("room_123"):
        ...     for channel in batch:
        ...         await backend.publish(channel, message)

        """
        if not self.redis:
            await self.connect()
        assert self.redis is not None

        group_key = f"{self.channel_prefix}group:{group}"
        redis_client: Any = self.redis
        cursor = 0

        while True:
            cursor, members = await redis_client.sscan(group_key, cursor, count=batch_size)
            if members:
                batch = [m.decode() if isinstance(m, (bytes, bytearray)) else m for m in members]
                yield batch
            if cursor == 0:
                break

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

        Cancels listener task, closes pubsub connection, closes Redis client,
        and disconnects the connection pool.
        Should be called during application shutdown.
        """
        if self._listener_task:
            self._listener_task.cancel()
            try:
                await self._listener_task
            except asyncio.CancelledError:
                pass

        if self.pubsub:
            await self.pubsub.close()

        if self.redis:
            await self.redis.close()

        if self._connection_pool:
            await self._connection_pool.disconnect()

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

    async def registry_refresh_ttl(self, connection_id: str, user_id: str | None = None) -> None:
        """Refresh TTL for connection registry keys to prevent premature expiry.

        Parameters
        ----------
        connection_id : str
            Connection identifier to refresh
        user_id : str | None, optional
            User identifier for user mapping TTL refresh. Default: None

        Notes
        -----
        Should be called periodically (e.g., on heartbeat or activity) to keep
        active connections from expiring. Uses Redis pipeline for efficiency.
        No-op if registry_expiry is not configured.

        """
        if self.registry_expiry is None:
            return

        if not self.redis:
            await self.connect()
        assert self.redis is not None

        redis_client: Any = self.redis
        pipe = redis_client.pipeline()

        # Refresh all relevant keys
        pipe.expire(self._registry_key("connections"), self.registry_expiry)
        pipe.expire(self._registry_key("connection", connection_id), self.registry_expiry)
        if user_id:
            pipe.expire(self._registry_key("user", user_id), self.registry_expiry)

        try:
            await pipe.execute()
        except Exception:
            pass  # Best effort - don't fail if refresh fails

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

        Consider using registry_get_user_connections_stream() for users
        with many connections to avoid loading all into memory.

        """
        if not self.redis:
            await self.connect()
        assert self.redis is not None

        redis_client: Any = self.redis
        members = await redis_client.smembers(self._registry_key("user", user_id))
        return {m.decode() if isinstance(m, (bytes, bytearray)) else str(m) for m in members}

    async def registry_get_user_connections_stream(
        self, user_id: str, batch_size: int = 100
    ) -> AsyncIterator[list[str]]:
        """Stream user connections using SSCAN to avoid loading all into memory.

        Parameters
        ----------
        user_id : str
            User identifier to query
        batch_size : int, optional
            Approximate number of connections per batch. Default: 100

        Yields
        ------
        list[str]
            Batch of connection IDs for the user

        Notes
        -----
        Uses Redis SSCAN for cursor-based iteration, which is memory-efficient
        for users with many connections. The batch_size is a hint to Redis
        and actual batch sizes may vary.

        Examples
        --------
        Processing user connections in batches:

        >>> async for batch in backend.registry_get_user_connections_stream("user123"):
        ...     for conn_id in batch:
        ...         await backend.publish(conn_id, message)

        """
        if not self.redis:
            await self.connect()
        assert self.redis is not None

        user_key = self._registry_key("user", user_id)
        redis_client: Any = self.redis
        cursor = 0

        while True:
            cursor, members = await redis_client.sscan(user_key, cursor, count=batch_size)
            if members:
                batch = [
                    m.decode() if isinstance(m, (bytes, bytearray)) else str(m) for m in members
                ]
                yield batch
            if cursor == 0:
                break

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

    async def cleanup_stale_connections(
        self, server_instance_id: str, timeout: float | None = 30
    ) -> CleanupStats:
        """Remove stale connection registry entries for other servers or expired TTLs.

        Parameters
        ----------
        server_instance_id : str
            Identifier for the current server instance. Connections associated
            with a different server instance are treated as stale.

        Returns
        -------
        CleanupStats
            Statistics about cleaned items.

        """

        async def _run() -> CleanupStats:
            if not self.redis:
                await self.connect()
            assert self.redis is not None

            redis_client: Any = self.redis
            connections_key = self._registry_key("connections")
            stats: CleanupStats = {"connections_removed": 0, "user_mappings_cleaned": 0}
            cursor = 0
            batch_size = 100

            while True:
                cursor, raw_conn_ids = await redis_client.sscan(
                    connections_key, cursor=cursor, count=batch_size
                )
                conn_ids = [
                    cid.decode() if isinstance(cid, (bytes, bytearray)) else str(cid)
                    for cid in raw_conn_ids
                ]
                if not conn_ids and cursor == 0:
                    break

                pipe = redis_client.pipeline()
                for conn_id in conn_ids:
                    conn_key = self._registry_key("connection", conn_id)
                    pipe.hgetall(conn_key)
                    pipe.ttl(conn_key)
                results = await pipe.execute()

                stale_entries: list[tuple[str, str | None, list[str]]] = []

                for idx, conn_id in enumerate(conn_ids):
                    data = results[idx * 2]
                    ttl = results[idx * 2 + 1]

                    if not data:
                        stale_entries.append((conn_id, None, []))
                        continue

                    decoded_data = {
                        (k.decode() if isinstance(k, (bytes, bytearray)) else str(k)): (
                            v.decode() if isinstance(v, (bytes, bytearray)) else v
                        )
                        for k, v in data.items()
                    }

                    metadata_raw = decoded_data.get("metadata") or ""
                    user_id_raw = decoded_data.get("user_id") or ""
                    groups_raw = decoded_data.get("groups") or "[]"

                    try:
                        metadata = json.loads(metadata_raw) if metadata_raw else {}
                    except json.JSONDecodeError:
                        metadata = {}

                    try:
                        groups_list = json.loads(groups_raw) if groups_raw else []
                    except json.JSONDecodeError:
                        groups_list = []

                    stored_instance = metadata.get("server_instance_id")

                    ttl_stale = False
                    if self.registry_expiry is not None:
                        # ttl < 0 means missing/expired (includes no-ttl and expired)
                        ttl_stale = ttl is None or ttl <= 0

                    # Connections without instance metadata or belonging to a different
                    # server instance are considered stale regardless of TTL to prevent
                    # accumulation after crashes/restarts.
                    instance_stale = (
                        stored_instance is None or stored_instance != server_instance_id
                    )

                    if ttl_stale or instance_stale:
                        user_id = user_id_raw or None
                        stale_entries.append((conn_id, user_id, groups_list))

                if stale_entries:
                    delete_pipe = redis_client.pipeline()
                    for conn_id, user_id, groups in stale_entries:
                        conn_key = self._registry_key("connection", conn_id)
                        delete_pipe.srem(connections_key, conn_id)
                        delete_pipe.delete(conn_key)
                        if user_id:
                            delete_pipe.srem(self._registry_key("user", user_id), conn_id)
                            stats["user_mappings_cleaned"] += 1
                        for group in groups:
                            delete_pipe.srem(f"{self.channel_prefix}group:{group}", conn_id)

                    try:
                        await delete_pipe.execute()
                    except Exception:
                        pass

                    stats["connections_removed"] += len(stale_entries)

                if cursor == 0:
                    break

            return stats

        return await self._with_optional_timeout(_run(), timeout)

    async def cleanup_orphaned_group_members(
        self, timeout: float | None = 30
    ) -> OrphanedGroupMembersStats:
        async def _run() -> OrphanedGroupMembersStats:
            if not self.redis:
                await self.connect()
            assert self.redis is not None

            redis_client: Any = self.redis
            stats: OrphanedGroupMembersStats = {
                "orphaned_members_removed": 0,
                "empty_groups_removed": 0,
            }
            cursor = 0
            batch_size = 100

            pattern = f"{self.channel_prefix}group:*"

            while True:
                cursor, group_keys = await redis_client.scan(
                    cursor=cursor, match=pattern, count=100
                )
                if not group_keys and cursor == 0:
                    break

                for raw_group_key in group_keys:
                    group_key = (
                        raw_group_key.decode()
                        if isinstance(raw_group_key, (bytes, bytearray))
                        else str(raw_group_key)
                    )

                    member_cursor = 0
                    while True:
                        member_cursor, members = await redis_client.sscan(
                            group_key, cursor=member_cursor, count=batch_size
                        )
                        members_str = [
                            m.decode() if isinstance(m, (bytes, bytearray)) else str(m)
                            for m in members
                        ]

                        if members_str:
                            pipe = redis_client.pipeline()
                            for member in members_str:
                                pipe.exists(self._registry_key("connection", member))
                            exists_results = await pipe.execute()

                            delete_pipe = redis_client.pipeline()
                            for member, exists in zip(members_str, exists_results):
                                if not exists:
                                    delete_pipe.srem(group_key, member)
                                    stats["orphaned_members_removed"] += 1

                            if delete_pipe.command_stack:
                                try:
                                    await delete_pipe.execute()
                                except Exception:
                                    pass

                        if member_cursor == 0:
                            break

                    # Remove group if empty
                    try:
                        group_size = await redis_client.scard(group_key)
                        if group_size == 0:
                            await redis_client.delete(group_key)
                            stats["empty_groups_removed"] += 1
                    except Exception:
                        pass

                if cursor == 0:
                    break

            return stats

        return await self._with_optional_timeout(_run(), timeout)

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
