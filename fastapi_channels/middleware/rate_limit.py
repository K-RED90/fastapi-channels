import logging
import time
from typing import TYPE_CHECKING

from fastapi_channels.exceptions import RateLimitError
from fastapi_channels.middleware import Middleware

if TYPE_CHECKING:
    from redis.asyncio import Redis

logger = logging.getLogger(__name__)


class TokenBucketRateLimiter:
    """Simple in-memory token bucket rate limiter per key.

    Note: This limiter is NOT distributed and only works within a single
    server instance. For distributed deployments, use RedisRateLimiter.
    """

    def __init__(self, rate: int, window_seconds: int, burst_size: int):
        self.rate = rate
        self.window = window_seconds
        self.burst = burst_size
        self.buckets: dict[str, tuple[float, int]] = {}

    def allow(self, key: str) -> bool:
        now = time.time()

        if key not in self.buckets:
            self.buckets[key] = (now, self.burst - 1)
            return True

        last_check, tokens = self.buckets[key]
        elapsed = now - last_check

        tokens = min(self.burst, tokens + int(elapsed * (self.rate / self.window)))

        if tokens > 0:
            self.buckets[key] = (now, tokens - 1)
            return True

        self.buckets[key] = (now, 0)
        return False


class RedisRateLimiter:
    """Distributed rate limiter using Redis sliding window.

    Uses Redis sorted sets to implement a sliding window rate limiter
    that works across multiple server instances.

    Parameters
    ----------
    redis : Redis
        Redis client instance
    rate : int
        Maximum requests allowed per window
    window_seconds : int
        Size of the sliding window in seconds
    key_prefix : str, optional
        Prefix for Redis keys. Default: "ratelimit:"

    Examples
    --------
    >>> limiter = RedisRateLimiter(redis, rate=100, window_seconds=60)
    >>> allowed = await limiter.allow("user:123")

    Notes
    -----
    Uses a Lua script for atomic operations ensuring accurate rate
    limiting even under high concurrency across distributed servers.

    """

    def __init__(
        self,
        redis: "Redis",
        rate: int,
        window_seconds: int,
        key_prefix: str = "ratelimit:",
    ):
        self.redis = redis
        self.rate = rate
        self.window = window_seconds
        self.key_prefix = key_prefix

        # Lua script for atomic sliding window rate limiting
        # This script:
        # 1. Removes expired entries from the sorted set
        # 2. Counts current entries in the window
        # 3. If under limit, adds new entry and returns 1 (allowed)
        # 4. If over limit, returns 0 (denied)
        self._lua_script = """
        local key = KEYS[1]
        local now = tonumber(ARGV[1])
        local window = tonumber(ARGV[2])
        local rate = tonumber(ARGV[3])
        local request_id = ARGV[4]

        -- Remove expired entries (older than window)
        local window_start = now - window
        redis.call('ZREMRANGEBYSCORE', key, '-inf', window_start)

        -- Count current requests in window
        local current_count = redis.call('ZCARD', key)

        if current_count < rate then
            -- Under limit: add this request and allow
            redis.call('ZADD', key, now, request_id)
            -- Set key expiry to window size + buffer for cleanup
            redis.call('EXPIRE', key, window + 1)
            return 1
        else
            -- Over limit: deny
            return 0
        end
        """

    async def allow(self, key: str) -> bool:
        """Check if request is allowed under rate limit.

        Parameters
        ----------
        key : str
            Unique identifier for the rate limit bucket (e.g., user_id, connection_id)

        Returns
        -------
        bool
            True if request is allowed, False if rate limited

        """
        import uuid

        full_key = f"{self.key_prefix}{key}"
        now = time.time()
        request_id = f"{now}:{uuid.uuid4().hex[:8]}"

        result = await self.redis.eval(  # type: ignore[awaitable]
            self._lua_script,
            1,  # Number of keys
            full_key,  # KEYS[1]
            str(now),  # ARGV[1]
            str(self.window),  # ARGV[2]
            str(self.rate),  # ARGV[3]
            request_id,  # ARGV[4]
        )

        return bool(result)

    async def get_remaining(self, key: str) -> int:
        """Get remaining requests allowed in current window.

        Parameters
        ----------
        key : str
            Rate limit bucket identifier

        Returns
        -------
        int
            Number of remaining requests allowed

        """
        full_key = f"{self.key_prefix}{key}"
        now = time.time()
        window_start = now - self.window

        # Remove expired and count
        await self.redis.zremrangebyscore(full_key, "-inf", window_start)
        current_count = await self.redis.zcard(full_key)

        return max(0, self.rate - current_count)


class RateLimitMiddleware(Middleware):
    """Apply rate limiting to messages.

    Supports both in-memory (single server) and Redis-based (distributed)
    rate limiting.

    Parameters
    ----------
    next_middleware : Middleware | None, optional
        Next middleware in chain. Default: None
    messages_per_window : int, optional
        Maximum messages per window. Default: 100
    window_seconds : int, optional
        Window size in seconds. Default: 60
    burst_size : int, optional
        Burst size for token bucket (in-memory only). Default: 100
    redis : Redis | None, optional
        Redis client for distributed rate limiting. Default: None (in-memory)
    key_prefix : str, optional
        Redis key prefix. Default: "ratelimit:"

    Examples
    --------
    In-memory rate limiting (single server):

    >>> middleware = RateLimitMiddleware(enabled=True, messages_per_window=100)

    Distributed rate limiting with Redis:

    >>> from redis.asyncio import Redis
    >>> redis = Redis.from_url("redis://localhost:6379/0")
    >>> middleware = RateLimitMiddleware(enabled=True, redis=redis)

    """

    def __init__(
        self,
        next_middleware: Middleware | None = None,
        messages_per_window: int = 100,
        window_seconds: int = 60,
        burst_size: int = 100,
        redis: "Redis | None" = None,
        key_prefix: str = "ratelimit:",
        excluded_message_types: set[str] | None = None,
    ):
        super().__init__(next_middleware)
        self.messages_per_window = messages_per_window
        self.window_seconds = window_seconds
        self.burst_size = burst_size
        self.redis = redis
        self.key_prefix = key_prefix
        self.excluded_message_types = excluded_message_types or set()

        # Use Redis-based limiter if Redis is provided, otherwise use in-memory
        if redis is not None:
            self._redis_limiter = RedisRateLimiter(
                redis=redis,
                rate=self.messages_per_window,
                window_seconds=self.window_seconds,
                key_prefix=self.key_prefix,
            )
            self._memory_limiter = None
        else:
            self._redis_limiter = None
            self._memory_limiter = TokenBucketRateLimiter(
                rate=self.messages_per_window,
                window_seconds=self.window_seconds,
                burst_size=self.burst_size,
            )

    async def _check_rate_limit(self, key: str) -> bool:
        """Check if request is allowed under rate limit."""
        if self._redis_limiter is not None:
            return await self._redis_limiter.allow(key)
        if self._memory_limiter is not None:
            return self._memory_limiter.allow(key)
        return True

    async def process(self, message, connection, consumer):
        if message.type in self.excluded_message_types:
            return message

        key = connection.channel_name
        if not await self._check_rate_limit(key):
            from fastapi_channels.exceptions import create_error_context

            context = create_error_context(
                user_id=connection.user_id,
                connection_id=connection.channel_name,
                message_type=message.type,
                component="rate_limit_middleware",
                rate_limit_key=key,
            )
            logger.warning("Rate limit exceeded for %s", key)
            raise RateLimitError(
                "Rate limit exceeded",
                context=context,
            )

        return message
