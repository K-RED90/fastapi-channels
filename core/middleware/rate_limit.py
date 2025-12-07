import logging
import time

from core.exceptions import RateLimitError
from core.middleware.base import Middleware

logger = logging.getLogger(__name__)


class TokenBucketRateLimiter:
    """Simple token bucket rate limiter per key."""

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


class RateLimitMiddleware(Middleware):
    """Apply token bucket rate limiting to messages."""

    def __init__(
        self,
        next_middleware: Middleware | None = None,
        enabled: bool = False,
        messages_per_window: int = 100,
        window_seconds: int = 60,
        burst_size: int = 100,
    ):
        super().__init__(next_middleware)
        self.enabled = enabled
        self.messages_per_window = messages_per_window
        self.window_seconds = window_seconds
        self.burst_size = burst_size
        self.limiter = TokenBucketRateLimiter(
            rate=self.messages_per_window,
            window_seconds=self.window_seconds,
            burst_size=self.burst_size,
        )

    async def process(self, message, connection, consumer):
        if not self.enabled:
            return message

        if message.type in {"ping", "pong"}:
            return message

        key = getattr(connection, "channel_name", None) or getattr(connection, "id", "unknown")
        if not self.limiter.allow(key):
            from core.exceptions import create_error_context

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
