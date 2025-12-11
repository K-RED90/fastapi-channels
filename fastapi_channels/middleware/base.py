from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

from fastapi_channels.connections import Connection
from fastapi_channels.typed import Message

if TYPE_CHECKING:
    from fastapi_channels.consumer import BaseConsumer


class Middleware(ABC):
    def __init__(self, next_middleware: Middleware | None = None):
        self.next_middleware = next_middleware

    async def __call__(
        self,
        message: Message,
        connection: Connection,
        consumer: BaseConsumer,
    ) -> Message | None:
        """Process message then pass to next middleware."""
        processed_message = await self.process(message, connection, consumer)
        if processed_message is not None and self.next_middleware:
            return await self.next_middleware(processed_message, connection, consumer)
        return processed_message

    def __or__(self, other: Middleware) -> Middleware:
        assert isinstance(other, Middleware), f"{other!r} is not a Middleware instance"
        if self.next_middleware is None:
            self.next_middleware = other
        else:
            self.next_middleware = self.next_middleware | other
        return self

    def __rshift__(self, other: Middleware) -> Middleware:
        return self.__or__(other)

    @abstractmethod
    async def process(
        self,
        message: Message,
        connection: Connection,
        consumer: BaseConsumer,
    ) -> Message | None:
        """Implement middleware logic and return message or None."""
        raise NotImplementedError
