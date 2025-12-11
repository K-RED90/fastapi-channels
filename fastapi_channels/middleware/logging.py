import logging

from fastapi_channels.middleware import Middleware


class LoggingMiddleware(Middleware):
    _logger = logging.getLogger("core.middleware.logging")

    def __init__(self, next_middleware: Middleware | None = None):
        super().__init__(next_middleware)
        if not self._logger.handlers:
            self._logger.setLevel(logging.INFO)

    async def process(self, message, connection, consumer):
        self._logger.info(
            "ws message type=%s channel=%s user=%s",
            message.type,
            connection.channel_name,
            connection.user_id,
        )
        return message
