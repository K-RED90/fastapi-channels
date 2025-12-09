import json

from core.exceptions import ValidationError, create_error_context
from core.middleware.base import Middleware
from core.typed import Message


class ValidationMiddleware(Middleware):
    def __init__(self, max_message_size: int, next_middleware: Middleware | None = None):
        super().__init__(next_middleware)
        self.max_message_size = max_message_size

    async def process(self, message: Message, connection, consumer) -> Message | None:
        try:
            size = len(json.dumps(message.to_dict()))
            if size > self.max_message_size:
                context = create_error_context(
                    user_id=connection.user_id,
                    connection_id=connection.channel_name,
                    message_type=message.type,
                    component="validation_middleware",
                    message_size=size,
                    max_size=self.max_message_size,
                )
                raise ValidationError(
                    f"Message too large: {size} bytes (max: {self.max_message_size})",
                    context=context,
                )
        except ValidationError:
            raise
        except Exception as exc:
            context = create_error_context(
                user_id=connection.user_id,
                connection_id=connection.channel_name,
                message_type=message.type,
                component="validation_middleware",
                original_error=str(exc),
            )
            raise ValidationError(
                f"Validation failed: {exc!s}",
                context=context,
            )

        if message.is_expired():
            return None

        return message
