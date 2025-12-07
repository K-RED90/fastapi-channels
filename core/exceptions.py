import logging
import time
import uuid
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class ErrorCategory(Enum):
    AUTHENTICATION = "authentication"
    AUTHORIZATION = "authorization"
    VALIDATION = "validation"
    RATE_LIMIT = "rate_limit"
    CONNECTION = "connection"
    BACKEND = "backend"
    MESSAGE = "message"
    SYSTEM = "system"
    UNKNOWN = "unknown"


class ErrorSeverity(Enum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


@dataclass
class ErrorContext:
    error_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    timestamp: float = field(default_factory=time.time)
    user_id: str | None = None
    connection_id: str | None = None
    message_type: str | None = None
    component: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "error_id": self.error_id,
            "timestamp": self.timestamp,
            "user_id": self.user_id,
            "connection_id": self.connection_id,
            "message_type": self.message_type,
            "component": self.component,
            "metadata": self.metadata,
        }


@dataclass
class ErrorResponse:
    error_code: str
    message: str
    category: ErrorCategory
    severity: ErrorSeverity
    context: ErrorContext
    details: dict[str, Any] | None = None
    retryable: bool = False

    def to_dict(self) -> dict[str, Any]:
        return {
            "type": "error",
            "error_code": self.error_code,
            "message": self.message,
            "category": self.category.value,
            "severity": self.severity.value,
            "error_id": self.context.error_id,
            "timestamp": self.context.timestamp,
            "retryable": self.retryable,
            "details": self.details or {},
        }


class BaseError(Exception, ABC):
    _logger = logging.getLogger("core.exceptions")

    def __init__(
        self,
        message: str,
        error_code: str,
        category: ErrorCategory,
        severity: ErrorSeverity,
        context: ErrorContext | None = None,
        details: dict[str, Any] | None = None,
        retryable: bool = False,
        cause: Exception | None = None,
    ):
        super().__init__(message)
        self.message = message
        self.error_code = error_code
        self.category = category
        self.severity = severity
        self.context = context or ErrorContext()
        self.details = details or {}
        self.retryable = retryable
        self.cause = cause

        if severity in (ErrorSeverity.CRITICAL, ErrorSeverity.HIGH):
            self._log_error()

    def _log_error(self) -> None:
        log_data = {
            "error_code": self.error_code,
            "category": self.category.value,
            "severity": self.severity.value,
            "error_message": self.message,
            "context": self.context.to_dict(),
            "details": self.details,
            "retryable": self.retryable,
        }

        if self.cause:
            log_data["cause"] = str(self.cause)

        if self.severity == ErrorSeverity.CRITICAL:
            self._logger.critical("Critical error occurred", extra=log_data)
        else:
            self._logger.error("High severity error occurred", extra=log_data)

    def to_response(self) -> ErrorResponse:
        return ErrorResponse(
            error_code=self.error_code,
            message=self.message,
            category=self.category,
            severity=self.severity,
            context=self.context,
            details=self.details,
            retryable=self.retryable,
        )

    @abstractmethod
    def should_disconnect(self) -> bool:
        pass


class WebSocketError(BaseError):
    def __init__(self, message: str, **kwargs):
        super().__init__(
            message=message,
            error_code="WEBSOCKET_ERROR",
            category=ErrorCategory.SYSTEM,
            severity=ErrorSeverity.MEDIUM,
            **kwargs,
        )

    def should_disconnect(self) -> bool:
        return True


class ConnectionError(BaseError):
    def __init__(
        self,
        message: str,
        error_code: str = "CONNECTION_ERROR",
        severity: ErrorSeverity = ErrorSeverity.HIGH,
        **kwargs,
    ):
        super().__init__(
            message=message,
            error_code=error_code,
            category=ErrorCategory.CONNECTION,
            severity=severity,
            **kwargs,
        )

    def should_disconnect(self) -> bool:
        return True


class MessageError(BaseError):
    def __init__(
        self,
        message: str,
        error_code: str = "MESSAGE_ERROR",
        severity: ErrorSeverity = ErrorSeverity.MEDIUM,
        **kwargs,
    ):
        super().__init__(
            message=message,
            error_code=error_code,
            category=ErrorCategory.MESSAGE,
            severity=severity,
            **kwargs,
        )

    def should_disconnect(self) -> bool:
        return False


class BackendError(BaseError):
    def __init__(
        self,
        message: str,
        error_code: str = "BACKEND_ERROR",
        severity: ErrorSeverity = ErrorSeverity.HIGH,
        **kwargs,
    ):
        super().__init__(
            message=message,
            error_code=error_code,
            category=ErrorCategory.BACKEND,
            severity=severity,
            **kwargs,
        )

    def should_disconnect(self) -> bool:
        return False


class AuthenticationError(BaseError):
    def __init__(
        self,
        message: str = "Authentication required",
        error_code: str = "AUTHENTICATION_FAILED",
        **kwargs,
    ):
        super().__init__(
            message=message,
            error_code=error_code,
            category=ErrorCategory.AUTHENTICATION,
            severity=ErrorSeverity.MEDIUM,
            **kwargs,
        )

    def should_disconnect(self) -> bool:
        return True


class ValidationError(BaseError):
    def __init__(
        self,
        message: str,
        error_code: str = "VALIDATION_FAILED",
        severity: ErrorSeverity = ErrorSeverity.LOW,
        **kwargs,
    ):
        super().__init__(
            message=message,
            error_code=error_code,
            category=ErrorCategory.VALIDATION,
            severity=severity,
            **kwargs,
        )

    def should_disconnect(self) -> bool:
        return False


class RateLimitError(BaseError):
    def __init__(
        self,
        message: str = "Rate limit exceeded",
        error_code: str = "RATE_LIMIT_EXCEEDED",
        **kwargs,
    ):
        super().__init__(
            message=message,
            error_code=error_code,
            category=ErrorCategory.RATE_LIMIT,
            severity=ErrorSeverity.LOW,
            retryable=True,
            **kwargs,
        )

    def should_disconnect(self) -> bool:
        return False


class AuthorizationError(BaseError):
    """Authorization/permission errors."""

    def __init__(
        self,
        message: str = "Insufficient permissions",
        error_code: str = "AUTHORIZATION_FAILED",
        **kwargs,
    ):
        super().__init__(
            message=message,
            error_code=error_code,
            category=ErrorCategory.AUTHORIZATION,
            severity=ErrorSeverity.MEDIUM,
            **kwargs,
        )

    def should_disconnect(self) -> bool:
        return False


class SystemError(BaseError):
    """System-level errors (database, network, etc.)."""

    def __init__(
        self,
        message: str,
        error_code: str = "SYSTEM_ERROR",
        severity: ErrorSeverity = ErrorSeverity.HIGH,
        **kwargs,
    ):
        super().__init__(
            message=message,
            error_code=error_code,
            category=ErrorCategory.SYSTEM,
            severity=severity,
            **kwargs,
        )

    def should_disconnect(self) -> bool:
        return self.severity == ErrorSeverity.CRITICAL


class TimeoutError(BaseError):
    """Timeout-related errors."""

    def __init__(
        self,
        message: str = "Operation timed out",
        error_code: str = "TIMEOUT_ERROR",
        **kwargs,
    ):
        super().__init__(
            message=message,
            error_code=error_code,
            category=ErrorCategory.SYSTEM,
            severity=ErrorSeverity.MEDIUM,
            retryable=True,
            **kwargs,
        )

    def should_disconnect(self) -> bool:
        return False


def create_error_context(
    user_id: str | None = None,
    connection_id: str | None = None,
    message_type: str | None = None,
    component: str | None = None,
    **metadata,
) -> ErrorContext:
    return ErrorContext(
        user_id=user_id,
        connection_id=connection_id,
        message_type=message_type,
        component=component,
        metadata=metadata,
    )
