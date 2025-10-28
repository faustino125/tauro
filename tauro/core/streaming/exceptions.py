"""
Copyright (c) 2025 Faustino Lopez Ramos. 
For licensing information, see the LICENSE file in the project root
"""
import traceback
from typing import Any, Dict, Optional
from functools import wraps


class StreamingError(Exception):
    """Base exception for streaming operations with enhanced context."""

    def __init__(
        self,
        message: str,
        error_code: Optional[str] = None,
        context: Optional[Dict[str, Any]] = None,
        cause: Optional[Exception] = None,
    ):
        self.message = message
        self.error_code = error_code
        self.context = context or {}
        self.cause = cause

        # Build enhanced error message
        enhanced_message = self._build_enhanced_message()
        super().__init__(enhanced_message)

    def _build_enhanced_message(self) -> str:
        """Build enhanced error message with context."""
        parts = [self.message]

        if self.error_code:
            parts.insert(0, f"[{self.error_code}]")

        if self.context:
            context_str = ", ".join([f"{k}={v}" for k, v in self.context.items()])
            parts.append(f"Context: {context_str}")

        if self.cause:
            parts.append(f"Caused by: {str(self.cause)}")

        return " - ".join(parts)

    def get_context(self, key: str, default: Any = None) -> Any:
        """Get specific context value."""
        return self.context.get(key, default)

    def add_context(self, key: str, value: Any) -> "StreamingError":
        """Add context information."""
        self.context[key] = value
        return self

    def get_full_traceback(self) -> str:
        """Get full traceback including cause chain."""
        lines = [f"{type(self).__name__}: {self.message}"]

        if self.error_code:
            lines.insert(0, f"[{self.error_code}]")

        if self.context:
            lines.append(f"Context: {self.context}")

        # Append cause traceback if available
        if self.cause:
            lines.append("--- Cause traceback ---")
            if isinstance(self.cause, Exception):
                # Format the cause exception and its traceback if present
                lines.extend(
                    traceback.format_exception(
                        type(self.cause),
                        self.cause,
                        getattr(self.cause, "__traceback__", None),
                    )
                )
            else:
                lines.append(str(self.cause))

        return "\n".join(lines)


class StreamingValidationError(StreamingError):
    """Raised when streaming configuration validation fails."""

    def __init__(
        self,
        message: str,
        field: Optional[str] = None,
        expected: Optional[str] = None,
        actual: Optional[str] = None,
        **kwargs,
    ):
        # Pop context from kwargs to avoid passing it twice to super
        context = kwargs.pop("context", {})

        if field:
            context["field"] = field
        if expected:
            context["expected"] = expected
        if actual:
            context["actual"] = actual

        super().__init__(message, error_code="VALIDATION_ERROR", context=context, **kwargs)


class StreamingFormatNotSupportedError(StreamingError):
    """Raised when a streaming format is not supported."""

    def __init__(
        self,
        message: str,
        format_name: Optional[str] = None,
        supported_formats: Optional[list] = None,
        **kwargs,
    ):
        context = kwargs.pop("context", {})

        if format_name:
            context["format_name"] = format_name
        if supported_formats:
            context["supported_formats"] = supported_formats

        super().__init__(message, error_code="UNSUPPORTED_FORMAT", context=context, **kwargs)


class StreamingQueryError(StreamingError):
    """Raised when streaming query operations fail."""

    def __init__(
        self,
        message: str,
        query_id: Optional[str] = None,
        query_name: Optional[str] = None,
        query_status: Optional[str] = None,
        **kwargs,
    ):
        context = kwargs.pop("context", {})

        if query_id:
            context["query_id"] = query_id
        if query_name:
            context["query_name"] = query_name
        if query_status:
            context["query_status"] = query_status

        super().__init__(message, error_code="QUERY_ERROR", context=context, **kwargs)


class StreamingPipelineError(StreamingError):
    """Raised when streaming pipeline operations fail."""

    def __init__(
        self,
        message: str,
        pipeline_name: Optional[str] = None,
        execution_id: Optional[str] = None,
        pipeline_status: Optional[str] = None,
        failed_nodes: Optional[list] = None,
        **kwargs,
    ):
        context = kwargs.pop("context", {})

        if pipeline_name:
            context["pipeline_name"] = pipeline_name
        if execution_id:
            context["execution_id"] = execution_id
        if pipeline_status:
            context["pipeline_status"] = pipeline_status
        if failed_nodes:
            context["failed_nodes"] = failed_nodes

        super().__init__(message, error_code="PIPELINE_ERROR", context=context, **kwargs)


class StreamingConnectionError(StreamingError):
    """Raised when streaming source/sink connection fails."""

    def __init__(
        self,
        message: str,
        connection_type: Optional[str] = None,
        endpoint: Optional[str] = None,
        **kwargs,
    ):
        context = kwargs.pop("context", {})

        if connection_type:
            context["connection_type"] = connection_type
        if endpoint:
            context["endpoint"] = endpoint

        super().__init__(message, error_code="CONNECTION_ERROR", context=context, **kwargs)


class StreamingConfigurationError(StreamingError):
    """Raised when streaming configuration is invalid."""

    def __init__(
        self,
        message: str,
        config_section: Optional[str] = None,
        config_value: Optional[Any] = None,
        **kwargs,
    ):
        context = kwargs.pop("context", {})

        if config_section:
            context["config_section"] = config_section
        if config_value is not None:
            context["config_value"] = config_value

        super().__init__(message, error_code="CONFIG_ERROR", context=context, **kwargs)


class StreamingTimeoutError(StreamingError):
    """Raised when streaming operations timeout."""

    def __init__(
        self,
        message: str,
        timeout_seconds: Optional[float] = None,
        operation: Optional[str] = None,
        **kwargs,
    ):
        context = kwargs.pop("context", {})

        if timeout_seconds:
            context["timeout_seconds"] = timeout_seconds
        if operation:
            context["operation"] = operation

        super().__init__(message, error_code="TIMEOUT_ERROR", context=context, **kwargs)


class StreamingResourceError(StreamingError):
    """Raised when streaming resource operations fail."""

    def __init__(
        self,
        message: str,
        resource_type: Optional[str] = None,
        resource_path: Optional[str] = None,
        **kwargs,
    ):
        context = kwargs.pop("context", {})

        if resource_type:
            context["resource_type"] = resource_type
        if resource_path:
            context["resource_path"] = resource_path

        super().__init__(message, error_code="RESOURCE_ERROR", context=context, **kwargs)


# Error handling utilities
def handle_streaming_error(func):
    """Decorator to handle streaming errors with enhanced context."""

    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except StreamingError:
            # Re-raise streaming errors as-is
            raise
        except Exception as e:
            # Convert other exceptions to StreamingError
            context = {
                "function": getattr(func, "__name__", str(func)),
                "args_count": len(args),
                "kwargs_keys": list(kwargs.keys()),
            }
            raise StreamingError(
                f"Unexpected error in {getattr(func, '__name__', str(func))}: {str(e)}",
                error_code="UNEXPECTED_ERROR",
                context=context,
                cause=e,
            ) from e

    return wrapper


def create_error_context(
    operation: str, component: Optional[str] = None, **additional_context
) -> Dict[str, Any]:
    """Create standardized error context."""
    context = {
        "operation": operation,
        "timestamp": str(__import__("datetime").datetime.now()),
    }

    if component:
        context["component"] = component

    context.update(additional_context)
    return context
