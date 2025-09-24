from __future__ import annotations
import logging
import time
from contextlib import contextmanager
from datetime import datetime
from typing import Any, Dict, Optional, Generator, Callable
from functools import wraps

from fastapi import HTTPException, status
from loguru import logger
import prometheus_client as prom

# Métricas Prometheus
REQUEST_DURATION = prom.Histogram(
    "http_request_duration_seconds",
    "HTTP request duration in seconds",
    ["method", "endpoint", "status"],
)
REQUEST_COUNT = prom.Counter(
    "http_requests_total", "Total HTTP requests", ["method", "endpoint", "status"]
)
DB_QUERY_DURATION = prom.Histogram(
    "db_query_duration_seconds",
    "Database query duration in seconds",
    ["operation", "table"],
)


class APIError(Exception):
    """Excepción base para errores de API"""

    def __init__(
        self,
        message: str,
        code: str = "internal_error",
        status_code: int = status.HTTP_500_INTERNAL_SERVER_ERROR,
        details: Optional[Dict[str, Any]] = None,
    ):
        self.message = message
        self.code = code
        self.status_code = status_code
        self.details = details or {}
        super().__init__(self.message)


class ValidationError(APIError):
    """Error de validación"""

    def __init__(self, message: str, details: Optional[Dict[str, Any]] = None):
        super().__init__(
            message=message,
            code="validation_error",
            status_code=status.HTTP_400_BAD_REQUEST,
            details=details,
        )


class NotFoundError(APIError):
    """Recurso no encontrado"""

    def __init__(self, resource: str, resource_id: str):
        super().__init__(
            message=f"{resource} '{resource_id}' not found",
            code="not_found",
            status_code=status.HTTP_404_NOT_FOUND,
            details={"resource": resource, "id": resource_id},
        )


class RateLimitError(APIError):
    """Límite de tasa excedido"""

    def __init__(self, retry_after: int = 60):
        super().__init__(
            message="Too many requests",
            code="rate_limit_exceeded",
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            details={"retry_after": retry_after},
        )


@contextmanager
def track_db_operation(operation: str, table: str) -> Generator[None, None, None]:
    """Context manager para tracking de operaciones de base de datos"""
    start_time = time.time()
    try:
        yield
    finally:
        duration = time.time() - start_time
        DB_QUERY_DURATION.labels(operation=operation, table=table).observe(duration)


def handle_exceptions(func: Callable) -> Callable:
    """Decorator para manejo consistente de excepciones"""

    @wraps(func)
    async def wrapper(*args, **kwargs):
        try:
            return await func(*args, **kwargs)
        except APIError as e:
            # Errores conocidos de la API
            raise HTTPException(
                status_code=e.status_code,
                detail={"message": e.message, "code": e.code, "details": e.details},
            )
        except Exception as e:
            # Loggear error inesperado
            logger.exception(f"Unhandled exception in {func.__name__}: {str(e)}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail={"message": "Internal server error", "code": "internal_error"},
            )

    return wrapper


def log_execution_time(func: Callable) -> Callable:
    """Decorator para loggear tiempo de ejecución"""

    @wraps(func)
    async def wrapper(*args, **kwargs):
        start_time = time.time()
        try:
            result = await func(*args, **kwargs)
            return result
        finally:
            duration = time.time() - start_time
            logger.debug(f"{func.__name__} executed in {duration:.3f}s")

    return wrapper


class StructuredLogger:
    """Logger estructurado para mejor trazabilidad"""

    def __init__(self, name: str = "tauro"):
        self.logger = logger.bind(module=name)

    def info(self, message: str, **context):
        self.logger.info(message, **context)

    def error(self, message: str, **context):
        self.logger.error(message, **context)

    def warning(self, message: str, **context):
        self.logger.warning(message, **context)

    def debug(self, message: str, **context):
        self.logger.debug(message, **context)

    def exception(self, message: str, **context):
        self.logger.exception(message, **context)


# Logger global
log = StructuredLogger()
