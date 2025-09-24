from __future__ import annotations
import time
from typing import Dict, Any, Optional
from fastapi import Request, HTTPException
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import Response

from .utils import RateLimitError, log
from .auth import _rate_limiter


class SecurityHeadersMiddleware(BaseHTTPMiddleware):
    """Middleware para añadir headers de seguridad"""

    async def dispatch(self, request: Request, call_next) -> Response:
        response = await call_next(request)

        # Headers de seguridad
        security_headers = {
            "X-Content-Type-Options": "nosniff",
            "X-Frame-Options": "DENY",
            "X-XSS-Protection": "1; mode=block",
            "Strict-Transport-Security": "max-age=31536000; includeSubDomains",
            "Content-Security-Policy": "default-src 'self'",
            "Referrer-Policy": "strict-origin-when-cross-origin",
            "Permissions-Policy": "geolocation=(), microphone=()",
        }

        for header, value in security_headers.items():
            response.headers[header] = value

        return response


class RateLimitMiddleware(BaseHTTPMiddleware):
    """Middleware para rate limiting global"""

    async def dispatch(self, request: Request, call_next) -> Response:
        client_ip = request.client.host if request.client else "unknown"

        if _rate_limiter.is_limited(client_ip):
            raise RateLimitError()

        response = await call_next(request)
        return response


class RequestLoggingMiddleware(BaseHTTPMiddleware):
    """Middleware para logging de requests"""

    async def dispatch(self, request: Request, call_next) -> Response:
        start_time = time.time()

        try:
            response = await call_next(request)
            duration = time.time() - start_time

            # Loggear información de la request
            log.info(
                "HTTP request",
                method=request.method,
                path=request.url.path,
                status_code=response.status_code,
                duration=duration,
                client_ip=request.client.host if request.client else "unknown",
                user_agent=request.headers.get("user-agent", ""),
            )

            return response
        except Exception as e:
            duration = time.time() - start_time
            log.error(
                "HTTP request failed",
                method=request.method,
                path=request.url.path,
                duration=duration,
                error=str(e),
                client_ip=request.client.host if request.client else "unknown",
            )
            raise
