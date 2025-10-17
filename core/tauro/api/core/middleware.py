from fastapi import Request, status
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from starlette.middleware.base import BaseHTTPMiddleware
from loguru import logger
import time
from typing import Callable
from collections import defaultdict, deque
from datetime import datetime, timedelta


# =============================================================================
# Error Handling Middleware
# =============================================================================


class ErrorHandlingMiddleware(BaseHTTPMiddleware):
    """Global error handler"""

    async def dispatch(self, request: Request, call_next: Callable):
        try:
            response = await call_next(request)
            return response
        except Exception as e:
            logger.error(f"Unhandled error: {e}", exc_info=True)

            return JSONResponse(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                content={
                    "error": "Internal server error",
                    "detail": str(e)
                    if request.app.state.settings.debug
                    else "An error occurred",
                    "path": request.url.path,
                },
            )


# =============================================================================
# Request Logging Middleware
# =============================================================================


class RequestLoggingMiddleware(BaseHTTPMiddleware):
    """Log all requests with timing"""

    async def dispatch(self, request: Request, call_next: Callable):
        start_time = time.time()

        # Log request
        logger.info(
            f"{request.method} {request.url.path}",
            extra={
                "method": request.method,
                "path": request.url.path,
                "client": request.client.host if request.client else "unknown",
            },
        )

        # Process request
        response = await call_next(request)

        # Calculate duration
        duration = time.time() - start_time

        # Log response
        logger.info(
            f"{request.method} {request.url.path} - {response.status_code} ({duration:.3f}s)",
            extra={
                "method": request.method,
                "path": request.url.path,
                "status_code": response.status_code,
                "duration": duration,
            },
        )

        # Add timing header
        response.headers["X-Process-Time"] = str(duration)

        return response


# =============================================================================
# Security Headers Middleware
# =============================================================================


class SecurityHeadersMiddleware(BaseHTTPMiddleware):
    """Add security headers to all responses"""

    async def dispatch(self, request: Request, call_next: Callable):
        response = await call_next(request)

        # Security headers
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["X-Frame-Options"] = "DENY"
        response.headers["X-XSS-Protection"] = "1; mode=block"
        response.headers[
            "Strict-Transport-Security"
        ] = "max-age=31536000; includeSubDomains"

        return response


# =============================================================================
# Rate Limiting Middleware (Simple Implementation)
# =============================================================================


class RateLimitMiddleware(BaseHTTPMiddleware):
    """Simple rate limiting based on client IP"""

    def __init__(self, app, requests_per_window: int = 100, window_seconds: int = 60):
        super().__init__(app)
        self.requests_per_window = requests_per_window
        self.window_seconds = window_seconds
        # clients: ip -> deque[timestamp]
        self.clients: dict[str, deque] = {}
        # track last seen time per client to allow pruning
        self._last_seen: dict[str, float] = {}
        # global cleanup control to avoid expensive sweeps on every request
        self._last_global_cleanup = time.time()
        # maximum number of distinct clients before triggering aggressive cleanup
        self._max_clients = 10000

    def _clean_old_requests(self, client_ip: str):
        """Remove requests older than the window"""
        now = datetime.now()
        cutoff = now - timedelta(seconds=self.window_seconds)
        dq = self.clients.get(client_ip)
        if not dq:
            return

        # Pop left while older than cutoff
        while dq and dq[0] <= cutoff:
            dq.popleft()

        # If deque empty, remove client entry to avoid growth
        if not dq:
            self.clients.pop(client_ip, None)
            self._last_seen.pop(client_ip, None)

    async def dispatch(self, request: Request, call_next: Callable):
        # Skip rate limiting for health checks
        if request.url.path in ["/health", "/metrics"]:
            return await call_next(request)

        client_ip = request.client.host if request.client else "unknown"

        # Clean old requests
        self._clean_old_requests(client_ip)

        # Check rate limit
        dq = self.clients.get(client_ip)
        if dq and len(dq) >= self.requests_per_window:
            logger.warning(f"Rate limit exceeded for {client_ip}")
            return JSONResponse(
                status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                content={
                    "error": "Rate limit exceeded",
                    "detail": f"Maximum {self.requests_per_window} requests per {self.window_seconds} seconds",
                },
            )

        # Add current request
        now = datetime.now()
        if client_ip not in self.clients:
            # store timestamps as deque for efficient pops
            self.clients[client_ip] = deque()
        self.clients[client_ip].append(now)
        self._last_seen[client_ip] = time.time()

        # If we have many distinct clients, run an opportunistic global cleanup
        total_clients = len(self.clients)
        if (
            total_clients > self._max_clients
            and time.time() - self._last_global_cleanup > self.window_seconds
        ):
            # remove clients that haven't been seen for twice the window
            stale_cutoff = time.time() - (self.window_seconds * 2)
            stale = [ip for ip, ts in self._last_seen.items() if ts < stale_cutoff]
            for ip in stale:
                self.clients.pop(ip, None)
                self._last_seen.pop(ip, None)
            self._last_global_cleanup = time.time()

        return await call_next(request)


# =============================================================================
# Helper Functions
# =============================================================================


def setup_cors(app, settings):
    """Setup CORS middleware"""
    if settings.cors_enabled:
        app.add_middleware(
            CORSMiddleware,
            allow_origins=settings.cors_origins,
            allow_credentials=settings.cors_allow_credentials,
            allow_methods=settings.cors_allow_methods,
            allow_headers=settings.cors_allow_headers,
        )
        logger.info(f"CORS enabled for origins: {settings.cors_origins}")


def setup_middleware(app, settings):
    """Setup all middleware"""

    # Error handling (first, catches all errors)
    app.add_middleware(ErrorHandlingMiddleware)

    # Request logging
    app.add_middleware(RequestLoggingMiddleware)

    # Security headers
    if settings.enable_security_headers:
        app.add_middleware(SecurityHeadersMiddleware)

    # Rate limiting
    if settings.enable_rate_limit:
        app.add_middleware(
            RateLimitMiddleware,
            requests_per_window=settings.rate_limit_requests,
            window_seconds=settings.rate_limit_window,
        )
        logger.info(
            f"Rate limiting enabled: {settings.rate_limit_requests} requests per {settings.rate_limit_window}s"
        )

    # CORS (last, should be outermost)
    setup_cors(app, settings)


# =============================================================================
# Exports
# =============================================================================

__all__ = [
    "ErrorHandlingMiddleware",
    "RequestLoggingMiddleware",
    "SecurityHeadersMiddleware",
    "RateLimitMiddleware",
    "setup_middleware",
    "setup_cors",
]
