from fastapi import Request, status
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from starlette.middleware.base import BaseHTTPMiddleware
from loguru import logger
import time
import uuid
import asyncio
from typing import Callable
from collections import deque, OrderedDict
from datetime import datetime, timedelta
import contextvars

# Context variables for request tracking
request_id_context: contextvars.ContextVar[str] = contextvars.ContextVar(
    "request_id", default=""
)
request_context: contextvars.ContextVar[dict] = contextvars.ContextVar(
    "request_context", default={}
)


# =============================================================================
# Context Manager for Request Tracking
# =============================================================================


class RequestContextManager:
    """Context manager to ensure proper cleanup of request context variables"""

    def __init__(self, req_id: str, ctx: dict):
        self.req_id = req_id
        self.ctx = ctx
        self.tokens = []

    async def __aenter__(self):
        # Set context variables and store tokens for cleanup
        self.tokens.append(request_id_context.set(self.req_id))
        self.tokens.append(request_context.set(self.ctx))
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        # Always clean up context variables, even if an exception occurred
        for token in reversed(self.tokens):
            try:
                if hasattr(token, "var"):
                    token.var.reset(token)
                else:
                    # Fallback for different token types
                    request_id_context.reset(token)
                    request_context.reset(token)
            except (RuntimeError, ValueError) as e:
                # Context might already be cleaned up or invalid
                logger.debug(f"Context cleanup warning: {e}")
        self.tokens.clear()


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
    """Log all requests with timing and context tracking"""

    async def dispatch(self, request: Request, call_next: Callable):
        # Generate unique request ID
        req_id = str(uuid.uuid4())[:8]

        # Capture request context
        client_ip = request.client.host if request.client else "unknown"
        start_time = time.time()

        # Try to extract user info from headers (if available)
        user_agent = request.headers.get("user-agent", "unknown")
        authorization = request.headers.get("authorization", "")
        user = "anonymous"
        if authorization.startswith("Bearer "):
            # Extract user info (would need proper JWT parsing in production)
            user = "bearer_token"

        # Context for this request
        ctx = {
            "request_id": req_id,
            "method": request.method,
            "path": request.url.path,
            "client_ip": client_ip,
            "user_agent": user_agent[:50],  # Truncate for log size
            "user": user,
            "query_params": dict(request.query_params),
        }

        # Use context manager to ensure proper cleanup
        async with RequestContextManager(req_id, ctx):
            # Log incoming request
            logger.info(
                f"[{req_id}] {request.method} {request.url.path}",
                extra=ctx,
            )

            # Process request
            try:
                response = await call_next(request)
            except Exception as e:
                duration = time.time() - start_time
                logger.error(
                    f"[{req_id}] Request failed: {type(e).__name__}",
                    extra={
                        **ctx,
                        "duration_ms": duration * 1000,
                        "error": str(e),
                    },
                )
                raise

            # Calculate duration
            duration = time.time() - start_time

            # Log response with more context
            logger.info(
                f"[{req_id}] {request.method} {request.url.path} - {response.status_code} ({duration:.3f}s)",
                extra={
                    **ctx,
                    "status_code": response.status_code,
                    "duration_ms": duration * 1000,
                    "response_size": response.headers.get("content-length", "0"),
                },
            )

            # Add timing and request ID headers
            response.headers["X-Process-Time"] = str(duration)
            response.headers["X-Request-ID"] = req_id

            return response


# =============================================================================
# Security Headers Middleware
# =============================================================================


class SecurityHeadersMiddleware(BaseHTTPMiddleware):
    """Add comprehensive security headers to all responses"""

    def __init__(self, app, csp_enabled: bool = True, hsts_enabled: bool = True):
        super().__init__(app)
        self.csp_enabled = csp_enabled
        self.hsts_enabled = hsts_enabled

    async def dispatch(self, request: Request, call_next: Callable):
        response = await call_next(request)

        # Content Security Policy (CSP)
        # Restricts sources of content to prevent XSS and injection attacks
        # Uses configuration from settings if available
        if self.csp_enabled:
            # Use settings CSP policy if available, otherwise default
            csp_policy = getattr(
                request.app.state.settings,
                "csp_policy",
                (
                    "default-src 'self'; "
                    "script-src 'self'; "
                    "style-src 'self' 'unsafe-inline'; "
                    "img-src 'self' data: https:; "
                    "font-src 'self'; "
                    "connect-src 'self'; "
                    "frame-ancestors 'none'; "
                    "base-uri 'self'; "
                    "form-action 'self'"
                ),
            )
            response.headers["Content-Security-Policy"] = csp_policy

        # Prevent browsers from MIME-sniffing
        response.headers["X-Content-Type-Options"] = "nosniff"

        # Prevent clickjacking
        response.headers["X-Frame-Options"] = "DENY"

        # Enable XSS protection (legacy, but still useful for older browsers)
        response.headers["X-XSS-Protection"] = "1; mode=block"

        # Enforce HTTPS (HSTS)
        if self.hsts_enabled:
            response.headers[
                "Strict-Transport-Security"
            ] = "max-age=31536000; includeSubDomains; preload"

        # Don't send referrer information to other sites
        response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"

        # Don't allow accessing geolocation, microphone, camera
        response.headers["Permissions-Policy"] = (
            "geolocation=(), "
            "microphone=(), "
            "camera=(), "
            "payment=(), "
            "usb=(), "
            "magnetometer=(), "
            "gyroscope=(), "
            "accelerometer=()"
        )

        # Prevent information leakage via headers
        if "Server" in response.headers:
            del response.headers["Server"]

        return response


# =============================================================================
# Request Limits Middleware
# =============================================================================


class RequestLimitsMiddleware(BaseHTTPMiddleware):
    """Middleware to enforce request size and timeout limits"""

    def __init__(
        self,
        app,
        max_body_size: int = 10 * 1024 * 1024,  # 10MB default
        request_timeout: float = 30.0,  # 30 seconds default
    ):
        super().__init__(app)
        self.max_body_size = max_body_size
        self.request_timeout = request_timeout

    async def dispatch(self, request: Request, call_next: Callable):
        # Check content-length header
        content_length = request.headers.get("content-length")
        if content_length:
            try:
                size = int(content_length)
                if size > self.max_body_size:
                    logger.warning(
                        f"Request body too large: {size} > {self.max_body_size}"
                    )
                    return JSONResponse(
                        status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
                        content={
                            "error": "Request body too large",
                            "detail": f"Maximum payload size: {self.max_body_size} bytes",
                        },
                    )
            except ValueError:
                pass  # Invalid content-length, continue

        # Apply request timeout
        try:
            response = await asyncio.wait_for(
                call_next(request),
                timeout=self.request_timeout,
            )
            return response
        except asyncio.TimeoutError:
            logger.error(f"Request timeout after {self.request_timeout}s")
            return JSONResponse(
                status_code=status.HTTP_408_REQUEST_TIMEOUT,
                content={
                    "error": "Request timeout",
                    "detail": f"Request exceeded {self.request_timeout}s timeout",
                },
            )


# =============================================================================
# Rate Limiting Middleware (Simple Implementation)
# =============================================================================


class RateLimitMiddleware(BaseHTTPMiddleware):
    """
    Rate limiting middleware with memory-efficient implementation.

    Uses OrderedDict to maintain order and efficient cleanup of old entries.
    Per-IP request tracking with automatic pruning of stale clients.
    """

    def __init__(self, app, requests_per_window: int = 100, window_seconds: int = 60):
        super().__init__(app)
        self.requests_per_window = requests_per_window
        self.window_seconds = window_seconds
        # Use OrderedDict to track clients by insertion order (useful for cleanup)
        self.clients: OrderedDict[str, deque] = OrderedDict()
        # track last seen time per client for stale removal
        self._last_seen: dict[str, float] = {}
        # global cleanup control to avoid expensive sweeps on every request
        self._last_global_cleanup = time.time()
        # maximum number of distinct clients before triggering aggressive cleanup
        self._max_clients = 10000
        # Time cutoff for considering a client stale (seconds)
        self._stale_timeout = 3600  # 1 hour
        # Cleanup interval for window-based cleanup (seconds)
        self._window_cleanup_interval = 300  # 5 minutes

    def _clean_old_requests(self, client_ip: str):
        """Remove requests older than the window from a client's deque"""
        now = datetime.now()
        cutoff = now - timedelta(seconds=self.window_seconds)
        dq = self.clients.get(client_ip)

        if not dq:
            return

        # Pop left while older than cutoff (efficient since deque.popleft is O(1))
        while dq and dq[0] <= cutoff:
            dq.popleft()

        # If deque empty, remove client entry completely to avoid memory growth
        if not dq:
            self.clients.pop(client_ip, None)
            self._last_seen.pop(client_ip, None)

    def _global_cleanup(self):
        """Aggressive cleanup: remove stale clients that haven't made requests recently"""
        current_time = time.time()

        # Find stale clients
        stale_ips = [
            ip
            for ip, last_time in self._last_seen.items()
            if current_time - last_time > self._stale_timeout
        ]

        # Remove them
        for ip in stale_ips:
            self.clients.pop(ip, None)
            self._last_seen.pop(ip, None)

        if stale_ips:
            logger.debug(f"Cleaned up {len(stale_ips)} stale clients from rate limiter")

    def _periodic_window_cleanup(self):
        """
        Periodic cleanup of old requests from active clients.
        Prevents memory growth even for active clients making many requests.
        """
        current_time = time.time()

        # Only run cleanup every _window_cleanup_interval seconds
        if current_time - self._last_global_cleanup < self._window_cleanup_interval:
            return

        cutoff = datetime.now() - timedelta(seconds=self.window_seconds)
        cleaned_count = 0

        # Create a snapshot of client IPs to avoid mutation during iteration
        client_ips = list(self.clients.keys())

        # Clean old requests from all active clients
        for client_ip in client_ips:
            dq = self.clients.get(client_ip)
            if not dq:
                continue

            # Remove requests older than the window
            while dq and dq[0] <= cutoff:
                dq.popleft()
                cleaned_count += 1

            # Remove client entry if deque is now empty
            if not dq:
                self.clients.pop(client_ip, None)

        self._last_global_cleanup = current_time

        if cleaned_count > 0:
            logger.debug(
                f"Periodic cleanup: removed {cleaned_count} old request records"
            )

    async def dispatch(self, request: Request, call_next: Callable):
        # Skip rate limiting for exempt paths (configurable)
        # This allows bypassing rate limits for health checks, docs, etc.
        if request.url.path in getattr(
            request.app.state.settings,
            "rate_limit_exempt_paths",
            ["/health", "/metrics"],
        ):
            return await call_next(request)

        client_ip = request.client.host if request.client else "unknown"

        # Clean old requests for this specific client
        self._clean_old_requests(client_ip)

        # Check rate limit
        dq = self.clients.get(client_ip)
        if dq and len(dq) >= self.requests_per_window:
            logger.warning(f"Rate limit exceeded for {client_ip}: {len(dq)} requests")
            return JSONResponse(
                status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                content={
                    "error": "Rate limit exceeded",
                    "detail": f"Maximum {self.requests_per_window} requests per {self.window_seconds} seconds",
                },
            )

        # Add current request timestamp
        now = datetime.now()
        if client_ip not in self.clients:
            self.clients[client_ip] = deque()

        self.clients[client_ip].append(now)
        self._last_seen[client_ip] = time.time()

        # If we have many distinct clients, run opportunistic cleanup
        total_clients = len(self.clients)
        current_time = time.time()

        if (
            total_clients > self._max_clients
            and current_time - self._last_global_cleanup > self.window_seconds
        ):
            logger.info(f"Rate limiter has {total_clients} clients, running cleanup")
            self._global_cleanup()
            self._last_global_cleanup = current_time

        # Periodic cleanup even for active clients
        self._periodic_window_cleanup()

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
        app.add_middleware(
            SecurityHeadersMiddleware,
            csp_enabled=settings.csp_enabled,
            hsts_enabled=settings.hsts_enabled,
        )

    # Rate limiting
    if settings.enable_rate_limit:
        app.add_middleware(
            RateLimitMiddleware,
            requests_per_window=settings.rate_limit_requests,
            window_seconds=settings.rate_limit_window,
        )
        logger.info(
            f"Rate limiting enabled: {settings.rate_limit_requests} requests per {settings.rate_limit_window}s. "
            f"Exempt paths: {settings.rate_limit_exempt_paths}"
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
    "RequestContextManager",
    "setup_middleware",
    "setup_cors",
]
