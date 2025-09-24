from __future__ import annotations
from functools import lru_cache
from typing import Optional, Dict, Any
from datetime import datetime, timedelta, timezone
import threading

from fastapi import Depends, HTTPException, status, Request
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from fastapi.security.api_key import APIKeyHeader
import hmac
from cachetools import TTLCache
from jose import jwt, JWTError
from passlib.context import CryptContext

from tauro.api.config import ApiSettings
from .config import settings

# Contexto de cifrado para contraseñas
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

# Cache con TTL para tokens verificados
_token_cache = TTLCache(maxsize=1000, ttl=300)  # 5 minutos TTL
_cache_lock = threading.RLock()


class RateLimiter:
    """Implementación thread-safe de rate limiting con ventana deslizante"""

    def __init__(self, max_requests: int = 100, time_window: int = 3600):
        self.max_requests = max_requests
        self.time_window = time_window
        self.requests = {}
        self._lock = threading.RLock()

    def is_limited(self, identifier: str) -> bool:
        with self._lock:
            now = datetime.now().timestamp()

            # Limpiar requests antiguos
            if identifier in self.requests:
                self.requests[identifier] = [
                    ts
                    for ts in self.requests[identifier]
                    if now - ts < self.time_window
                ]

            # Inicializar si no existe
            if identifier not in self.requests:
                self.requests[identifier] = []

            # Verificar límite
            if len(self.requests[identifier]) >= self.max_requests:
                return True

            # Registrar nuevo request
            self.requests[identifier].append(now)
            return False


# Rate limiter global thread-safe
_rate_limiter = RateLimiter()


def get_password_hash(password: str) -> str:
    """Hashea la contraseña con bcrypt"""
    return pwd_context.hash(password)


def verify_password(plain_password: str, hashed_password: str) -> bool:
    """Verifica contraseña"""
    return pwd_context.verify(plain_password, hashed_password)


def create_access_token(
    subject: str,
    expires_delta: Optional[timedelta] = None,
    extra: Optional[Dict[str, Any]] = None,
) -> str:
    """Crea JWT firmado"""
    if expires_delta is None:
        expires_delta = timedelta(minutes=settings.ACCESS_TOKEN_EXPIRE_MINUTES)

    to_encode = {
        "sub": str(subject),
        "exp": datetime.now(timezone.utc) + expires_delta,
        "iat": datetime.now(timezone.utc),
        "jti": str(uuid.uuid4()),  # ID único para el token
    }

    if extra:
        to_encode.update(extra)

    return jwt.encode(
        to_encode, settings.JWT_SECRET_KEY, algorithm=settings.JWT_ALGORITHM
    )


def decode_access_token(token: str) -> Dict[str, Any]:
    """Decodifica y valida JWT"""
    try:
        payload = jwt.decode(
            token, settings.JWT_SECRET_KEY, algorithms=[settings.JWT_ALGORITHM]
        )
        return payload
    except JWTError as e:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=f"Invalid token: {str(e)}",
        )


def verify_token(token: str, settings: ApiSettings) -> bool:
    """Verificar token con cache thread-safe"""
    with _cache_lock:
        if token in _token_cache:
            return _token_cache[token]

    # Verificar token
    is_valid = bool(
        settings.auth_token and hmac.compare_digest(token, settings.auth_token)
    )

    with _cache_lock:
        _token_cache[token] = is_valid

    return is_valid


# Esquemas de autenticación
_security_bearer = HTTPBearer(auto_error=False)
_api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)


def get_current_user(
    request: Request,
    settings: ApiSettings = Depends(get_settings),
    bearer_creds: Optional[HTTPAuthorizationCredentials] = Depends(_security_bearer),
    api_key: Optional[str] = Depends(_api_key_header),
) -> Dict[str, Any]:
    # Verificar rate limiting
    client_ip = request.client.host if request.client else "unknown"
    if _rate_limiter.is_limited(client_ip):
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS, detail="Too many requests"
        )

    if not settings.auth_enabled:
        return {"id": "anonymous", "role": "guest", "ip": client_ip}

    # Verificar ambos esquemas de autenticación
    token = None
    if bearer_creds and bearer_creds.credentials:
        token = bearer_creds.credentials
    elif api_key:
        token = api_key

    if not token:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing authentication token",
        )

    if not verify_token(token, settings):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN, detail="Invalid token"
        )

    # Si es JWT, extraer información del payload
    if token.startswith("eyJ"):  # heuristic para detectar JWT
        try:
            payload = decode_access_token(token)
            return {
                "id": payload.get("sub", "unknown"),
                "role": payload.get("role", "user"),
                "ip": client_ip,
                "jti": payload.get("jti"),
            }
        except HTTPException:
            raise
        except Exception:
            # Fallback a autenticación básica si el JWT falla
            pass

    return {"id": "api-user", "role": "user", "ip": client_ip}


def get_admin_user(user: Dict[str, Any] = Depends(get_current_user)) -> Dict[str, Any]:
    if user.get("role") != "admin":
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN, detail="Admin privileges required"
        )
    return user
