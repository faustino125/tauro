from __future__ import annotations
import os
import secrets
from pathlib import Path
from typing import Optional, List, Dict, Any
from pydantic import BaseSettings, AnyHttpUrl, Field, RedisDsn, validator, HttpUrl
from pydantic.errors import PydanticValueError
import json
from enum import Enum


class EnvironmentEnum(str, Enum):
    DEVELOPMENT = "dev"
    PRODUCTION = "prod"
    SANDBOX = "sandbox"
    TESTING = "test"

    @classmethod
    def _missing_(cls, value):
        """Handle sandbox_* environments."""
        if isinstance(value, str) and value.startswith("sandbox_"):
            return cls.SANDBOX
        return None


class LogLevelEnum(str, Enum):
    DEBUG = "debug"
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


class CacheBackendEnum(str, Enum):
    MEMORY = "memory"
    REDIS = "redis"


class InvalidCORSOriginsError(PydanticValueError):
    code = "invalid_cors_origins"
    msg_template = "Invalid CORS origins format: {reason}"


class ApiSettings(BaseSettings):
    # App
    app_name: str = Field("Tauro Orchestrator API", env="TAURO_APP_NAME")
    # Preserve the raw environment string (e.g. 'sandbox_juan') while keeping
    # the typed enum for common cases. Some parts of the codebase (CLI,
    # AppConfigManager) expect the full sandbox_<dev> name, so we keep it.
    raw_environment: Optional[str] = Field(default=None, env="TAURO_ENVIRONMENT")
    environment: EnvironmentEnum = Field(
        EnvironmentEnum.DEVELOPMENT, env="TAURO_ENVIRONMENT"
    )
    api_prefix: str = Field("/api/v1", env="TAURO_API_PREFIX")
    docs_url: Optional[str] = Field("/docs", env="TAURO_DOCS_URL")
    openapi_url: Optional[str] = Field("/openapi.json", env="TAURO_OPENAPI_URL")
    log_level: LogLevelEnum = Field(LogLevelEnum.INFO, env="TAURO_LOG_LEVEL")

    # Server
    host: str = Field("0.0.0.0", env="TAURO_HOST")
    port: int = Field(8000, env="TAURO_PORT")
    workers: int = Field(4, env="TAURO_WORKERS")

    # Context discovery
    base_path: Optional[Path] = Field(default=None, env="TAURO_BASE_PATH")
    layer_name: Optional[str] = Field(default=None, env="TAURO_LAYER_NAME")
    use_case: Optional[str] = Field(default=None, env="TAURO_USE_CASE")
    config_type: Optional[str] = Field(default=None, env="TAURO_CONFIG_TYPE")
    default_env: str = Field(default="dev", env="TAURO_DEFAULT_ENV")

    # DB/Store
    orchestrator_db_path: Optional[Path] = Field(default=None, env="TAURO_ORCH_DB")
    database_pool_size: int = Field(10, ge=1, le=100, env="TAURO_DB_POOL_SIZE")
    database_timeout: int = Field(30, ge=1, le=300, env="TAURO_DB_TIMEOUT")
    database_retry_attempts: int = Field(3, ge=1, le=10, env="TAURO_DB_RETRY_ATTEMPTS")

    # Security
    auth_enabled: bool = Field(False, env="TAURO_AUTH_ENABLED")
    auth_token: Optional[str] = Field(default=None, env="TAURO_AUTH_TOKEN")
    jwt_secret_key: str = Field(env="TAURO_JWT_SECRET_KEY")
    jwt_algorithm: str = Field("HS256", env="TAURO_JWT_ALGORITHM")
    jwt_expiration_minutes: int = Field(60, ge=1, env="TAURO_JWT_EXPIRATION")
    jwt_refresh_expiration_days: int = Field(
        7, ge=1, env="TAURO_JWT_REFRESH_EXPIRATION"
    )

    cors_allow_origins: List[str] = Field(
        default_factory=lambda: ["*"], env="TAURO_CORS_ALLOW_ORIGINS"
    )
    cors_allow_credentials: bool = Field(True, env="TAURO_CORS_ALLOW_CREDENTIALS")
    cors_allow_methods: List[str] = Field(
        default_factory=lambda: ["GET", "POST", "PUT", "DELETE", "OPTIONS", "PATCH"],
        env="TAURO_CORS_ALLOW_METHODS",
    )
    cors_allow_headers: List[str] = Field(
        default_factory=lambda: ["Authorization", "Content-Type", "X-API-Key"],
        env="TAURO_CORS_ALLOW_HEADERS",
    )

    # Rate limiting
    rate_limit_requests: int = Field(100, ge=1, env="TAURO_RATE_LIMIT_REQUESTS")
    rate_limit_time_window: int = Field(3600, ge=1, env="TAURO_RATE_LIMIT_WINDOW")

    # Scheduler
    scheduler_enabled: bool = Field(False, env="TAURO_SCHEDULER_ENABLED")
    scheduler_poll_seconds: float = Field(
        1.0, ge=0.1, le=60, env="TAURO_SCHEDULER_POLL"
    )
    scheduler_max_concurrent_runs: int = Field(
        10, ge=1, env="TAURO_SCHEDULER_MAX_CONCURRENT"
    )
    scheduler_stuck_timeout_minutes: int = Field(
        120, ge=1, env="TAURO_SCHEDULER_STUCK_TIMEOUT"
    )

    # Services
    pipeline_service_max_workers: int = Field(
        10, ge=1, le=100, env="TAURO_PIPELINE_SERVICE_WORKERS"
    )
    pipeline_timeout_seconds: int = Field(3600, ge=1, env="TAURO_PIPELINE_TIMEOUT")

    # Cache
    cache_backend: CacheBackendEnum = Field(
        CacheBackendEnum.MEMORY, env="TAURO_CACHE_BACKEND"
    )
    redis_url: Optional[RedisDsn] = Field(None, env="TAURO_REDIS_URL")
    cache_ttl: int = Field(300, ge=1, env="TAURO_CACHE_TTL")
    cache_max_size: int = Field(10000, ge=1, env="TAURO_CACHE_MAX_SIZE")

    # Metrics and monitoring
    metrics_enabled: bool = Field(False, env="TAURO_METRICS_ENABLED")
    metrics_port: int = Field(9090, env="TAURO_METRICS_PORT")
    health_check_timeout: int = Field(5, env="TAURO_HEALTH_CHECK_TIMEOUT")
    health_check_interval: int = Field(30, env="TAURO_HEALTH_CHECK_INTERVAL")

    # External services
    sentry_dsn: Optional[HttpUrl] = Field(None, env="TAURO_SENTRY_DSN")
    prometheus_multiproc_dir: Optional[Path] = Field(
        None, env="PROMETHEUS_MULTIPROC_DIR"
    )

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        case_sensitive = False
        validate_assignment = True
        json_encoders = {
            Path: lambda v: str(v),
        }

    @validator("cors_allow_origins", pre=True)
    def _parse_cors(cls, v):
        if isinstance(v, str):
            v = v.strip()
            if not v:
                return []
            if v.startswith("[") and v.endswith("]"):
                try:
                    parsed = json.loads(v)
                    if isinstance(parsed, list):
                        return parsed
                    else:
                        raise InvalidCORSOriginsError(reason="Not a valid list")
                except json.JSONDecodeError:
                    raise InvalidCORSOriginsError(reason="Invalid JSON format")
            return [p.strip() for p in v.split(",") if p.strip()]
        return v

    @validator("orchestrator_db_path", pre=True, always=True)
    def _set_default_db_path(cls, v, values):
        if v is not None:
            return v

        home = Path(os.path.expanduser("~"))

        env = values.get("raw_environment") or values.get("environment") or "dev"
        return home / ".tauro" / f"orchestrator_{env}.db"

    @validator("jwt_secret_key", pre=True, always=True)
    def _generate_jwt_secret(cls, v, values):
        if not v:
            env_enum = values.get("environment")
            if env_enum == EnvironmentEnum.PRODUCTION:
                raise ValueError("JWT secret key is required in production")
            return secrets.token_urlsafe(64)
        return v

    @validator("auth_token", always=True)
    def _validate_auth_token(cls, v, values):
        if values.get("auth_enabled", False) and not v:
            raise ValueError("Auth token is required when authentication is enabled")
        return v

    @validator("redis_url", always=True)
    def _validate_redis_url(cls, v, values):
        if values.get("cache_backend") == CacheBackendEnum.REDIS and not v:
            raise ValueError("Redis URL is required when using Redis cache backend")
        return v


def resolve_db_path(settings: ApiSettings) -> Path:
    return settings.orchestrator_db_path


# Instancia global de settings
settings = ApiSettings()
