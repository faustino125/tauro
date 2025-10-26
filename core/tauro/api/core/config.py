from pydantic import BaseSettings, Field, validator
from typing import Optional, List
from pathlib import Path


class Settings(BaseSettings):
    """
    Configuración global de la aplicación.

    Las variables de entorno tienen precedencia sobre los valores por defecto.
    Prefijo: TAURO_ (ejemplo: TAURO_API_HOST)
    """

    # =========================================================================
    # API Configuration
    # =========================================================================
    api_title: str = "Tauro Pipeline API"
    api_version: str = "2.0.0"
    api_description: str = "API REST para gestión de pipelines de datos"

    api_host: str = Field(default="0.0.0.0", env="API_HOST")
    api_port: int = Field(default=8000, env="API_PORT")
    api_reload: bool = Field(default=False, env="API_RELOAD")
    api_workers: int = Field(default=1, env="API_WORKERS")

    # API Prefix
    api_prefix: str = "/api/v1"

    # =========================================================================
    # CORS Configuration
    # =========================================================================
    cors_enabled: bool = Field(default=True, env="CORS_ENABLED")
    cors_origins: List[str] = Field(
        default=["http://localhost:3000", "http://localhost:8000"], env="CORS_ORIGINS"
    )
    cors_allow_credentials: bool = True
    cors_allow_methods: List[str] = ["*"]
    cors_allow_headers: List[str] = ["*"]

    # =========================================================================
    # Tauro Core Configuration
    # =========================================================================
    base_path: Path = Field(default=Path("./examples"), env="TAURO_BASE_PATH")
    layer_name: str = Field(default="silver_layer", env="TAURO_LAYER")
    use_case: Optional[str] = Field(default=None, env="TAURO_USE_CASE")
    config_type: str = Field(default="catalog", env="TAURO_CONFIG_TYPE")
    environment: str = Field(default="dev", env="TAURO_ENV")

    # Config source selection (file vs mongo)
    config_source: str = Field(default="file", env="CONFIG_SOURCE")
    mongo_url: Optional[str] = Field(default=None, env="MONGO_URL")
    mongo_db: Optional[str] = Field(default=None, env="MONGO_DB")
    mongo_ca_file: Optional[str] = Field(default=None, env="MONGO_CA_FILE")

    @validator("base_path", pre=True)
    def parse_base_path(cls, v):
        """Convert string to Path"""
        if isinstance(v, str):
            return Path(v)
        return v

    # =========================================================================
    # Database Configuration (SQLite por defecto)
    # =========================================================================
    database_url: str = Field(default="sqlite:///./tauro.db", env="DATABASE_URL")
    database_echo: bool = Field(default=False, env="DATABASE_ECHO")

    # =========================================================================
    # MongoDB Configuration
    # =========================================================================
    mongodb_uri: str = Field(
        default="mongodb://localhost:27017",
        env="MONGODB_URI",
        description="MongoDB connection string",
    )
    mongodb_db_name: str = Field(
        default="tauro",
        env="MONGODB_DB_NAME",
        description="MongoDB database name",
    )
    mongodb_timeout_seconds: int = Field(
        default=30,
        env="MONGODB_TIMEOUT_SECONDS",
        description="MongoDB connection timeout in seconds",
    )

    # =========================================================================
    # Logging Configuration
    # =========================================================================
    log_level: str = Field(default="INFO", env="LOG_LEVEL")
    log_format: str = Field(
        default="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan> - <level>{message}</level>",
        env="LOG_FORMAT",
    )
    log_file: Optional[str] = Field(default=None, env="LOG_FILE")

    @validator("log_level")
    def validate_log_level(cls, v):
        """Validate log level"""
        valid_levels = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
        if v.upper() not in valid_levels:
            raise ValueError(f"Log level must be one of: {valid_levels}")
        return v.upper()

    @validator("config_source")
    def validate_config_source(cls, v):
        """Ensure config source is supported."""
        if not isinstance(v, str):
            raise ValueError("config_source must be a string")
        value = v.strip().lower()
        if value not in {"file", "mongo"}:
            raise ValueError("config_source must be either 'file' or 'mongo'")
        return value

    # =========================================================================
    # Monitoring & Metrics
    # =========================================================================
    enable_metrics: bool = Field(default=True, env="ENABLE_METRICS")
    metrics_port: int = Field(default=9090, env="METRICS_PORT")

    # =========================================================================
    # Security
    # =========================================================================
    enable_rate_limit: bool = Field(default=True, env="ENABLE_RATE_LIMIT")
    rate_limit_requests: int = Field(default=100, env="RATE_LIMIT_REQUESTS")
    rate_limit_window: int = Field(default=60, env="RATE_LIMIT_WINDOW")  # seconds
    rate_limit_exempt_paths: List[str] = Field(
        default=["/health", "/metrics", "/docs", "/redoc", "/openapi.json"],
        env="RATE_LIMIT_EXEMPT_PATHS",
    )

    max_request_size: int = Field(
        default=10 * 1024 * 1024, env="MAX_REQUEST_SIZE"
    )  # 10MB

    # Security headers
    enable_security_headers: bool = Field(default=True, env="ENABLE_SECURITY_HEADERS")
    csp_enabled: bool = Field(default=True, env="CSP_ENABLED")
    hsts_enabled: bool = Field(default=True, env="HSTS_ENABLED")

    # =========================================================================
    # Scheduler Configuration
    # =========================================================================
    scheduler_enabled: bool = Field(default=True, env="SCHEDULER_ENABLED")
    scheduler_poll_interval: float = Field(default=1.0, env="SCHEDULER_POLL_INTERVAL")
    scheduler_max_workers: int = Field(default=10, env="SCHEDULER_MAX_WORKERS")

    # =========================================================================
    # Execution Configuration
    # =========================================================================
    default_timeout: int = Field(default=3600, env="DEFAULT_TIMEOUT")  # 1 hour
    max_concurrent_runs: int = Field(default=5, env="MAX_CONCURRENT_RUNS")

    # =========================================================================
    # Development Settings
    # =========================================================================
    debug: bool = Field(default=False, env="DEBUG")
    testing: bool = Field(default=False, env="TESTING")

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        case_sensitive = False

    def is_development(self) -> bool:
        """Check if running in development mode"""
        return self.environment in ("dev", "development")

    def is_production(self) -> bool:
        """Check if running in production mode"""
        return self.environment in ("prod", "production")

    def get_tauro_config(self) -> dict:
        """Get Tauro-specific configuration as dict"""
        return {
            "base_path": str(self.base_path),
            "layer_name": self.layer_name,
            "use_case": self.use_case,
            "config_type": self.config_type,
            "environment": self.environment,
            "config_source": self.config_source,
            "mongo_db": self.mongo_db,
        }


# Singleton instance
settings = Settings()


def get_settings() -> Settings:
    """Get settings instance (for dependency injection)"""
    return settings


# Export commonly used settings
__all__ = [
    "Settings",
    "settings",
    "get_settings",
]
