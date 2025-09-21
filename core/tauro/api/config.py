from __future__ import annotations
import os
from pathlib import Path
from typing import Optional, List
from pydantic import BaseSettings, Field, validator


class ApiSettings(BaseSettings):
    # App
    app_name: str = "Tauro Orchestrator API"
    api_prefix: str = "/api/v1"
    docs_url: Optional[str] = "/docs"
    openapi_url: Optional[str] = "/openapi.json"

    # Context discovery (reusa tu CLI infra)
    base_path: Optional[Path] = Field(default=None, env="TAURO_BASE_PATH")
    layer_name: Optional[str] = Field(default=None, env="TAURO_LAYER_NAME")
    use_case: Optional[str] = Field(default=None, env="TAURO_USE_CASE")
    config_type: Optional[str] = Field(default=None, env="TAURO_CONFIG_TYPE")
    default_env: str = Field(default="dev", env="TAURO_DEFAULT_ENV")

    # DB/Store
    orchestrator_db_path: Optional[Path] = Field(default=None, env="TAURO_ORCH_DB")

    # Security
    auth_enabled: bool = Field(default=False, env="TAURO_AUTH_ENABLED")
    auth_token: Optional[str] = Field(default=None, env="TAURO_AUTH_TOKEN")
    cors_allow_origins: List[str] = Field(
        default_factory=lambda: ["*"], env="TAURO_CORS_ALLOW_ORIGINS"
    )

    # Scheduler in-process (para MVP; en prod: proceso separado)
    scheduler_enabled: bool = Field(default=False, env="TAURO_SCHEDULER_ENABLED")
    scheduler_poll_seconds: float = Field(default=1.0, env="TAURO_SCHEDULER_POLL")

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"

    @validator("cors_allow_origins", pre=True)
    def _parse_cors(cls, v):
        # Permitir definir en env como JSON list, CSV o único valor
        if isinstance(v, str):
            v = v.strip()
            if not v:
                return []
            # intentar JSON-like list
            if v.startswith("[") and v.endswith("]"):
                try:
                    import json

                    parsed = json.loads(v)
                    if isinstance(parsed, list):
                        return parsed
                except Exception:
                    pass
            # usar coma como separador
            return [p.strip() for p in v.split(",") if p.strip()]
        return v


def resolve_db_path(settings: ApiSettings) -> Path:
    if settings.orchestrator_db_path:
        return settings.orchestrator_db_path
    home = Path(os.path.expanduser("~"))
    return home / ".tauro" / "orchestrator.db"
