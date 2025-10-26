from pydantic import BaseModel, Field, validator
from typing import Optional, Dict, Any, List
from datetime import datetime

from tauro.api.schemas.validators import (
    validate_pipeline_id,
    validate_cron_expression,
    validate_json_params,
)
from tauro.orchest.models import (
    RunState as CoreRunState,
    ScheduleKind as CoreScheduleKind,
)


# Re-export core enums so API schemas stay aligned with orchestrator states
RunState = CoreRunState
ScheduleKind = CoreScheduleKind


# =============================================================================
# Pipeline Requests
# =============================================================================


class PipelineRunRequest(BaseModel):
    """Request para ejecutar un pipeline"""

    params: Optional[Dict[str, Any]] = Field(
        default=None, description="Parámetros opcionales para la ejecución"
    )
    timeout: Optional[int] = Field(
        default=None, gt=0, description="Timeout en segundos (opcional)"
    )
    tags: Optional[Dict[str, str]] = Field(
        default=None, description="Tags opcionales para la ejecución"
    )

    _validate_params = validator("params", allow_reuse=True)(validate_json_params)

    class Config:
        schema_extra = {
            "example": {
                "params": {"start_date": "2024-01-01", "end_date": "2024-01-31"},
                "timeout": 3600,
                "tags": {"env": "production", "team": "data-eng"},
            }
        }


# =============================================================================
# Schedule Requests
# =============================================================================


class ScheduleCreateRequest(BaseModel):
    """Request para crear un schedule"""

    pipeline_id: str = Field(..., description="ID del pipeline a programar")
    kind: ScheduleKind = Field(..., description="Tipo de schedule (INTERVAL o CRON)")
    expression: str = Field(
        ...,
        description="Expresión del schedule (intervalo en segundos o expresión cron)",
    )
    enabled: bool = Field(default=True, description="Si el schedule está habilitado")
    max_concurrency: Optional[int] = Field(
        default=1, gt=0, le=100, description="Máximo número de ejecuciones concurrentes"
    )
    timeout_seconds: Optional[int] = Field(
        default=None,
        gt=0,
        le=86400,
        description="Timeout en segundos para cada ejecución",
    )
    retry_policy: Optional[Dict[str, Any]] = Field(
        default=None,
        description="Política de reintentos (por ejemplo {'retries': 1, 'delay': 60})",
    )
    next_run_at: Optional[datetime] = Field(
        default=None, description="Fecha/hora del próximo run (opcional)"
    )

    _validate_pipeline_id = validator("pipeline_id", allow_reuse=True)(
        validate_pipeline_id
    )
    _validate_retry_policy = validator("retry_policy", allow_reuse=True)(
        validate_json_params
    )

    @validator("expression")
    def validate_expression(cls, v, values):
        """Validar expresión según el tipo"""
        kind = values.get("kind")

        if kind == ScheduleKind.INTERVAL:
            try:
                interval = int(v)
                if interval <= 0:
                    raise ValueError("Interval must be positive")
                return v
            except ValueError:
                raise ValueError(
                    "INTERVAL expression must be a positive integer (seconds)"
                )

        elif kind == ScheduleKind.CRON:
            # validate_cron_expression expects only the expression string
            return validate_cron_expression(v)

        return v

    class Config:
        schema_extra = {
            "example": {
                "pipeline_id": "etl_daily",
                "kind": "CRON",
                "expression": "0 2 * * *",
                "enabled": True,
                "max_concurrency": 1,
                "timeout_seconds": 3600,
                "retry_policy": {"retries": 1, "delay": 300},
            }
        }


class ScheduleUpdateRequest(BaseModel):
    """Request para actualizar un schedule"""

    expression: Optional[str] = None
    enabled: Optional[bool] = None
    max_concurrency: Optional[int] = Field(default=None, gt=0, le=100)
    timeout_seconds: Optional[int] = Field(default=None, gt=0, le=86400)
    retry_policy: Optional[Dict[str, Any]] = Field(default=None)
    next_run_at: Optional[datetime] = None

    _validate_retry_policy = validator("retry_policy", allow_reuse=True)(
        validate_json_params
    )

    class Config:
        schema_extra = {
            "example": {
                "enabled": False,
                "max_concurrency": 2,
                "retry_policy": {"retries": 0, "delay": 0},
            }
        }


# =============================================================================
# Control Requests
# =============================================================================


class RunCancelRequest(BaseModel):
    """Request para cancelar una ejecución"""

    reason: Optional[str] = Field(
        default=None, max_length=500, description="Razón de la cancelación"
    )

    class Config:
        schema_extra = {"example": {"reason": "Cancelado por el usuario"}}


# =============================================================================
# Exports
# =============================================================================

__all__ = [
    # Enums
    "ScheduleKind",
    "RunState",
    # Requests
    "PipelineRunRequest",
    "ScheduleCreateRequest",
    "ScheduleUpdateRequest",
    "RunCancelRequest",
]
