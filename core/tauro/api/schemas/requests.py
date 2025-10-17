from pydantic import BaseModel, Field, validator
from typing import Optional, Dict, Any, List
from datetime import datetime
from enum import Enum

from tauro.api.schemas.validators import (
    validate_pipeline_id,
    validate_cron_expression,
    validate_json_params,
)


# =============================================================================
# Enums
# =============================================================================


class ScheduleKind(str, Enum):
    """Tipos de schedule"""

    INTERVAL = "INTERVAL"
    CRON = "CRON"


class RunState(str, Enum):
    """Estados de ejecución"""

    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


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
    params: Optional[Dict[str, Any]] = Field(
        default=None, description="Parámetros por defecto para las ejecuciones"
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

    _validate_pipeline_id = validator("pipeline_id", allow_reuse=True)(
        validate_pipeline_id
    )
    _validate_params = validator("params", allow_reuse=True)(validate_json_params)

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
                "params": {"mode": "incremental"},
                "enabled": True,
                "max_concurrency": 1,
                "timeout_seconds": 3600,
            }
        }


class ScheduleUpdateRequest(BaseModel):
    """Request para actualizar un schedule"""

    expression: Optional[str] = None
    params: Optional[Dict[str, Any]] = None
    enabled: Optional[bool] = None
    max_concurrency: Optional[int] = Field(default=None, gt=0, le=100)
    timeout_seconds: Optional[int] = Field(default=None, gt=0, le=86400)

    _validate_params = validator("params", allow_reuse=True)(validate_json_params)

    class Config:
        schema_extra = {"example": {"enabled": False, "max_concurrency": 2}}


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
