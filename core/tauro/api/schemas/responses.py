from pydantic import BaseModel, Field
from typing import Optional, Dict, Any, List
from datetime import datetime

from tauro.api.schemas.requests import RunState, ScheduleKind


# =============================================================================
# Common Responses
# =============================================================================


class MessageResponse(BaseModel):
    """Response genérico con mensaje"""

    message: str
    detail: Optional[str] = None


class ErrorResponse(BaseModel):
    """Response de error"""

    error: str
    detail: Optional[str] = None
    path: Optional[str] = None


# =============================================================================
# Pipeline Responses
# =============================================================================


class PipelineInfo(BaseModel):
    """Información básica de un pipeline"""

    id: str
    name: Optional[str] = None
    description: Optional[str] = None
    type: Optional[str] = None
    nodes: List[str] = Field(default_factory=list)


class PipelineListResponse(BaseModel):
    """Lista de pipelines"""

    pipelines: List[PipelineInfo]
    total: int


class PipelineRunResponse(BaseModel):
    """Respuesta de ejecución de pipeline"""

    run_id: str
    pipeline_id: str
    state: RunState
    created_at: datetime
    started_at: Optional[datetime] = None
    finished_at: Optional[datetime] = None
    params: Optional[Dict[str, Any]] = None
    error: Optional[str] = None
    tags: Optional[Dict[str, str]] = None

    class Config:
        schema_extra = {
            "example": {
                "run_id": "run_20241013_123456_abc123",
                "pipeline_id": "etl_daily",
                "state": "RUNNING",
                "created_at": "2024-10-13T12:34:56Z",
                "started_at": "2024-10-13T12:34:57Z",
                "params": {"start_date": "2024-01-01"},
            }
        }


class RunListResponse(BaseModel):
    """Lista de ejecuciones"""

    runs: List[PipelineRunResponse]
    total: int


# =============================================================================
# Schedule Responses
# =============================================================================


class ScheduleResponse(BaseModel):
    """Respuesta de schedule"""

    id: str
    pipeline_id: str
    kind: ScheduleKind
    expression: str
    params: Optional[Dict[str, Any]] = None
    enabled: bool
    max_concurrency: int
    timeout_seconds: Optional[int] = None
    created_at: datetime
    updated_at: Optional[datetime] = None
    last_run_at: Optional[datetime] = None
    next_run_at: Optional[datetime] = None

    class Config:
        schema_extra = {
            "example": {
                "id": "schedule_abc123",
                "pipeline_id": "etl_daily",
                "kind": "CRON",
                "expression": "0 2 * * *",
                "enabled": True,
                "max_concurrency": 1,
                "timeout_seconds": 3600,
                "created_at": "2024-10-13T12:00:00Z",
                "next_run_at": "2024-10-14T02:00:00Z",
            }
        }


class ScheduleListResponse(BaseModel):
    """Lista de schedules"""

    schedules: List[ScheduleResponse]
    total: int


# =============================================================================
# Monitoring Responses
# =============================================================================


class HealthCheck(BaseModel):
    """Health check response"""

    status: str
    version: str
    timestamp: datetime
    components: Dict[str, str] = Field(default_factory=dict)

    class Config:
        schema_extra = {
            "example": {
                "status": "healthy",
                "version": "2.0.0",
                "timestamp": "2024-10-13T12:34:56Z",
                "components": {
                    "api": "healthy",
                    "database": "healthy",
                    "scheduler": "healthy",
                },
            }
        }


class APIInfo(BaseModel):
    """Información de la API"""

    name: str
    version: str
    description: str
    environment: str
    tauro_config: Dict[str, Any]


class StatsResponse(BaseModel):
    """Estadísticas generales"""

    total_pipelines: int
    total_runs: int
    total_schedules: int
    active_runs: int
    failed_runs: int

    class Config:
        schema_extra = {
            "example": {
                "total_pipelines": 15,
                "total_runs": 1234,
                "total_schedules": 8,
                "active_runs": 2,
                "failed_runs": 5,
            }
        }


# =============================================================================
# Exports
# =============================================================================

__all__ = [
    # Common
    "MessageResponse",
    "ErrorResponse",
    # Pipelines
    "PipelineInfo",
    "PipelineListResponse",
    "PipelineRunResponse",
    "RunListResponse",
    # Schedules
    "ScheduleResponse",
    "ScheduleListResponse",
    # Monitoring
    "HealthCheck",
    "APIInfo",
    "StatsResponse",
]
