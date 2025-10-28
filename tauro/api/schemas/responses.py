"""
Copyright (c) 2025 Faustino Lopez Ramos.
For licensing information, see the LICENSE file in the project root
"""
from pydantic import BaseModel, Field, validator
from typing import Optional, Dict, Any, List
from datetime import datetime

from tauro.api.schemas.requests import RunState, ScheduleKind


# =============================================================================
# Common Responses
# =============================================================================


class MessageResponse(BaseModel):
    """Generic response with message"""

    message: str
    detail: Optional[str] = None


class ErrorResponse(BaseModel):
    """Error response"""

    error: str
    detail: Optional[str] = None
    path: Optional[str] = None


# =============================================================================
# Pipeline Responses
# =============================================================================


class PipelineInfo(BaseModel):
    """Basic pipeline information"""

    id: str
    name: Optional[str] = None
    description: Optional[str] = None
    type: Optional[str] = None
    nodes: List[str] = Field(default_factory=list)


class PipelineListResponse(BaseModel):
    """List of pipelines"""

    pipelines: List[PipelineInfo]
    total: int


class PipelineRunResponse(BaseModel):
    """Pipeline execution response"""

    run_id: str
    pipeline_id: str
    state: RunState
    created_at: datetime
    started_at: Optional[datetime] = None
    finished_at: Optional[datetime] = None
    params: Optional[Dict[str, Any]] = None
    error: Optional[str] = None
    tags: Optional[Dict[str, str]] = None

    @validator("started_at", pre=True, always=False)
    def validate_started_at(cls, v, values):
        """Validate that started_at >= created_at"""
        if v is not None and "created_at" in values and v < values["created_at"]:
            raise ValueError("started_at cannot be before created_at")
        return v

    @validator("finished_at", pre=True, always=False)
    def validate_finished_at(cls, v, values):
        """Validate that finished_at >= started_at and >= created_at"""
        if v is not None:
            if "created_at" in values and v < values["created_at"]:
                raise ValueError("finished_at cannot be before created_at")
            if "started_at" in values and values["started_at"] is not None:
                if v < values["started_at"]:
                    raise ValueError("finished_at cannot be before started_at")
        return v

    class Config:
        schema_extra = {
            "example": {
                "run_id": "run_20241013_123456_abc123",
                "pipeline_id": "etl_daily",
                "state": "SUCCESS",
                "created_at": "2024-10-13T12:34:56Z",
                "started_at": "2024-10-13T12:34:57Z",
                "params": {"start_date": "2024-01-01"},
            }
        }


class RunListResponse(BaseModel):
    """List of executions"""

    runs: List[PipelineRunResponse]
    total: int


# =============================================================================
# Configuration Responses
# =============================================================================


class ConfigContextResponse(BaseModel):
    """Representation of the active configuration context."""

    project_id: str
    environment: str
    version: str
    global_settings: Dict[str, Any]
    pipelines_config: Dict[str, Any]
    nodes_config: Dict[str, Any]
    input_config: Dict[str, Any]
    output_config: Dict[str, Any]


class ConfigVersionMetadataResponse(BaseModel):
    """Metadata of the active version."""

    project_id: str
    environment: str
    version: str
    metadata: Dict[str, Any] = Field(default_factory=dict)
    release: Dict[str, Any] = Field(default_factory=dict)


# =============================================================================
# Schedule Responses
# =============================================================================


class ScheduleResponse(BaseModel):
    """Schedule response"""

    id: str
    pipeline_id: str
    kind: ScheduleKind
    expression: str
    enabled: bool
    max_concurrency: int
    retry_policy: Dict[str, Any] = Field(default_factory=dict)
    timeout_seconds: Optional[int] = None
    created_at: datetime
    updated_at: Optional[datetime] = None
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
                "retry_policy": {"retries": 1, "delay": 300},
                "timeout_seconds": 3600,
                "created_at": "2024-10-13T12:00:00Z",
                "next_run_at": "2024-10-14T02:00:00Z",
            }
        }


class ScheduleListResponse(BaseModel):
    """List of schedules"""

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
    """API information"""

    name: str
    version: str
    description: str
    environment: str
    tauro_config: Dict[str, Any]


class StatsResponse(BaseModel):
    """General statistics"""

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
    "ConfigContextResponse",
    "ConfigVersionMetadataResponse",
    # Schedules
    "ScheduleResponse",
    "ScheduleListResponse",
    # Monitoring
    "HealthCheck",
    "APIInfo",
    "StatsResponse",
]
