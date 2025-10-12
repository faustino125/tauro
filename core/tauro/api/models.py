from __future__ import annotations
from typing import Optional, List, Dict, Any
from datetime import datetime
from pydantic import BaseModel, Field, validator, HttpUrl
from enum import Enum
from uuid import UUID

RETRIES_DESCRIPTION = "Number of retries for failed tasks"
RETRY_DELAY_DESCRIPTION = "Delay between retries in seconds"


class RunState(str, Enum):
    PENDING = "PENDING"
    QUEUED = "QUEUED"
    RUNNING = "RUNNING"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    SKIPPED = "SKIPPED"
    CANCELLED = "CANCELLED"


class ScheduleKind(str, Enum):
    INTERVAL = "INTERVAL"
    CRON = "CRON"


class HealthStatus(str, Enum):
    OK = "ok"
    DEGRADED = "degraded"
    ERROR = "error"


class RunCreateRequest(BaseModel):
    pipeline_id: str = Field(..., description="ID of the pipeline to run")
    params: Dict[str, Any] = Field(
        default_factory=dict, description="Parameters for the pipeline run"
    )

    @validator("pipeline_id")
    def validate_pipeline_id(cls, v):
        if not v or not v.strip():
            raise ValueError("Pipeline ID cannot be empty")
        return v.strip()

    retries: int = Field(0, ge=0, le=10, description=RETRIES_DESCRIPTION)
    retry_delay_sec: int = Field(0, ge=0, le=3600, description=RETRY_DELAY_DESCRIPTION)
    concurrency: Optional[int] = Field(
        None, ge=1, le=50, description="Maximum concurrent tasks"
    )
    timeout_seconds: Optional[int] = Field(
        None, ge=1, le=86400, description="Timeout for the entire run in seconds"
    )
    env: Optional[str] = Field(None, description="Environment to use for this run")


class RunOptions(BaseModel):
    retries: int = Field(0, ge=0, le=10, description=RETRIES_DESCRIPTION)


class RunOptions(BaseModel):
    retries: int = Field(0, ge=0, le=10, description=RETRIES_DESCRIPTION)
    retry_delay_sec: int = Field(0, ge=0, le=3600, description=RETRY_DELAY_DESCRIPTION)
    concurrency: Optional[int] = Field(
        None, ge=1, le=100, description="Maximum concurrent tasks"
    )
    timeout_seconds: Optional[int] = Field(
        None, ge=1, le=86400, description="Timeout for the entire run in seconds"
    )
    env: Optional[str] = Field(None, description="Environment to use for this run")
    execution_mode: Optional[str] = Field(
        None, description="Execution mode for streaming (sync|async)"
    )
    model_version: Optional[str] = Field(
        None, description="Model version for ML pipelines"
    )
    hyperparams: Optional[Dict[str, Any]] = Field(
        None, description="Hyperparameters for ML pipelines"
    )
    hyperparams: Optional[Dict[str, Any]] = Field(
        None, description="Hyperparameters for ML pipelines"
    )


class TaskRunResponse(BaseModel):
    id: UUID
    task_id: str
    state: RunState
    try_number: int
    started_at: Optional[datetime]
    finished_at: Optional[datetime]
    error: Optional[str]
    duration_seconds: Optional[float] = Field(
        None, description="Task duration in seconds"
    )

    @validator("duration_seconds", always=True)
    def calculate_duration(cls, v, values):
        started_at = values.get("started_at")
        finished_at = values.get("finished_at")
        if started_at and finished_at:
            return (finished_at - started_at).total_seconds()
        return None


class RunResponse(BaseModel):
    id: UUID
    pipeline_id: str
    state: RunState
    created_at: datetime
    started_at: Optional[datetime]
    finished_at: Optional[datetime]
    params: Dict[str, Any]
    tasks: Optional[List[TaskRunResponse]] = None
    duration_seconds: Optional[float] = Field(
        None, description="Run duration in seconds"
    )

    @validator("duration_seconds", always=True)
    def calculate_duration(cls, v, values):
        started_at = values.get("started_at")
        finished_at = values.get("finished_at")
        if started_at and finished_at:
            return (finished_at - started_at).total_seconds()
        return None


class ScheduleCreateRequest(BaseModel):
    pipeline_id: str = Field(..., description="ID of the pipeline to schedule")
    kind: ScheduleKind = Field(ScheduleKind.INTERVAL, description="Type of schedule")
    expression: str = Field("60", description="Interval seconds or cron expression")
    retries: int = Field(
        0, ge=0, le=10, description="Number of retries for failed tasks"
    )
    retry_delay_sec: int = Field(0, ge=0, le=3600, description=RETRY_DELAY_DESCRIPTION)
    timeout_seconds: Optional[int] = Field(
        None, ge=1, le=86400, description="Timeout for runs in seconds"
    )

    enabled: bool = Field(True, description="Whether the schedule is enabled")

    @validator("expression")
    def validate_expression(cls, v, values):
        kind = values.get("kind")
        if kind == ScheduleKind.INTERVAL:
            try:
                interval = int(v)
                if interval < 1:
                    raise ValueError("Interval must be at least 1 second")
            except ValueError:
                raise ValueError("Interval must be a positive integer")
        elif kind == ScheduleKind.CRON:
            # Validación básica de expresión cron
            parts = v.split()
            if len(parts) != 5:
                raise ValueError("Cron expression must have 5 parts")
        return v


class ScheduleResponse(BaseModel):
    id: UUID
    pipeline_id: str
    kind: ScheduleKind
    expression: str
    enabled: bool
    max_concurrency: int
    retry_policy: Dict[str, Any]
    timeout_seconds: Optional[int]
    next_run_at: Optional[datetime]
    created_at: datetime
    updated_at: datetime


class BackfillRequest(BaseModel):
    pipeline_id: str = Field(..., description="ID of the pipeline to backfill")
    count: int = Field(1, ge=1, le=1000, description="Number of runs to create")


class HealthResponse(BaseModel):
    status: HealthStatus
    timestamp: datetime
    service: str
    version: str
    database: Optional[Dict[str, Any]] = None
    details: Optional[Dict[str, Any]] = None


class MetricsResponse(BaseModel):
    requests_total: int
    requests_duration_seconds: Dict[str, float]
    db_queries_total: int
    db_query_duration_seconds: Dict[str, float]


class ErrorResponse(BaseModel):
    detail: str
    code: Optional[str] = None
    context: Optional[Dict[str, Any]] = None


class PaginatedResponse(BaseModel):
    items: List[Any]
    total: int
    page: int
    size: int
    pages: int


# Modelos para documentación de API
class OpenAPITags:
    PIPELINES = "Pipelines"
    RUNS = "Runs"
    SCHEDULES = "Schedules"
    CONTROL = "Control"
    METRICS = "Metrics"
    HEALTH = "Health"
