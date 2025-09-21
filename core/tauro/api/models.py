from __future__ import annotations
from typing import Optional, List, Dict
from datetime import datetime
from pydantic import BaseModel, Field


class RunCreateRequest(BaseModel):
    pipeline_id: str
    params: Dict = Field(default_factory=dict)


class RunStartRequest(BaseModel):
    retries: int = 0
    retry_delay_sec: int = 0
    concurrency: Optional[int] = None
    env: Optional[str] = None  # permite cambiar de env por request


class TaskRunResponse(BaseModel):
    id: str
    task_id: str
    state: str
    try_number: int
    started_at: Optional[datetime]
    finished_at: Optional[datetime]
    error: Optional[str]


class RunResponse(BaseModel):
    id: str
    pipeline_id: str
    state: str
    created_at: datetime
    started_at: Optional[datetime]
    finished_at: Optional[datetime]
    params: Dict
    tasks: Optional[List[TaskRunResponse]] = None


class ScheduleCreateRequest(BaseModel):
    pipeline_id: str
    kind: str = "INTERVAL"  # INTERVAL o CRON
    expression: str = "60"  # segundos para INTERVAL
    max_concurrency: int = 1
    retries: int = 0
    retry_delay_sec: int = 0
    timeout_seconds: Optional[int] = None


class ScheduleResponse(BaseModel):
    id: str
    pipeline_id: str
    kind: str
    expression: str
    enabled: bool
    max_concurrency: int
    retry_policy: Dict
    timeout_seconds: Optional[int]
    next_run_at: Optional[datetime]
    created_at: datetime
    updated_at: datetime


class BackfillRequest(BaseModel):
    pipeline_id: str
    count: int = 1
