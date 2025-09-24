from __future__ import annotations
from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, Optional, Literal, Any, List
from datetime import datetime, timezone
from uuid import uuid4

RunStateLiteral = Literal[
    "PENDING", "QUEUED", "RUNNING", "SUCCESS", "FAILED", "SKIPPED", "CANCELLED"
]


class RunState(str, Enum):
    PENDING = "PENDING"
    QUEUED = "QUEUED"
    RUNNING = "RUNNING"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    SKIPPED = "SKIPPED"
    CANCELLED = "CANCELLED"

    def is_terminal(self) -> bool:
        return self in (
            RunState.SUCCESS,
            RunState.FAILED,
            RunState.SKIPPED,
            RunState.CANCELLED,
        )


@dataclass
class TaskRun:
    id: str = field(default_factory=lambda: str(uuid4()))
    pipeline_run_id: str = ""
    task_id: str = ""
    state: RunState = RunState.PENDING
    try_number: int = 0  # 1-based recommended when updating on completion
    started_at: Optional[datetime] = None
    finished_at: Optional[datetime] = None
    log_uri: Optional[str] = None
    error: Optional[str] = None


@dataclass
class PipelineRun:
    id: str = field(default_factory=lambda: str(uuid4()))
    pipeline_id: str = ""
    state: RunState = RunState.PENDING
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    started_at: Optional[datetime] = None
    finished_at: Optional[datetime] = None
    params: Dict[str, Any] = field(default_factory=dict)  # start_date, end_date, etc.
    error: Optional[str] = None  # Campo añadido para almacenar errores


class ScheduleKind(str, Enum):
    INTERVAL = "INTERVAL"  # cada N segundos
    CRON = "CRON"  # expresión cron


@dataclass
class Schedule:
    id: str = field(default_factory=lambda: str(uuid4()))
    pipeline_id: str = ""
    kind: ScheduleKind = ScheduleKind.INTERVAL
    expression: str = "60"  # "60" intervalos (segundos) o "*/5 * * * *" cron
    enabled: bool = True
    max_concurrency: int = 1
    retry_policy: Dict[str, Any] = field(
        default_factory=lambda: {"retries": 0, "delay": 0}
    )
    timeout_seconds: Optional[int] = None
    next_run_at: Optional[datetime] = None
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


@dataclass
class SchedulerMetrics:
    """Métricas del scheduler"""

    cycles: int = 0
    schedules_processed: int = 0
    runs_created: int = 0
    errors: int = 0
    last_cycle_time: float = 0
    avg_cycle_time: float = 0
    start_time: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    consecutive_failures: int = 0


@dataclass
class DatabaseStats:
    """Estadísticas de la base de datos"""

    pipeline_runs_by_state: Dict[str, int] = field(default_factory=dict)
    task_runs_by_state: Dict[str, int] = field(default_factory=dict)
    schedules_by_status: Dict[str, int] = field(default_factory=dict)
    database_size_bytes: int = 0
    record_count: Dict[str, int] = field(default_factory=dict)
