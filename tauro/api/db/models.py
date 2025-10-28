"""
Copyright (c) 2025 Faustino Lopez Ramos.
For licensing information, see the LICENSE file in the project root
"""
from typing import TypedDict, Optional, List, Dict, Any
from datetime import datetime


class ProjectDocument(TypedDict, total=False):
    """MongoDB Project Document"""

    _id: str  # MongoDB ObjectId as string
    id: str  # UUID
    name: str
    description: Optional[str]
    global_settings: Dict[str, Any]
    pipelines: List[Dict[str, Any]]
    created_at: datetime
    updated_at: datetime
    created_by: str
    tags: Dict[str, str]
    status: str  # "active", "archived", "draft"


class PipelineRunDocument(TypedDict, total=False):
    """MongoDB Pipeline Run Document"""

    _id: str
    id: str  # UUID
    project_id: str
    pipeline_id: str
    state: str  # "PENDING", "RUNNING", "SUCCESS", "FAILED", "CANCELLED"
    params: Dict[str, Any]
    priority: str  # "low", "normal", "high"
    created_at: datetime
    started_at: Optional[datetime]
    ended_at: Optional[datetime]
    created_by: str
    tags: Dict[str, str]
    progress: Dict[str, Any]  # total_tasks, completed_tasks, failed_tasks
    error: Optional[str]
    retry_count: int


class TaskRunDocument(TypedDict, total=False):
    """MongoDB Task Run Document"""

    _id: str
    id: str  # UUID
    pipeline_run_id: str
    node_id: str
    state: str  # "PENDING", "RUNNING", "SUCCESS", "FAILED", "SKIPPED"
    started_at: Optional[datetime]
    ended_at: Optional[datetime]
    duration_seconds: Optional[float]
    input_data: Dict[str, Any]
    output_data: Optional[Dict[str, Any]]
    error: Optional[str]
    retry_count: int
    logs: Optional[str]


class ScheduleDocument(TypedDict, total=False):
    """MongoDB Schedule Document"""

    _id: str
    id: str  # UUID
    project_id: str
    pipeline_id: str
    kind: str  # "CRON", "INTERVAL"
    expression: str  # cron expression or interval
    enabled: bool
    max_concurrency: int
    timeout_seconds: Optional[int]
    retry_policy: Dict[str, Any]
    created_at: datetime
    updated_at: datetime
    created_by: str
    next_run_at: Optional[datetime]
    last_run_at: Optional[datetime]
    last_run_id: Optional[str]
    tags: Dict[str, str]


class ConfigVersionDocument(TypedDict, total=False):
    """MongoDB Config Version Document"""

    _id: str
    project_id: str
    pipeline_id: str
    version_number: int
    config_hash: str
    config_snapshot: Dict[str, Any]
    changes: Dict[str, Any]
    created_at: datetime
    created_by: str
    change_reason: Optional[str]
    promoted: bool
    promoted_at: Optional[datetime]
    tags: Dict[str, str]
