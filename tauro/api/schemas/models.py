"""
Copyright (c) 2025 Faustino Lopez Ramos.
For licensing information, see the LICENSE file in the project root
"""
from pydantic import BaseModel, Field, field_validator, model_validator, validator
try:
    from pydantic.v1 import root_validator
except ImportError:
    from pydantic import root_validator  # type: ignore
from typing import Optional, Dict, Any, List
from datetime import datetime
from uuid import UUID, uuid4
from enum import Enum
import re

# Constants for validation
VALID_NAME_PATTERN = re.compile(r"^[a-zA-Z_]\w*$")
MAX_NAME_LENGTH = 63
RESERVED_NAMES = {"admin", "system", "root", "api", "health", "metrics"}

EXAMPLE_TIMESTAMP = "2025-01-15T10:30:00Z"

# Common example user for schema examples
DEFAULT_CREATED_BY = "user@example.com"


# =============================================================================
# ENUMS
# =============================================================================


class ProjectStatus(str, Enum):
    """Project status enum"""

    ACTIVE = "active"
    ARCHIVED = "archived"
    DRAFT = "draft"


class PipelineType(str, Enum):
    """Pipeline type enum"""

    BATCH = "batch"
    STREAMING = "streaming"
    ML = "ml"


class NodeType(str, Enum):
    """Node type enum"""

    SOURCE = "source"
    TRANSFORM = "transform"
    SINK = "sink"


class RunState(str, Enum):
    """Run state enum"""

    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


class TaskState(str, Enum):
    """Task state enum"""

    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    SKIPPED = "SKIPPED"


class ScheduleKind(str, Enum):
    """Schedule kind enum"""

    CRON = "CRON"
    INTERVAL = "INTERVAL"


class Priority(str, Enum):
    """Priority enum"""

    LOW = "low"
    NORMAL = "normal"
    HIGH = "high"


# =============================================================================
# RETRY POLICY
# =============================================================================


class RetryPolicy(BaseModel):
    """Retry policy configuration"""

    max_retries: int = Field(default=1, ge=0, le=10)
    initial_delay: int = Field(default=60, ge=1, description="Initial delay in seconds")
    max_delay: int = Field(default=3600, ge=1, description="Maximum delay in seconds")
    backoff_strategy: str = Field(
        default="exponential", description="exponential, linear, or fixed"
    )

    @field_validator("backoff_strategy")
    @classmethod
    def validate_backoff_strategy(cls, v):
        allowed = ["exponential", "linear", "fixed"]
        if v not in allowed:
            raise ValueError(f"backoff_strategy must be one of {allowed}")
        return v

    @model_validator(mode='after')
    def validate_delays(self):
        """Ensure max_delay >= initial_delay for retry strategy"""
        if self.max_delay < self.initial_delay:
            raise ValueError(
                f"max_delay ({self.max_delay}s) must be >= "
                f"initial_delay ({self.initial_delay}s)"
            )
        return self

    model_config = {
        "json_schema_extra": {
            "example": {
                "max_retries": 3,
                "initial_delay": 60,
                "max_delay": 3600,
                "backoff_strategy": "exponential",
            }
        }
    }


# =============================================================================
# GLOBAL SETTINGS
# =============================================================================


class GlobalSettings(BaseModel):
    """Global project settings"""

    input_path: str = Field(..., description="Input data path (S3, GCS, local, etc)")
    output_path: str = Field(..., description="Output data path (S3, GCS, local, etc)")
    mode: str = Field(
        default="batch",
        description="Execution mode: batch, streaming, or hybrid",
    )
    max_parallel_nodes: int = Field(
        default=4, ge=1, le=128, description="Maximum parallel node execution"
    )
    default_timeout_seconds: Optional[int] = Field(
        default=3600, ge=1, description="Default timeout for tasks in seconds"
    )
    retry_policy: Optional[RetryPolicy] = Field(
        default_factory=RetryPolicy, description="Default retry policy"
    )

    @validator("mode")
    def validate_mode(cls, v):
        allowed = ["batch", "streaming", "hybrid"]
        if v not in allowed:
            raise ValueError(f"mode must be one of {allowed}")
        return v

    @validator("input_path")
    def validate_input_path(cls, v):
        if not v or not v.strip():
            raise ValueError("input_path cannot be empty")
        # Basic validation for common path formats
        if not (v.startswith(("s3://", "gs://", "file://", "/")) or "://" not in v):
            raise ValueError("input_path must be a valid path (S3, GCS, local, etc)")
        return v

    @validator("output_path")
    def validate_output_path(cls, v):
        if not v or not v.strip():
            raise ValueError("output_path cannot be empty")
        # Basic validation for common path formats
        if not (v.startswith(("s3://", "gs://", "file://", "/")) or "://" not in v):
            raise ValueError("output_path must be a valid path (S3, GCS, local, etc)")
        return v

    @root_validator
    def validate_paths_not_same(cls, values):
        """Ensure input and output paths are different"""
        input_path = values.get("input_path")
        output_path = values.get("output_path")
        if input_path and output_path and input_path == output_path:
            raise ValueError("input_path and output_path cannot be the same")
        return values

    class Config:
        schema_extra = {
            "example": {
                "input_path": "s3://data-lake/raw/",
                "output_path": "s3://data-lake/processed/",
                "mode": "batch",
                "max_parallel_nodes": 8,
                "default_timeout_seconds": 3600,
                "retry_policy": {
                    "max_retries": 3,
                    "initial_delay": 60,
                    "max_delay": 3600,
                    "backoff_strategy": "exponential",
                },
            }
        }


# =============================================================================
# NODE CONFIGURATION
# =============================================================================


class NodeConfig(BaseModel):
    """Node configuration"""

    id: UUID = Field(default_factory=uuid4)
    name: str = Field(..., min_length=1, max_length=255)
    type: NodeType = Field(..., description="Node type: source, transform, or sink")
    implementation: str = Field(..., description="Implementation reference (module path, SQL, etc)")
    config: Dict[str, Any] = Field(default_factory=dict, description="Node-specific configuration")
    dependencies: Optional[List[str]] = Field(default=None, description="Names of dependent nodes")

    @validator("name")
    def validate_name(cls, v):
        if not VALID_NAME_PATTERN.match(v):
            raise ValueError(
                "Node name must start with letter or underscore and contain only "
                "alphanumeric characters and underscores"
            )
        if len(v) > MAX_NAME_LENGTH:
            raise ValueError(f"Node name must be {MAX_NAME_LENGTH} characters or less")
        return v

    @validator("dependencies", always=True)
    def validate_dependencies(cls, v):
        return v or []

    class Config:
        schema_extra = {
            "example": {
                "id": "550e8400-e29b-41d4-a716-446655440000",
                "name": "extract_data",
                "type": "source",
                "implementation": "tauro.sources.s3_loader",
                "config": {"bucket": "my-bucket", "prefix": "data/"},
                "dependencies": [],
            }
        }


# =============================================================================
# INPUT/OUTPUT CONFIGURATION
# =============================================================================


class InputOutput(BaseModel):
    """Input or Output configuration"""

    id: UUID = Field(default_factory=uuid4)
    name: str = Field(..., min_length=1, max_length=255)
    format: str = Field(..., description="Data format: csv, parquet, json, avro, etc")
    location: str = Field(..., description="Data location (S3 path, etc)")

    class Config:
        schema_extra = {
            "example": {
                "id": "550e8400-e29b-41d4-a716-446655440001",
                "name": "raw_data",
                "format": "csv",
                "location": "s3://bucket/data/raw.csv",
            }
        }


# =============================================================================
# PIPELINE CONFIGURATION
# =============================================================================


class PipelineConfig(BaseModel):
    """Pipeline configuration"""

    id: UUID = Field(default_factory=uuid4)
    name: str = Field(..., min_length=1, max_length=255)
    type: PipelineType = Field(..., description="Pipeline type")
    description: Optional[str] = Field(None, max_length=1000)
    nodes: List[NodeConfig] = Field(..., min_items=1, description="Pipeline nodes")
    inputs: List[InputOutput] = Field(default_factory=list, description="Pipeline inputs")
    outputs: List[InputOutput] = Field(default_factory=list, description="Pipeline outputs")

    @validator("name")
    def validate_name(cls, v):
        if not VALID_NAME_PATTERN.match(v):
            raise ValueError(
                "Pipeline name must start with letter or underscore and contain only "
                "alphanumeric characters and underscores"
            )
        if len(v) > MAX_NAME_LENGTH:
            raise ValueError(f"Pipeline name must be {MAX_NAME_LENGTH} characters or less")
        return v

    @validator("nodes")
    def validate_nodes_unique(cls, v):
        names = [node.name for node in v]
        if len(names) != len(set(names)):
            raise ValueError("Node names must be unique within pipeline")
        return v

    class Config:
        schema_extra = {
            "example": {
                "id": "550e8400-e29b-41d4-a716-446655440002",
                "name": "etl_pipeline",
                "type": "batch",
                "description": "Main ETL pipeline",
                "nodes": [],
                "inputs": [],
                "outputs": [],
            }
        }


# =============================================================================
# PROJECT MODELS
# =============================================================================


class ProjectCreate(BaseModel):
    """Project creation request"""

    name: str = Field(..., min_length=1, max_length=255)
    description: Optional[str] = Field(None, max_length=1000)
    global_settings: GlobalSettings = Field(...)
    pipelines: Optional[List[PipelineConfig]] = Field(
        default_factory=list, description="Initial pipelines"
    )
    tags: Optional[Dict[str, str]] = Field(default_factory=dict, description="Project tags")
    status: Optional[ProjectStatus] = Field(
        default=ProjectStatus.ACTIVE, description="Initial project status"
    )

    @validator("name")
    def validate_name(cls, v):
        if not VALID_NAME_PATTERN.match(v):
            raise ValueError(
                "Project name must start with letter or underscore and contain only "
                "alphanumeric characters and underscores"
            )
        if len(v) > MAX_NAME_LENGTH:
            raise ValueError(f"Project name must be {MAX_NAME_LENGTH} characters or less")
        if v.lower() in RESERVED_NAMES:
            raise ValueError(f"Project name '{v}' is reserved and cannot be used")
        return v

    class Config:
        schema_extra = {
            "example": {
                "name": "etl_pipeline_v2",
                "description": "Main ETL pipeline",
                "global_settings": {
                    "input_path": "s3://bucket/input/",
                    "output_path": "s3://bucket/output/",
                    "mode": "batch",
                    "max_parallel_nodes": 8,
                },
                "pipelines": [],
                "tags": {"team": "data-eng", "env": "production"},
                "status": "active",
            }
        }


class ProjectUpdate(BaseModel):
    """Project update request"""

    description: Optional[str] = Field(None, max_length=1000)
    global_settings: Optional[GlobalSettings] = None
    pipelines: Optional[List[PipelineConfig]] = None
    tags: Optional[Dict[str, str]] = None
    status: Optional[ProjectStatus] = None

    class Config:
        schema_extra = {
            "example": {
                "description": "Updated description",
                "tags": {"updated": "true"},
            }
        }


class ProjectResponse(ProjectCreate):
    """Project response with metadata"""

    id: UUID = Field(..., description="Project UUID")

    class Config:
        schema_extra = {
            "example": {
                "id": "550e8400-e29b-41d4-a716-446655440003",
                "name": "etl_pipeline_v2",
                "created_at": EXAMPLE_TIMESTAMP,
                "updated_at": "2025-01-15T11:00:00Z",
                "created_by": DEFAULT_CREATED_BY,
                "run_count": 42,
            }
        }


# =============================================================================
# RUN MODELS
# =============================================================================


class RunProgress(BaseModel):
    """Run progress information"""

    total_tasks: int = Field(..., ge=0)
    completed_tasks: int = Field(..., ge=0)
    failed_tasks: int = Field(..., ge=0)
    skipped_tasks: int = Field(..., ge=0)

    @property
    def pending_tasks(self) -> int:
        return self.total_tasks - self.completed_tasks - self.failed_tasks - self.skipped_tasks

    @property
    def progress_percentage(self) -> float:
        if self.total_tasks == 0:
            return 0.0
        return round((self.completed_tasks / self.total_tasks) * 100, 2)


class TaskRunResponse(BaseModel):
    """Task run response"""

    id: UUID
    node_id: str
    state: TaskState
    started_at: Optional[datetime] = None
    ended_at: Optional[datetime] = None
    duration_seconds: Optional[float] = None
    error: Optional[str] = None


class RunCreate(BaseModel):
    """Run creation request"""

    project_id: UUID = Field(...)
    pipeline_id: UUID = Field(...)
    params: Optional[Dict[str, Any]] = Field(default_factory=dict)
    priority: Optional[Priority] = Field(default=Priority.NORMAL)
    tags: Optional[Dict[str, str]] = Field(default_factory=dict)

    class Config:
        schema_extra = {
            "example": {
                "project_id": "550e8400-e29b-41d4-a716-446655440003",
                "pipeline_id": "550e8400-e29b-41d4-a716-446655440002",
                "params": {"start_date": "2025-01-01", "end_date": "2025-01-31"},
                "priority": "normal",
                "tags": {"env": "production"},
            }
        }


class RunResponse(BaseModel):
    """Run response with full details"""

    id: UUID
    project_id: UUID
    pipeline_id: UUID
    state: RunState
    params: Dict[str, Any]
    priority: Priority
    tags: Dict[str, str]
    created_at: datetime
    started_at: Optional[datetime] = None
    ended_at: Optional[datetime] = None
    created_by: str

    class Config:
        schema_extra = {
            "example": {
                "id": "550e8400-e29b-41d4-a716-446655440004",
                "project_id": "550e8400-e29b-41d4-a716-446655440003",
                "pipeline_id": "550e8400-e29b-41d4-a716-446655440002",
                "state": "RUNNING",
                "params": {},
                "priority": "normal",
                "tags": {},
                "created_at": EXAMPLE_TIMESTAMP,
                "started_at": "2025-01-15T10:31:00Z",
                "created_by": DEFAULT_CREATED_BY,
            }
        }


# =============================================================================
# SCHEDULE MODELS
# =============================================================================


class ScheduleCreate(BaseModel):
    """Schedule creation request"""

    project_id: UUID = Field(...)
    pipeline_id: UUID = Field(...)
    kind: ScheduleKind = Field(...)
    expression: str = Field(..., min_length=1, max_length=255)
    enabled: bool = Field(default=True)
    max_concurrency: int = Field(default=1, ge=1, le=100)
    timeout_seconds: Optional[int] = Field(None, ge=1, le=86400)
    retry_policy: Optional[RetryPolicy] = None
    tags: Optional[Dict[str, str]] = Field(default_factory=dict)

    class Config:
        schema_extra = {
            "example": {
                "project_id": "550e8400-e29b-41d4-a716-446655440003",
                "pipeline_id": "550e8400-e29b-41d4-a716-446655440002",
                "kind": "CRON",
                "expression": "0 2 * * *",
                "enabled": True,
                "max_concurrency": 2,
            }
        }


class ScheduleResponse(ScheduleCreate):
    """Schedule response with metadata"""

    id: UUID
    created_at: datetime
    updated_at: datetime

    class Config:
        schema_extra = {
            "example": {
                "id": "550e8400-e29b-41d4-a716-446655440005",
                "project_id": "550e8400-e29b-41d4-a716-446655440003",
                "pipeline_id": "550e8400-e29b-41d4-a716-446655440002",
                "kind": "CRON",
                "expression": "0 2 * * *",
                "enabled": True,
                "max_concurrency": 2,
                "created_at": EXAMPLE_TIMESTAMP,
                "updated_at": EXAMPLE_TIMESTAMP,
                "created_by": DEFAULT_CREATED_BY,
                "next_run_at": "2025-01-16T02:00:00Z",
            }
        }


# =============================================================================
# CONFIG VERSION MODELS
# =============================================================================


class ConfigVersionResponse(BaseModel):
    """Config version response"""

    version_number: int
    config_hash: str
    changes: Dict[str, Any]
    created_at: datetime
    created_by: str
    change_reason: Optional[str] = None

    class Config:
        schema_extra = {
            "example": {
                "version_number": 2,
                "config_hash": "sha256-abc123...",
                "changes": {"global_settings.max_parallel_nodes": {"from": 4, "to": 8}},
                "created_at": "2025-01-15T11:00:00Z",
                "created_by": DEFAULT_CREATED_BY,
                "change_reason": "Increase parallelism for performance",
                "promoted": False,
            }
        }


# =============================================================================
# PAGINATION
# =============================================================================


class PaginationMetadata(BaseModel):
    """Pagination metadata"""

    total: int = Field(..., ge=0)
    limit: int = Field(..., ge=1)
    offset: int = Field(..., ge=0)
    has_next: bool = Field(...)
    has_prev: bool = Field(...)

    @property
    def pages(self) -> int:
        return (self.total + self.limit - 1) // self.limit

    @property
    def current_page(self) -> int:
        return (self.offset // self.limit) + 1


# =============================================================================
# RESPONSE WRAPPERS
# =============================================================================


class SuccessResponse(BaseModel):
    """Standard success response"""

    status: str = "success"
    data: Any = Field(...)
    timestamp: datetime = Field(default_factory=datetime.utcnow)


class PaginatedResponse(BaseModel):
    """Paginated response"""

    status: str = "success"
    data: List[Any] = Field(...)
    pagination: PaginationMetadata = Field(...)
    timestamp: datetime = Field(default_factory=datetime.utcnow)


class ErrorResponse(BaseModel):
    """Error response"""

    status: str = "error"
    error: Dict[str, Any] = Field(...)
    timestamp: datetime = Field(default_factory=datetime.utcnow)


class AsyncTaskResponse(BaseModel):
    """Async task response (202 Accepted)"""

    status: str = "accepted"
    task_id: UUID = Field(...)
    status_url: str = Field(...)
    message: str = Field(...)
    timestamp: datetime = Field(default_factory=datetime.utcnow)
