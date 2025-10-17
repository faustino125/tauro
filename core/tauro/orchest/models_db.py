from sqlalchemy import (
    Column,
    String,
    Integer,
    DateTime,
    Text,
    Boolean,
    Float,
    JSON,
    ForeignKey,
)
from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime

Base = declarative_base()


class PipelineRunDB(Base):
    __tablename__ = "pipeline_runs"

    id = Column(String, primary_key=True)
    pipeline_id = Column(String, nullable=False, index=True)
    state = Column(String, nullable=False)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    started_at = Column(DateTime, nullable=True)
    finished_at = Column(DateTime, nullable=True)
    params = Column(JSON, nullable=True)  # Store as JSON
    error = Column(Text, nullable=True)


class TaskRunDB(Base):
    __tablename__ = "task_runs"

    id = Column(String, primary_key=True)
    pipeline_run_id = Column(
        String, ForeignKey("pipeline_runs.id"), nullable=False, index=True
    )
    task_id = Column(String, nullable=False)
    state = Column(String, nullable=False)
    try_number = Column(Integer, default=0)
    started_at = Column(DateTime, nullable=True)
    finished_at = Column(DateTime, nullable=True)
    log_uri = Column(String, nullable=True)
    error = Column(Text, nullable=True)


class ScheduleDB(Base):
    __tablename__ = "schedules"

    id = Column(String, primary_key=True)
    pipeline_id = Column(String, nullable=False, index=True)
    kind = Column(String, nullable=False)
    expression = Column(String, nullable=False)
    enabled = Column(Boolean, default=True)
    max_concurrency = Column(Integer, default=1)
    retry_policy = Column(JSON, nullable=True)
    timeout_seconds = Column(Integer, nullable=True)
    next_run_at = Column(DateTime, nullable=True)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at = Column(DateTime, nullable=False, default=datetime.utcnow)


class ScheduleDeadLetterDB(Base):
    __tablename__ = "schedule_dead_letter"

    id = Column(String, primary_key=True)
    schedule_id = Column(String, nullable=False, index=True)
    pipeline_id = Column(String, nullable=False)
    error = Column(Text, nullable=False)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)


class PipelineMetricsDB(Base):
    __tablename__ = "pipeline_metrics"

    pipeline_id = Column(String, primary_key=True)
    total_runs = Column(Integer, default=0)
    successful_runs = Column(Integer, default=0)
    failed_runs = Column(Integer, default=0)
    avg_execution_time_seconds = Column(Float, default=0.0)
    last_run_at = Column(DateTime, nullable=True)
    last_success_at = Column(DateTime, nullable=True)
    last_failure_at = Column(DateTime, nullable=True)
    updated_at = Column(DateTime, nullable=False, default=datetime.utcnow)


class RunResultDB(Base):
    __tablename__ = "run_results"

    run_id = Column(String, primary_key=True)
    status = Column(String, nullable=False)
    result = Column(JSON, nullable=True)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
