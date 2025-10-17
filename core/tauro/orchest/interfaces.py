from __future__ import annotations
from typing import Protocol, Optional, Dict, Any, List
from datetime import datetime

from tauro.orchest.models import PipelineRun, TaskRun, RunState


class OrchestratorStoreProtocol(Protocol):
    def create_pipeline_run(
        self, pipeline_id: str, params: Optional[Dict[str, Any]] = None
    ) -> PipelineRun:
        ...

    def get_pipeline_run(self, run_id: str) -> Optional[PipelineRun]:
        ...

    def update_pipeline_run_state(
        self,
        run_id: str,
        new_state: RunState,
        started_at: Optional[datetime] = None,
        finished_at: Optional[datetime] = None,
        error: Optional[str] = None,
    ) -> None:
        ...

    def list_pipeline_runs(
        self,
        pipeline_id: Optional[str] = None,
        state: Optional[RunState] = None,
        limit: int = 50,
        offset: int = 0,
        created_after: Optional[datetime] = None,
        created_before: Optional[datetime] = None,
    ) -> List[PipelineRun]:
        ...

    def create_task_run(self, pipeline_run_id: str, task_id: str) -> TaskRun:
        ...

    def update_task_run_state(
        self,
        task_run_id: str,
        new_state: RunState,
        try_number: Optional[int] = None,
        started_at: Optional[datetime] = None,
        finished_at: Optional[datetime] = None,
        error: Optional[str] = None,
        log_uri: Optional[str] = None,
    ) -> None:
        ...

    def list_task_runs(
        self, pipeline_run_id: str, state: Optional[RunState] = None
    ) -> List[TaskRun]:
        ...

    def close(self) -> None:
        ...

    def get_database_stats(self) -> Dict[str, Any]:
        ...


class OrchestratorRunnerProtocol(Protocol):
    def create_run(
        self, pipeline_id: str, params: Optional[Dict[str, Any]] = None
    ) -> PipelineRun:
        ...

    def start_run(
        self,
        run_id: str,
        retries: int = 0,
        retry_delay_sec: int = 0,
        concurrency: Optional[int] = None,
        timeout_seconds: Optional[int] = None,
    ) -> RunState:
        ...

    def run_pipeline(
        self, pipeline_id: str, params: Optional[Dict[str, Any]] = None, **kwargs
    ) -> str:
        ...

    def cancel_run(self, run_id: str) -> bool:
        ...

    def get_run(self, run_id: str) -> Optional[PipelineRun]:
        ...

    def list_runs(
        self,
        pipeline_id: Optional[str] = None,
        state: Optional[RunState] = None,
        limit: int = 50,
        created_after: Optional[datetime] = None,
        created_before: Optional[datetime] = None,
    ) -> List[PipelineRun]:
        ...

    def list_task_runs(
        self, run_id: str, state: Optional[RunState] = None
    ) -> List[TaskRun]:
        ...

    def shutdown(self, wait: bool = True) -> None:
        ...
