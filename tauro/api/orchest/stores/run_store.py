"""
Copyright (c) 2025 Faustino Lopez Ramos.
For licensing information, see the LICENSE file in the project root
"""
from typing import Any, Dict, Optional, List
from datetime import datetime

from loguru import logger  # type: ignore
from tauro.api.orchest.models import PipelineRun, TaskRun, RunState
from tauro.api.orchest.store import OrchestratorStore

from tauro.api.orchest.stores.base_store import PipelineRunStore, TaskRunStore


class RunStore(PipelineRunStore, TaskRunStore):
    """
    Combined store for PipelineRun and TaskRun operations.
    """

    def __init__(self, context: Optional[Any] = None):
        """
        Initialize RunStore.

        Args:
            context: Tauro execution context (passed to OrchestratorStore)
        """
        super().__init__(context)
        self._orchestrator_store = OrchestratorStore(context=context)
        logger.debug("RunStore initialized, delegating to OrchestratorStore")

    def create_pipeline_run(
        self, pipeline_id: str, params: Optional[Dict[str, Any]] = None
    ) -> PipelineRun:
        """
        Create a new pipeline run.
        """
        if not pipeline_id or not isinstance(pipeline_id, str):
            raise ValueError("pipeline_id must be a non-empty string")

        return self._orchestrator_store.create_pipeline_run(pipeline_id, params)

    def get_pipeline_run(self, run_id: str) -> Optional[PipelineRun]:
        """
        Get a pipeline run by ID.

        Args:
            run_id: ID of the pipeline run

        Returns:
            PipelineRun object or None if not found
        """
        return self._orchestrator_store.get_pipeline_run(run_id)

    def update_pipeline_run_state(
        self,
        run_id: str,
        new_state: RunState,
        started_at: Optional[datetime] = None,
        finished_at: Optional[datetime] = None,
        error: Optional[str] = None,
    ) -> None:
        """
        Update pipeline run state.
        """
        self._orchestrator_store.update_pipeline_run_state(
            run_id, new_state, started_at, finished_at, error
        )

    def list_pipeline_runs(
        self,
        pipeline_id: Optional[str] = None,
        state: Optional[RunState] = None,
        limit: int = 100,
        offset: int = 0,
        created_after: Optional[datetime] = None,
        created_before: Optional[datetime] = None,
    ) -> List[PipelineRun]:
        """
        List pipeline runs with optional filtering.
        """
        return self._orchestrator_store.list_pipeline_runs(
            pipeline_id=pipeline_id,
            state=state,
            limit=limit,
            offset=offset,
            created_after=created_after,
            created_before=created_before,
        )

    def create_task_run(self, pipeline_run_id: str, task_id: str) -> TaskRun:
        """
        Create a new task run.
        """
        if not pipeline_run_id or not isinstance(pipeline_run_id, str):
            raise ValueError("pipeline_run_id must be a non-empty string")
        if not task_id or not isinstance(task_id, str):
            raise ValueError("task_id must be a non-empty string")

        return self._orchestrator_store.create_task_run(pipeline_run_id, task_id)

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
        """
        Update task run state.
        """
        self._orchestrator_store.update_task_run_state(
            task_run_id, new_state, try_number, started_at, finished_at, error, log_uri
        )

    def list_task_runs(
        self, pipeline_run_id: str, state: Optional[RunState] = None
    ) -> List[TaskRun]:
        """
        List task runs for a pipeline run.
        """
        return self._orchestrator_store.list_task_runs(pipeline_run_id, state=state)

    def close(self) -> None:
        """Close the underlying OrchestratorStore."""
        if hasattr(self._orchestrator_store, "close"):
            self._orchestrator_store.close()

    def health_check(self) -> Dict[str, Any]:
        """
        Perform health check.
        """
        return self._orchestrator_store.health_check()
