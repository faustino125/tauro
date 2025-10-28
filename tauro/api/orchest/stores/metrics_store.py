"""
Copyright (c) 2025 Faustino Lopez Ramos.
For licensing information, see the LICENSE file in the project root
"""
from typing import Any, Dict, Optional, List
from datetime import datetime

from loguru import logger  # type: ignore
from tauro.api.orchest.store import OrchestratorStore

from .base_store import BaseStore

PIPELINE_ID_ERROR_MSG = "pipeline_id must be a non-empty string"


class MetricsStore(BaseStore):
    """
    Store for pipeline metrics and run results.
    """

    def __init__(self, context: Optional[Any] = None):
        """
        Initialize MetricsStore.

        Args:
            context: Tauro execution context (passed to OrchestratorStore)
        """
        super().__init__(context)
        self._orchestrator_store = OrchestratorStore(context=context)
        logger.debug("MetricsStore initialized, delegating to OrchestratorStore")

    def record_pipeline_metrics(
        self,
        pipeline_id: str,
        total_runs: int = 0,
        successful_runs: int = 0,
        failed_runs: int = 0,
        cancelled_runs: int = 0,
        avg_execution_time: float = 0.0,
        max_execution_time: float = 0.0,
        min_execution_time: float = float("inf"),
        last_run_at: Optional[datetime] = None,
        circuit_breaker_state: Optional[str] = None,
    ) -> None:
        """
        Record pipeline execution metrics.
        """
        if not pipeline_id or not isinstance(pipeline_id, str):
            raise ValueError(PIPELINE_ID_ERROR_MSG)

        self._orchestrator_store.record_pipeline_metrics(
            pipeline_id=pipeline_id,
            total_runs=total_runs,
            successful_runs=successful_runs,
            failed_runs=failed_runs,
            cancelled_runs=cancelled_runs,
            avg_execution_time=avg_execution_time,
            max_execution_time=max_execution_time,
            min_execution_time=min_execution_time,
            last_run_at=last_run_at,
            circuit_breaker_state=circuit_breaker_state,
        )

    def get_pipeline_metrics(self, pipeline_id: str) -> Optional[Dict[str, Any]]:
        """
        Get aggregated metrics for a pipeline.
        """
        if not pipeline_id or not isinstance(pipeline_id, str):
            raise ValueError(PIPELINE_ID_ERROR_MSG)

        return self._orchestrator_store.get_pipeline_metrics(pipeline_id)

    def record_run_result(
        self,
        run_id: str,
        pipeline_id: str,
        success: bool,
        execution_time_seconds: float,
        task_count: int = 0,
        successful_tasks: int = 0,
        failed_tasks: int = 0,
        error_message: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        """
        Record result of a single pipeline run.
        """
        if not pipeline_id or not isinstance(pipeline_id, str):
            raise ValueError(PIPELINE_ID_ERROR_MSG)

        self._orchestrator_store.record_run_result(
            run_id=run_id,
            pipeline_id=pipeline_id,
            success=success,
            execution_time_seconds=execution_time_seconds,
            task_count=task_count,
            successful_tasks=successful_tasks,
            failed_tasks=failed_tasks,
            error_message=error_message,
            metadata=metadata,
        )

    def get_run_results(
        self,
        pipeline_id: Optional[str] = None,
        success_only: bool = False,
        limit: int = 100,
        offset: int = 0,
    ) -> List[Dict[str, Any]]:
        """
        Get run results with optional filtering.
        """
        return self._orchestrator_store.get_run_results(
            pipeline_id=pipeline_id,
            success_only=success_only,
            limit=limit,
            offset=offset,
        )

    def get_performance_statistics(self, pipeline_id: str) -> Optional[Dict[str, Any]]:
        """
        Get performance statistics for a pipeline.
        """
        metrics = self.get_pipeline_metrics(pipeline_id)
        if not metrics:
            return None

        total = metrics.get("total_runs", 0)
        successful = metrics.get("successful_runs", 0)

        return {
            "total_runs": total,
            "success_rate": (successful / total * 100) if total > 0 else 0,
            "avg_execution_time": metrics.get("avg_execution_time", 0),
            "max_execution_time": metrics.get("max_execution_time", 0),
            "min_execution_time": metrics.get("min_execution_time", float("inf")),
            "last_run_at": metrics.get("last_run_at"),
        }

    def close(self) -> None:
        """Close the underlying OrchestratorStore."""
        if hasattr(self._orchestrator_store, "close"):
            self._orchestrator_store.close()

    def health_check(self) -> Dict[str, Any]:
        """
        Perform health check.
        """
        return self._orchestrator_store.health_check()
