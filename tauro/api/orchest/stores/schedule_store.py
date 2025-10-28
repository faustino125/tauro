"""
Copyright (c) 2025 Faustino Lopez Ramos.
For licensing information, see the LICENSE file in the project root
"""
from typing import Any, Dict, Optional, List
from datetime import datetime

from loguru import logger  # type: ignore
from tauro.api.orchest.models import Schedule, ScheduleKind
from tauro.api.orchest.store import OrchestratorStore

from .base_store import BaseStore


class ScheduleStore(BaseStore):
    """
    Store for schedule and schedule dead letter queue operations.
    """

    def __init__(self, context: Optional[Any] = None):
        """
        Initialize ScheduleStore.

        Args:
            context: Tauro execution context (passed to OrchestratorStore)
        """
        super().__init__(context)
        self._orchestrator_store = OrchestratorStore(context=context)
        logger.debug("ScheduleStore initialized, delegating to OrchestratorStore")

    def create_schedule(
        self,
        pipeline_id: str,
        kind: ScheduleKind,
        expression: str,
        max_concurrency: int = 1,
        retry_policy: Optional[Dict[str, Any]] = None,
        timeout_seconds: Optional[int] = None,
        next_run_at: Optional[datetime] = None,
        enabled: bool = True,
    ) -> Schedule:
        """
        Create a new schedule for pipeline execution.
        """
        if not pipeline_id or not isinstance(pipeline_id, str):
            raise ValueError("pipeline_id must be a non-empty string")

        return self._orchestrator_store.create_schedule(
            pipeline_id=pipeline_id,
            kind=kind,
            expression=expression,
            max_concurrency=max_concurrency,
            retry_policy=retry_policy,
            timeout_seconds=timeout_seconds,
            next_run_at=next_run_at,
            enabled=enabled,
        )

    def get_schedule(self, schedule_id: str) -> Optional[Schedule]:
        """
        Get a schedule by ID.
        """
        return self._orchestrator_store.get_schedule(schedule_id)

    def list_schedules(
        self,
        pipeline_id: Optional[str] = None,
        enabled: Optional[bool] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> List[Schedule]:
        """
        List schedules with optional filtering.
        """
        return self._orchestrator_store.list_schedules(
            pipeline_id=pipeline_id,
            enabled=enabled,
            limit=limit,
            offset=offset,
        )

    def update_schedule(self, schedule_id: str, **fields) -> None:
        """
        Update schedule fields.
        """
        self._orchestrator_store.update_schedule(schedule_id, **fields)

    def get_schedule_failures(
        self,
        schedule_id: str,
        limit: int = 50,
        before: Optional[datetime] = None,
    ) -> List[Dict[str, Any]]:
        """
        Get failed schedule runs from dead letter queue.
        """
        return self._orchestrator_store.get_schedule_failures(
            schedule_id=schedule_id,
            limit=limit,
            before=before,
        )

    def remove_from_dead_letter(self, dead_letter_id: str) -> bool:
        """
        Remove a dead letter entry (failed schedule run).
        """
        return self._orchestrator_store.remove_from_dead_letter(dead_letter_id)

    def close(self) -> None:
        """Close the underlying OrchestratorStore."""
        if hasattr(self._orchestrator_store, "close"):
            self._orchestrator_store.close()

    def health_check(self) -> Dict[str, Any]:
        """
        Perform health check.
        """
        return self._orchestrator_store.health_check()
