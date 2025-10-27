"""
ScheduleStore: Focused store for schedule and dead letter queue operations.

Delegates to OrchestratorStore while providing:
- Clear type contracts for schedule management
- Dead letter queue operations for failed schedules
- Single responsibility (schedule management only)
- Better discoverability

This is part of the store refactoring to split 1,187-line monolithic store
into focused, maintainable modules.
"""

from typing import Any, Dict, Optional, List
from datetime import datetime

from loguru import logger  # type: ignore
from tauro.orchest.models import Schedule, ScheduleKind
from tauro.orchest.store import OrchestratorStore

from .base_store import BaseStore


class ScheduleStore(BaseStore):
    """
    Store for schedule and schedule dead letter queue operations.

    This store encapsulates all schedule-related operations, including:
    - Schedule CRUD operations (create, read, update, list)
    - Dead letter queue management for failed schedule runs
    - Circuit breaker state per schedule

    Delegates to OrchestratorStore for persistence while providing a cleaner,
    more focused API.

    Example:
        ```python
        store = ScheduleStore(context)

        # Create interval schedule (every 60 seconds)
        schedule = store.create_schedule(
            pipeline_id="my_pipeline",
            kind=ScheduleKind.INTERVAL,
            expression="60"
        )

        # Update schedule
        store.update_schedule(schedule.id, enabled=False)

        # List active schedules
        schedules = store.list_schedules(enabled=True)

        # Handle dead letter entries (failed schedule runs)
        failures = store.get_schedule_failures(schedule.id, limit=10)
        ```
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

        Args:
            pipeline_id: ID of the pipeline to schedule
            kind: ScheduleKind.INTERVAL or ScheduleKind.CRON
            expression: Schedule expression:
                - For INTERVAL: seconds as string (e.g., "60" for 60 seconds)
                - For CRON: cron expression (e.g., "*/5 * * * *" for every 5 minutes)
            max_concurrency: Max concurrent runs allowed (default: 1)
            retry_policy: Retry policy dict with 'retries' and 'delay_seconds' (optional)
            timeout_seconds: Timeout for each run (optional)
            next_run_at: Next scheduled run time (optional, auto-computed)
            enabled: Whether schedule is active (default: True)

        Returns:
            Created Schedule object

        Raises:
            ValueError: If parameters are invalid
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

        Args:
            schedule_id: ID of the schedule

        Returns:
            Schedule object or None if not found
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

        Args:
            pipeline_id: Filter by pipeline ID (optional)
            enabled: Filter by enabled status (optional)
            limit: Maximum number of results (default: 100)
            offset: Number of results to skip (default: 0)

        Returns:
            List of Schedule objects
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

        Args:
            schedule_id: ID of the schedule
            **fields: Fields to update (enabled, expression, max_concurrency, etc.)
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

        Dead letter entries are created when a scheduled run fails.
        Use this to debug and monitor failed schedules.

        Args:
            schedule_id: ID of the schedule
            limit: Maximum number of failures to return (default: 50)
            before: Filter failures before this date (optional)

        Returns:
            List of failure records with timestamps, errors, and metadata
        """
        return self._orchestrator_store.get_schedule_failures(
            schedule_id=schedule_id,
            limit=limit,
            before=before,
        )

    def remove_from_dead_letter(self, dead_letter_id: str) -> bool:
        """
        Remove a dead letter entry (failed schedule run).

        Use after resolving the issue that caused the failure.

        Args:
            dead_letter_id: ID of the dead letter entry

        Returns:
            True if removed, False if not found
        """
        return self._orchestrator_store.remove_from_dead_letter(dead_letter_id)

    def close(self) -> None:
        """Close the underlying OrchestratorStore."""
        if hasattr(self._orchestrator_store, "close"):
            self._orchestrator_store.close()

    def health_check(self) -> Dict[str, Any]:
        """
        Perform health check.

        Returns:
            Dictionary with health status
        """
        return self._orchestrator_store.health_check()
