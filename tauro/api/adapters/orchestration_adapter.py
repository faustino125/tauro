from __future__ import annotations
from typing import Dict, Optional, List, Any
from datetime import datetime

from loguru import logger

from tauro.config.contexts import Context
from tauro.orchest import (
    RunService,
    ScheduleService,
    MetricsService,
    OrchestratorStore,
    RunState,
    ScheduleKind,
    PipelineRun,
    TaskRun,
    Schedule,
)


class APIOrchestrationAdapter:
    """
    API adapter for orchestration services.

    This adapter provides a REST API-friendly interface for orchestration
    operations without coupling to the CLI layer.
    """

    def __init__(
        self,
        context: Optional[Context] = None,
        store: Optional[OrchestratorStore] = None,
    ):
        """
        Initialize the API orchestration adapter.

        Args:
            context: Optional execution context (can be set per-operation)
            store: Optional orchestrator store
        """
        self.default_context = context
        self.store = store or OrchestratorStore(context=context)

        # Services will be initialized when needed
        self._run_services: Dict[str, RunService] = {}
        self._schedule_services: Dict[str, ScheduleService] = {}
        self.metrics_service = MetricsService(self.store)

        logger.debug("APIOrchestrationAdapter initialized")

    def _get_run_service(self, context: Context) -> RunService:
        """Get or create a run service for the given context."""
        context_id = id(context)
        if context_id not in self._run_services:
            self._run_services[context_id] = RunService(context, self.store)
        return self._run_services[context_id]

    def _get_schedule_service(self, context: Context) -> ScheduleService:
        """Get or create a schedule service for the given context."""
        context_id = id(context)
        if context_id not in self._schedule_services:

            def run_executor(run_id: str, **kwargs):
                """Executor function for scheduled runs."""
                try:
                    run_service = self._get_run_service(context)
                    run_service.start_run(run_id, **kwargs)
                except Exception as e:
                    logger.error(f"Error executing scheduled run {run_id}: {e}")

            self._schedule_services[context_id] = ScheduleService(
                context, self.store, run_executor=run_executor
            )
        return self._schedule_services[context_id]

    # =========================================================================
    # Run Operations
    # =========================================================================

    def create_run(
        self,
        context: Context,
        pipeline_id: str,
        params: Optional[Dict[str, Any]] = None,
    ) -> PipelineRun:
        """
        Create a new pipeline run.

        Args:
            context: Execution context
            pipeline_id: Pipeline identifier
            params: Optional run parameters

        Returns:
            PipelineRun instance
        """
        run_service = self._get_run_service(context)
        return run_service.create_run(pipeline_id, params)

    def start_run(
        self,
        context: Context,
        run_id: str,
        retries: int = 0,
        retry_delay_sec: int = 0,
        concurrency: Optional[int] = None,
        timeout_seconds: Optional[int] = None,
    ) -> RunState:
        """
        Start a pipeline run synchronously.

        Args:
            context: Execution context
            run_id: Run identifier
            retries: Number of retries
            retry_delay_sec: Delay between retries
            concurrency: Maximum concurrent tasks
            timeout_seconds: Execution timeout

        Returns:
            Final run state
        """
        run_service = self._get_run_service(context)
        return run_service.start_run(
            run_id,
            retries=retries,
            retry_delay_sec=retry_delay_sec,
            concurrency=concurrency,
            timeout_seconds=timeout_seconds,
        )

    def start_run_async(
        self,
        context: Context,
        pipeline_id: str,
        params: Optional[Dict[str, Any]] = None,
        **kwargs,
    ) -> str:
        """
        Create and start a run asynchronously.

        Args:
            context: Execution context
            pipeline_id: Pipeline identifier
            params: Optional run parameters
            **kwargs: Additional execution parameters

        Returns:
            Run ID
        """
        run_service = self._get_run_service(context)
        return run_service.start_run_async(pipeline_id, params, **kwargs)

    def get_run(
        self,
        run_id: str,
    ) -> Optional[PipelineRun]:
        """
        Get a pipeline run by ID.

        Args:
            run_id: Run identifier

        Returns:
            PipelineRun instance or None
        """
        # Use store directly since context is not needed for reads
        return self.store.get_pipeline_run(run_id)

    def list_runs(
        self,
        pipeline_id: Optional[str] = None,
        state: Optional[RunState] = None,
        limit: int = 50,
        created_after: Optional[datetime] = None,
        created_before: Optional[datetime] = None,
    ) -> List[PipelineRun]:
        """
        List pipeline runs.

        Args:
            pipeline_id: Optional filter by pipeline ID
            state: Optional filter by state
            limit: Maximum number of results
            created_after: Filter by creation date
            created_before: Filter by creation date

        Returns:
            List of PipelineRun instances
        """
        return self.store.list_pipeline_runs(
            pipeline_id=pipeline_id,
            state=state,
            limit=limit,
            created_after=created_after,
            created_before=created_before,
        )

    def list_task_runs(
        self,
        run_id: str,
        state: Optional[RunState] = None,
    ) -> List[TaskRun]:
        """
        List task runs for a pipeline run.

        Args:
            run_id: Pipeline run ID
            state: Optional filter by state

        Returns:
            List of TaskRun instances
        """
        return self.store.list_task_runs(run_id, state=state)

    def cancel_run(
        self,
        context: Context,
        run_id: str,
    ) -> bool:
        """
        Cancel a running pipeline.

        Args:
            context: Execution context
            run_id: Run identifier

        Returns:
            True if cancelled successfully
        """
        run_service = self._get_run_service(context)
        return run_service.cancel_run(run_id)

    # =========================================================================
    # Schedule Operations
    # =========================================================================

    def create_schedule(
        self,
        context: Context,
        pipeline_id: str,
        kind: ScheduleKind,
        expression: str,
        max_concurrency: int = 1,
        retry_policy: Optional[Dict[str, int]] = None,
        timeout_seconds: Optional[int] = None,
        enabled: bool = True,
    ) -> Schedule:
        """
        Create a new schedule.

        Args:
            context: Execution context
            pipeline_id: Pipeline identifier
            kind: Schedule kind
            expression: Schedule expression
            max_concurrency: Maximum concurrent runs
            retry_policy: Retry policy configuration
            timeout_seconds: Execution timeout
            enabled: Whether schedule is enabled

        Returns:
            Schedule instance
        """
        schedule_service = self._get_schedule_service(context)
        return schedule_service.create_schedule(
            pipeline_id=pipeline_id,
            kind=kind,
            expression=expression,
            max_concurrency=max_concurrency,
            retry_policy=retry_policy,
            timeout_seconds=timeout_seconds,
            enabled=enabled,
        )

    def update_schedule(self, schedule_id: str, **updates) -> bool:
        """
        Update a schedule.

        Args:
            schedule_id: Schedule identifier
            **updates: Fields to update

        Returns:
            True if updated successfully
        """
        # Use store directly for updates
        try:
            self.store.update_schedule(schedule_id, **updates)
            return True
        except Exception as e:
            logger.error(f"Failed to update schedule {schedule_id}: {e}")
            return False

    def delete_schedule(
        self,
        schedule_id: str,
    ) -> bool:
        """
        Delete a schedule.

        Args:
            schedule_id: Schedule identifier

        Returns:
            True if deleted successfully
        """
        try:
            self.store.delete_schedule(schedule_id)
            return True
        except Exception as e:
            logger.error(f"Failed to delete schedule {schedule_id}: {e}")
            return False

    def list_schedules(
        self,
        pipeline_id: Optional[str] = None,
        enabled_only: bool = False,
    ) -> List[Schedule]:
        """
        List schedules.

        Args:
            pipeline_id: Optional filter by pipeline ID
            enabled_only: Only return enabled schedules

        Returns:
            List of Schedule instances
        """
        return self.store.list_schedules(
            pipeline_id=pipeline_id,
            enabled_only=enabled_only,
        )

    def get_schedule(
        self,
        schedule_id: str,
    ) -> Optional[Schedule]:
        """
        Get a schedule by ID.

        Args:
            schedule_id: Schedule identifier

        Returns:
            Schedule instance or None
        """
        schedules = self.store.list_schedules()
        return next((s for s in schedules if s.id == schedule_id), None)

    def backfill_schedule(
        self,
        context: Context,
        schedule_id: str,
        count: int,
    ) -> Dict[str, Any]:
        """
        Create backfill runs for a schedule.

        Args:
            context: Execution context
            schedule_id: Schedule identifier
            count: Number of runs to create

        Returns:
            Dictionary with backfill result
        """
        schedule = self.get_schedule(schedule_id)
        if not schedule:
            return {"error": "Schedule not found"}

        schedule_service = self._get_schedule_service(context)
        schedule_service.backfill(schedule.pipeline_id, count)

        return {
            "schedule_id": schedule_id,
            "pipeline_id": schedule.pipeline_id,
            "runs_created": count,
        }

    # =========================================================================
    # Scheduler Management
    # =========================================================================

    def start_scheduler(
        self,
        context: Context,
        poll_interval: float = 1.0,
    ):
        """
        Start the scheduler service.

        Args:
            context: Execution context
            poll_interval: Polling interval in seconds
        """
        schedule_service = self._get_schedule_service(context)
        schedule_service.start(poll_interval)

    def stop_scheduler(
        self,
        context: Context,
        timeout: float = 30.0,
    ):
        """
        Stop the scheduler service.

        Args:
            context: Execution context
            timeout: Maximum time to wait for shutdown
        """
        context_id = id(context)
        if context_id in self._schedule_services:
            schedule_service = self._schedule_services[context_id]
            schedule_service.stop(timeout)

    def get_scheduler_status(
        self,
        context: Context,
    ) -> Dict[str, Any]:
        """
        Get scheduler status.

        Args:
            context: Execution context

        Returns:
            Dictionary with scheduler status
        """
        context_id = id(context)
        if context_id not in self._schedule_services:
            return {
                "running": False,
                "message": "Scheduler not initialized",
            }

        schedule_service = self._schedule_services[context_id]
        health = schedule_service.get_health_status()

        return {
            "running": health["running"],
            "healthy": health["healthy"],
            "issues": health["issues"],
            "warnings": health.get("warnings", []),
            "metrics": health["metrics"],
        }

    # =========================================================================
    # Metrics Operations
    # =========================================================================

    def get_pipeline_metrics(
        self,
        pipeline_id: str,
    ) -> Dict[str, Any]:
        """
        Get metrics for a specific pipeline.

        Args:
            pipeline_id: Pipeline identifier

        Returns:
            Dictionary with pipeline metrics
        """
        return self.metrics_service.get_pipeline_metrics(pipeline_id)

    def get_global_metrics(self) -> Dict[str, Any]:
        """
        Get global orchestration metrics.

        Returns:
            Dictionary with global metrics
        """
        return self.metrics_service.get_global_metrics()

    def get_schedule_metrics(
        self,
        schedule_id: str,
    ) -> Dict[str, Any]:
        """
        Get metrics for a specific schedule.

        Args:
            schedule_id: Schedule identifier

        Returns:
            Dictionary with schedule metrics
        """
        return self.metrics_service.get_schedule_metrics(schedule_id)

    def get_run_service_health(
        self,
        context: Context,
    ) -> Dict[str, Any]:
        """
        Get run service health status.

        Args:
            context: Execution context

        Returns:
            Dictionary with health status
        """
        run_service = self._get_run_service(context)
        return run_service.get_health_status()

    # =========================================================================
    # Database Operations
    # =========================================================================

    def get_database_stats(self) -> Dict[str, Any]:
        """
        Get database statistics.

        Returns:
            Dictionary with database statistics
        """
        return self.store.get_database_stats()

    def cleanup_old_data(
        self,
        max_days: int = 30,
        batch_size: int = 1000,
    ) -> Dict[str, int]:
        """
        Clean up old data from database.

        Args:
            max_days: Maximum age of data to keep
            batch_size: Batch size for deletion

        Returns:
            Dictionary with cleanup results
        """
        return self.store.cleanup_old_data(max_days, batch_size)

    # =========================================================================
    # Lifecycle
    # =========================================================================

    def shutdown(self, timeout: float = 30.0):
        """
        Shutdown the adapter and all services.

        Args:
            timeout: Maximum time to wait for shutdown
        """
        logger.info("Shutting down APIOrchestrationAdapter...")

        # Stop all schedule services
        for schedule_service in self._schedule_services.values():
            try:
                schedule_service.stop(timeout)
            except Exception as e:
                logger.error(f"Error stopping scheduler: {e}")

        # Shutdown all run services
        for run_service in self._run_services.values():
            try:
                run_service.shutdown(timeout=timeout)
            except Exception as e:
                logger.error(f"Error shutting down run service: {e}")

        # Clear services
        self._run_services.clear()
        self._schedule_services.clear()

        logger.info("APIOrchestrationAdapter shutdown complete")
