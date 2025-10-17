from typing import Any, Dict, List, Optional
import asyncio

from loguru import logger

from tauro.api.services.base import BaseService, ServiceConfig, ServicePriority


class PipelineService(BaseService):
    """Service for pipeline execution management."""

    def __init__(self, store, runner=None, config: Optional[ServiceConfig] = None):
        super().__init__(config, "PipelineService")
        self.store = store
        self.runner = runner

        # Active runs tracking
        self._active_runs: Dict[str, asyncio.Task] = {}
        self._runs_lock = asyncio.Lock()

        # Execution queues by priority
        self._execution_queues = {
            priority: asyncio.Queue() for priority in ServicePriority
        }

        # Worker tasks
        self._workers: List[asyncio.Task] = []

    async def _initialize(self) -> None:
        """Initialize pipeline service"""
        # Start worker tasks
        num_workers = self.config.max_concurrent_requests
        for i in range(num_workers):
            worker = asyncio.create_task(self._worker_loop(i))
            self._workers.append(worker)

        logger.info(f"Started {num_workers} pipeline workers")

    async def _cleanup(self) -> None:
        """Cleanup pipeline service"""
        # Cancel all workers
        for worker in self._workers:
            worker.cancel()

        # Wait for workers to finish
        if self._workers:
            await asyncio.gather(*self._workers, return_exceptions=True)

        # Cancel active runs
        async with self._runs_lock:
            for run_id, task in self._active_runs.items():
                if not task.done():
                    task.cancel()
                    logger.info(f"Cancelled run {run_id} during shutdown")

    async def run_pipeline(
        self,
        pipeline_id: str,
        params: Dict[str, Any],
        priority: ServicePriority = ServicePriority.NORMAL,
    ) -> str:
        """
        Execute pipeline with priority.

        Args:
            pipeline_id: Pipeline identifier
            params: Pipeline parameters
            priority: Execution priority

        Returns:
            Run ID
        """
        async with self._acquire_slot(priority):
            try:
                # Create run record
                run = self.store.create_run(pipeline_id, params)
                run_id = run.id

                # Queue for execution
                await self._execution_queues[priority].put(
                    {
                        "run_id": run_id,
                        "pipeline_id": pipeline_id,
                        "params": params,
                        "priority": priority,
                    }
                )

                async with self._lock:
                    self.metrics.successful_requests += 1

                logger.info(
                    f"Queued pipeline run {run_id} "
                    f"(pipeline={pipeline_id}, priority={priority.name})"
                )

                return run_id

            except Exception as e:
                async with self._lock:
                    self.metrics.failed_requests += 1
                logger.exception(f"Failed to queue pipeline {pipeline_id}: {e}")
                raise

    async def cancel_run(self, run_id: str) -> bool:
        """
        Cancel an active pipeline run.

        Args:
            run_id: Run identifier

        Returns:
            True if cancelled, False if run not found
        """
        async with self._acquire_slot(ServicePriority.HIGH):
            try:
                async with self._runs_lock:
                    task = self._active_runs.get(run_id)

                    if task and not task.done():
                        task.cancel()
                        logger.info(f"Cancelled run {run_id}")

                        # Update store
                        if hasattr(self.store, "update_run_status"):
                            self.store.update_run_status(run_id, "cancelled")

                        return True

                logger.warning(f"Run {run_id} not found or already completed")
                return False

            except Exception as e:
                logger.exception(f"Failed to cancel run {run_id}: {e}")
                raise

    async def get_run(self, run_id: str) -> Optional[Dict[str, Any]]:
        """Get run information"""
        try:
            run = self.store.get_run(run_id)
            if run:
                return {
                    "id": run.id,
                    "pipeline_id": run.pipeline_id,
                    "status": run.status,
                    "created_at": run.created_at.isoformat()
                    if hasattr(run.created_at, "isoformat")
                    else str(run.created_at),
                    "started_at": run.started_at.isoformat()
                    if run.started_at and hasattr(run.started_at, "isoformat")
                    else None,
                    "completed_at": run.completed_at.isoformat()
                    if run.completed_at and hasattr(run.completed_at, "isoformat")
                    else None,
                    "result": run.result,
                    "error": run.error,
                }
            return None
        except Exception as e:
            logger.exception(f"Failed to get run {run_id}: {e}")
            return None

    async def list_runs(
        self,
        pipeline_id: Optional[str] = None,
        status: Optional[str] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> List[Dict[str, Any]]:
        """List pipeline runs with filters"""
        try:
            runs = self.store.list_runs(limit=limit, offset=offset)

            # Apply filters
            if pipeline_id:
                runs = [r for r in runs if r.pipeline_id == pipeline_id]
            if status:
                runs = [r for r in runs if r.status == status]

            return [
                {
                    "id": r.id,
                    "pipeline_id": r.pipeline_id,
                    "status": r.status,
                    "created_at": r.created_at.isoformat()
                    if hasattr(r.created_at, "isoformat")
                    else str(r.created_at),
                }
                for r in runs
            ]
        except Exception as e:
            logger.exception(f"Failed to list runs: {e}")
            return []

    async def _worker_loop(self, worker_id: int) -> None:
        """
        Worker loop for processing queued pipeline executions.
        Processes highest priority work first.
        """
        logger.debug(f"Worker {worker_id} started")

        while self.state.value == "running":
            try:
                work_item, queue_priority = self._get_next_work_item()

                if work_item:
                    await self._process_work_item(work_item, worker_id)
                    # Mark task done
                    if queue_priority:
                        self._execution_queues[queue_priority].task_done()
                else:
                    # No work available, wait
                    await asyncio.sleep(0.1)

            except asyncio.CancelledError:
                logger.debug(f"Worker {worker_id} cancelled")
                raise  # Re-raise to properly propagate cancellation
            except Exception as e:
                logger.exception(f"Worker {worker_id} error: {e}")
                await asyncio.sleep(1)  # Back off on errors

        logger.debug(f"Worker {worker_id} stopped")

    def _get_next_work_item(
        self,
    ) -> tuple[Optional[Dict[str, Any]], Optional[ServicePriority]]:
        """
        Get next work item from highest priority queue.

        Returns:
            Tuple of (work_item, queue_priority) or (None, None) if no work available
        """
        for priority in ServicePriority:
            try:
                work_item = self._execution_queues[priority].get_nowait()
                return work_item, priority
            except asyncio.QueueEmpty:
                continue

        return None, None

    async def _process_work_item(
        self, work_item: Dict[str, Any], worker_id: int
    ) -> None:
        """Process a queued pipeline execution"""
        run_id = work_item["run_id"]
        pipeline_id = work_item["pipeline_id"]
        params = work_item["params"]
        priority = work_item.get("priority", ServicePriority.NORMAL)

        logger.info(
            f"Worker {worker_id} executing run {run_id} "
            f"(pipeline={pipeline_id}, priority={priority.name})"
        )

        # Create task for this run
        task = asyncio.create_task(self._execute_pipeline(run_id, pipeline_id, params))

        # Track active run
        async with self._runs_lock:
            self._active_runs[run_id] = task

        try:
            await task
        except asyncio.CancelledError:
            logger.info(f"Run {run_id} was cancelled")
            # Re-raise to propagate cancellation after cleanup in finally block
            raise
        except Exception as e:
            logger.exception(f"Run {run_id} failed: {e}")
        finally:
            # Remove from active runs
            async with self._runs_lock:
                self._active_runs.pop(run_id, None)

    async def _execute_pipeline(
        self, run_id: str, pipeline_id: str, params: Dict[str, Any]
    ) -> None:
        """Execute pipeline and update status"""
        try:
            # Update status to running
            if hasattr(self.store, "update_run_status"):
                self.store.update_run_status(run_id, "running")

            # Execute pipeline
            if self.runner and hasattr(self.runner, "run_pipeline"):
                result = await self._maybe_await(
                    self.runner.run_pipeline(pipeline_id, params)
                )
            else:
                # Fallback execution
                result = await self._execute_pipeline_fallback(pipeline_id, params)

            # Update with result
            if hasattr(self.store, "update_run_result"):
                self.store.update_run_result(run_id, result, "success")

            logger.info(f"Run {run_id} completed successfully")

        except asyncio.CancelledError:
            # Update status to cancelled
            if hasattr(self.store, "update_run_status"):
                self.store.update_run_status(run_id, "cancelled")
            raise

        except Exception as e:
            # Update with error
            if hasattr(self.store, "update_run_error"):
                self.store.update_run_error(run_id, str(e))
            logger.exception(f"Run {run_id} failed: {e}")
            raise

    async def _execute_pipeline_fallback(
        self, pipeline_id: str, params: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Fallback pipeline execution (placeholder)"""
        logger.warning(
            f"Using fallback execution for {pipeline_id} with params: {params}"
        )
        await asyncio.sleep(1)  # Simulate work
        return {
            "status": "completed",
            "pipeline_id": pipeline_id,
            "params": params,
            "message": "Executed with fallback (no runner available)",
        }

    async def _maybe_await(self, value):
        """Await value if it's a coroutine"""
        if asyncio.iscoroutine(value):
            return await value
        return value

    async def _perform_health_check(self) -> Dict[str, Any]:
        """Pipeline service health check"""
        base_health = await super()._perform_health_check()

        async with self._runs_lock:
            base_health.update(
                {
                    "active_runs": len(self._active_runs),
                    "queued_runs": sum(
                        q.qsize() for q in self._execution_queues.values()
                    ),
                }
            )

        return base_health
