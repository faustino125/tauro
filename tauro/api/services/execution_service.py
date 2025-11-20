"""
Copyright (c) 2025 Faustino Lopez Ramos.
For licensing information, see the LICENSE file in the project root
"""
import logging
import asyncio
from typing import Optional, Dict, Any
from datetime import datetime, timezone
from motor.motor_asyncio import AsyncIOMotorDatabase  # type: ignore

from tauro.api.schemas.models import RunCreate, RunState


logger = logging.getLogger(__name__)


class ExecutionNotFoundError(Exception):
    """Execution not found"""

    pass


class ExecutionAlreadyRunningError(Exception):
    """An execution is already in progress"""

    pass


class InvalidExecutionError(Exception):
    """Invalid execution parameters"""

    pass


class ExecutionService:
    """
    Centralized orchestration service.
    """

    def __init__(self, db: AsyncIOMotorDatabase):
        """
        Initialize the service with a MongoDB instance.

        Args:
            db: AsyncIOMotorDatabase instance from Motor
        """
        self.db = db
        self.runs_collection = db["pipeline_runs"]
        self.tasks_collection = db["task_runs"]
        self.projects_collection = db["projects"]
        self.schedules_collection = db["schedules"]

        # Note: OrchestratorRunner will be injected at deployment time
        self.orchestrator_runner = None

        # Dictionary to track active async tasks
        self._active_tasks: Dict[str, asyncio.Task] = {}

    def set_orchestrator_runner(self, orchestrator_runner):
        """
        Inject the OrchestratorRunner instance.
        """
        self.orchestrator_runner = orchestrator_runner
        logger.info("OrchestratorRunner injected into ExecutionService")

    async def submit_execution(
        self,
        run_id: str,
        timeout_seconds: Optional[int] = None,
    ) -> Dict[str, Any]:
        """
        Submit a run for centralized execution.
        """
        logger.info(f"Submit execution for run {run_id}")

        try:
            # 1. VALIDATE: Run exists
            run = await self.runs_collection.find_one({"id": run_id})
            if not run:
                raise ExecutionNotFoundError(f"Run {run_id} not found")

            # 2. VALIDATE: Not already executing
            if run.get("state") == RunState.RUNNING.value:
                raise ExecutionAlreadyRunningError(f"Run {run_id} already executing")

            # 3. VALIDATE: In PENDING state
            if run.get("state") != RunState.PENDING.value:
                raise InvalidExecutionError(
                    f"Cannot execute a run in state {run.get('state')}. "
                    "Only runs in PENDING can be executed."
                )

            # 4. CHANGE STATE: PENDING → RUNNING
            now = datetime.now(timezone.utc)
            await self.runs_collection.update_one(
                {"id": run_id},
                {
                    "$set": {
                        "state": RunState.RUNNING.value,
                        "started_at": now,
                        "progress": {
                            "total_tasks": 0,
                            "completed_tasks": 0,
                            "failed_tasks": 0,
                            "tasks_by_state": {},
                        },
                    }
                },
            )
            logger.info(f"Run {run_id} changed to RUNNING")

            # 5. START ASYNC TASK (non-blocking HTTP response)
            task = asyncio.create_task(self._execute_run_async(run_id, timeout_seconds))
            self._active_tasks[run_id] = task
            logger.info(f"Async task created for run {run_id}")

            # 6. RETURN 202 ACCEPTED immediately
            return {
                "status": "accepted",
                "run_id": run_id,
                "status_url": f"/api/v1/runs/{run_id}",
                "timestamp": now.isoformat(),
                "message": "Pipeline execution started in background",
            }

        except (
            ExecutionNotFoundError,
            ExecutionAlreadyRunningError,
            InvalidExecutionError,
        ):
            raise
        except Exception as e:
            logger.error(f"Error submitting execution for run {run_id}: {e}", exc_info=True)
            raise InvalidExecutionError(f"Error submitting execution: {str(e)}")

    # =========================================================================
    # ASYNC EXECUTION (BACKGROUND)
    # =========================================================================

    async def _execute_run_async(self, run_id: str, timeout_seconds: Optional[int] = None) -> None:
        """
        Execute the run asynchronously in background.
        """
        logger.info(f"Starting async execution of run {run_id}")

        try:
            # Cargar y validar
            run, project = await self._load_run_and_project(run_id)

            # Construir contexto
            context = self._build_execution_context(project)
            pipeline_id = run.get("pipeline_id")
            params = run.get("params", {})

            logger.info(
                f"Context built for pipeline {pipeline_id}, params={params}, "
                f"timeout={timeout_seconds}s"
            )

            # Execute pipeline (internal handles timeout and simulation)
            result = await self._invoke_orchestrator(
                context, pipeline_id, params, run_id, timeout_seconds
            )

            # Mark success
            logger.info(f"Pipeline {pipeline_id} completed successfully")
            completion_time = datetime.now(timezone.utc)
            await self.runs_collection.update_one(
                {"id": run_id},
                {
                    "$set": {
                        "state": RunState.SUCCESS.value,
                        "completed_at": completion_time,
                        "progress": result.get("progress", {}),
                        "error": None,
                    }
                },
            )
            logger.info(f"Run {run_id} completed with SUCCESS")

        except asyncio.TimeoutError:
            logger.error(f"Run {run_id} timeout after {timeout_seconds}s")
            await self._handle_timeout(run_id, timeout_seconds)

        except ExecutionNotFoundError as e:
            logger.error(f"Validation error in run {run_id}: {e}")
            await self._mark_run_failed(run_id, str(e))

        except Exception as e:
            logger.error(f"Error executing run {run_id}: {e}", exc_info=True)
            await self._mark_run_failed(run_id, str(e))

        finally:
            # Clean up task from active list
            if run_id in self._active_tasks:
                del self._active_tasks[run_id]
            logger.info(f"Task cleaned up for run {run_id}")

    async def _load_run_and_project(self, rid: str):
        run_doc = await self.runs_collection.find_one({"id": rid})
        if not run_doc:
            raise ExecutionNotFoundError(f"Run {rid} disappeared")

        project_doc = await self.projects_collection.find_one({"id": run_doc.get("project_id")})
        if not project_doc:
            raise InvalidExecutionError(f"Project {run_doc.get('project_id')} not found")
        return run_doc, project_doc

    async def _invoke_orchestrator(
        self, ctx, pid, prms, rid: str, timeout_seconds: Optional[int] = None
    ):
        if not self.orchestrator_runner:
            logger.warning(f"OrchestratorRunner not available, simulating execution for {rid}")
            await asyncio.sleep(1)
            return {
                "progress": {
                    "total_tasks": 1,
                    "completed_tasks": 1,
                    "failed_tasks": 0,
                }
            }

        # Execute in OrchestratorRunner with/without timeout
        if timeout_seconds:
            return await asyncio.wait_for(
                self.orchestrator_runner.execute_async(
                    context=ctx, pipeline_id=pid, params=prms, run_id=rid
                ),
                timeout=timeout_seconds,
            )
        return await self.orchestrator_runner.execute_async(
            context=ctx, pipeline_id=pid, params=prms, run_id=rid
        )

    async def _handle_timeout(self, run_id: str, timeout_seconds: Optional[int]):
        # Try to cancel operations in OrchestratorRunner
        if self.orchestrator_runner and hasattr(self.orchestrator_runner, "cancel_execution"):
            try:
                await self.orchestrator_runner.cancel_execution(run_id)
                logger.info(f"Cancelled in OrchestratorRunner due to timeout: {run_id}")
            except Exception as e:
                logger.warning(f"Error cancelling in OrchestratorRunner due to timeout: {e}")

        await self._mark_run_failed(run_id, f"Execution timeout after {timeout_seconds} seconds")

    # =========================================================================
    # GET EXECUTION STATUS
    # =========================================================================

    async def get_execution_status(
        self,
        run_id: str,
    ) -> Dict[str, Any]:
        """
        Get the current status of an execution.
        """
        run = await self.runs_collection.find_one({"id": run_id})

        if not run:
            raise ExecutionNotFoundError(f"Run {run_id} not found")

        # Remover _id de MongoDB
        run.pop("_id", None)

        return {
            "run_id": run_id,
            "state": run.get("state", RunState.PENDING.value),
            "progress": run.get("progress", {}),
            "started_at": run.get("started_at"),
            "completed_at": run.get("completed_at"),
            "error": run.get("error"),
            "timeout_seconds": run.get("timeout_seconds"),
        }

    # =========================================================================
    # CANCEL EXECUTION
    # =========================================================================

    async def cancel_execution(self, run_id: str, reason: str = "User cancelled") -> bool:
        """
        Cancel an execution in progress.
        """
        logger.info(f"Cancelling execution run {run_id}: {reason}")

        run = await self.runs_collection.find_one({"id": run_id})
        if not run:
            raise ExecutionNotFoundError(f"Run {run_id} not found")

        current_state = run.get("state")

        # Only cancel if in RUNNING or PENDING
        if current_state not in [RunState.RUNNING.value, RunState.PENDING.value]:
            logger.warning(
                f"Cannot cancel run {run_id}: state is {current_state}, not RUNNING/PENDING"
            )
            return False

        # Change state to CANCELLED
        now = datetime.now(timezone.utc)
        await self.runs_collection.update_one(
            {"id": run_id},
            {
                "$set": {
                    "state": RunState.CANCELLED.value,
                    "completed_at": now,
                    "error": reason,
                }
            },
        )

        # Delegate signals and task cancellation to simplified helpers
        await self._signal_orchestrator_cancel(run_id)
        await self._cancel_active_task(run_id)

        logger.info(f"Run {run_id} marked as CANCELLED")
        return True

    async def _signal_orchestrator_cancel(self, run_id: str) -> None:
        """
        Try to notify the OrchestratorRunner to cancel execution.
        Silences errors and logs warnings if fails.
        """
        if not self.orchestrator_runner or not hasattr(
            self.orchestrator_runner, "cancel_execution"
        ):
            return

        try:
            await self.orchestrator_runner.cancel_execution(run_id)
            logger.info(f"Cancel request forwarded to OrchestratorRunner: {run_id}")
        except Exception as e:
            logger.warning(f"Error cancelling in OrchestratorRunner: {e}")

    async def _cancel_active_task(self, run_id: str) -> None:
        """
        Cancel the asyncio.Task associated with the run (if exists) and wait for brief timeout.
        """
        task = self._active_tasks.get(run_id)
        if not task:
            return

        if task.done():
            logger.debug(f"Task already completed for run {run_id}")
            return

        task.cancel()
        try:
            await asyncio.wait_for(task, timeout=5.0)
            logger.info(f"Async task successfully cancelled: {run_id}")
        except asyncio.TimeoutError:
            logger.warning(f"Task cancelled but did not finish in timeout: {run_id}")
        except asyncio.CancelledError:
            logger.info(f"Task was already cancelled: {run_id}")
            raise
        except Exception as e:
            logger.warning(f"Error waiting for task cancellation: {e}")

    # =========================================================================
    # HELPER METHODS
    # =========================================================================

    def _build_execution_context(self, project: Dict[str, Any]) -> Any:
        """
        Build a Tauro context for execution.
        """
        logger.debug(f"Building context for project {project.get('id')}")

        try:
            from tauro.core.config.contexts import Context

            # Create context from project settings
            settings = project.get("global_settings", {})

            context = Context(
                input_path=settings.get("input_path", "/tmp/input"),
                output_path=settings.get("output_path", "/tmp/output"),
                mode=settings.get("mode", "batch"),
            )

            return context

        except ImportError as e:
            logger.warning(f"Tauro Context not available: {e}, using simulated")
            # Return simulated dict if Tauro not available
            return {
                "input_path": project.get("global_settings", {}).get("input_path"),
                "output_path": project.get("global_settings", {}).get("output_path"),
            }

    async def _mark_run_failed(self, run_id: str, error_message: str) -> None:
        """
        Mark a run as FAILED with error message.
        """
        await self.runs_collection.update_one(
            {"id": run_id},
            {
                "$set": {
                    "state": RunState.FAILED.value,
                    "completed_at": datetime.now(timezone.utc),
                    "error": error_message,
                }
            },
        )
        logger.info(f"Run {run_id} marked as FAILED: {error_message}")

    async def check_execution_health(self) -> Dict[str, Any]:
        """
        Check the health of the execution service.
        """
        try:
            # Contar runs por estado
            running = await self.runs_collection.count_documents({"state": RunState.RUNNING.value})
            pending = await self.runs_collection.count_documents({"state": RunState.PENDING.value})
            failed = await self.runs_collection.count_documents({"state": RunState.FAILED.value})

            orchestrator_status = "available" if self.orchestrator_runner else "unavailable"

            return {
                "status": "healthy",
                "orchestrator": orchestrator_status,
                "active_runs": {
                    "running": running,
                    "pending": pending,
                },
                "recent_failures": failed,
                "active_tasks": len(self._active_tasks),
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }

        except Exception as e:
            logger.error(f"Error checking health: {e}", exc_info=True)
            return {
                "status": "unhealthy",
                "error": str(e),
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }
