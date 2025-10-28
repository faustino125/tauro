"""
Copyright (c) 2025 Faustino Lopez Ramos.
For licensing information, see the LICENSE file in the project root
"""
import logging
from typing import List, Optional, Dict, Any
from uuid import uuid4
from datetime import datetime, timezone
from motor.motor_asyncio import AsyncDatabase  # type: ignore

from tauro.api.schemas.models import (
    RunCreate,
    RunResponse,
    RunState,
    TaskState,
    RunProgress,
)
from tauro.api.db.models import PipelineRunDocument, TaskRunDocument
from tauro.api.schemas.project_validators import ProjectValidator


logger = logging.getLogger(__name__)


class RunNotFoundError(Exception):
    """Run not found"""

    pass


class RunStateError(Exception):
    """Error related to run state"""

    pass


class InvalidRunError(Exception):
    """Invalid run data"""

    pass


class RunService:
    """
    Pipeline run management service.
    """

    def __init__(self, db: AsyncDatabase):
        """
        Initialize the service with a MongoDB instance.

        Args:
            db: AsyncDatabase instance from Motor
        """
        self.db = db
        self.runs_collection = db["pipeline_runs"]
        self.tasks_collection = db["task_runs"]
        self.projects_collection = db["projects"]

    async def create_run(
        self,
        run_data: RunCreate,
        created_by: str,
    ) -> RunResponse:
        """
        Create a new run (pipeline execution).
        """
        try:
            # Validate that the project exists
            project = await self.projects_collection.find_one({"id": str(run_data.project_id)})
            if not project:
                raise InvalidRunError(f"Project {run_data.project_id} not found")

            # Validate that the pipeline exists in the project
            pipelines = project.get("pipelines", [])
            pipeline_found = any(str(p.get("id")) == str(run_data.pipeline_id) for p in pipelines)
            if not pipeline_found:
                raise InvalidRunError(f"Pipeline {run_data.pipeline_id} not found in project")

            # Create run document
            run_id = str(uuid4())
            now = datetime.now(timezone.utc)

            run_doc: PipelineRunDocument = {
                "id": run_id,
                "project_id": str(run_data.project_id),
                "pipeline_id": str(run_data.pipeline_id),
                "state": RunState.PENDING.value,
                "params": run_data.params or {},
                "priority": run_data.priority or "normal",
                "tags": run_data.tags or {},
                "progress": {
                    "total_tasks": 0,
                    "completed_tasks": 0,
                    "failed_tasks": 0,
                    "tasks_by_state": {},
                },
                "created_at": now,
                "started_at": None,
                "completed_at": None,
                "created_by": created_by,
                "error": None,
                "timeout_seconds": run_data.timeout_seconds,
            }

            # Insert into MongoDB
            await self.runs_collection.insert_one(run_doc)
            logger.info(f"Run created: {run_id} for pipeline {run_data.pipeline_id}")

            # Remove MongoDB _id
            run_doc.pop("_id", None)
            return RunResponse(**run_doc)

        except InvalidRunError:
            raise
        except Exception as e:
            logger.error(f"Error creating run: {str(e)}")
            raise InvalidRunError(f"Error creating run: {str(e)}")

    async def get_run(self, run_id: str) -> RunResponse:
        """
        Get a run by ID.
        """
        run_doc = await self.runs_collection.find_one({"id": run_id})

        if not run_doc:
            raise RunNotFoundError(f"Run {run_id} not found")

        run_doc.pop("_id", None)
        return RunResponse(**run_doc)

    async def list_runs(
        self,
        project_id: Optional[str] = None,
        pipeline_id: Optional[str] = None,
        state: Optional[str] = None,
        limit: int = 50,
        offset: int = 0,
    ) -> tuple[List[RunResponse], int]:
        """
        List runs with optional filters.
        """
        # Build filter
        query = {}

        if project_id:
            query["project_id"] = project_id

        if pipeline_id:
            query["pipeline_id"] = pipeline_id

        if state:
            query["state"] = state

        # Count total
        total = await self.runs_collection.count_documents(query)

        # Get page sorted by most recent first
        cursor = self.runs_collection.find(query).sort("created_at", -1).skip(offset).limit(limit)

        runs = []
        async for doc in cursor:
            doc.pop("_id", None)
            runs.append(RunResponse(**doc))

        logger.debug(
            f"Listed {len(runs)} runs with filters: "
            f"project_id={project_id}, pipeline_id={pipeline_id}, state={state}"
        )

        return runs, total

    async def update_run_state(
        self,
        run_id: str,
        new_state: str,
        error: Optional[str] = None,
    ) -> RunResponse:
        """
        Update the state of a run.
        """
        # Get current run
        run = await self.get_run(run_id)

        # Validate state transition
        valid_transitions = {
            RunState.PENDING.value: [
                RunState.RUNNING.value,
                RunState.CANCELLED.value,
            ],
            RunState.RUNNING.value: [
                RunState.SUCCESS.value,
                RunState.FAILED.value,
                RunState.CANCELLED.value,
            ],
            RunState.SUCCESS.value: [],
            RunState.FAILED.value: [],
            RunState.CANCELLED.value: [],
        }

        if new_state not in valid_transitions.get(run.state, []) and new_state != run.state:
            raise RunStateError(f"Invalid state transition from {run.state} to {new_state}")

        # Prepare update
        update_dict = {"state": new_state}

        # Add timestamps based on state
        now = datetime.now(timezone.utc)
        if new_state == RunState.RUNNING.value and not run.started_at:
            update_dict["started_at"] = now

        if new_state in [
            RunState.SUCCESS.value,
            RunState.FAILED.value,
            RunState.CANCELLED.value,
        ]:
            update_dict["completed_at"] = now

        # Add error if applicable
        if error:
            update_dict["error"] = error

        # Update in MongoDB
        result = await self.runs_collection.update_one(
            {"id": run_id},
            {"$set": update_dict},
        )

        if result.matched_count == 0:
            raise RunNotFoundError(f"Run {run_id} not found")

        logger.info(f"Run {run_id} transitioned to state {new_state}. " f"Error: {error or 'None'}")

        return await self.get_run(run_id)

    async def cancel_run(self, run_id: str, reason: str = "") -> RunResponse:
        """
        Cancel a run.
        """
        run_data = await self.get_run(run_id)

        # Can only cancel if in PENDING or RUNNING
        if run_data.state not in [RunState.PENDING.value, RunState.RUNNING.value]:
            raise RunStateError(f"Cannot cancel a run in state {run_data.state}")

        error_msg = f"Cancelled: {reason}" if reason else "Cancelled by user"
        return await self.update_run_state(
            run_id,
            RunState.CANCELLED.value,
            error=error_msg,
        )

    async def get_run_progress(self, run_id: str) -> RunProgress:
        """
        Get the current progress of a run.
        """
        await self.get_run(run_id)

        # Count tasks by state
        tasks = []
        cursor = self.tasks_collection.find({"run_id": run_id})
        async for task in cursor:
            tasks.append(task)

        tasks_by_state = {}
        for task in tasks:
            state = task.get("state")
            tasks_by_state[state] = tasks_by_state.get(state, 0) + 1

        # Calculate progress
        total_tasks = len(tasks)
        completed_tasks = sum(
            1 for t in tasks if t.get("state") in [TaskState.SUCCESS.value, TaskState.FAILED.value]
        )
        failed_tasks = sum(1 for t in tasks if t.get("state") == TaskState.FAILED.value)

        return RunProgress(
            total_tasks=total_tasks,
            completed_tasks=completed_tasks,
            failed_tasks=failed_tasks,
            tasks_by_state=tasks_by_state,
        )

    async def add_task(
        self,
        run_id: str,
        task_name: str,
        node_id: str,
    ) -> str:
        """
        Add a task to a run.
        """
        # Verify that the run exists
        run = await self.get_run(run_id)

        # Create task document
        task_id = str(uuid4())
        task_doc: TaskRunDocument = {
            "id": task_id,
            "run_id": run_id,
            "project_id": run.project_id,
            "task_name": task_name,
            "node_id": node_id,
            "state": TaskState.PENDING.value,
            "created_at": datetime.now(timezone.utc),
            "started_at": None,
            "completed_at": None,
            "error": None,
            "output": None,
        }

        # Insert into MongoDB
        await self.tasks_collection.insert_one(task_doc)
        logger.info(f"Task {task_id} added to run {run_id}")

        return task_id

    async def update_task_state(
        self,
        task_id: str,
        new_state: str,
        error: Optional[str] = None,
        output: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Update the state of a task.
        """
        update_dict = {"state": new_state}

        now = datetime.now(timezone.utc)
        if new_state == TaskState.RUNNING.value:
            update_dict["started_at"] = now

        if new_state in [TaskState.SUCCESS.value, TaskState.FAILED.value]:
            update_dict["completed_at"] = now

        if error:
            update_dict["error"] = error

        if output:
            update_dict["output"] = output

        await self.tasks_collection.update_one(
            {"id": task_id},
            {"$set": update_dict},
        )

        logger.info(f"Task {task_id} updated to state {new_state}")

        # Return updated task
        task = await self.tasks_collection.find_one({"id": task_id})
        task.pop("_id", None)
        return task

    async def get_run_tasks(self, run_id: str) -> List[Dict[str, Any]]:
        """
        Get all tasks for a run.
        """
        # Verify that the run exists
        await self.get_run(run_id)

        tasks = []
        cursor = self.tasks_collection.find({"run_id": run_id}).sort("created_at", 1)
        async for task in cursor:
            task.pop("_id", None)
            tasks.append(task)

        return tasks

    # =========================================================================
    # START AND CANCEL RUN - CRITICAL NEW METHODS
    # =========================================================================

    async def start_run(
        self,
        run_id: str,
        timeout_seconds: Optional[int] = None,
    ) -> RunResponse:
        """
        Start the execution of a run that is in PENDING state.
        """
        try:
            logger.info(f"Starting execution of run {run_id}")

            # 1. VALIDATE: Run exists
            run = await self.runs_collection.find_one({"id": run_id})
            if not run:
                raise RunNotFoundError(f"Run {run_id} not found")

            # 2. VALIDATE: State is PENDING
            current_state = run.get("state", RunState.PENDING.value)
            if current_state != RunState.PENDING.value:
                raise RunStateError(
                    f"Run {run_id} is not in PENDING (state: {current_state}). "
                    f"Only runs in PENDING can be started."
                )

            # 3. UPDATE timeout if provided
            if timeout_seconds:
                await self.runs_collection.update_one(
                    {"id": run_id}, {"$set": {"timeout_seconds": timeout_seconds}}
                )

            # NOTE: Actual execution (changing to RUNNING, etc.) is done by ExecutionService
            # This method simply validates and returns the updated run
            # so the caller (route) knows it can proceed

            # 4. RETURN UPDATED RUN
            run = await self.runs_collection.find_one({"id": run_id})
            run.pop("_id", None)

            return RunResponse(**run)

        except (RunNotFoundError, RunStateError, InvalidRunError):
            raise
        except Exception as e:
            logger.error(f"Error starting run {run_id}: {e}", exc_info=True)
            raise InvalidRunError(f"Error starting run: {str(e)}")

    async def cancel_run(
        self,
        run_id: str,
        reason: str = "User cancelled",
    ) -> RunResponse:
        """
        Cancel a run that is in execution.
        """
        try:
            logger.info(f"Cancelling run {run_id}: {reason}")

            # 1. VALIDATE: Run exists
            run = await self.runs_collection.find_one({"id": run_id})
            if not run:
                raise RunNotFoundError(f"Run {run_id} not found")

            # NOTE: Actual cancellation (changing state, etc.) is done by ExecutionService
            # This method simply validates and returns the run

            # 2. RETURN UPDATED RUN
            run = await self.runs_collection.find_one({"id": run_id})
            run.pop("_id", None)

            return RunResponse(**run)

        except RunNotFoundError:
            raise
        except Exception as e:
            logger.error(f"Error cancelling run {run_id}: {e}", exc_info=True)
            raise InvalidRunError(f"Error cancelling run: {str(e)}")
