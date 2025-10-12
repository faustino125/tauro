from __future__ import annotations
from typing import Dict, Optional, List
from datetime import datetime, timezone
import threading
import time

from loguru import logger  # type: ignore
from fastapi import APIRouter, Request, HTTPException
from starlette.concurrency import run_in_threadpool
from pydantic import BaseModel
from typing import Any, Dict
import asyncio

from tauro.config.contexts import Context
from .models import PipelineRun, TaskRun, RunState
from .store import OrchestratorStore
from .executor.local import LocalDagExecutor


class OrchestratorRunner:
    """
    - Crea y ejecuta PipelineRuns
    - Persiste estados de PipelineRun/TaskRun
    - Ejecuta DAGs por nodos usando LocalDagExecutor
    """

    def __init__(self, context: Context, store: Optional[OrchestratorStore] = None):
        self.context = context
        self.store = store or OrchestratorStore()

    def create_run(
        self, pipeline_id: str, params: Optional[Dict] = None
    ) -> PipelineRun:
        pr = self.store.create_pipeline_run(pipeline_id, params or {})
        logger.info(f"Created PipelineRun {pr.id} for pipeline '{pipeline_id}'")
        return pr

    def start_run(
        self,
        run_id: str,
        retries: int = 0,
        retry_delay_sec: int = 0,
        concurrency: Optional[int] = None,
        timeout_seconds: Optional[int] = None,
    ) -> RunState:
        pr = self.store.get_pipeline_run(run_id)
        if not pr:
            raise ValueError(f"PipelineRun '{run_id}' not found")

        if pr.state not in (RunState.PENDING, RunState.QUEUED, RunState.FAILED):
            logger.warning(f"Run {run_id} in state {pr.state}, cannot start")
            return pr.state

        self.store.update_pipeline_run_state(
            pr.id, RunState.RUNNING, started_at=datetime.now(timezone.utc)
        )

        start_date = (pr.params or {}).get("start_date")
        end_date = (pr.params or {}).get("end_date")

        persisted_tasks: Dict[str, str] = {}
        persisted_tasks_lock = threading.Lock()

        def on_task_state(tr: TaskRun):
            # on_task_state can be called concurrently from worker threads -> protect shared dict
            try:
                with persisted_tasks_lock:
                    if tr.task_id not in persisted_tasks:
                        stored_tr = self.store.create_task_run(pr.id, tr.task_id)
                        tr.id = stored_tr.id
                        persisted_tasks[tr.task_id] = tr.id

                    self.store.update_task_run_state(
                        persisted_tasks[tr.task_id],
                        tr.state,
                        try_number=tr.try_number,
                        started_at=tr.started_at,
                        finished_at=tr.finished_at,
                        error=tr.error,
                        log_uri=tr.log_uri,
                    )
            except Exception:
                logger.exception("Failed to persist task state")

        ex = LocalDagExecutor(self.context)
        start_time = time.time()
        try:
            ex.execute(
                pr.pipeline_id,
                start_date=start_date,
                end_date=end_date,
                on_task_state=on_task_state,
                retries=retries,
                retry_delay_sec=retry_delay_sec,
                concurrency=concurrency,
                timeout_seconds=timeout_seconds,
            )

            execution_time = time.time() - start_time
            logger.info(
                f"Run {run_id} completed successfully in {execution_time:.2f} seconds"
            )

            self.store.update_pipeline_run_state(
                pr.id, RunState.SUCCESS, finished_at=datetime.now(timezone.utc)
            )
            return RunState.SUCCESS
        except Exception as e:
            execution_time = time.time() - start_time
            logger.exception(
                f"Run {run_id} failed after {execution_time:.2f} seconds: {e}"
            )
            # Ensure finished_at is set even on failure
            self.store.update_pipeline_run_state(
                pr.id,
                RunState.FAILED,
                finished_at=datetime.now(timezone.utc),
                error=str(e),
            )
            return RunState.FAILED

    def get_run(self, run_id: str) -> Optional[PipelineRun]:
        return self.store.get_pipeline_run(run_id)

    def list_runs(
        self,
        pipeline_id: Optional[str] = None,
        state: Optional[RunState] = None,
        limit: int = 50,
        created_after: Optional[datetime] = None,
        created_before: Optional[datetime] = None,
    ) -> List[PipelineRun]:
        return self.store.list_pipeline_runs(
            pipeline_id=pipeline_id,
            state=state,
            limit=limit,
            created_after=created_after,
            created_before=created_before,
        )

    def list_task_runs(
        self, run_id: str, state: Optional[RunState] = None
    ) -> List[TaskRun]:
        return self.store.list_task_runs(run_id, state=state)

    def cancel_run(self, run_id: str) -> bool:
        """Cancelar una ejecución en curso"""
        pr = self.store.get_pipeline_run(run_id)
        if not pr:
            logger.warning(f"PipelineRun '{run_id}' not found")
            return False

        if pr.state.is_terminal():
            logger.warning(
                f"PipelineRun '{run_id}' is already in terminal state {pr.state}"
            )
            return False

        # Marcar la ejecución como cancelada
        self.store.update_pipeline_run_state(
            pr.id,
            RunState.CANCELLED,
            finished_at=datetime.now(timezone.utc),
            error="Run was manually cancelled",
        )

        # También marcar todas las tareas en ejecución como canceladas
        task_runs = self.store.list_task_runs(run_id)
        for task_run in task_runs:
            if not task_run.state.is_terminal():
                self.store.update_task_run_state(
                    task_run.id,
                    RunState.CANCELLED,
                    finished_at=datetime.now(timezone.utc),
                    error="Parent pipeline run was cancelled",
                )

        logger.info(f"Cancelled run {run_id}")
        return True


SERVICE_NOT_INITIALIZED = "Service not initialized"

router = APIRouter()


class RunCreate(BaseModel):
    pipeline_id: str
    params: Dict[str, Any] = {}


@router.post("/")
async def create_run(req: Request, body: RunCreate):
    """
    Crear y lanzar una ejecución usando pipeline_service.
    """
    pipeline_service = getattr(req.app.state, "pipeline_service", None)
    if pipeline_service is None:
        raise HTTPException(status_code=500, detail=SERVICE_NOT_INITIALIZED)

    maybe = pipeline_service.run_pipeline(body.pipeline_id, body.params)
    if asyncio.iscoroutine(maybe):
        run_id = await maybe
    else:
        # If the implementation is synchronous/blocking, run it in a threadpool
        run_id = await run_in_threadpool(
            pipeline_service.run_pipeline, body.pipeline_id, body.params
        )
    return {"run_id": run_id}


@router.get("/{run_id}")
async def get_run(req: Request, run_id: str):
    pipeline_service = getattr(req.app.state, "pipeline_service", None)
    if pipeline_service is None:
        raise HTTPException(status_code=500, detail=SERVICE_NOT_INITIALIZED)

    res = pipeline_service.get_run(run_id)
    if asyncio.iscoroutine(res):
        res = await res
    if not res:
        raise HTTPException(status_code=404, detail="Run not found")
    return res


@router.get("/")
async def list_runs(req: Request):
    pipeline_service = getattr(req.app.state, "pipeline_service", None)
    if pipeline_service is None:
        raise HTTPException(status_code=500, detail=SERVICE_NOT_INITIALIZED)

    res = pipeline_service.list_runs()
    if asyncio.iscoroutine(res):
        res = await res
    return res


@router.post("/{run_id}/cancel")
async def cancel_run(req: Request, run_id: str):
    pipeline_service = getattr(req.app.state, "pipeline_service", None)
    if pipeline_service is None:
        raise HTTPException(status_code=500, detail=SERVICE_NOT_INITIALIZED)

    maybe = pipeline_service.cancel_run(run_id)
    if asyncio.iscoroutine(maybe):
        cancelled = await maybe
    else:
        cancelled = await run_in_threadpool(pipeline_service.cancel_run, run_id)
    if not cancelled:
        raise HTTPException(status_code=404, detail="Run not found or cannot cancel")
    return {"cancelled": True}
