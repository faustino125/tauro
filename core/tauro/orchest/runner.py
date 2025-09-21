from __future__ import annotations
from typing import Dict, Optional
from datetime import datetime, timezone
import threading

from loguru import logger  # type: ignore

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
            self.store.update_pipeline_run_state(
                pr.id, RunState.SUCCESS, finished_at=datetime.now(timezone.utc)
            )
            return RunState.SUCCESS
        except Exception as e:
            logger.exception(f"Run {run_id} failed: {e}")
            # Ensure finished_at is set even on failure
            self.store.update_pipeline_run_state(
                pr.id, RunState.FAILED, finished_at=datetime.now(timezone.utc)
            )
            return RunState.FAILED

    def get_run(self, run_id: str) -> Optional[PipelineRun]:
        return self.store.get_pipeline_run(run_id)

    def list_runs(
        self,
        pipeline_id: Optional[str] = None,
        state: Optional[RunState] = None,
        limit: int = 50,
    ):
        return self.store.list_pipeline_runs(
            pipeline_id=pipeline_id, state=state, limit=limit
        )

    def list_task_runs(self, run_id: str):
        return self.store.list_task_runs(run_id)
