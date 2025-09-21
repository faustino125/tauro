from __future__ import annotations
from typing import Dict, List, Callable, Optional, Set, Any
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, Future, wait, FIRST_EXCEPTION
from collections import defaultdict, deque
import threading

from loguru import logger  # type: ignore

from tauro.config.contexts import Context
from tauro.exec.dependency_resolver import DependencyResolver
from tauro.exec.pipeline_validator import PipelineValidator
from tauro.exec.node_executor import NodeExecutor
from tauro.io.input import InputLoader
from tauro.io.output import OutputManager

from ..models import TaskRun, RunState


class LocalDagExecutor:
    """
    Ejecuta un DAG batch por nodos, con estados de TaskRun y concurrencia local.
    Integra con NodeExecutor existente para ejecutar cada nodo.
    """

    def __init__(self, context: Context, max_workers: Optional[int] = None):
        self.context = context
        gs = getattr(self.context, "global_settings", {}) or {}
        self.max_workers = max_workers or gs.get("max_parallel_nodes", 4)

        self.input_loader = InputLoader(self.context)
        self.output_manager = OutputManager(self.context)
        self.node_executor = NodeExecutor(
            self.context, self.input_loader, self.output_manager, self.max_workers
        )
        # Event to signal a global stop when a failure occurs
        self._stop_event = threading.Event()

    def _get_pipeline_nodes_and_configs(
        self, pipeline_name: str
    ) -> tuple[List[str], Dict[str, Dict]]:
        pipeline = self.context.pipelines.get(pipeline_name)
        if not pipeline:
            raise ValueError(f"Pipeline '{pipeline_name}' not found")
        pipeline_nodes = []
        nodes_config = self.context.nodes_config or {}

        maybe_nodes = []
        if isinstance(pipeline, dict):
            maybe_nodes = list(
                {n for n in (pipeline.get("nodes") or []) if isinstance(n, str)}
            )
        pipeline_nodes = maybe_nodes

        node_cfgs = {n: nodes_config[n] for n in pipeline_nodes if n in nodes_config}
        if not node_cfgs:
            raise ValueError(f"No node configs found for pipeline '{pipeline_name}'")

        PipelineValidator.validate_node_configs(pipeline_nodes, node_cfgs)
        return pipeline_nodes, node_cfgs

    def _submit_task(
        self,
        pool: ThreadPoolExecutor,
        node_name: str,
        task_runs: Dict[str, TaskRun],
        running: Dict[str, Future],
        on_task_state: Callable[[TaskRun], None],
        start_date: Optional[str],
        end_date: Optional[str],
        retries: int,
        retry_delay_sec: int,
        timeout_seconds: Optional[int] = None,
    ):
        tr = TaskRun(task_id=node_name)
        tr.state = RunState.RUNNING
        tr.try_number = 1
        tr.started_at = datetime.now(timezone.utc)
        task_runs[node_name] = tr
        on_task_state(tr)

        def _work():
            # If another node failure flagged global stop, don't start work
            if self._stop_event.is_set():
                raise RuntimeError("Execution cancelled due to previous task failure")

            # Delegate retry + execution logic to a helper to reduce complexity here
            self._execute_node_with_retries(
                node_name,
                start_date,
                end_date,
                retries,
                retry_delay_sec,
                tr,
                on_task_state,
            )
            return None

        fut = pool.submit(_work)
        running[node_name] = fut

    def _execute_node_with_retries(
        self,
        node_name: str,
        start_date: Optional[str],
        end_date: Optional[str],
        retries: int,
        retry_delay_sec: int,
        tr: TaskRun,
        on_task_state: Callable[[TaskRun], None],
    ):
        last_exc: Optional[Exception] = None
        for attempt in range(retries + 1):
            try:
                self.node_executor.execute_single_node(
                    node_name, start_date, end_date, ml_info={}
                )
                return None
            except Exception as e:  # noqa: PERF203
                last_exc = e
                if attempt < retries:
                    self._prepare_retry(tr, attempt, retry_delay_sec, on_task_state)
                else:
                    raise
        if last_exc:
            raise last_exc

    def _prepare_retry(
        self,
        tr: TaskRun,
        attempt: int,
        retry_delay_sec: int,
        on_task_state: Callable[[TaskRun], None],
    ) -> None:
        # Sleep progressively if configured, update try number and notify state
        if retry_delay_sec > 0:
            import time as _t

            _t.sleep(retry_delay_sec * (attempt + 1))
        tr.try_number = attempt + 2  # siguiente intento es 2..N
        on_task_state(tr)

    def _handle_finished(
        self,
        node: str,
        fut: Future,
        task_runs: Dict[str, TaskRun],
        running: Dict[str, Future],
        children: Dict[str, List[str]],
        on_task_state: Callable[[TaskRun], None],
    ) -> None:
        """
        Procesa un future finalizado para un nodo.
        - Si éxito: marca SUCCESS y notifica.
        - Si falla: marca FAILED, notifica, cancela pendientes y re-lanza excepción para que el caller maneje fallo global.
        """
        tr = task_runs[node]
        try:
            fut.result()
            tr.state = RunState.SUCCESS
            tr.finished_at = datetime.now(timezone.utc)
            on_task_state(tr)
        except Exception as e:
            tr.state = RunState.FAILED
            tr.error = str(e)
            tr.finished_at = datetime.now(timezone.utc)
            on_task_state(tr)
            # Signal global stop for other tasks
            self._stop_event.set()
            # Try to cancel other queued futures and mark their TaskRun appropriately
            for other, ofut in list(running.items()):
                if other != node:
                    cancelled = ofut.cancel()
                    other_tr = task_runs.get(other)
                    if other_tr:
                        if cancelled:
                            other_tr.state = RunState.CANCELLED
                            other_tr.finished_at = datetime.now(timezone.utc)
                            other_tr.error = "Cancelled due to another task failure"
                        else:
                            # Already running — we will mark as SKIPPED once they end (best-effort)
                            other_tr.state = RunState.SKIPPED
                            other_tr.finished_at = datetime.now(timezone.utc)
                            other_tr.error = (
                                "Marked as skipped due to another task failure"
                            )
                        on_task_state(other_tr)
            # Propagate failure
            raise

    def _wait_and_process(
        self,
        running: Dict[str, Future],
        task_runs: Dict[str, TaskRun],
        children: Dict[str, List[str]],
        on_task_state: Callable[[TaskRun], None],
        indegree: Dict[str, int],
        ready: deque,
    ) -> None:
        """
        Wait briefly for running futures and process any that finished:
        - Calls _handle_finished which may raise to indicate a failure.
        - Updates indegree and ready queue for downstream nodes.
        """
        done, _pending = wait(
            list(running.values()), timeout=0.1, return_when=FIRST_EXCEPTION
        )

        finished_nodes: List[str] = []
        for node, fut in list(running.items()):
            if fut in done:
                finished_nodes.append(node)
                # handle may raise to caller to indicate failure
                self._handle_finished(
                    node, fut, task_runs, running, children, on_task_state
                )

        for node in finished_nodes:
            running.pop(node, None)
            for v in children.get(node, []):
                indegree[v] -= 1
                if indegree[v] == 0:
                    ready.append(v)

    def execute(
        self,
        pipeline_name: str,
        start_date: Optional[str],
        end_date: Optional[str],
        on_task_state: Callable[[TaskRun], None],
        retries: int = 0,
        retry_delay_sec: int = 0,
        concurrency: Optional[int] = None,
        timeout_seconds: Optional[int] = None,
    ):
        if timeout_seconds:
            logger.warning(
                "Per-node hard timeouts are not enforced with thread-based execution. Consider process-based executors for strict timeouts."
            )

        pipeline_nodes, node_cfgs = self._get_pipeline_nodes_and_configs(pipeline_name)
        dag = DependencyResolver.build_dependency_graph(pipeline_nodes, node_cfgs)

        indegree: Dict[str, int] = defaultdict(int)
        children: Dict[str, List[str]] = defaultdict(list)
        for u, vs in dag.items():
            _ = indegree[u]  # fuerza presencia con valor 0 por defecto
            for v in vs:
                indegree[v] += 1
                children[u].append(v)
        ready = deque([n for n in pipeline_nodes if indegree.get(n, 0) == 0])

        max_workers = concurrency or self.max_workers
        running: Dict[str, Future] = {}
        task_runs: Dict[str, TaskRun] = {}

        # Reset stop event at start of execution
        self._stop_event.clear()

        with ThreadPoolExecutor(
            max_workers=max_workers, thread_name_prefix=f"node-{pipeline_name}-"
        ) as pool:
            try:
                while (ready or running) and not self._stop_event.is_set():
                    while (
                        ready
                        and len(running) < max_workers
                        and not self._stop_event.is_set()
                    ):
                        node_to_run = ready.popleft()
                        self._submit_task(
                            pool,
                            node_to_run,
                            task_runs,
                            running,
                            on_task_state,
                            start_date,
                            end_date,
                            retries,
                            retry_delay_sec,
                            timeout_seconds,
                        )

                    if not running:
                        # Nothing running and nothing ready (might be due to stop or empty)
                        continue

                    # Extracted wait-and-process logic into helper to reduce cognitive complexity
                    self._wait_and_process(
                        running, task_runs, children, on_task_state, indegree, ready
                    )
            finally:
                # Best-effort: mark any tasks that were not started or still running as cancelled/skipped
                for other, ofut in list(running.items()):
                    cancelled = ofut.cancel()
                    other_tr = task_runs.get(other)
                    if other_tr:
                        if cancelled:
                            other_tr.state = RunState.CANCELLED
                            other_tr.finished_at = datetime.now(timezone.utc)
                            other_tr.error = "Cancelled during shutdown"
                        else:
                            other_tr.state = RunState.SKIPPED
                            other_tr.finished_at = datetime.now(timezone.utc)
                            other_tr.error = "Skipped during shutdown"
                        on_task_state(other_tr)
                # clear stop_event for potential reuse
                self._stop_event.clear()
