from __future__ import annotations
from typing import Dict, List, Callable, Optional
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, Future, wait, FIRST_EXCEPTION
from collections import defaultdict, deque
import threading
import time
import signal

from loguru import logger  # type: ignore

from tauro.config.contexts import Context
from tauro.exec.dependency_resolver import DependencyResolver
from tauro.exec.pipeline_validator import PipelineValidator
from tauro.exec.node_executor import NodeExecutor
from tauro.io.input import InputLoader
from tauro.io.output import DataOutputManager

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
        self.output_manager = DataOutputManager(self.context)
        self.node_executor = NodeExecutor(
            self.context, self.input_loader, self.output_manager, self.max_workers
        )
        self._stop_event = threading.Event()
        try:
            if threading.current_thread() is threading.main_thread():
                signal.signal(signal.SIGINT, self._signal_handler)
                signal.signal(signal.SIGTERM, self._signal_handler)
        except Exception:
            logger.debug(
                "Signal handlers not registered for LocalDagExecutor (platform/thread limitation)"
            )

    def _signal_handler(self, signum, frame):
        """Manejar señales de terminación"""
        logger.info(f"Received signal {signum}, shutting down gracefully...")
        self._stop_event.set()

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
            if self._stop_event.is_set():
                raise RuntimeError("Execution cancelled due to previous task failure")

            self._execute_node_with_retries(
                node_name,
                start_date,
                end_date,
                retries,
                retry_delay_sec,
                tr,
                on_task_state,
                timeout_seconds,
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
        timeout_seconds: Optional[int] = None,
    ):
        def _attempt():
            """Attempt a single execution (raises on failure)."""
            if timeout_seconds:
                self._execute_with_timeout(
                    node_name, start_date, end_date, timeout_seconds
                )
            else:
                self.node_executor.execute_single_node(
                    node_name, start_date, end_date, ml_info={}
                )

        last_exc: Optional[Exception] = None
        for attempt in range(retries + 1):
            try:
                _attempt()
                return None
            except TimeoutError:
                last_exc = TimeoutError(
                    f"Node {node_name} timed out after {timeout_seconds} seconds"
                )
                if attempt < retries:
                    self._prepare_retry(tr, attempt, retry_delay_sec, on_task_state)
                    continue
                raise last_exc
            except Exception as e:  # noqa: PERF203
                last_exc = e
                if attempt < retries:
                    self._prepare_retry(tr, attempt, retry_delay_sec, on_task_state)
                    continue
                raise
        if last_exc:
            raise last_exc

    def _execute_with_timeout(
        self,
        node_name: str,
        start_date: Optional[str],
        end_date: Optional[str],
        timeout_seconds: int,
    ):
        """Ejecutar un nodo con timeout usando threads"""
        result = None
        exception = None
        done = threading.Event()

        def _worker():
            nonlocal result, exception
            try:
                result = self.node_executor.execute_single_node(
                    node_name, start_date, end_date, ml_info={}
                )
            except Exception as e:
                exception = e
            finally:
                done.set()

        thread = threading.Thread(target=_worker, daemon=True)
        thread.start()

        done.wait(timeout=timeout_seconds)

        if not done.is_set():
            raise TimeoutError(
                f"Node {node_name} execution timed out after {timeout_seconds} seconds"
            )

        if exception:
            raise exception

        return result

    def _prepare_retry(
        self,
        tr: TaskRun,
        attempt: int,
        retry_delay_sec: int,
        on_task_state: Callable[[TaskRun], None],
    ) -> None:
        if retry_delay_sec > 0:
            sleep_time = retry_delay_sec * (attempt + 1)
            logger.info(
                f"Waiting {sleep_time} seconds before retry attempt {attempt + 2}"
            )
            time.sleep(sleep_time)
        tr.try_number = attempt + 2  # siguiente intento es 2..N
        on_task_state(tr)

    def _handle_finished(
        self,
        node: str,
        fut: Future,
        task_runs: Dict[str, TaskRun],
        running: Dict[str, Future],
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
            self._stop_event.set()
            self._cancel_and_mark_others(node, running, task_runs, on_task_state)
            raise

    def _cancel_and_mark_others(
        self,
        failed_node: str,
        running: Dict[str, Future],
        task_runs: Dict[str, TaskRun],
        on_task_state: Callable[[TaskRun], None],
    ) -> None:
        """Cancel other running futures and update their TaskRun states."""
        for other, ofut in running.items():
            if other == failed_node:
                continue
            try:
                cancelled = ofut.cancel()
            except Exception:
                cancelled = False
            other_tr = task_runs.get(other)
            if not other_tr:
                continue
            if cancelled:
                other_tr.state = RunState.CANCELLED
                other_tr.finished_at = datetime.now(timezone.utc)
                other_tr.error = "Cancelled due to another task failure"
            else:
                other_tr.state = RunState.SKIPPED
                other_tr.finished_at = datetime.now(timezone.utc)
                other_tr.error = "Marked as skipped due to another task failure"
            on_task_state(other_tr)

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
            running.values(), timeout=0.1, return_when=FIRST_EXCEPTION
        )

        finished_nodes: List[str] = []
        for node, fut in running.items():
            if fut in done:
                finished_nodes.append(node)
                self._handle_finished(node, fut, task_runs, running, on_task_state)

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

        indegree, children, ready = self._build_graph_state(pipeline_nodes, dag)

        max_workers = concurrency or self.max_workers
        running: Dict[str, Future] = {}
        task_runs: Dict[str, TaskRun] = {}

        self._stop_event.clear()

        with ThreadPoolExecutor(
            max_workers=max_workers, thread_name_prefix=f"node-{pipeline_name}-"
        ) as pool:
            try:
                self._dispatch_and_wait(
                    pool,
                    ready,
                    running,
                    task_runs,
                    children,
                    indegree,
                    on_task_state,
                    start_date,
                    end_date,
                    retries,
                    retry_delay_sec,
                    timeout_seconds,
                    max_workers,
                )
            except Exception as e:
                logger.exception(f"DAG execution failed: {e}")
                raise
            finally:
                self._finalize_running_tasks(running, task_runs, on_task_state)
                self._stop_event.clear()

    def _dispatch_and_wait(
        self,
        pool: ThreadPoolExecutor,
        ready: deque,
        running: Dict[str, Future],
        task_runs: Dict[str, TaskRun],
        children: Dict[str, List[str]],
        indegree: Dict[str, int],
        on_task_state: Callable[[TaskRun], None],
        start_date: Optional[str],
        end_date: Optional[str],
        retries: int,
        retry_delay_sec: int,
        timeout_seconds: Optional[int],
        max_workers: int,
    ) -> None:
        while (ready or running) and not self._stop_event.is_set():
            while (
                ready and len(running) < max_workers and not self._stop_event.is_set()
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
                continue

            self._wait_and_process(
                running, task_runs, children, on_task_state, indegree, ready
            )

    def _finalize_running_tasks(
        self,
        running: Dict[str, Future],
        task_runs: Dict[str, TaskRun],
        on_task_state: Callable[[TaskRun], None],
    ) -> None:
        for other, ofut in running.items():
            try:
                cancelled = ofut.cancel()
            except Exception:
                cancelled = False
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

    def _build_graph_state(self, pipeline_nodes: List[str], dag: Dict[str, List[str]]):
        """Build indegree map, children map and initial ready queue for the DAG."""
        indegree: Dict[str, int] = defaultdict(int)
        children: Dict[str, List[str]] = defaultdict(list)
        for u, vs in dag.items():
            _ = indegree[u]
            for v in vs:
                indegree[v] += 1
                children[u].append(v)
        ready = deque([n for n in pipeline_nodes if indegree.get(n, 0) == 0])
        return indegree, children, ready
