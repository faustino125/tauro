from __future__ import annotations
import asyncio
from typing import Any, Dict, List, Optional

from tauro.api.deps import get_context
from tauro.exec.executor import PipelineExecutor
from tauro.api.models import RunOptions


class OrchestratorClient:
    """Fachada programática que replica operaciones comunes de la CLI.

    Provee métodos async y wrappers sync. Prioriza servicios compartidos
    (p. ej. `pipeline_service`, `runner`, `store`) cuando se construye con
    referencias a éstos; si no existen hace fallbacks seguros usando
    `PipelineExecutor` u `OrchestratorRunner` según disponibilidad.
    """

    def __init__(
        self,
        context=None,
        store=None,
        runner=None,
        pipeline_service=None,
        scheduler=None,
    ) -> None:
        self.context = context
        self.store = store
        self.runner = runner
        self.pipeline_service = pipeline_service
        self.scheduler = scheduler

    # --- Helpers -----------------------------------------------------------------
    async def _maybe_await(self, value):
        if asyncio.iscoroutine(value):
            return await value
        return value

    def _sync_from_async(self, coro):
        """Ejecutar coroutine desde contexto sync. Si ya hay un loop en ejecución
        levanta RuntimeError (esperar usar la versión async).
        """
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            # No hay loop corriendo, seguro usar asyncio.run
            return asyncio.run(coro)
        raise RuntimeError(
            "There is already a running event loop; use the async method instead"
        )

    # --- Operaciones que imitan la CLI -----------------------------------------
    async def list_pipelines_async(self, context=None) -> List[str]:
        ctx = context or self.context or get_context()
        executor = PipelineExecutor(ctx, None)
        res = executor.list_pipelines()
        return await self._maybe_await(res)

    def list_pipelines(self, context=None) -> List[str]:
        return self._sync_from_async(self.list_pipelines_async(context=context))

    async def get_pipeline_info_async(
        self, pipeline_id: str, context=None
    ) -> Dict[str, Any]:
        ctx = context or self.context or get_context()
        executor = PipelineExecutor(ctx, None)
        res = executor.get_pipeline_info(pipeline_id)
        return await self._maybe_await(res)

    def get_pipeline_info(self, pipeline_id: str, context=None) -> Dict[str, Any]:
        return self._sync_from_async(
            self.get_pipeline_info_async(pipeline_id, context=context)
        )

    async def list_nodes_async(self, pipeline_id: str, context=None) -> List[str]:
        info = await self.get_pipeline_info_async(pipeline_id, context=context)
        nodes = info.get("nodes") or info.get("pipeline", {}).get("nodes", [])
        return nodes

    def list_nodes(self, pipeline_id: str, context=None) -> List[str]:
        return self._sync_from_async(
            self.list_nodes_async(pipeline_id, context=context)
        )

    async def run_pipeline_async(
        self,
        pipeline_id: str,
        params: Optional[Dict[str, Any]] = None,
        options: Optional[RunOptions] = None,
    ) -> str:
        params = params or {}
        options = options or RunOptions()
        # Preferir pipeline_service cuando esté disponible
        if self.pipeline_service and hasattr(self.pipeline_service, "run_pipeline"):
            # Intentar pasar options como parte de params si el service lo permite
            payload = dict(params or {})
            payload.setdefault("__run_options__", options.dict(exclude_none=True))
            res = self.pipeline_service.run_pipeline(pipeline_id, payload)
            return await self._maybe_await(res)

        # Intentar runner
        if self.runner and hasattr(self.runner, "run_pipeline"):
            res = self.runner.run_pipeline(pipeline_id, params)
            return await self._maybe_await(res)

        # Fallback a PipelineExecutor (mapeamos opciones a los parámetros conocidos)
        ctx = self.context or get_context()
        executor = PipelineExecutor(ctx, None)
        # Mapear opciones a los argumentos del executor.run_pipeline
        res = executor.run_pipeline(
            pipeline_id,
            None,  # node_name
            None,  # start_date
            None,  # end_date
            options.model_version,
            options.hyperparams,
            options.execution_mode or "async",
        )
        return await self._maybe_await(res)

    def run_pipeline(
        self, pipeline_id: str, params: Optional[Dict[str, Any]] = None
    ) -> str:
        return self._sync_from_async(
            self.run_pipeline_async(pipeline_id, params=params)
        )

    async def run_node_async(
        self,
        pipeline_id: str,
        node_id: str,
        params: Optional[Dict[str, Any]] = None,
        options: Optional[RunOptions] = None,
    ) -> Any:
        """Ejecuta un nodo específico si el runner o executor lo soporta.

        El comportamiento exacto depende de la implementación del runner.
        """
        params = params or {}
        options = options or RunOptions()
        # Intentar runner
        if self.runner and hasattr(self.runner, "run_node"):
            # intentar pasar options si el runner acepta
            try:
                res = self.runner.run_node(
                    pipeline_id, node_id, params, options.dict(exclude_none=True)
                )
            except TypeError:
                res = self.runner.run_node(pipeline_id, node_id, params)
            return await self._maybe_await(res)

        # Fallback a PipelineExecutor si expone run_node
        ctx = self.context or get_context()
        executor = PipelineExecutor(ctx, None)
        # PipelineExecutor supports running a specific node via run_pipeline(node_name=...)
        try:
            res = executor.run_pipeline(
                pipeline_id,
                node_id,
                None,
                None,
                options.model_version,
                options.hyperparams,
                options.execution_mode or "async",
            )
            return await self._maybe_await(res)
        except Exception:
            # If run_pipeline fails as fallback, raise NotImplementedError
            raise NotImplementedError(
                "Runner/Executor does not support running individual nodes"
            )

        raise NotImplementedError(
            "Runner/Executor does not support running individual nodes"
        )

    def run_node(
        self, pipeline_id: str, node_id: str, params: Optional[Dict[str, Any]] = None
    ) -> Any:
        return self._sync_from_async(
            self.run_node_async(pipeline_id, node_id, params=params)
        )

    async def cancel_run_async(self, run_id: str) -> bool:
        if self.pipeline_service and hasattr(self.pipeline_service, "cancel_run"):
            res = self.pipeline_service.cancel_run(run_id)
            return await self._maybe_await(res)

        if self.runner and hasattr(self.runner, "cancel_run"):
            res = self.runner.cancel_run(run_id)
            return await self._maybe_await(res)

        # Fallback: intentar marcar en store
        if self.store and hasattr(self.store, "update_run_status"):
            try:
                self.store.update_run_status(run_id, "CANCELLED")
                return True
            except Exception:
                return False

        return False

    def cancel_run(self, run_id: str) -> bool:
        return self._sync_from_async(self.cancel_run_async(run_id))

    async def list_runs_async(
        self, pipeline_id: Optional[str] = None, limit: int = 100, offset: int = 0
    ):
        if self.pipeline_service and hasattr(self.pipeline_service, "list_runs"):
            res = self.pipeline_service.list_runs(limit=limit, offset=offset)
            return await self._maybe_await(res)

        if self.runner and hasattr(self.runner, "list_runs"):
            res = self.runner.list_runs(
                pipeline_id=pipeline_id, limit=limit, offset=offset
            )
            return await self._maybe_await(res)

        if self.store and hasattr(self.store, "list_runs"):
            return self.store.list_runs(limit=limit, offset=offset)

        return []

    def list_runs(
        self, pipeline_id: Optional[str] = None, limit: int = 100, offset: int = 0
    ):
        return self._sync_from_async(
            self.list_runs_async(pipeline_id=pipeline_id, limit=limit, offset=offset)
        )
