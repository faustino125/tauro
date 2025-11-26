"""
MLflow Pipeline Adapter for Tauro.

Permite que los nodos de un pipeline de Tauro sean "steps" de un run de MLflow,
con tracking automático de métricas, parámetros y artefactos por paso.

Features:
- Cada nodo se ejecuta como un MLflow step
- Tracking automático de duración, inputs/outputs por nodo
- Nested runs para pipelines complejos
- Compatibilidad con MLflow Tracking Server y Databricks
"""

from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Union
import time

from loguru import logger

try:
    import mlflow
    from mlflow.entities import RunStatus as MLflowRunStatus
    from mlflow.tracking import MlflowClient

    MLFLOW_AVAILABLE = True
except ImportError:
    MLFLOW_AVAILABLE = False
    logger.warning("MLflow not installed. Install with: pip install mlflow")


class MLflowPipelineTracker:
    """
    Tracker de pipelines usando MLflow como backend.

    Convierte cada nodo de Tauro en un step de MLflow, permitiendo:
    - Tracking jerárquico (pipeline -> nodes como nested runs)
    - Métricas y parámetros por nodo
    - Artifacts por nodo y por pipeline
    - Visualización en MLflow UI

    Example:
        >>> tracker = MLflowPipelineTracker(
        ...     experiment_name="my_ml_pipeline",
        ...     tracking_uri="http://localhost:5000"
        ... )
        >>> with tracker.start_pipeline_run("training_pipeline") as run_id:
        ...     for node in ["preprocess", "train", "evaluate"]:
        ...         with tracker.start_node_step(node, run_id):
        ...             tracker.log_node_metric("accuracy", 0.95)
    """

    def __init__(
        self,
        experiment_name: str,
        tracking_uri: Optional[str] = None,
        artifact_location: Optional[str] = None,
        enable_autolog: bool = True,
        nested_runs: bool = True,
        tags: Optional[Dict[str, str]] = None,
    ):
        """
        Initialize MLflow Pipeline Tracker.

        Args:
            experiment_name: Nombre del experimento MLflow
            tracking_uri: URI del tracking server (None = local)
            artifact_location: Ubicación de artifacts (None = default)
            enable_autolog: Habilitar autologging de ML frameworks
            nested_runs: Usar nested runs para nodos (recomendado)
            tags: Tags adicionales para el experimento
        """
        if not MLFLOW_AVAILABLE:
            raise ImportError("MLflow is required. Install with: pip install mlflow")

        self.experiment_name = experiment_name
        self.tracking_uri = tracking_uri
        self.artifact_location = artifact_location
        self.enable_autolog = enable_autolog
        self.nested_runs = nested_runs
        self.tags = tags or {}

        # MLflow client
        self._client: Optional[MlflowClient] = None
        self._experiment_id: Optional[str] = None

        # State tracking
        self._active_pipeline_run: Optional[str] = None
        self._active_node_runs: Dict[str, str] = {}  # node_name -> run_id
        self._node_start_times: Dict[str, float] = {}

        self._initialize_mlflow()

    def _initialize_mlflow(self) -> None:
        """Inicializar MLflow client y experiment."""
        try:
            # Set tracking URI
            if self.tracking_uri:
                mlflow.set_tracking_uri(self.tracking_uri)
                logger.info(f"MLflow tracking URI set to: {self.tracking_uri}")

            # Create/get experiment
            self._client = MlflowClient()

            try:
                experiment = self._client.get_experiment_by_name(self.experiment_name)
                if experiment:
                    self._experiment_id = experiment.experiment_id
                    logger.info(
                        f"Using existing experiment: {self.experiment_name} (ID: {self._experiment_id})"
                    )
                else:
                    self._experiment_id = self._client.create_experiment(
                        self.experiment_name,
                        artifact_location=self.artifact_location,
                        tags=self.tags,
                    )
                    logger.info(
                        f"Created new experiment: {self.experiment_name} (ID: {self._experiment_id})"
                    )
            except Exception:
                # Fallback: set by name
                mlflow.set_experiment(self.experiment_name)
                experiment = mlflow.get_experiment_by_name(self.experiment_name)
                if experiment:
                    self._experiment_id = experiment.experiment_id

            # Enable autologging if requested
            if self.enable_autolog:
                self._enable_autologging()

        except Exception as e:
            logger.error(f"Failed to initialize MLflow: {e}")
            raise

    def _enable_autologging(self) -> None:
        """Enable autologging para frameworks populares."""
        try:
            mlflow.sklearn.autolog(silent=True)
            logger.debug("Enabled MLflow autologging for scikit-learn")
        except Exception:
            pass

        try:
            mlflow.xgboost.autolog(silent=True)
            logger.debug("Enabled MLflow autologging for XGBoost")
        except Exception:
            pass

        try:
            mlflow.pytorch.autolog(silent=True)
            logger.debug("Enabled MLflow autologging for PyTorch")
        except Exception:
            pass

        try:
            mlflow.tensorflow.autolog(silent=True)
            logger.debug("Enabled MLflow autologging for TensorFlow")
        except Exception:
            pass

    @contextmanager
    def start_pipeline_run(
        self,
        pipeline_name: str,
        run_name: Optional[str] = None,
        parameters: Optional[Dict[str, Any]] = None,
        tags: Optional[Dict[str, str]] = None,
        description: Optional[str] = None,
    ):
        """
        Context manager para iniciar un pipeline run en MLflow.

        Args:
            pipeline_name: Nombre del pipeline
            run_name: Nombre del run (opcional, se genera automáticamente)
            parameters: Parámetros del pipeline
            tags: Tags adicionales
            description: Descripción del run

        Yields:
            run_id del pipeline principal

        Example:
            >>> with tracker.start_pipeline_run("training") as run_id:
            ...     # Execute nodes
            ...     pass
        """
        if not run_name:
            timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
            run_name = f"{pipeline_name}_{timestamp}"

        run_tags = {
            "pipeline_name": pipeline_name,
            "tauro_pipeline": "true",
            "mlflow.note.content": description or f"Tauro pipeline: {pipeline_name}",
        }
        if tags:
            run_tags.update(tags)

        try:
            # Start parent run
            with mlflow.start_run(
                experiment_id=self._experiment_id,
                run_name=run_name,
                tags=run_tags,
                nested=False,
            ) as run:
                self._active_pipeline_run = run.info.run_id
                logger.info(
                    f"Started MLflow pipeline run: {run_name} (ID: {self._active_pipeline_run})"
                )

                # Log pipeline parameters
                if parameters:
                    mlflow.log_params(parameters)

                # Log pipeline start
                mlflow.log_param("pipeline_start_time", datetime.now(timezone.utc).isoformat())

                yield self._active_pipeline_run

                # Log pipeline end
                mlflow.log_param("pipeline_end_time", datetime.now(timezone.utc).isoformat())
                logger.info(f"Completed MLflow pipeline run: {run_name}")

        except Exception as e:
            logger.error(f"Pipeline run failed: {e}")
            if self._active_pipeline_run:
                try:
                    self._client.set_terminated(self._active_pipeline_run, status="FAILED")
                except Exception:
                    pass
            raise
        finally:
            self._active_pipeline_run = None
            self._active_node_runs.clear()
            self._node_start_times.clear()

    @contextmanager
    def start_node_step(
        self,
        node_name: str,
        parent_run_id: Optional[str] = None,
        parameters: Optional[Dict[str, Any]] = None,
        tags: Optional[Dict[str, str]] = None,
    ):
        """
        Context manager para ejecutar un nodo como MLflow step (nested run).

        Args:
            node_name: Nombre del nodo
            parent_run_id: Run ID del pipeline (usa el activo si no se especifica)
            parameters: Parámetros del nodo
            tags: Tags adicionales

        Yields:
            run_id del nodo step

        Example:
            >>> with tracker.start_node_step("preprocess", run_id) as node_run_id:
            ...     # Execute node logic
            ...     tracker.log_node_metric("rows_processed", 1000)
        """
        parent_run_id = parent_run_id or self._active_pipeline_run

        if not parent_run_id:
            raise ValueError("No active pipeline run. Start pipeline run first.")

        step_tags = {
            "node_name": node_name,
            "tauro_node": "true",
            "mlflow.parentRunId": parent_run_id,
        }
        if tags:
            step_tags.update(tags)

        start_time = time.perf_counter()
        self._node_start_times[node_name] = start_time

        try:
            # Start nested run for node
            with mlflow.start_run(
                experiment_id=self._experiment_id,
                run_name=f"step_{node_name}",
                tags=step_tags,
                nested=self.nested_runs,
            ) as run:
                node_run_id = run.info.run_id
                self._active_node_runs[node_name] = node_run_id

                logger.info(f"Started MLflow node step: {node_name} (ID: {node_run_id})")

                # Log node parameters
                if parameters:
                    mlflow.log_params(parameters)

                mlflow.log_param("node_start_time", datetime.now(timezone.utc).isoformat())

                yield node_run_id

                # Log duration
                duration = time.perf_counter() - start_time
                mlflow.log_metric("node_duration_seconds", duration)
                mlflow.log_param("node_end_time", datetime.now(timezone.utc).isoformat())

                logger.info(f"Completed MLflow node step: {node_name} (duration: {duration:.2f}s)")

        except Exception as e:
            logger.error(f"Node step {node_name} failed: {e}")

            # Log error
            if node_name in self._active_node_runs:
                try:
                    mlflow.log_param("error_message", str(e))
                    self._client.set_terminated(self._active_node_runs[node_name], status="FAILED")
                except Exception:
                    pass

            raise
        finally:
            self._active_node_runs.pop(node_name, None)
            self._node_start_times.pop(node_name, None)

    def log_node_metric(
        self,
        key: str,
        value: Union[float, int],
        step: Optional[int] = None,
        node_name: Optional[str] = None,
    ) -> None:
        """
        Log métrica para el nodo actual o especificado.

        Args:
            key: Nombre de la métrica
            value: Valor numérico
            step: Step (para métricas iterativas)
            node_name: Nombre del nodo (usa el activo si no se especifica)
        """
        try:
            if node_name and node_name in self._active_node_runs:
                run_id = self._active_node_runs[node_name]
                self._client.log_metric(run_id, key, value, step=step or 0)
            else:
                mlflow.log_metric(key, value, step=step)

            logger.debug(f"Logged metric {key}={value}")
        except Exception as e:
            logger.warning(f"Failed to log metric {key}: {e}")

    def log_node_param(
        self,
        key: str,
        value: Any,
        node_name: Optional[str] = None,
    ) -> None:
        """
        Log parámetro para el nodo actual o especificado.

        Args:
            key: Nombre del parámetro
            value: Valor
            node_name: Nombre del nodo (usa el activo si no se especifica)
        """
        try:
            if node_name and node_name in self._active_node_runs:
                run_id = self._active_node_runs[node_name]
                self._client.log_param(run_id, key, value)
            else:
                mlflow.log_param(key, value)

            logger.debug(f"Logged param {key}={value}")
        except Exception as e:
            logger.warning(f"Failed to log param {key}: {e}")

    def log_node_artifact(
        self,
        local_path: Union[str, Path],
        artifact_path: Optional[str] = None,
        node_name: Optional[str] = None,
    ) -> None:
        """
        Log artifact para el nodo actual o especificado.

        Args:
            local_path: Path local al archivo/directorio
            artifact_path: Path en artifacts store (opcional)
            node_name: Nombre del nodo (usa el activo si no se especifica)
        """
        try:
            local_path = str(local_path)

            if node_name and node_name in self._active_node_runs:
                run_id = self._active_node_runs[node_name]
                with mlflow.start_run(run_id=run_id):
                    if Path(local_path).is_dir():
                        mlflow.log_artifacts(local_path, artifact_path)
                    else:
                        mlflow.log_artifact(local_path, artifact_path)
            else:
                if Path(local_path).is_dir():
                    mlflow.log_artifacts(local_path, artifact_path)
                else:
                    mlflow.log_artifact(local_path, artifact_path)

            logger.info(f"Logged artifact: {local_path}")
        except Exception as e:
            logger.warning(f"Failed to log artifact {local_path}: {e}")

    def log_model(
        self,
        model: Any,
        artifact_path: str,
        flavor: Optional[str] = None,
        node_name: Optional[str] = None,
        **kwargs,
    ) -> None:
        """
        Log modelo ML para el nodo actual.

        Args:
            model: Modelo a guardar
            artifact_path: Path en artifacts
            flavor: Flavor de MLflow (sklearn, xgboost, pytorch, etc.)
            node_name: Nombre del nodo
            **kwargs: Argumentos adicionales para log_model
        """
        try:
            if node_name and node_name in self._active_node_runs:
                run_id = self._active_node_runs[node_name]
                with mlflow.start_run(run_id=run_id):
                    self._log_model_by_flavor(model, artifact_path, flavor, **kwargs)
            else:
                self._log_model_by_flavor(model, artifact_path, flavor, **kwargs)

            logger.info(f"Logged model to: {artifact_path}")
        except Exception as e:
            logger.warning(f"Failed to log model: {e}")

    def _log_model_by_flavor(
        self,
        model: Any,
        artifact_path: str,
        flavor: Optional[str],
        **kwargs,
    ) -> None:
        """Log modelo usando el flavor apropiado."""
        if flavor == "sklearn":
            mlflow.sklearn.log_model(model, artifact_path, **kwargs)
        elif flavor == "xgboost":
            mlflow.xgboost.log_model(model, artifact_path, **kwargs)
        elif flavor == "pytorch":
            mlflow.pytorch.log_model(model, artifact_path, **kwargs)
        elif flavor == "tensorflow":
            mlflow.tensorflow.log_model(model, artifact_path, **kwargs)
        elif flavor == "pyfunc":
            mlflow.pyfunc.log_model(artifact_path, python_model=model, **kwargs)
        else:
            # Auto-detect
            if hasattr(model, "fit") and hasattr(model, "predict"):
                mlflow.sklearn.log_model(model, artifact_path, **kwargs)
            else:
                mlflow.pyfunc.log_model(artifact_path, python_model=model, **kwargs)

    def log_pipeline_metric(
        self,
        key: str,
        value: Union[float, int],
        step: Optional[int] = None,
    ) -> None:
        """
        Log métrica a nivel de pipeline (no de nodo).

        Args:
            key: Nombre de la métrica
            value: Valor numérico
            step: Step (opcional)
        """
        if not self._active_pipeline_run:
            logger.warning("No active pipeline run")
            return

        try:
            self._client.log_metric(self._active_pipeline_run, key, value, step=step or 0)
            logger.debug(f"Logged pipeline metric {key}={value}")
        except Exception as e:
            logger.warning(f"Failed to log pipeline metric {key}: {e}")

    def get_node_run_id(self, node_name: str) -> Optional[str]:
        """Get run ID for a specific node."""
        return self._active_node_runs.get(node_name)

    def get_pipeline_run_id(self) -> Optional[str]:
        """Get active pipeline run ID."""
        return self._active_pipeline_run

    def is_tracking_active(self) -> bool:
        """Check if tracking is currently active."""
        return self._active_pipeline_run is not None

    @staticmethod
    def from_context(context, experiment_name: Optional[str] = None) -> "MLflowPipelineTracker":
        """
        Factory method para crear tracker desde Tauro context.

        Args:
            context: Tauro execution context
            experiment_name: Nombre del experimento (usa project_name del context si no se especifica)

        Returns:
            MLflowPipelineTracker configurado
        """
        if not MLFLOW_AVAILABLE:
            raise ImportError("MLflow is required")

        # Get config from context
        gs = getattr(context, "global_settings", {}) or {}
        mlflow_config = gs.get("mlflow", {}) or {}

        experiment_name = (
            experiment_name
            or mlflow_config.get("experiment_name")
            or getattr(context, "project_name", "tauro_pipeline")
        )

        tracking_uri = mlflow_config.get("tracking_uri")
        artifact_location = mlflow_config.get("artifact_location")
        enable_autolog = mlflow_config.get("enable_autolog", True)
        nested_runs = mlflow_config.get("nested_runs", True)

        tags = {
            "project": getattr(context, "project_name", "unknown"),
            "environment": gs.get("env", "unknown"),
        }
        tags.update(mlflow_config.get("tags", {}) or {})

        return MLflowPipelineTracker(
            experiment_name=experiment_name,
            tracking_uri=tracking_uri,
            artifact_location=artifact_location,
            enable_autolog=enable_autolog,
            nested_runs=nested_runs,
            tags=tags,
        )


def is_mlflow_available() -> bool:
    """Check if MLflow is installed and available."""
    return MLFLOW_AVAILABLE
