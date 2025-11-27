from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Dict, List, Optional
from pathlib import Path

from loguru import logger

from engine.mlops.config import MLOpsContext
from engine.mlops.experiment_tracking import RunStatus

if TYPE_CHECKING:
    from engine.config.contexts import Context


class MLOpsExecutorIntegration:
    """
    Integration between Executor and MLOps layer.
    """

    def __init__(
        self,
        context: Optional["Context"] = None,
        mlops_context: Optional[MLOpsContext] = None,
        auto_init: bool = True,
    ):
        """
        Initialize MLOps-Executor integration.
        """
        self.context = context
        self.mlops_context = mlops_context
        self.active_experiment_id: Optional[str] = None
        self.active_run_id: Optional[str] = None
        self.pipeline_runs: Dict[str, str] = {}  # pipeline_name -> run_id
        self.node_artifacts: Dict[str, List[str]] = {}  # run_id -> artifact_paths

        # Auto-init with priority: context > mlops_context > env
        if self.mlops_context is None and auto_init:
            if self.context is not None:
                # ✅ PREFERRED: Use factory from context
                try:
                    self.mlops_context = MLOpsContext.from_context(self.context)
                    logger.info("MLOpsContext initialized from execution context")
                except Exception as e:
                    logger.warning(f"Could not init MLOpsContext from context: {e}")
            else:
                # Fallback to env vars (legacy)
                try:
                    self.mlops_context = MLOpsContext.from_env()
                    logger.info("MLOpsContext initialized from environment (legacy)")
                except Exception as e:
                    logger.warning(f"Could not auto-initialize MLOpsContext: {e}")

    def is_available(self) -> bool:
        """Check if MLOps context is available."""
        return self.mlops_context is not None

    def create_pipeline_experiment(
        self,
        pipeline_name: str,
        pipeline_type: str,
        description: str = "",
        tags: Optional[Dict[str, str]] = None,
    ) -> Optional[str]:
        """
        Create experiment for pipeline execution.
        """
        if not self.is_available():
            return None

        try:
            tags = tags or {}
            tags.update(
                {
                    "pipeline_type": pipeline_type,
                    "pipeline_name": pipeline_name,
                }
            )

            exp = self.mlops_context.experiment_tracker.create_experiment(
                name=pipeline_name,
                description=description,
                tags=tags,
            )

            self.active_experiment_id = exp.experiment_id
            logger.info(f"Created MLOps experiment {pipeline_name} (ID: {exp.experiment_id})")
            return exp.experiment_id

        except Exception as e:
            logger.error(f"Failed to create experiment: {e}")
            return None

    def start_pipeline_run(
        self,
        experiment_id: str,
        pipeline_name: str,
        model_version: Optional[str] = None,
        hyperparams: Optional[Dict[str, Any]] = None,
        tags: Optional[Dict[str, str]] = None,
    ) -> Optional[str]:
        """
        Start a run for pipeline execution.
        """
        if not self.is_available():
            return None

        try:
            tags = tags or {}
            tags.update(
                {
                    "pipeline_name": pipeline_name,
                    "model_version": model_version or "unknown",
                }
            )

            run = self.mlops_context.experiment_tracker.start_run(
                experiment_id=experiment_id,
                name=f"{pipeline_name}-{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}",
                parameters=hyperparams or {},
                tags=tags,
            )

            self.active_run_id = run.run_id
            self.pipeline_runs[pipeline_name] = run.run_id
            self.node_artifacts[run.run_id] = []

            logger.info(f"Started MLOps run {pipeline_name} (ID: {run.run_id})")
            return run.run_id

        except Exception as e:
            logger.error(f"Failed to start run: {e}")
            return None

    def log_node_execution(
        self,
        run_id: str,
        node_name: str,
        status: str,
        duration_seconds: float,
        metrics: Optional[Dict[str, float]] = None,
        error: Optional[str] = None,
    ) -> None:
        """
        Log node execution details.
        """
        if not self.is_available() or not run_id:
            return

        try:
            # Log node status as parameter
            self.mlops_context.experiment_tracker.log_parameter(
                run_id,
                f"node_{node_name}_status",
                status,
            )

            # Log duration as metric
            self.mlops_context.experiment_tracker.log_metric(
                run_id,
                f"node_{node_name}_duration_seconds",
                duration_seconds,
                step=0,
            )

            # Log custom metrics
            if metrics:
                for metric_name, value in metrics.items():
                    if isinstance(value, (int, float)):
                        self.mlops_context.experiment_tracker.log_metric(
                            run_id,
                            f"node_{node_name}_{metric_name}",
                            float(value),
                            step=0,
                        )

            # Log error if present
            if error:
                self.mlops_context.experiment_tracker.log_parameter(
                    run_id,
                    f"node_{node_name}_error",
                    error,
                )

            logger.debug(f"Logged execution for node {node_name}")

        except Exception as e:
            logger.warning(f"Could not log node execution: {e}")

    def log_pipeline_metrics(
        self,
        run_id: str,
        metrics: Dict[str, float],
    ) -> None:
        """
        Log pipeline-level metrics.
        """
        if not self.is_available() or not run_id:
            return

        try:
            for metric_name, value in metrics.items():
                if isinstance(value, (int, float)):
                    self.mlops_context.experiment_tracker.log_metric(
                        run_id,
                        metric_name,
                        float(value),
                        step=0,
                    )

            logger.debug("Logged pipeline metrics")

        except Exception as e:
            logger.warning(f"Could not log pipeline metrics: {e}")

    def log_artifact(
        self,
        run_id: str,
        artifact_path: str,
        artifact_type: str = "model",
    ) -> Optional[str]:
        """
        Log artifact for run.
        """
        if not self.is_available() or not run_id:
            return None

        try:
            artifact_uri = self.mlops_context.experiment_tracker.log_artifact(
                run_id,
                artifact_path,
                destination=f"artifacts/{artifact_type}",
            )

            if run_id in self.node_artifacts:
                self.node_artifacts[run_id].append(artifact_uri)

            logger.info(f"Logged artifact {artifact_type}: {artifact_uri}")
            return artifact_uri

        except Exception as e:
            logger.warning(f"Could not log artifact: {e}")
            return None

    def end_pipeline_run(
        self,
        run_id: str,
        status: RunStatus = RunStatus.COMPLETED,
        summary: Optional[Dict[str, Any]] = None,
    ) -> None:
        """
        End pipeline run.
        """
        if not self.is_available() or not run_id:
            return

        try:
            # Log summary if provided
            if summary:
                for key, value in summary.items():
                    if isinstance(value, (int, float)):
                        self.mlops_context.experiment_tracker.log_metric(
                            run_id,
                            f"summary_{key}",
                            float(value),
                            step=0,
                        )

            # End the run
            self.mlops_context.experiment_tracker.end_run(run_id, status)

            logger.info(f"Ended MLOps run {run_id} with status {status.value}")

            # Clean up
            self.active_run_id = None
            if run_id in self.node_artifacts:
                del self.node_artifacts[run_id]

        except Exception as e:
            logger.error(f"Failed to end run: {e}")

    def register_model_from_run(
        self,
        run_id: str,
        model_name: str,
        artifact_path: str,
        artifact_type: str,
        framework: str,
        description: str = "",
        hyperparams: Optional[Dict[str, Any]] = None,
        metrics: Optional[Dict[str, float]] = None,
        tags: Optional[Dict[str, str]] = None,
    ) -> None:
        """
        Register trained model in Model Registry from run artifacts.
        """
        if not self.is_available():
            return

        try:
            model_version = self.mlops_context.model_registry.register_model(
                name=model_name,
                artifact_path=artifact_path,
                artifact_type=artifact_type,
                framework=framework,
                description=description,
                hyperparams=hyperparams,
                metrics=metrics,
                tags=tags,
                experiment_run_id=run_id,
            )

            logger.info(f"Registered model {model_name} v{model_version.version} from run {run_id}")

        except Exception as e:
            logger.error(f"Failed to register model: {e}")

    def get_run_comparison(
        self,
        experiment_id: str,
        metric_filter: Optional[Dict[str, tuple]] = None,
    ) -> Any:
        """
        Get DataFrame comparing runs in experiment.
        """
        if not self.is_available():
            return None

        try:
            run_ids = self.mlops_context.experiment_tracker.search_runs(
                experiment_id,
                metric_filter=metric_filter,
            )

            if not run_ids:
                return None

            comparison_df = self.mlops_context.experiment_tracker.compare_runs(run_ids)

            logger.info(f"Generated comparison DataFrame for {len(run_ids)} runs")
            return comparison_df

        except Exception as e:
            logger.warning(f"Could not generate comparison: {e}")
            return None


def _rt_resolve_model_name(
    ml_context: Dict[str, Any], node_config: Dict[str, Any], cfg: Dict[str, Any]
) -> str:
    name = cfg.get("model_name")
    if name:
        return name
    pipeline_cfg = ml_context.get("pipeline_config", {}) or {}
    name = pipeline_cfg.get("model_name") or pipeline_cfg.get("name")
    if name:
        return name
    if isinstance(node_config, dict):
        return node_config.get("name") or "unnamed_model"
    return "unnamed_model"


def _rt_extract_fields(trainer_result: Dict[str, Any], ml_context: Dict[str, Any]) -> tuple:
    artifact_type = trainer_result.get("artifact_type", "generic")
    framework = trainer_result.get("framework", "generic")
    metrics = trainer_result.get("metrics") or {}
    hyperparams = trainer_result.get("hyperparams") or ml_context.get("hyperparams", {}) or {}
    metadata = trainer_result.get("metadata") or {}
    return artifact_type, framework, metrics, hyperparams, metadata


def _rt_log_and_register(
    mlops_integration: Any,
    run_id: str,
    model_name: str,
    artifact_path: str,
    artifact_type: str,
    framework: str,
    trainer_result: Dict[str, Any],
    hyperparams: Dict[str, Any],
    metrics: Dict[str, Any],
    stage: str,
) -> None:
    # Registrar artifact en el tracker (queda asociado al run)
    logged_artifact_path = mlops_integration.log_artifact(
        run_id=run_id,
        artifact_path=artifact_path,
        artifact_type="model",
    )

    if not logged_artifact_path:
        logger.warning(
            f"No se pudo loggear el artifact del modelo para '{model_name}'; se omite el registro"
        )
        return

    # Registrar modelo en el ModelRegistry a partir del run
    tags = {"stage": str(stage)}

    mlops_integration.register_model_from_run(
        run_id=run_id,
        model_name=model_name,
        artifact_path=logged_artifact_path,
        artifact_type=artifact_type,
        framework=framework,
        description=str(trainer_result.get("description", "")),
        hyperparams=hyperparams,
        metrics=metrics,
        tags=tags,
    )


def register_trained_model_from_result(
    ml_context: Dict[str, Any],
    trainer_result: Dict[str, Any],
    node_config: Dict[str, Any],
    default_stage: str = "Staging",
) -> None:
    """Helper para registrar un modelo entrenado a partir del resultado de un nodo trainer."""

    if not isinstance(ml_context, dict):
        return

    mlops_integration = ml_context.get("mlops_integration")
    mlops_run_id = ml_context.get("mlops_run_id")

    if not mlops_integration or not mlops_run_id:
        return

    if not isinstance(trainer_result, dict):
        logger.debug("trainer_result no es un dict; se omite el registro de modelo")
        return

    artifact_path = trainer_result.get("artifact_path")
    if not artifact_path:
        logger.debug("trainer_result no contiene 'artifact_path'; se omite el registro de modelo")
        return

    cfg = node_config.get("config", {}) if isinstance(node_config, dict) else {}
    model_name = _rt_resolve_model_name(ml_context, node_config, cfg)
    artifact_type, framework, metrics, hyperparams, _ = _rt_extract_fields(
        trainer_result, ml_context
    )
    stage = cfg.get("default_stage", default_stage)

    try:
        _rt_log_and_register(
            mlops_integration=mlops_integration,
            run_id=mlops_run_id,
            model_name=model_name,
            artifact_path=artifact_path,
            artifact_type=artifact_type,
            framework=framework,
            trainer_result=trainer_result,
            hyperparams=hyperparams,
            metrics=metrics,
            stage=stage,
        )
    except Exception as e:
        logger.warning(f"Error en register_trained_model_from_result para '{model_name}': {e}")


class MLInfoConfigLoader:
    """
    Loader for ML configuration from YAML/JSON/DSL files.
    """

    @staticmethod
    def load_ml_info_from_file(
        filepath: str,
    ) -> Dict[str, Any]:
        """
        Load ML info from YAML or JSON file.
        """
        path = Path(filepath)

        if not path.exists():
            logger.warning(f"ML info file not found: {filepath}")
            return {}

        try:
            if path.suffix in [".yml", ".yaml"]:
                import yaml

                with open(path) as f:
                    data = yaml.safe_load(f) or {}
            elif path.suffix == ".json":
                import json

                with open(path) as f:
                    data = json.load(f)
            else:
                logger.warning(f"Unsupported file format: {path.suffix}")
                return {}

            logger.info(f"Loaded ML info from {filepath}")
            return data

        except Exception as e:
            logger.error(f"Failed to load ML info from {filepath}: {e}")
            return {}

    @staticmethod
    def load_ml_info_from_context(
        context,
        pipeline_name: str,
    ) -> Dict[str, Any]:
        """
        Load ML info from context using existing method.
        """
        if not hasattr(context, "get_pipeline_ml_config"):
            return {}

        try:
            ml_config = context.get_pipeline_ml_config(pipeline_name)
            return ml_config or {}
        except Exception as e:
            logger.warning(f"Could not load ML config from context: {e}")
            return {}

    @staticmethod
    def merge_ml_info(
        base_ml_info: Dict[str, Any],
        override_ml_info: Dict[str, Any],
    ) -> Dict[str, Any]:
        """
        Merge ML info dictionaries with override taking precedence.
        """
        merged = dict(base_ml_info)
        merged.update(override_ml_info)

        # Merge nested dicts
        for key in ["hyperparams", "metrics", "tags"]:
            if (
                key in base_ml_info
                and key in override_ml_info
                and isinstance(base_ml_info[key], dict)
                and isinstance(override_ml_info[key], dict)
            ):
                merged_nested = dict(base_ml_info[key])
                merged_nested.update(override_ml_info[key])
                merged[key] = merged_nested

        return merged
