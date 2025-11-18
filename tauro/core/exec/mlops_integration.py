from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Dict, List, Optional
from pathlib import Path

from loguru import logger

from tauro.core.mlops.config import MLOpsContext
from tauro.core.mlops.experiment_tracking import RunStatus

if TYPE_CHECKING:
    from tauro.core.config.contexts import Context


class MLOpsExecutorIntegration:
    """
    Integration between Executor and MLOps layer.

    Manages:
    - Experiment creation and run lifecycle
    - Metric logging during node execution
    - Artifact storage
    - Model registration on pipeline completion
    """

    def __init__(
        self,
        context: Optional["Context"] = None,
        mlops_context: Optional[MLOpsContext] = None,
        auto_init: bool = True,
    ):
        """
        Initialize MLOps-Executor integration.

        Args:
            context: Tauro execution context (PREFERRED - auto-detects mode)
            mlops_context: MLOpsContext instance (LEGACY, optional)
            auto_init: Auto-initialize from context/env if not provided
            
        Example:
            >>> # PREFERRED: Use context for auto-detection
            >>> integration = MLOpsExecutorIntegration(context=execution_context)
            >>> 
            >>> # LEGACY: Manual MLOpsContext
            >>> mlops = MLOpsContext.from_env()
            >>> integration = MLOpsExecutorIntegration(mlops_context=mlops)
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

        Args:
            pipeline_name: Pipeline name
            pipeline_type: Pipeline type (batch, streaming, hybrid)
            description: Pipeline description
            tags: Custom tags

        Returns:
            experiment_id or None
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

        Args:
            experiment_id: Experiment ID
            pipeline_name: Pipeline name
            model_version: Model version (if applicable)
            hyperparams: Hyperparameters
            tags: Custom tags

        Returns:
            run_id or None
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

        Args:
            run_id: Run ID
            node_name: Node name
            status: Execution status (completed, failed, etc.)
            duration_seconds: Execution duration
            metrics: Node-level metrics
            error: Error message if failed
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

        Args:
            run_id: Run ID
            metrics: Dictionary of metrics
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

        Args:
            run_id: Run ID
            artifact_path: Path to artifact
            artifact_type: Type of artifact (model, dataset, plot, etc.)

        Returns:
            Artifact URI or None
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

        Args:
            run_id: Run ID
            status: Final status
            summary: Summary of execution
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

        Args:
            run_id: Source run ID
            model_name: Model name
            artifact_path: Path to model artifact
            artifact_type: Model type (sklearn, xgboost, pytorch, etc.)
            framework: ML framework
            description: Model description
            hyperparams: Hyperparameters used
            metrics: Performance metrics
            tags: Custom tags
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

            logger.info(
                f"Registered model {model_name} v{model_version.version} from run {run_id}"
            )

        except Exception as e:
            logger.error(f"Failed to register model: {e}")

    def get_run_comparison(
        self,
        experiment_id: str,
        metric_filter: Optional[Dict[str, tuple]] = None,
    ) -> Any:
        """
        Get DataFrame comparing runs in experiment.

        Args:
            experiment_id: Experiment ID
            metric_filter: Optional metric filters

        Returns:
            pandas.DataFrame or None
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


class MLInfoConfigLoader:
    """
    Loader for ML configuration from YAML/JSON/DSL files.

    Supports:
    - ml_config.yml / ml_config.json
    - Inline DSL in node_config
    """

    @staticmethod
    def load_ml_info_from_file(
        filepath: str,
    ) -> Dict[str, Any]:
        """
        Load ML info from YAML or JSON file.

        Args:
            filepath: Path to configuration file

        Returns:
            ML info dictionary
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

        Args:
            context: Execution context
            pipeline_name: Pipeline name

        Returns:
            ML info dictionary
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

        Args:
            base_ml_info: Base configuration
            override_ml_info: Override configuration

        Returns:
            Merged configuration
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
