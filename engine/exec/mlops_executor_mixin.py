import os
from typing import Any, Dict, Optional

from loguru import logger

from engine.exec.mlops_integration import (
    MLOpsExecutorIntegration,
    MLInfoConfigLoader,
)
from engine.mlops.experiment_tracking import RunStatus


class MLOpsExecutorMixin:
    """
    Mixin to add MLOps capabilities to BaseExecutor.

    Provides:
    - Experiment tracking during pipeline execution
    - Automatic artifact registration
    - ML config loading from files
    - Auto-detection of execution mode (local vs Databricks)
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._init_mlops_integration()

    def _init_mlops_integration(self) -> None:
        """Initialize MLOps integration with context awareness."""
        try:
            auto_init = os.getenv("TAURO_MLOPS_AUTO_INIT", "true").lower() == "true"

            # ✅ NEW: Pass self.context to integration for auto-detection
            self.mlops_integration = MLOpsExecutorIntegration(
                context=self.context,  # Enables auto mode detection
                auto_init=auto_init,
            )

            if self.mlops_integration.is_available():
                mode = getattr(self.context, "execution_mode", "unknown")
                logger.info(f"MLOps integration enabled for executor (mode: {mode})")
            else:
                logger.info("MLOps integration available but not initialized")

        except Exception as e:
            logger.warning(f"MLOps integration init warning: {e}")
            self.mlops_integration = None

    def _setup_mlops_experiment(
        self,
        pipeline_name: str,
        pipeline_type: str,
        model_version: Optional[str] = None,
        hyperparams: Optional[Dict[str, Any]] = None,
    ) -> Optional[str]:
        """
        Setup MLOps experiment and run for pipeline execution.

        Args:
            pipeline_name: Pipeline name
            pipeline_type: Pipeline type (BATCH, STREAMING, HYBRID)
            model_version: Model version (optional)
            hyperparams: Hyperparameters (optional)

        Returns:
            run_id or None
        """
        if not self.mlops_integration or not self.mlops_integration.is_available():
            return None

        try:
            # Create experiment
            experiment_id = self.mlops_integration.create_pipeline_experiment(
                pipeline_name=pipeline_name,
                pipeline_type=pipeline_type,
                description=f"Pipeline execution for {pipeline_name}",
                tags={
                    "execution_date": str(__import__("datetime").datetime.utcnow()),
                },
            )

            if not experiment_id:
                return None

            # Start run
            run_id = self.mlops_integration.start_pipeline_run(
                experiment_id=experiment_id,
                pipeline_name=pipeline_name,
                model_version=model_version,
                hyperparams=hyperparams,
                tags={"executor_type": pipeline_type},
            )

            return run_id

        except Exception as e:
            logger.warning(f"Could not setup MLOps experiment: {e}")
            return None

    def _log_pipeline_execution_summary(
        self,
        run_id: Optional[str],
        execution_results: Dict[str, Any],
        status: RunStatus = RunStatus.COMPLETED,
    ) -> None:
        """
        Log pipeline execution summary to MLOps.

        Args:
            run_id: Run ID
            execution_results: Execution results by node
            status: Final status
        """
        if not self.mlops_integration or not run_id:
            return

        try:
            # Build summary
            total_nodes = len(execution_results)
            completed_nodes = sum(
                1
                for r in execution_results.values()
                if isinstance(r, dict) and r.get("status") == "completed"
            )

            summary = {
                "total_nodes": total_nodes,
                "completed_nodes": completed_nodes,
            }

            # Log metrics
            self.mlops_integration.log_pipeline_metrics(run_id, summary)

            # End run
            self.mlops_integration.end_pipeline_run(run_id, status=status)

            logger.info(f"Logged MLOps execution summary: {summary}")

        except Exception as e:
            logger.warning(f"Could not log execution summary: {e}")

    def _load_ml_info_enhanced(
        self,
        pipeline_name: str,
        model_version: Optional[str] = None,
        hyperparams: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Enhanced ML info loading with file and context support.

        Args:
            pipeline_name: Pipeline name
            model_version: Model version override
            hyperparams: Hyperparams override

        Returns:
            ML info dictionary
        """
        ml_info = {}

        # Try to load from file
        ml_config_path = "ml_config.yml"
        if os.path.exists(ml_config_path):
            file_ml_info = MLInfoConfigLoader.load_ml_info_from_file(ml_config_path)
            ml_info.update(file_ml_info)
            logger.info(f"Loaded ML info from {ml_config_path}")

        # Try to load from context
        if hasattr(self.context, "get_pipeline_ml_config"):
            context_ml_info = MLInfoConfigLoader.load_ml_info_from_context(
                self.context,
                pipeline_name,
            )
            ml_info = MLInfoConfigLoader.merge_ml_info(ml_info, context_ml_info)
            logger.info(f"Loaded ML info from context for {pipeline_name}")

        # Apply overrides
        if model_version:
            ml_info["model_version"] = model_version

        if hyperparams:
            current_hyperparams = ml_info.get("hyperparams", {})
            if isinstance(current_hyperparams, dict):
                current_hyperparams.update(hyperparams)
            else:
                current_hyperparams = hyperparams
            ml_info["hyperparams"] = current_hyperparams

        return ml_info
