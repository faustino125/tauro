import json
import time
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Protocol

from loguru import logger  # type: ignore


class NodeFunction(Protocol):
    def __call__(self, *dfs: Any, start_date: str, end_date: str) -> Any:
        ...


class Command(ABC):
    """Abstract base class for command pattern implementation."""

    @abstractmethod
    def execute(self) -> Any:
        """Execute the command and return the result."""
        pass


class NodeCommand(Command):
    """Command implementation for executing a specific node in a data pipeline."""

    def __init__(
        self,
        function: NodeFunction,
        input_dfs: List[Any],
        start_date: str,
        end_date: str,
        node_name: str,
    ):
        self.function = function
        self.input_dfs = input_dfs
        self.start_date = start_date
        self.end_date = end_date
        self.node_name = node_name

    def execute(self) -> Any:
        """Execute the node function with the specified parameters."""
        logger.info(
            f"Executing node '{self.node_name}' with date range: {self.start_date} to {self.end_date}"
        )
        try:
            result = self.function(*self.input_dfs, self.start_date, self.end_date)
            logger.debug(f"Node '{self.node_name}' executed successfully")
            return result
        except Exception as e:
            logger.error(f"Error executing node '{self.node_name}': {str(e)}")
            raise


class MLNodeCommand(NodeCommand):
    """Enhanced command implementation for executing ML nodes with advanced features."""

    def __init__(
        self,
        function: NodeFunction,
        input_dfs: List[Any],
        start_date: str,
        end_date: str,
        node_name: str,
        model_version: str,
        hyperparams: Optional[Dict[str, Any]] = None,
        node_config: Optional[Dict[str, Any]] = None,
        pipeline_config: Optional[Dict[str, Any]] = None,
        spark=None,
    ):
        super().__init__(function, input_dfs, start_date, end_date, node_name)
        self.model_version = model_version
        self.hyperparams = hyperparams or {}
        self.node_config = node_config or {}
        self.pipeline_config = pipeline_config or {}
        self.spark = spark

        self.node_hyperparams = self.node_config.get("hyperparams", {})
        self.metrics = self.node_config.get("metrics", [])
        self.description = self.node_config.get("description", "")

        self.merged_hyperparams = self._merge_hyperparams()

        self.execution_metadata = {
            "node_name": self.node_name,
            "model_version": self.model_version,
            "start_time": None,
            "end_time": None,
            "duration_seconds": None,
            "hyperparams": self.merged_hyperparams,
            "metrics": self.metrics,
        }

    def execute(self) -> Any:
        """Execute the ML node function with enhanced ML capabilities."""
        self.execution_metadata["start_time"] = datetime.now().isoformat()
        start_time = time.time()

        try:
            if self.spark:
                self._configure_spark_parameters()

            logger.info(
                f"Executing ML node '{self.node_name}' with model version: {self.model_version}"
            )
            logger.info(f"Description: {self.description}")

            if self.merged_hyperparams:
                logger.info(
                    f"Using merged hyperparameters: {json.dumps(self.merged_hyperparams, indent=2)}"
                )

            if self.metrics:
                logger.info(f"Expected metrics: {', '.join(self.metrics)}")

            result = self._execute_with_ml_context()

            end_time = time.time()
            duration = end_time - start_time

            self.execution_metadata.update(
                {
                    "end_time": datetime.now().isoformat(),
                    "duration_seconds": round(duration, 2),
                    "status": "success",
                }
            )

            logger.success(
                f"ML node '{self.node_name}' executed successfully in {duration:.2f}s"
            )
            self._log_execution_summary()

            return result

        except Exception as e:
            self.execution_metadata.update(
                {
                    "end_time": datetime.now().isoformat(),
                    "duration_seconds": round(time.time() - start_time, 2),
                    "status": "failed",
                    "error": str(e),
                }
            )
            logger.error(f"Error executing ML node '{self.node_name}': {str(e)}")
            raise

    def _execute_with_ml_context(self) -> Any:
        """Execute function with ML-enhanced context."""
        ml_context = {
            "model_version": self.model_version,
            "hyperparams": self.merged_hyperparams,
            "node_config": self.node_config,
            "pipeline_config": self.pipeline_config,
            "execution_metadata": self.execution_metadata,
            "spark": self.spark,
        }

        try:
            import inspect

            sig = inspect.signature(self.function)

            if "ml_context" in sig.parameters:
                logger.debug(
                    "Function supports ML context - passing enhanced parameters"
                )
                return self.function(
                    *self.input_dfs,
                    self.start_date,
                    self.end_date,
                    ml_context=ml_context,
                )
            else:
                logger.debug("Function uses standard parameters")
                return self.function(*self.input_dfs, self.start_date, self.end_date)

        except Exception as e:
            logger.warning(
                f"Error analyzing function signature: {e}, falling back to standard execution"
            )
            return self.function(*self.input_dfs, self.start_date, self.end_date)

    def _merge_hyperparams(self) -> Dict[str, Any]:
        """Merge hyperparameters from pipeline and node levels."""
        merged = {}

        merged.update(self.hyperparams)

        merged.update(self.node_hyperparams)

        return merged

    def _configure_spark_parameters(self) -> None:
        """Configure ML-related parameters in the Spark session."""
        if self.spark is None:
            logger.warning(
                "Spark session not available. Skipping parameter configuration."
            )
            return

        try:
            for param_name, param_value in self.merged_hyperparams.items():
                config_key = f"ml.hyperparams.{param_name}"
                self.spark.conf.set(config_key, str(param_value))
                logger.debug(f"Set Spark config: {config_key} = {param_value}")

            self.spark.conf.set("ml.model_version", self.model_version)
            self.spark.conf.set("ml.node_name", self.node_name)
            self.spark.conf.set(
                "ml.execution_timestamp", self.execution_metadata["start_time"]
            )

            logger.debug("Spark ML parameters configured successfully")

        except Exception as e:
            logger.error(f"Failed to configure Spark parameters: {str(e)}")
            raise

    def _log_execution_summary(self) -> None:
        """Log comprehensive execution summary."""
        logger.info("=== ML Node Execution Summary ===")
        logger.info(f"Node: {self.node_name}")
        logger.info(f"Model Version: {self.model_version}")
        logger.info(f"Duration: {self.execution_metadata['duration_seconds']}s")
        logger.info(f"Status: {self.execution_metadata['status']}")

        if self.merged_hyperparams:
            logger.info("Hyperparameters used:")
            for key, value in self.merged_hyperparams.items():
                logger.info(f"  {key}: {value}")

        if self.metrics:
            logger.info(f"Tracked metrics: {', '.join(self.metrics)}")

        logger.info("=== End Summary ===")

    def get_execution_metadata(self) -> Dict[str, Any]:
        """Get execution metadata for tracking and monitoring."""
        return self.execution_metadata.copy()


class ExperimentCommand(MLNodeCommand):
    """Specialized command for ML experimentation and hyperparameter tuning."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.experiment_config = self.node_config.get("experiment", {})
        self.is_experiment = bool(self.experiment_config)

    def execute(self) -> Any:
        """Execute with experimentation tracking."""
        if self.is_experiment:
            logger.info(f"Running ML experiment for node '{self.node_name}'")
            return self._execute_experiment()
        else:
            return super().execute()

    def _execute_experiment(self) -> Any:
        """Execute with experiment tracking and multiple runs."""
        experiment_results = []

        param_grid = self._generate_parameter_grid()

        logger.info(f"Running {len(param_grid)} experiment configurations")

        for i, params in enumerate(param_grid):
            logger.info(f"Experiment run {i+1}/{len(param_grid)}")

            original_hyperparams = self.merged_hyperparams.copy()
            self.merged_hyperparams.update(params)

            try:
                result = super().execute()
                experiment_results.append(
                    {
                        "run_id": i + 1,
                        "parameters": params,
                        "result": result,
                        "metadata": self.get_execution_metadata(),
                    }
                )
            except Exception as e:
                logger.error(f"Experiment run {i+1} failed: {e}")
                experiment_results.append(
                    {
                        "run_id": i + 1,
                        "parameters": params,
                        "error": str(e),
                        "metadata": self.get_execution_metadata(),
                    }
                )
            finally:
                self.merged_hyperparams = original_hyperparams

        return {
            "experiment_results": experiment_results,
            "best_run": self._find_best_run(experiment_results),
        }

    def _generate_parameter_grid(self) -> List[Dict[str, Any]]:
        """Generate parameter grid for experimentation."""
        grid_params = {}

        for key, value in self.merged_hyperparams.items():
            if isinstance(value, list):
                grid_params[key] = value

        if not grid_params:
            return [{}]

        import itertools

        keys = grid_params.keys()
        values = grid_params.values()
        combinations = list(itertools.product(*values))

        return [dict(zip(keys, combo)) for combo in combinations]

    def _find_best_run(self, results: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        """Find the best performing run based on configured criteria."""
        successful_runs = [r for r in results if "error" not in r]

        if not successful_runs:
            return None

        return successful_runs[0]
