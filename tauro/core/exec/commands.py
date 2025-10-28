"""
Copyright (c) 2025 Faustino Lopez Ramos. 
For licensing information, see the LICENSE file in the project root
"""
import json
import random
import time
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, Dict, List, Optional, Protocol

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
            result = self.function(
                *self.input_dfs, start_date=self.start_date, end_date=self.end_date
            )
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
        model_version: Optional[str],
        hyperparams: Optional[Dict[str, Any]] = None,
        node_config: Optional[Dict[str, Any]] = None,
        pipeline_config: Optional[Dict[str, Any]] = None,
        spark=None,
    ):
        super().__init__(function, input_dfs, start_date, end_date, node_name)

        self.model_version = model_version or "unknown"
        self.hyperparams = hyperparams or {}
        self.node_config = node_config or {}
        self.pipeline_config = pipeline_config or {}
        self.spark = spark

        self.node_hyperparams = self.node_config.get("hyperparams", {}) or {}
        self.metrics = self.node_config.get("metrics", []) or []
        self.description = self.node_config.get("description", "") or ""

        self.merged_hyperparams = self._merge_hyperparams()

        self.execution_metadata: Dict[str, Any] = {
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

            logger.success(f"ML node '{self.node_name}' executed successfully in {duration:.2f}s")
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
            params = sig.parameters
            accepts_ml_context = "ml_context" in params
            accepts_kwargs = any(p.kind == inspect.Parameter.VAR_KEYWORD for p in params.values())

            if accepts_ml_context or accepts_kwargs:
                logger.debug(
                    "Function supports ML context (explicit or **kwargs) - passing enhanced parameters"
                )
                return self.function(
                    *self.input_dfs,
                    start_date=self.start_date,
                    end_date=self.end_date,
                    ml_context=ml_context,
                )
            else:
                logger.debug("Function does not accept ml_context - calling standard signature")
                return self.function(
                    *self.input_dfs,
                    start_date=self.start_date,
                    end_date=self.end_date,
                )

        except Exception as e:
            logger.warning(
                f"Error analyzing function signature, falling back to standard execution: {e}"
            )
            return self.function(
                *self.input_dfs,
                start_date=self.start_date,
                end_date=self.end_date,
            )

    def _merge_hyperparams(self) -> Dict[str, Any]:
        """Merge hyperparameters from pipeline and node levels."""
        merged: Dict[str, Any] = {}
        merged.update(self.hyperparams or {})
        merged.update(self.node_hyperparams or {})
        return merged

    def _configure_spark_parameters(self) -> None:
        """Configure ML-related parameters in the Spark session."""
        if self.spark is None:
            logger.warning("Spark session not available. Skipping parameter configuration.")
            return

        try:
            if not hasattr(self.spark, "conf"):
                logger.debug("Spark session has no conf attribute; skipping spark param config")
                return

            for param_name, param_value in self.merged_hyperparams.items():
                key = f"tauro.ml.{param_name}"
                try:
                    self.spark.conf.set(key, str(param_value))
                except Exception:
                    logger.debug(f"Failed to set Spark conf {key}={param_value}", exc_info=True)
        except Exception as e:
            logger.debug(
                f"Unexpected error while configuring Spark parameters: {e}",
                exc_info=True,
            )

    def _log_execution_summary(self) -> None:
        """Log a concise execution summary for the ML node."""
        meta = self.execution_metadata
        status = meta.get("status", "unknown")
        duration = meta.get("duration_seconds", None)
        logger.info(
            f"ML node '{self.node_name}' status={status}, duration={duration}s, "
            f"model_version={self.model_version}"
        )


class ExperimentCommand(Command):
    """Hyperparameter optimization or experimentation command."""

    def __init__(self, objective_func, space, n_calls: int = 20, random_state=None):
        self.objective_func = objective_func
        self.space = space
        self.n_calls = n_calls
        self.random_state = random_state or random.randint(1, 1_000_000)

    def execute(self) -> Any:
        """Execute the experiment (e.g., Bayesian optimization)."""
        try:
            from skopt import gp_minimize  # type: ignore
        except Exception as e:
            raise RuntimeError(
                "skopt is required for ExperimentCommand. Install with: pip install scikit-optimize"
            ) from e

        return gp_minimize(
            self.objective_func,
            self.space,
            n_calls=self.n_calls,
            random_state=self.random_state,
        )
