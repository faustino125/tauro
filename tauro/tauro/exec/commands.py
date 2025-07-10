from abc import ABC, abstractmethod
from typing import Any, Callable, Dict, List, Optional

from loguru import logger  # type: ignore


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
        function: Callable,
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
    """Command implementation for executing a machine learning node in a pipeline."""

    def __init__(
        self,
        function: Callable,
        input_dfs: List[Any],
        start_date: str,
        end_date: str,
        node_name: str,
        model_version: str,
        hyperparams: Optional[Dict[str, Any]] = None,
        spark=None,
    ):
        super().__init__(function, input_dfs, start_date, end_date, node_name)
        self.model_version = model_version
        self.hyperparams = hyperparams or {}
        self.spark = spark

    def execute(self) -> Any:
        """Execute the ML node function with the specified parameters."""
        if self.spark:
            self._configure_spark_parameters()

        logger.info(
            f"Executing ML node '{self.node_name}' with model version: {self.model_version}"
        )
        if self.hyperparams:
            logger.debug(f"Using hyperparameters: {self.hyperparams}")

        return super().execute()

    def _configure_spark_parameters(self) -> None:
        """Configure ML-related parameters in the Spark session."""
        if self.spark is None:
            logger.warning(
                "Spark session not available. Skipping parameter configuration."
            )
            return

        try:
            for param_name, param_value in self.hyperparams.items():
                self.spark.conf.set(f"ml.hyperparams.{param_name}", str(param_value))
            self.spark.conf.set("ml.model_version", self.model_version)
            logger.debug("Spark ML parameters configured successfully")
        except Exception as e:
            logger.error(f"Failed to configure Spark parameters: {str(e)}")
            raise
