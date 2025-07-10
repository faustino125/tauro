from typing import Literal

from loguru import logger  # type: ignore


class SparkSessionFactory:
    """
    Factory for creating Spark sessions based on the execution mode.
    """

    @staticmethod
    def create_session(mode: Literal["local", "databricks"] = "databricks"):
        """Create a Spark session based on the specified mode."""
        logger.info(f"Attempting to create Spark session in {mode} mode")

        if mode.lower() == "databricks":
            return SparkSessionFactory._create_databricks_session()
        elif mode.lower() == "local":
            return SparkSessionFactory._create_local_session()
        else:
            raise ValueError(
                f"Invalid execution mode: {mode}. Use 'local' or 'databricks'."
            )

    @staticmethod
    def _create_databricks_session():
        """
        Create a Databricks Connect session for remote execution.
        """
        try:
            from databricks.connect import DatabricksSession  # type: ignore
            from databricks.sdk.core import Config  # type: ignore

            config = Config()

            SparkSessionFactory._validate_databricks_config(config)

            logger.info("Creating remote session with Databricks Connect")
            return DatabricksSession.builder.remote(
                host=config.host, token=config.token, cluster_id=config.cluster_id
            ).getOrCreate()

        except Exception as e:
            logger.error(f"Error connecting to Databricks: {str(e)}")
            logger.warning("Falling back to local mode")
            return SparkSessionFactory._create_local_session()

    @staticmethod
    def _create_local_session():
        """Create a local Spark session."""
        try:
            from pyspark.sql import SparkSession  # type: ignore

            logger.info("Initializing local Spark session")
            return (
                SparkSession.builder.appName("LocalSparkApplication")
                .config("spark.sql.execution.arrow.pyspark.enabled", "true")
                .config("spark.driver.memory", "4g")
                .master("local[*]")
                .getOrCreate()
            )
        except ImportError as e:
            logger.error(f"Could not import SparkSession: {str(e)}")
            raise ImportError(
                "Failed to create a local Spark session. Make sure PySpark is installed."
            ) from e

    @staticmethod
    def _validate_databricks_config(config) -> None:
        """
        Validate that the Databricks configuration is complete.
        """
        required_params = {
            "host": config.host,
            "token": config.token,
            "cluster_id": config.cluster_id,
        }

        missing_params = [
            param for param, value in required_params.items() if not value
        ]

        if missing_params:
            raise ValueError(
                f"Incomplete Databricks configuration. Missing: {', '.join(missing_params)}"
            )
