from typing import Any, Dict, Literal

from loguru import logger  # type: ignore


class SparkSessionFactory:
    """
    Factory for creating Spark sessions based on the execution mode with ML optimizations.
    """

    @staticmethod
    def create_session(
        mode: Literal["local", "databricks"] = "databricks",
        ml_config: Dict[str, Any] = None,
    ):
        """Create a Spark session based on the specified mode with ML configurations."""
        logger.info(f"Attempting to create Spark session in {mode} mode")

        if ml_config:
            logger.info("Applying ML-specific Spark configurations")

        if mode.lower() == "databricks":
            return SparkSessionFactory._create_databricks_session(ml_config)
        elif mode.lower() == "local":
            return SparkSessionFactory._create_local_session(ml_config)
        else:
            raise ValueError(
                f"Invalid execution mode: {mode}. Use 'local' or 'databricks'."
            )

    @staticmethod
    def _create_databricks_session(ml_config: Dict[str, Any] = None):
        """
        Create a Databricks Connect session for remote execution with ML configs.
        """
        try:
            from databricks.connect import DatabricksSession  # type: ignore
            from databricks.sdk.core import Config  # type: ignore

            config = Config()

            SparkSessionFactory._validate_databricks_config(config)

            logger.info("Creating remote session with Databricks Connect")
            builder = DatabricksSession.builder.remote(
                host=config.host, token=config.token, cluster_id=config.cluster_id
            )

            # Apply ML configurations
            if ml_config:
                builder = SparkSessionFactory._apply_ml_configs(builder, ml_config)

            return builder.getOrCreate()

        except Exception as e:
            logger.error(f"Error connecting to Databricks: {str(e)}")
            logger.warning("Falling back to local mode")
            return SparkSessionFactory._create_local_session(ml_config)

    @staticmethod
    def _create_local_session(ml_config: Dict[str, Any] = None):
        """Create a local Spark session with ML optimizations."""
        try:
            from pyspark.sql import SparkSession  # type: ignore

            logger.info("Initializing local Spark session")
            builder = (
                SparkSession.builder.appName("LocalSparkApplication")
                .config("spark.sql.execution.arrow.pyspark.enabled", "true")
                .config("spark.driver.memory", "4g")
                .master("local[*]")
            )

            # Apply default ML configurations
            default_ml_configs = {
                "spark.sql.adaptive.enabled": "true",
                "spark.sql.adaptive.coalescePartitions.enabled": "true",
                "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
                "spark.sql.adaptive.advisoryPartitionSizeInBytes": "128MB",
                "spark.sql.adaptive.skewJoin.enabled": "true",
            }

            for key, value in default_ml_configs.items():
                builder = builder.config(key, value)

            # Apply custom ML configurations
            if ml_config:
                builder = SparkSessionFactory._apply_ml_configs(builder, ml_config)

            return builder.getOrCreate()

        except ImportError as e:
            logger.error(f"Could not import SparkSession: {str(e)}")
            raise ImportError(
                "Failed to create a local Spark session. Make sure PySpark is installed."
            ) from e

    @staticmethod
    def _apply_ml_configs(builder, ml_config: Dict[str, Any]):
        """Apply ML-specific configurations to Spark builder, skipping protected ones."""
        PROTECTED_CONFIGS = [
            "spark.sql.shuffle.partitions",
            "spark.executor.memory",
            "spark.driver.memory",
            "spark.master",
        ]

        for key, value in ml_config.items():
            if key in PROTECTED_CONFIGS:
                logger.warning(f"Skipping protected Spark config: {key}")
                continue
            logger.debug(f"Setting Spark config: {key} = {value}")
            builder = builder.config(key, str(value))
        return builder

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
