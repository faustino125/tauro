"""
Copyright (c) 2025 Faustino Lopez Ramos. 
For licensing information, see the LICENSE file in the project root
"""
import sys
import os
from typing import Any, Dict, List, Literal, Optional
import threading

from loguru import logger  # type: ignore


class SparkSessionFactory:
    """
    Factory for creating Spark sessions based on the execution mode with ML optimizations.
    """

    _session = None
    _lock = threading.Lock()

    @classmethod
    def get_session(
        cls,
        mode: Literal["local", "databricks", "distributed"] = "databricks",
        ml_config: Optional[Dict[str, Any]] = None,
    ):
        """Singleton Spark session with thread-safe initialization"""
        with cls._lock:
            if cls._session is None:
                cls._session = SparkSessionFactory.create_session(mode, ml_config)
        return cls._session

    @classmethod
    def reset_session(cls):
        """Reset session for testing or reconfiguration"""
        if cls._session:
            try:
                cls._session.stop()
            except Exception:
                logger.warning("Error stopping Spark session during reset", exc_info=True)
        cls._session = None

    PROTECTED_CONFIGS = [
        "spark.sql.shuffle.partitions",
        "spark.executor.memory",
        "spark.driver.memory",
        "spark.master",
        "spark.submit.deployMode",
        "spark.dynamicAllocation.enabled",
        "spark.executor.instances",
    ]

    @classmethod
    def set_protected_configs(cls, configs: List[str]) -> None:
        """Set custom protected configurations"""
        cls.PROTECTED_CONFIGS = configs

    @staticmethod
    def create_session(
        mode: Literal["local", "databricks", "distributed"] = "databricks",
        ml_config: Optional[Dict[str, Any]] = None,
    ):
        """Create a Spark session based on the specified mode with ML configurations."""
        logger.info(f"Attempting to create Spark session in {mode} mode")

        if ml_config:
            logger.info("Applying ML-specific Spark configurations")

        normalized = str(mode).lower()
        if normalized in ("databricks", "distributed"):
            return SparkSessionFactory._create_databricks_session(ml_config)
        elif normalized == "local":
            return SparkSessionFactory._create_local_session(ml_config)
        else:
            raise ValueError(
                f"Invalid execution mode: {mode}. Use 'local', 'databricks' or 'distributed'."
            )

    @staticmethod
    def _create_databricks_session(ml_config: Optional[Dict[str, Any]] = None):
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

            if ml_config:
                builder = SparkSessionFactory._apply_ml_configs(builder, ml_config)

            return builder.getOrCreate()

        except ImportError as e:
            logger.error(f"Databricks Connect not installed: {str(e)}")
            raise
        except ValueError as e:
            logger.error(f"Invalid configuration: {str(e)}")
            raise
        except RuntimeError as e:
            logger.error(f"Connection failed: {str(e)}")
            raise
        except Exception as e:
            logger.critical(f"Unhandled exception: {str(e)}")
            raise RuntimeError("Critical error creating session") from e

    @staticmethod
    def _create_local_session(ml_config: Optional[Dict[str, Any]] = None):
        """Create a local Spark session with ML optimizations."""
        try:
            from pyspark.sql import SparkSession  # type: ignore

            os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
            builder = SparkSession.builder.appName("TauroLocal").master("local[*]")

            if ml_config and isinstance(ml_config, dict):
                apply_fn = getattr(SparkSessionFactory, "_apply_ml_configs", None)
                if callable(apply_fn):
                    builder = apply_fn(builder, ml_config)

            return builder.getOrCreate()
        except Exception:
            logger.error("Session creation failed", exc_info=True)
            raise

    @staticmethod
    def _validate_databricks_config(config) -> None:
        """Validate required Databricks configuration parameters."""
        required = ["host", "token", "cluster_id"]
        missing = [k for k in required if not getattr(config, k, None)]
        if missing:
            raise ValueError(f"Missing Databricks config values: {', '.join(missing)}")

    @staticmethod
    def _apply_ml_configs(builder: Any, ml_config: Dict[str, Any]) -> Any:
        """Apply ML-related configurations to the Spark builder."""
        protected = set(SparkSessionFactory.PROTECTED_CONFIGS or [])
        for k, v in (ml_config or {}).items():
            if k in protected:
                logger.warning(f"Skipping ML config '{k}' because it's in PROTECTED_CONFIGS")
                continue
            try:
                builder = builder.config(k, v)
            except Exception:
                logger.debug(f"Failed to apply Spark config {k}={v}", exc_info=True)
        return builder
