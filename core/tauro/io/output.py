import re
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import pandas as pd  # type: ignore
import polars as pl  # type: ignore
from loguru import logger  # type: ignore
from pyspark.sql import DataFrame  # type: ignore

from tauro.io.base import BaseIO
from tauro.io.constants import (
    DEFAULT_VACUUM_RETENTION_HOURS,
    MIN_VACUUM_RETENTION_HOURS,
    SupportedFormats,
    WriteMode,
)
from tauro.io.exceptions import ConfigurationError, WriteOperationError
from tauro.io.factories import WriterFactory
from tauro.io.validators import ConfigValidator, DataValidator


class UnityCatalogOperations:
    """Handles Unity Catalog specific operations."""

    def __init__(self, context: Any, config_validator: ConfigValidator):
        self.context = context
        self.config_validator = config_validator
        self._unity_catalog_enabled = self._check_unity_catalog_support()

    def _check_unity_catalog_support(self) -> bool:
        """Verify if Unity Catalog is enabled."""
        return (
            self._spark_available()
            and self.context.spark.conf.get(
                "spark.databricks.unityCatalog.enabled", "false"
            ).lower()
            == "true"
        )

    def _spark_available(self) -> bool:
        """Check if Spark is available."""
        return hasattr(self.context, "spark") and self.context.spark is not None

    def optimize_table(
        self, full_table_name: str, partition_col: str, start_date: str, end_date: str
    ) -> None:
        """Optimize table for specified date range."""
        if not self._spark_available():
            logger.warning("Spark not available for table optimization")
            return

        logger.info(f"Optimizing table {full_table_name}")
        try:
            self.context.spark.sql(
                f"""
                OPTIMIZE {full_table_name}
                WHERE {partition_col} BETWEEN '{start_date}' AND '{end_date}'
            """
            )
            logger.info(f"Table {full_table_name} optimized successfully")
        except Exception as e:
            logger.error(f"Error optimizing table: {str(e)}")

    def add_table_comment(
        self,
        full_table_name: str,
        description: Optional[str],
        partition_col: Optional[str],
    ) -> None:
        """Add descriptive comment to table."""
        if not self._spark_available():
            return

        safe_description = (description or "Data table").replace("'", "''")
        comment = f"{safe_description}. Partition: {partition_col or 'N/A'}"

        try:
            self.context.spark.sql(f"COMMENT ON TABLE {full_table_name} IS '{comment}'")
            logger.info(f"Comment added to table {full_table_name}")
        except Exception as e:
            logger.error(f"Error adding comment: {str(e)}")

    def execute_vacuum(
        self, full_table_name: str, retention_hours: Optional[int] = None
    ) -> None:
        """Execute VACUUM to clean up old versions."""
        if not self._spark_available():
            return

        hours = max(
            MIN_VACUUM_RETENTION_HOURS,
            retention_hours or DEFAULT_VACUUM_RETENTION_HOURS,
        )
        logger.info(f"Executing VACUUM on {full_table_name}")

        try:
            self.context.spark.sql(f"VACUUM {full_table_name} RETAIN {hours} HOURS")
            logger.info(f"VACUUM completed on {full_table_name}")
        except Exception as e:
            logger.error(f"Error executing VACUUM: {str(e)}")

    def ensure_schema_exists(
        self, catalog: str, schema: str, managed_location: Optional[str] = None
    ) -> None:
        """Ensure schema exists in Unity Catalog."""
        if not self._unity_catalog_enabled:
            logger.warning("Unity Catalog is not enabled. Cannot create schema.")
            return

        if not catalog or not schema:
            raise ConfigurationError("Catalog and schema cannot be empty")

        create_sql = f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}"
        if managed_location:
            create_sql += f" MANAGED LOCATION '{managed_location}'"

        try:
            logger.info(f"Verifying schema: {catalog}.{schema}")
            self.context.spark.sql(create_sql)
            logger.info(f"Schema {catalog}.{schema} created/verified successfully")
        except Exception as e:
            logger.error(f"Error creating schema {catalog}.{schema}: {str(e)}")
            raise WriteOperationError(f"Failed to create schema: {e}") from e


class UnityCatalogManager(BaseIO):
    """Enhanced Unity Catalog Manager with better separation of concerns."""

    def __init__(self, context: Dict[str, Any]):
        """Initialize the Unity Catalog Manager."""
        super().__init__(context)
        self.data_validator = DataValidator()
        self.uc_operations = UnityCatalogOperations(context, self.config_validator)
        self.writer_factory = WriterFactory(context)

    def write_to_unity_catalog(
        self,
        df: Any,
        config: Dict[str, Any],
        start_date: Optional[str],
        end_date: Optional[str],
        out_key: str,
    ) -> None:
        """Write data to Unity Catalog following best practices."""
        self.data_validator.validate_dataframe(df, allow_empty=True)
        if hasattr(df, "isEmpty") and df.isEmpty():
            logger.warning("Empty DataFrame. No data will be written to Unity Catalog.")
            return

        parsed = self._parse_output_key(out_key)
        self._validate_uc_config(config)

        catalog = config["catalog_name"]
        schema = config.get("schema", parsed["schema"])
        sub_folder = config.get("sub_folder", parsed["sub_folder"])
        table_name = config.get("table_name", parsed["table_name"])
        full_table_name = f"{catalog}.{schema}.{table_name}"
        storage_location = f"{self.context.output_path}/{schema}"

        try:
            self.uc_operations.ensure_schema_exists(
                catalog, schema, config.get("output_path")
            )
            writer_config = self._prepare_writer_config(config, start_date, end_date)
            writer = self.writer_factory.get_writer("delta")
            self._execute_write_operation(
                writer,
                df,
                table_name,
                full_table_name,
                writer_config,
                storage_location,
                sub_folder,
            )
            self._post_write_operations(config, full_table_name, start_date, end_date)
        except Exception as e:
            logger.error(f"Error writing to Unity Catalog {full_table_name}: {str(e)}")
            raise WriteOperationError(f"Unity Catalog write failed: {e}") from e

    def _validate_uc_config(self, config: Dict[str, Any]) -> None:
        """Validate required configuration for Unity Catalog."""
        self._validate_config(
            config, ["table_name", "schema", "catalog_name"], "Unity Catalog"
        )

    def _prepare_writer_config(
        self,
        config: Dict[str, Any],
        start_date: Optional[str],
        end_date: Optional[str],
    ) -> Dict[str, Any]:
        """Prepare configuration for the writer."""
        writer_config = {
            "format": "delta",
            "write_mode": config.get("write_mode", WriteMode.OVERWRITE.value),
            "overwrite_schema": config.get("overwrite_schema", True),
            "partition": config.get("partition_col"),
            "options": config.get("options", {}),
        }

        if config.get("overwrite_strategy") == "replaceWhere":
            writer_config.update(
                {
                    "overwrite_strategy": "replaceWhere",
                    "partition_col": config.get("partition_col"),
                    "start_date": start_date,
                    "end_date": end_date,
                }
            )

        return writer_config

    def _execute_write_operation(
        self,
        writer: Any,
        df: Any,
        table_name: str,
        full_table_name: str,
        config: Dict[str, Any],
        storage_location: str,
        sub_folder: str,
    ) -> None:
        """Execute the data write operation to the table."""
        if not storage_location or not table_name:
            raise ConfigurationError("Incomplete configuration to determine path")

        path = f"{storage_location}/{sub_folder}/{table_name}"
        writer.write(df, path, config)
        logger.info(f"Data saved to: {path}")

        self.context.spark.sql(
            f"""
            CREATE TABLE IF NOT EXISTS {full_table_name}
            USING DELTA
            LOCATION '{path}'
        """
        )
        logger.success(f"Table {full_table_name} registered in Unity Catalog")

    def _post_write_operations(
        self,
        config: Dict[str, Any],
        full_table_name: str,
        start_date: Optional[str],
        end_date: Optional[str],
    ) -> None:
        """Execute operations after writing data."""
        try:
            if partition_col := config.get("partition_col"):
                if start_date and end_date:
                    self.uc_operations.optimize_table(
                        full_table_name, partition_col, start_date, end_date
                    )

                self.uc_operations.add_table_comment(
                    full_table_name, config.get("description"), partition_col
                )

            if config.get("vacuum", True):
                self.uc_operations.execute_vacuum(
                    full_table_name, config.get("vacuum_retention_hours")
                )
        except Exception as e:
            logger.warning(f"Error in post-write operations: {str(e)}")


class DataWriter(BaseIO):
    """Enhanced DataWriter using factory pattern."""

    def __init__(self, context: Dict[str, Any]):
        """Initialize the DataWriter."""
        super().__init__(context)
        self.writer_factory = WriterFactory(context)
        self.data_validator = DataValidator()

    def write_data(self, df: Any, path: str, config: Dict[str, Any]) -> None:
        """Write data to traditional storage systems."""
        if not path:
            raise ConfigurationError("Output path cannot be empty")

        self._validate_write_config(config)
        self._prepare_local_directory(path)

        try:
            format_name = config["format"].lower()
            writer = self.writer_factory.get_writer(format_name)
            writer.write(df, path, config)
        except Exception as e:
            logger.error(f"Error saving to {path}: {str(e)}")
            raise WriteOperationError(f"Failed to write data: {e}") from e

    def _validate_write_config(self, config: Dict[str, Any]) -> None:
        """Validate basic write configuration."""
        self._validate_config(config, ["format"], "write operation")

        format_name = config.get("format", "").lower()
        supported_formats = [
            fmt.value
            for fmt in SupportedFormats
            if fmt != SupportedFormats.UNITY_CATALOG
        ]

        if format_name not in supported_formats:
            raise ConfigurationError(
                f"Unsupported format: {format_name}. Supported formats: {supported_formats}"
            )


class ModelArtifactManager(BaseIO):
    """Enhanced ModelArtifactManager with better validation."""

    def __init__(self, context: Dict[str, Any]):
        """Initialize the ModelArtifactManager."""
        super().__init__(context)

    def save_model_artifacts(self, node: Dict[str, Any], model_version: str) -> None:
        """Save model artifacts to the model registry."""
        self._validate_inputs(node, model_version)

        model_registry_path = getattr(self.context, "global_settings", {}).get(
            "model_registry_path"
        )
        if not model_registry_path:
            logger.warning("Model registry path not configured")
            return

        for artifact in node.get("model_artifacts", []):
            if not self._is_valid_artifact(artifact):
                logger.warning("Invalid artifact found, skipping")
                continue

            try:
                artifact_path = self._build_artifact_path(
                    model_registry_path, artifact, model_version
                )
                self._create_artifact_directory(artifact_path)
                logger.info(
                    f"Artifact '{artifact.get('name', 'unnamed')}' saved to: {artifact_path}"
                )
            except Exception as e:
                logger.error(
                    f"Error saving artifact {artifact.get('name', 'unknown')}: {str(e)}"
                )

    def _validate_inputs(self, node: Dict[str, Any], model_version: str) -> None:
        """Validate input parameters."""
        if not node or not isinstance(node, dict):
            raise ConfigurationError("Node must be a valid dictionary")
        if not model_version:
            raise ConfigurationError("Model version cannot be empty")

    def _is_valid_artifact(self, artifact: Any) -> bool:
        """Check if artifact is valid."""
        return artifact and isinstance(artifact, dict) and artifact.get("name")

    def _build_artifact_path(
        self, base_path: str, artifact: Dict[str, Any], version: str
    ) -> Path:
        """Build the complete path for the artifact."""
        if not base_path:
            raise ConfigurationError("Model registry base path cannot be empty")

        artifact_name = artifact.get("name")
        if not artifact_name:
            raise ConfigurationError("Artifact must have a name")

        return Path(base_path) / artifact_name / version

    def _create_artifact_directory(self, artifact_path: Path) -> None:
        """Create artifact directory if it doesn't exist."""
        try:
            artifact_path.parent.mkdir(parents=True, exist_ok=True)
        except Exception as e:
            raise WriteOperationError(
                f"Failed to create artifact directory: {e}"
            ) from e


class DataFrameConverter:
    """Handles DataFrame conversion between different types."""

    def __init__(self, context: Any):
        self.context = context

    def convert_to_spark(self, df: Any) -> DataFrame:
        """Convert any compatible DataFrame to Spark DataFrame."""
        if not hasattr(self.context, "spark") or not self.context.spark:
            raise WriteOperationError("Spark session unavailable for conversion")

        try:
            if self._is_spark_dataframe(df):
                logger.debug(f"DataFrame is already a Spark DataFrame: {type(df)}")
                return df
            elif isinstance(df, pd.DataFrame):
                logger.info("Converting pandas DataFrame to Spark")
                return self.context.spark.createDataFrame(df)
            elif isinstance(df, pl.DataFrame):
                logger.info("Converting Polars DataFrame to Spark via pandas")
                return self.context.spark.createDataFrame(df.to_pandas())
            else:
                raise ConfigurationError(f"Unsupported DataFrame type: {type(df)}")
        except Exception as e:
            logger.error(f"DataFrame conversion failed: {str(e)}")
            raise WriteOperationError(f"DataFrame conversion failed: {e}") from e

    def _is_spark_dataframe(self, df: Any) -> bool:
        """Check if DataFrame is a Spark DataFrame."""
        return (
            hasattr(df, "sql_ctx")
            or hasattr(df, "sparkSession")
            or "pyspark.sql" in str(type(df))
        )


class PathResolver:
    """Handles path resolution for different environments."""

    def __init__(self, context: Any, config_validator: ConfigValidator):
        self.context = context
        self.config_validator = config_validator

    def resolve_output_path(
        self, dataset_config: Dict[str, Any], out_key: str
    ) -> Union[Path, str]:
        """Build paths compatible with multi-environment (Azure, AWS, GCP, local)."""
        self._validate_inputs(dataset_config, out_key)

        components = self._extract_path_components(dataset_config, out_key)
        base_path = self._get_base_path()

        return self._build_final_path(base_path, components)

    def _validate_inputs(self, dataset_config: Dict[str, Any], out_key: str) -> None:
        """Validate input parameters."""
        if not isinstance(dataset_config, dict):
            raise ConfigurationError("dataset_config must be a dictionary")
        if not out_key or not isinstance(out_key, str):
            raise ConfigurationError("out_key must be a non-empty string")

    def _extract_path_components(
        self, dataset_config: Dict[str, Any], out_key: str
    ) -> Dict[str, str]:
        """Extract path components from config and output key."""
        try:
            parsed_key = self.config_validator.validate_output_key(out_key)
            components = {
                "table_name": str(
                    dataset_config.get("table_name", parsed_key["table_name"])
                ).strip(),
                "schema": str(
                    dataset_config.get("schema", parsed_key["schema"])
                ).strip(),
                "sub_folder": str(
                    dataset_config.get("sub_folder", parsed_key["sub_folder"])
                ).strip(),
            }

            if not all(components.values()):
                raise ConfigurationError("Path components cannot be empty")

            return components
        except (KeyError, AttributeError) as e:
            raise ConfigurationError(f"Error parsing out_key: {str(e)}") from e

    def _get_base_path(self) -> str:
        """Get base output path from context."""
        if not hasattr(self.context, "output_path") or not self.context.output_path:
            raise ConfigurationError("Context does not have output_path configured")
        return self.context.output_path

    def _build_final_path(
        self, base_path: str, components: Dict[str, str]
    ) -> Union[Path, str]:
        """Build the final path based on the base path type."""
        path_parts = [
            components["schema"],
            components["sub_folder"],
            components["table_name"],
        ]

        if self._is_local_path(base_path):
            return self._build_local_path(base_path, path_parts)
        else:
            return self._build_cloud_path(base_path, path_parts)

    def _is_local_path(self, base_path: str) -> bool:
        """Check if base path is local or DBFS."""
        return isinstance(base_path, Path) or (
            isinstance(base_path, str) and not re.match(r"^[a-z0-9]+://", base_path)
        )

    def _build_local_path(self, base_path: str, path_parts: List[str]) -> Path:
        """Build local or DBFS path."""
        try:
            base = Path(base_path)
            # Special handling for DBFS in Databricks
            if str(base).startswith(("/dbfs", "dbfs:")):
                return Path("/dbfs") / "/".join(path_parts).lstrip("/")
            return base.joinpath(*path_parts)
        except Exception as e:
            raise ConfigurationError(f"Error building local path: {str(e)}") from e

    def _build_cloud_path(self, base_path: str, path_parts: List[str]) -> str:
        """Build cloud storage path."""
        base_str = str(base_path).rstrip("/")
        full_path = "/".join([p for p in path_parts if p])

        # Prevent double slashes
        separator = "" if base_str.endswith("/") else "/"
        return f"{base_str}{separator}{full_path}"


class OutputManager(BaseIO):
    """Enhanced OutputManager with better separation of concerns and dependency injection."""

    def __init__(self, context: Dict[str, Any]):
        """Initialize the Output Manager."""
        super().__init__(context)
        self.unity_catalog_manager = UnityCatalogManager(context)
        self.data_writer = DataWriter(context)
        self.model_artifact_manager = ModelArtifactManager(context)
        self.dataframe_converter = DataFrameConverter(context)
        self.path_resolver = PathResolver(context, self.config_validator)
        self.data_validator = DataValidator()

    def save_output(
        self,
        node: Dict[str, Any],
        df: Any,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        model_version: Optional[str] = None,
    ) -> None:
        """Save output data according to node configuration."""
        if not isinstance(df, DataFrame):
            df = self.dataframe_converter.convert_to_spark(df)

        self._validate_inputs(node, df)

        out_keys = self._get_output_keys(node)
        error_handler = ErrorHandler(
            self.context.global_settings.get("fail_on_error", True)
        )

        for out_key in out_keys or []:
            error_handler.execute_with_error_handling(
                lambda: self._save_single_output(out_key, df, start_date, end_date),
                f"Error saving output '{out_key}'",
            )

        if model_version:
            error_handler.execute_with_error_handling(
                lambda: self.model_artifact_manager.save_model_artifacts(
                    node, model_version
                ),
                "Error saving model artifacts",
            )

    def _validate_inputs(self, node: Dict[str, Any], df: Any) -> None:
        """Validate input parameters."""
        if not node:
            raise ConfigurationError("Parameter 'node' cannot be None")
        if not isinstance(node, dict):
            raise ConfigurationError(
                f"Parameter 'node' must be a dictionary, received: {type(node)}"
            )

        self.data_validator.validate(df)

    def _get_output_keys(self, node: Dict[str, Any]) -> List[str]:
        """Get output keys from a node."""
        keys = node.get("output", [])
        return (
            [keys]
            if isinstance(keys, str)
            else (keys if isinstance(keys, list) else [])
        )

    def _save_single_output(
        self, out_key: str, df: Any, start_date: Optional[str], end_date: Optional[str]
    ) -> None:
        """Save a single output."""
        if not out_key:
            raise ConfigurationError("Output key cannot be empty")

        dataset_config = self.context.output_config.get(out_key)
        if not dataset_config:
            raise ConfigurationError(f"Output configuration '{out_key}' not found")

        try:
            if self._should_use_unity_catalog(dataset_config):
                self.unity_catalog_manager.write_to_unity_catalog(
                    df, dataset_config, start_date, end_date, out_key
                )
            else:
                base_path = self.path_resolver.resolve_output_path(
                    dataset_config, out_key
                )
                self.data_writer.write_data(df, str(base_path), dataset_config)
        except Exception as e:
            logger.error(f"Error saving output '{out_key}': {str(e)}")
            raise WriteOperationError(f"Failed to save output '{out_key}': {e}") from e

    def _should_use_unity_catalog(self, dataset_config: Dict[str, Any]) -> bool:
        """Determine if Unity Catalog should be used for writing."""
        return (
            dataset_config
            and dataset_config.get("format") == SupportedFormats.UNITY_CATALOG.value
            and self.unity_catalog_manager.uc_operations._unity_catalog_enabled
        )


class ErrorHandler:
    """Handles error management and propagation."""

    def __init__(self, fail_on_error: bool = True):
        self.fail_on_error = fail_on_error

    def execute_with_error_handling(
        self, operation: callable, error_message: str
    ) -> None:
        """Execute operation with proper error handling."""
        try:
            operation()
        except Exception as e:
            logger.error(f"{error_message}: {str(e)}")
            if self.fail_on_error:
                raise
