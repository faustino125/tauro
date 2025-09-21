"""
Copyright (c) 2025 Faustino Lopez Ramos. 
For licensing information, see the LICENSE file in the project root
"""
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

from loguru import logger  # type: ignore

try:
    from pyspark.sql import DataFrame  # type: ignore
except Exception:  # pragma: no cover

    class DataFrame:  # type: ignore
        pass


try:
    import pandas as pd  # type: ignore
except Exception:  # pragma: no cover
    pd = None  # type: ignore

try:
    import polars as pl  # type: ignore
except Exception:  # pragma: no cover
    pl = None  # type: ignore

from tauro.io.base import BaseIO
from tauro.io.constants import (
    DEFAULT_VACUUM_RETENTION_HOURS,
    MIN_VACUUM_RETENTION_HOURS,
    SupportedFormats,
    WriteMode,
    CLOUD_URI_PREFIXES,
)
from tauro.io.exceptions import ConfigurationError, WriteOperationError
from tauro.io.factories import WriterFactory
from tauro.io.validators import ConfigValidator, DataValidator


class DataFrameConverter(BaseIO):
    """Handles DataFrame conversion between different types."""

    def __init__(self, context: Any):
        super().__init__(context)

    def convert_to_spark(self, df: Any) -> DataFrame:
        """Convert any compatible DataFrame to Spark DataFrame."""
        if not self._spark_available():
            raise WriteOperationError("Spark session unavailable for conversion")

        spark = self._ctx_spark()
        try:
            if self._is_spark_dataframe(df):
                logger.debug(f"DataFrame is already a Spark DataFrame: {type(df)}")
                return df
            elif pd is not None and isinstance(df, pd.DataFrame):
                logger.info("Converting pandas DataFrame to Spark")
                return spark.createDataFrame(df)
            elif pl is not None and isinstance(df, pl.DataFrame):
                logger.info("Converting Polars DataFrame to Spark via pandas")
                return spark.createDataFrame(df.to_pandas())
            else:
                raise ConfigurationError(f"Unsupported DataFrame type: {type(df)}")
        except Exception as e:
            logger.error(f"DataFrame conversion failed: {str(e)}")
            raise WriteOperationError(f"DataFrame conversion failed: {e}") from e

    def _is_spark_dataframe(self, df: Any) -> bool:
        """Check if DataFrame is a Spark DataFrame without hard dependency on pyspark."""
        try:
            return isinstance(df, DataFrame)  # funciona si pyspark está disponible
        except Exception:
            return (
                hasattr(df, "sparkSession")
                and hasattr(df, "schema")
                and hasattr(df, "write")
            )


class PathResolver(BaseIO):
    """Handles path resolution for different environments."""

    def __init__(self, context: Any, config_validator: ConfigValidator):
        super().__init__(context)
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
        output_path = self._ctx_get("output_path")
        if not output_path:
            raise ConfigurationError("Context does not have output_path configured")
        return str(output_path)

    def _build_final_path(
        self, base_path: str, components: Dict[str, str]
    ) -> Union[Path, str]:
        """Build final path joining base and components."""
        schema = components["schema"]
        sub_folder = components["sub_folder"]
        table_name = components["table_name"]

        if base_path.startswith(CLOUD_URI_PREFIXES):
            return f"{base_path.rstrip('/')}/{schema}/{sub_folder}/{table_name}"
        return Path(base_path) / schema / sub_folder / table_name


class DataWriter(BaseIO):
    """Enhanced DataWriter using factory pattern."""

    def __init__(self, context: Dict[str, Any]):
        """Initialize the DataWriter."""
        super().__init__(context)
        self.writer_factory = WriterFactory(context)
        self.data_validator = DataValidator()

    def write_data(self, df: Any, path: str, config: Dict[str, Any]) -> None:
        """Write data to traditional storage systems (multiplataforma)."""
        if not path:
            raise ConfigurationError("Output path cannot be empty")

        self._validate_write_config(config)
        path_str = str(path)
        is_remote = any(path_str.startswith(pfx) for pfx in CLOUD_URI_PREFIXES)

        # Only prepare local directories for local filesystem paths
        if not is_remote:
            self._prepare_local_directory(path_str)

        try:
            format_name = config["format"].lower()
            writer = self.writer_factory.get_writer(format_name)
            writer.write(df, path_str, config)
        except Exception as e:
            logger.error(f"Error saving to {path_str}: {str(e)}")
            raise WriteOperationError(f"Failed to write data: {e}") from e

    def _validate_write_config(self, config: Dict[str, Any]) -> None:
        """Validate basic write configuration."""
        self._validate_config(config, ["format"], "write operation")

        format_name = config.get("format", "").lower()
        writable_formats = {"delta", "parquet", "csv", "json", "orc"}
        if format_name not in writable_formats:
            raise ConfigurationError(
                f"Unsupported format: {format_name}. Supported formats: {sorted(writable_formats)}"
            )
        if "options" in config and not isinstance(config["options"], dict):
            raise ConfigurationError("Write 'options' must be a dictionary")


class ModelArtifactManager(BaseIO):
    """Enhanced ModelArtifactManager with better validation."""

    def __init__(self, context: Dict[str, Any]):
        """Initialize the ModelArtifactManager."""
        super().__init__(context)

    def save_model_artifacts(self, node: Dict[str, Any], model_version: str) -> None:
        """Save model artifacts to the model registry."""
        import json
        from datetime import datetime, timezone

        self._validate_inputs(node, model_version)

        global_settings = self._ctx_get("global_settings", {}) or {}
        model_registry_path = global_settings.get("model_registry_path")
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

                metadata = {
                    "artifact": artifact.get("name"),
                    "version": model_version,
                    "node": node.get("name"),
                    "saved_at": datetime.now(timezone.utc)
                    .isoformat()
                    .replace("+00:00", "Z"),
                }
                (artifact_path / "metadata.json").write_text(
                    json.dumps(metadata, ensure_ascii=False, indent=2), encoding="utf-8"
                )

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

        artifact_name = str(artifact.get("name", "")).strip()
        if not artifact_name:
            raise ConfigurationError("Artifact name cannot be empty")

        return Path(base_path) / artifact_name / version

    def _create_artifact_directory(self, path: Path) -> None:
        """Create the artifact directory if it doesn't exist (local mode)."""
        self._prepare_local_directory(str(path))
        try:
            path.mkdir(parents=True, exist_ok=True)
        except Exception as e:
            raise WriteOperationError(
                f"Failed to create artifact directory {path}: {e}"
            ) from e


class UnityCatalogOperations(BaseIO):
    """Handles Unity Catalog specific operations."""

    def __init__(self, context: Any, config_validator: ConfigValidator):
        super().__init__(context)
        self.config_validator = config_validator
        self._unity_catalog_enabled = self._check_unity_catalog_support()

    def _check_unity_catalog_support(self) -> bool:
        """Verify if Unity Catalog is enabled."""
        spark = self._ctx_spark()
        return bool(
            spark
            and str(
                spark.conf.get("spark.databricks.unityCatalog.enabled", "false")
            ).lower()
            == "true"
        )

    def is_enabled(self) -> bool:
        """Public helper to check if Unity Catalog is enabled."""
        return self._unity_catalog_enabled

    def _quote_identifier(self, name: str) -> str:
        if not isinstance(name, str) or not name:
            raise ConfigurationError("Invalid identifier")
        safe = name.replace("`", "``")
        return f"`{safe}`"

    def _quote_full_name(self, full_table_name: str) -> str:
        """Quote a full name like catalog.schema.table safely."""
        parts = [p for p in str(full_table_name).split(".") if p]
        if len(parts) != 3:
            return self._quote_identifier(full_table_name)
        return ".".join(self._quote_identifier(p) for p in parts)

    def _escape_sql_string(self, s: str) -> str:
        """Escape single quotes in SQL string literals."""
        return str(s).replace("'", "''")

    def ensure_schema_exists(
        self, catalog: str, schema: str, output_path_override: Optional[str] = None
    ) -> None:
        """Ensure catalog and schema exist."""
        if not self._spark_available():
            logger.warning("Spark not available for schema creation")
            return
        spark = self._ctx_spark()
        try:
            q_catalog = self._quote_identifier(catalog)
            q_schema = self._quote_identifier(schema)

            spark.sql(f"CREATE CATALOG IF NOT EXISTS {q_catalog}")
            if output_path_override:
                base = str(output_path_override).rstrip("/")
                location = f"{base}/{schema}"
                loc_escaped = self._escape_sql_string(location)
                spark.sql(
                    f"CREATE SCHEMA IF NOT EXISTS {q_catalog}.{q_schema} LOCATION '{loc_escaped}'"
                )
            else:
                spark.sql(f"CREATE SCHEMA IF NOT EXISTS {q_catalog}.{q_schema}")
            logger.info(f"Verified/created {catalog}.{schema}")
        except Exception as e:
            logger.error(f"Error ensuring schema {catalog}.{schema}: {e}")
            raise

    def optimize_table(
        self, full_table_name: str, partition_col: str, start_date: str, end_date: str
    ) -> None:
        """Optimize table for specified date range."""
        if not self._spark_available():
            logger.warning("Spark not available for table optimization")
            return

        logger.info(f"Optimizing table {full_table_name}")
        try:
            spark = self._ctx_spark()
            q_full = self._quote_full_name(full_table_name)
            q_col = self._quote_identifier(partition_col)
            spark.sql(
                f"""
                OPTIMIZE {q_full}
                WHERE {q_col} BETWEEN '{self._escape_sql_string(start_date)}' AND '{self._escape_sql_string(end_date)}'
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
            spark = self._ctx_spark()
            q_full = self._quote_full_name(full_table_name)
            spark.sql(f"COMMENT ON TABLE {q_full} IS '{comment}'")
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
            spark = self._ctx_spark()
            q_full = self._quote_full_name(full_table_name)
            spark.sql(f"VACUUM {q_full} RETAIN {hours} HOURS")
            logger.info(f"VACUUM completed on {full_table_name}")
        except Exception as e:
            logger.error(f"Error executing VACUUM on {full_table_name}: {str(e)}")


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
        env: str,
    ) -> None:
        """Write data to Unity Catalog following best practices."""
        if not self.uc_operations.is_enabled():
            raise ConfigurationError("Unity Catalog is not enabled")

        self.data_validator.validate_dataframe(df, allow_empty=True)
        if hasattr(df, "isEmpty") and df.isEmpty():
            logger.warning("Empty DataFrame. No data will be written to Unity Catalog.")
            return

        parsed = self._parse_output_key(out_key)
        self._validate_uc_config(config)

        catalog = config["catalog_name"].format(environment=env)
        schema = config.get("schema", parsed["schema"]).format(environment=env)
        table_name = config.get("table_name", parsed["table_name"]).format(
            environment=env
        )
        full_table_name = f"{catalog}.{schema}.{table_name}"

        base_output = config.get("output_path") or self._ctx_get("output_path", "")
        storage_location = f"{base_output}/{schema}" if base_output else ""

        try:
            self.uc_operations.ensure_schema_exists(catalog, schema, base_output)
            writer_config = self._prepare_writer_config(config, start_date, end_date)
            writer = self.writer_factory.get_writer("delta")
            self._execute_write_operation(
                writer,
                df,
                table_name,
                full_table_name,
                writer_config,
                storage_location,
                parsed["sub_folder"],
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
            "partition_col": config.get("partition_col"),
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
        if not table_name or not full_table_name:
            raise ConfigurationError("Incomplete configuration to determine table name")

        if not storage_location:
            raise ConfigurationError(
                "Storage location cannot be empty for Unity Catalog writes"
            )

        destination_path = f"{storage_location.rstrip('/')}/{sub_folder}/{table_name}"
        logger.info(f"Writing Delta data to path: {destination_path}")
        writer.write(df, destination_path, config)

        try:
            spark = self._ctx_spark()
            q_full = self.uc_operations._quote_full_name(full_table_name)
            loc_escaped = self.uc_operations._escape_sql_string(destination_path)
            spark.sql(
                f"""
                CREATE TABLE IF NOT EXISTS {q_full}
                USING DELTA
                LOCATION '{loc_escaped}'
                """
            )
            logger.info(
                f"Ensured table exists and is linked to location: {full_table_name}"
            )
        except Exception as e:
            logger.error(f"Error creating/ensuring table {full_table_name}: {e}")
            raise

    def _post_write_operations(
        self,
        config: Dict[str, Any],
        full_table_name: str,
        start_date: Optional[str],
        end_date: Optional[str],
    ) -> None:
        """Execute post-write operations like comments, optimize and vacuum."""
        description = config.get("description")
        partition_col = config.get("partition_col")

        self.uc_operations.add_table_comment(
            full_table_name, description, partition_col
        )

        should_optimize = config.get("optimize", True)
        if should_optimize and partition_col and start_date and end_date:
            self.uc_operations.optimize_table(
                full_table_name, partition_col, start_date, end_date
            )

        retention_hours = config.get("vacuum_retention_hours")
        if retention_hours is not None or config.get("vacuum", False):
            self.uc_operations.execute_vacuum(full_table_name, retention_hours)


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
        env: str,
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
        global_settings = self._ctx_get("global_settings", {}) or {}
        error_handler = ErrorHandler(global_settings.get("fail_on_error", True))

        for out_key in out_keys or []:
            error_handler.execute_with_error_handling(
                lambda out_key=out_key: self._save_single_output(
                    out_key, df, start_date, end_date, env
                ),
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
        """Get output keys from a node with validation."""
        keys = node.get("output", [])
        if isinstance(keys, str):
            keys = [keys]
        elif not isinstance(keys, list):
            return []

        result: List[str] = []
        for key in keys:
            if not isinstance(key, str) or not key.strip():
                raise ConfigurationError(
                    f"Invalid output key: {key}. All output keys must be non-empty strings."
                )
            result.append(key.strip())
        return result

    def _save_single_output(
        self,
        out_key: str,
        df: Any,
        start_date: Optional[str],
        end_date: Optional[str],
        env: str,
    ) -> None:
        """Save a single output."""
        if not out_key:
            raise ConfigurationError("Output key cannot be empty")

        output_cfg = self._ctx_get("output_config", {}) or {}
        dataset_config = output_cfg.get(out_key)
        if not dataset_config:
            raise ConfigurationError(f"Output configuration '{out_key}' not found")

        if (
            dataset_config.get("format") == SupportedFormats.UNITY_CATALOG.value
            and not self.unity_catalog_manager.uc_operations.is_enabled()
        ):
            raise ConfigurationError(
                "Unity Catalog is configured for this output but not enabled in Spark. "
                "Set spark.databricks.unityCatalog.enabled=true or change the format."
            )

        try:
            if self._should_use_unity_catalog(dataset_config):
                self.unity_catalog_manager.write_to_unity_catalog(
                    df, dataset_config, start_date, end_date, out_key, env
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
            and self.unity_catalog_manager.uc_operations.is_enabled()
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
