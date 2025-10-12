"""
Copyright (c) 2025 Faustino Lopez Ramos.
For licensing information, see the LICENSE file in the project root
"""
from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, List, Optional, Protocol

from loguru import logger  # type: ignore

try:
    from pyspark.sql import DataFrame as SparkDataFrame  # type: ignore
    from pyspark.sql.connect.dataframe import DataFrame as ConnectDataFrame  # type: ignore
except ImportError:
    SparkDataFrame = type("SparkDataFrame", (), {})
    ConnectDataFrame = type("ConnectDataFrame", (), {})

try:
    import pandas as pd  # type: ignore
except ImportError:
    pd = None

try:
    import polars as pl  # type: ignore
except ImportError:
    pl = None

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


@dataclass
class PathComponents:
    """Path components for output configuration."""

    table_name: str
    schema: str
    sub_folder: str = ""

    def __post_init__(self):
        if not self.table_name.strip() or not self.schema.strip():
            raise ConfigurationError("table_name and schema cannot be empty")
        self.table_name = self.table_name.strip()
        self.schema = self.schema.strip()
        self.sub_folder = self.sub_folder.strip()


@dataclass
class UnityCatalogConfig:
    """Configuration for Unity Catalog operations."""

    catalog_name: str
    schema: str
    table_name: str
    uc_table_mode: str = "external"
    optimize: bool = True
    vacuum: bool = False
    vacuum_retention_hours: Optional[int] = None
    write_mode: str = WriteMode.OVERWRITE.value
    overwrite_schema: bool = True
    partition_col: Optional[str] = None
    description: Optional[str] = None
    options: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        if self.uc_table_mode not in ["external", "managed"]:
            raise ConfigurationError(f"Invalid uc_table_mode: {self.uc_table_mode}")


class DataFrameWriter(Protocol):
    """Protocol for DataFrame writers."""

    def write(self, df: Any, path: str, config: Dict[str, Any]) -> None:
        ...


def is_cloud_path(path: str) -> bool:
    """Check if path is a cloud storage path."""
    return any(str(path).startswith(prefix) for prefix in CLOUD_URI_PREFIXES)


def join_cloud_path(*parts: str) -> str:
    """Join cloud storage paths properly."""
    cleaned = [p.strip() for p in parts if p and str(p).strip()]
    if not cleaned:
        return ""

    head = cleaned[0].rstrip("/")
    tail = [p.strip("/") for p in cleaned[1:]]

    return head + ("/" + "/".join(tail) if tail else "")


def parse_iso_datetime(value: str) -> datetime:
    """Parse ISO datetime with proper error handling."""
    try:
        return datetime.fromisoformat(value)
    except ValueError as e:
        raise ConfigurationError(f"Invalid ISO-8601 date: '{value}'") from e


def validate_date_range(start_date: str, end_date: str) -> tuple[datetime, datetime]:
    """Validate and parse date range."""
    start = parse_iso_datetime(start_date)
    end = parse_iso_datetime(end_date)
    if start > end:
        raise ConfigurationError("start_date must be <= end_date")
    return start, end


class DataFrameManager(BaseIO):
    """Unified DataFrame operations manager."""

    def __init__(self, context: Any):
        super().__init__(context)

    def convert_to_spark(self, df: Any, schema: Any = None):
        """Convert any DataFrame to Spark DataFrame."""
        if not self._spark_available():
            raise WriteOperationError("Spark session unavailable")

        if self._is_spark_dataframe(df):
            return df

        spark = self._ctx_spark()

        if pd and isinstance(df, pd.DataFrame):
            logger.info("Converting pandas DataFrame to Spark")
            return (
                spark.createDataFrame(df, schema=schema)
                if schema
                else spark.createDataFrame(df)
            )

        if pl and isinstance(df, pl.DataFrame):
            logger.info("Converting Polars DataFrame to Spark")
            try:
                if hasattr(df, "to_arrow"):
                    arrow_table = df.to_arrow()
                    pdf = arrow_table.to_pandas()
                else:
                    pdf = df.to_pandas()
                return (
                    spark.createDataFrame(pdf, schema=schema)
                    if schema
                    else spark.createDataFrame(pdf)
                )
            except Exception as e:
                raise WriteOperationError(f"Polars conversion failed: {e}") from e

        raise ConfigurationError(f"Unsupported DataFrame type: {type(df)}")

    def _is_spark_dataframe(self, df: Any) -> bool:
        """Detect Spark DataFrames (classic and Connect)."""
        if isinstance(df, (SparkDataFrame, ConnectDataFrame)):
            return True

        module = getattr(type(df), "__module__", "")
        if "pyspark.sql" in module:
            return hasattr(df, "schema") and (
                hasattr(df, "write") or hasattr(df, "toPandas")
            )

        return False


class PathManager(BaseIO):
    """Manages path resolution and validation."""

    def __init__(self, context: Any, config_validator: ConfigValidator):
        super().__init__(context)
        self.config_validator = config_validator

    def resolve_output_path(
        self, dataset_config: Dict[str, Any], out_key: str, env: Optional[str] = None
    ) -> str:
        """Resolve complete output path."""
        components = self._extract_components(dataset_config, out_key)
        base_path = self._get_base_path()
        return self._build_path(base_path, components, env)

    def _extract_components(
        self, dataset_config: Dict[str, Any], out_key: str
    ) -> PathComponents:
        """Extract and validate path components."""
        parsed_key = self.config_validator.validate_output_key(out_key)

        return PathComponents(
            table_name=dataset_config.get("table_name", parsed_key["table_name"]),
            schema=dataset_config.get("schema", parsed_key["schema"]),
            sub_folder=dataset_config.get(
                "sub_folder", parsed_key.get("sub_folder", "")
            ),
        )

    def _get_base_path(self) -> str:
        """Get base output path from context."""
        output_path = self._ctx_get("output_path")
        if not output_path:
            raise ConfigurationError("output_path not configured")
        return str(output_path)

    def _build_path(
        self, base_path: str, components: PathComponents, env: Optional[str] = None
    ) -> str:
        """Build final path from components."""
        execution_mode = self._get_execution_mode()
        should_include_env = (
            execution_mode in ("local", "databricks", "distributed") and env
        )

        parts = []
        if should_include_env:
            parts.append(env)

        parts.append(components.schema)
        if components.sub_folder:
            parts.append(components.sub_folder)
        parts.append(components.table_name)

        if is_cloud_path(base_path):
            return join_cloud_path(base_path, *parts)
        return str(Path(base_path).joinpath(*parts))


class SqlSafetyMixin:
    """Mixin for SQL safety operations."""

    @staticmethod
    def quote_identifier(name: str) -> str:
        """Quote SQL identifier safely."""
        if not name or not isinstance(name, str):
            raise ConfigurationError("Invalid identifier")
        return f"`{name.replace('`', '``')}`"

    @staticmethod
    def escape_string(value: str) -> str:
        """Escape SQL string literal."""
        return str(value).replace("'", "''")

    def quote_table_name(self, full_name: str) -> str:
        """Quote full table name (catalog.schema.table)."""
        parts = [p.strip() for p in str(full_name).split(".") if p.strip()]
        if len(parts) != 3:
            raise ConfigurationError(f"Expected 3 parts in table name: {full_name}")
        return ".".join(self.quote_identifier(p) for p in parts)


class UnityCatalogManager(BaseIO, SqlSafetyMixin):
    """Simplified Unity Catalog operations."""

    def __init__(self, context: Any):
        super().__init__(context)
        self._enabled = self._check_enabled()

    def _check_enabled(self) -> bool:
        """Check if Unity Catalog is enabled."""
        spark = self._ctx_spark()
        if not spark:
            return False
        return (
            spark.conf.get("spark.databricks.unityCatalog.enabled", "false").lower()
            == "true"
        )

    def is_enabled(self) -> bool:
        """Public check for UC availability."""
        return self._enabled

    @lru_cache(maxsize=32)
    def _catalog_exists(self, catalog: str) -> bool:
        """Check catalog existence with caching."""
        if not self._spark_available():
            return False

        try:
            spark = self._ctx_spark()
            escaped = self.escape_string(catalog)
            result = spark.sql(
                f"SELECT 1 FROM system.information_schema.catalogs "
                f"WHERE catalog_name = '{escaped}' LIMIT 1"
            )
            return result.count() > 0
        except Exception as e:
            logger.error(f"Error checking catalog {catalog}: {e}")
            return False

    @lru_cache(maxsize=128)
    def _schema_exists(self, catalog: str, schema: str) -> bool:
        """Check schema existence with caching."""
        if not self._spark_available():
            return False

        try:
            spark = self._ctx_spark()
            cat_escaped = self.escape_string(catalog)
            sch_escaped = self.escape_string(schema)
            result = spark.sql(
                f"SELECT 1 FROM system.information_schema.schemata "
                f"WHERE catalog_name = '{cat_escaped}' AND schema_name = '{sch_escaped}' LIMIT 1"
            )
            return result.count() > 0
        except Exception as e:
            logger.error(f"Error checking schema {catalog}.{schema}: {e}")
            return False

    def ensure_schema_exists(
        self,
        catalog: str,
        schema: str,
        location: Optional[str] = None,
        managed: bool = False,
    ) -> None:
        """Ensure catalog and schema exist."""
        if not self._spark_available():
            logger.warning("Spark unavailable for schema creation")
            return

        spark = self._ctx_spark()

        if not self._catalog_exists(catalog):
            quoted_cat = self.quote_identifier(catalog)
            spark.sql(f"CREATE CATALOG {quoted_cat}")
            logger.info(f"Created catalog: {catalog}")
            self._catalog_exists.cache_clear()

        if not self._schema_exists(catalog, schema):
            quoted_cat = self.quote_identifier(catalog)
            quoted_sch = self.quote_identifier(schema)

            sql = f"CREATE SCHEMA IF NOT EXISTS {quoted_cat}.{quoted_sch}"
            if location:
                location_type = "MANAGED LOCATION" if managed else "LOCATION"
                escaped_loc = self.escape_string(location)
                sql += f" {location_type} '{escaped_loc}'"

            spark.sql(sql)
            logger.info(f"Created schema: {catalog}.{schema}")
            self._schema_exists.cache_clear()

    def write_managed_table(
        self, df: any, full_table_name: str, config: UnityCatalogConfig
    ) -> None:
        """Write as managed UC table."""
        quoted_name = self.quote_table_name(full_table_name)

        writer = (
            df.write.format("delta")
            .mode(config.write_mode)
            .option("overwriteSchema", str(config.overwrite_schema).lower())
        )

        for k, v in config.options.items():
            writer = writer.option(k, v)

        writer.saveAsTable(quoted_name)
        logger.info(f"Created managed table: {full_table_name}")

    def write_external_table(
        self, df: any, full_table_name: str, location: str, config: UnityCatalogConfig
    ) -> None:
        """Write as external UC table."""
        writer_config = {
            "format": "delta",
            "write_mode": config.write_mode,
            "overwrite_schema": config.overwrite_schema,
            "options": config.options,
        }

        from tauro.io.factories import WriterFactory

        writer = WriterFactory(self._context).get_writer("delta")
        writer.write(df, location, writer_config)

        quoted_name = self.quote_table_name(full_table_name)
        escaped_loc = self.escape_string(location)

        self._ctx_spark().sql(
            f"""
            CREATE TABLE IF NOT EXISTS {quoted_name}
            USING DELTA
            LOCATION '{escaped_loc}'
        """
        )
        logger.info(f"Created external table: {full_table_name}")

    def post_write_operations(
        self,
        full_table_name: str,
        config: UnityCatalogConfig,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> None:
        """Execute post-write operations."""
        if not self._spark_available():
            return

        spark = self._ctx_spark()
        quoted_name = self.quote_table_name(full_table_name)

        self._add_comment_if_needed(spark, quoted_name, full_table_name, config)
        self._optimize_if_needed(
            spark, quoted_name, full_table_name, config, start_date, end_date
        )
        self._vacuum_if_needed(spark, quoted_name, full_table_name, config)

    def _add_comment_if_needed(
        self,
        spark,
        quoted_name: str,
        full_table_name: str,
        config: UnityCatalogConfig,
    ) -> None:
        """Add comment to table if description or partition_col is provided."""
        if not (config.description or config.partition_col):
            return

        comment = f"{config.description or 'Data table'}. Partition: {config.partition_col or 'N/A'}"
        escaped_comment = self.escape_string(comment)
        try:
            spark.sql(f"COMMENT ON TABLE {quoted_name} IS '{escaped_comment}'")
            logger.info(f"Added comment to {full_table_name}")
        except Exception as e:
            logger.error(f"Error adding comment: {e}")

    def _optimize_if_needed(
        self,
        spark,
        quoted_name: str,
        full_table_name: str,
        config: UnityCatalogConfig,
        start_date: Optional[str],
        end_date: Optional[str],
    ) -> None:
        """Optimize table for the given partition range if requested."""
        if not (config.optimize and config.partition_col and start_date and end_date):
            return

        try:
            quoted_col = self.quote_identifier(config.partition_col)
            start_esc = self.escape_string(start_date)
            end_esc = self.escape_string(end_date)
            sql = f"OPTIMIZE {quoted_name} WHERE {quoted_col} BETWEEN '{start_esc}' AND '{end_esc}'"
            spark.sql(sql)
            logger.info(f"Optimized table {full_table_name}")
        except Exception as e:
            logger.error(f"Error optimizing table: {e}")

    def _vacuum_if_needed(
        self,
        spark,
        quoted_name: str,
        full_table_name: str,
        config: UnityCatalogConfig,
    ) -> None:
        """Run VACUUM on table if requested or retention configured."""
        if not (config.vacuum or config.vacuum_retention_hours):
            return

        hours = max(
            MIN_VACUUM_RETENTION_HOURS,
            config.vacuum_retention_hours or DEFAULT_VACUUM_RETENTION_HOURS,
        )
        try:
            spark.sql(f"VACUUM {quoted_name} RETAIN {hours} HOURS")
            logger.info(f"Vacuumed table {full_table_name}")
        except Exception as e:
            logger.error(f"Error vacuuming table: {e}")


class DataOutputManager(BaseIO):
    """Simplified output manager with unified interface."""

    def __init__(self, context: Dict[str, Any]):
        super().__init__(context)
        self.df_manager = DataFrameManager(context)
        self.path_manager = PathManager(context, self.config_validator)
        self.uc_manager = UnityCatalogManager(context)
        self.writer_factory = WriterFactory(context)
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
        """Main entry point for saving output data."""
        if not self._is_spark_dataframe(df):
            df = self.df_manager.convert_to_spark(df)

        self.data_validator.validate_dataframe(df, allow_empty=True)

        if hasattr(df, "isEmpty") and df.isEmpty():
            logger.warning("Empty DataFrame, skipping write")
            return

        output_keys = self._get_output_keys(node)
        fail_on_error = self._ctx_get("global_settings", {}).get("fail_on_error", True)

        for out_key in output_keys:
            try:
                self._save_single_output(out_key, df, start_date, end_date, env)
            except Exception as e:
                logger.error(f"Error saving output '{out_key}': {e}")
                if fail_on_error:
                    raise

        if model_version:
            try:
                self._save_model_artifacts(node, model_version)
            except Exception as e:
                logger.error(f"Error saving model artifacts: {e}")
                if fail_on_error:
                    raise

    def _save_single_output(
        self,
        out_key: str,
        df: any,
        start_date: Optional[str],
        end_date: Optional[str],
        env: str,
    ) -> None:
        """Save a single output configuration."""
        output_config = self._ctx_get("output_config", {}).get(out_key)
        if not output_config:
            raise ConfigurationError(f"Output configuration '{out_key}' not found")

        if (
            output_config.get("format") == SupportedFormats.UNITY_CATALOG.value
            and self.uc_manager.is_enabled()
        ):
            self._write_unity_catalog(
                df, output_config, start_date, end_date, out_key, env
            )
        else:
            self._write_traditional(df, output_config, out_key, env)

    def _write_unity_catalog(
        self,
        df: any,
        config: Dict[str, Any],
        start_date: Optional[str],
        end_date: Optional[str],
        out_key: str,
        env: str,
    ) -> None:
        """Write to Unity Catalog."""
        parsed = self.config_validator.validate_output_key(out_key)

        uc_config = UnityCatalogConfig(
            catalog_name=config["catalog_name"].format(environment=env),
            schema=config.get("schema", parsed["schema"]).format(environment=env),
            table_name=config.get("table_name", parsed["table_name"]).format(
                environment=env
            ),
            uc_table_mode=config.get("uc_table_mode", "external"),
            write_mode=config.get("write_mode", WriteMode.OVERWRITE.value),
            overwrite_schema=config.get("overwrite_schema", True),
            partition_col=config.get("partition_col"),
            description=config.get("description"),
            optimize=config.get("optimize", True),
            vacuum=config.get("vacuum", False),
            vacuum_retention_hours=config.get("vacuum_retention_hours"),
            options=config.get("options", {}),
        )

        if config.get("overwrite_strategy", "").lower() == "replacewhere":
            if not (start_date and end_date):
                raise ConfigurationError(
                    "replaceWhere requires start_date and end_date"
                )
            validate_date_range(start_date, end_date)
            uc_config.options[
                "replaceWhere"
            ] = f"{uc_config.partition_col} BETWEEN '{start_date}' AND '{end_date}'"

        full_table_name = (
            f"{uc_config.catalog_name}.{uc_config.schema}.{uc_config.table_name}"
        )

        base_location = config.get("output_path") or self._ctx_get("output_path", "")
        self.uc_manager.ensure_schema_exists(
            uc_config.catalog_name,
            uc_config.schema,
            location=base_location,
            managed=bool(base_location),
        )

        start = time.time()
        try:
            if uc_config.uc_table_mode == "managed":
                self.uc_manager.write_managed_table(df, full_table_name, uc_config)
            else:
                table_location = join_cloud_path(
                    base_location,
                    env,
                    parsed.get("schema"),
                    parsed.get("sub_folder", ""),
                    uc_config.table_name,
                )
                self.uc_manager.write_external_table(
                    df, full_table_name, table_location, uc_config
                )

            logger.info(f"UC write completed in {time.time() - start:.2f}s")

            self.uc_manager.post_write_operations(
                full_table_name, uc_config, start_date, end_date
            )

        except Exception as e:
            raise WriteOperationError(f"Unity Catalog write failed: {e}") from e

    def _write_traditional(
        self, df: any, config: Dict[str, Any], out_key: str, env: Optional[str] = None
    ) -> None:
        """Write to traditional storage."""
        path = self.path_manager.resolve_output_path(config, out_key, env)

        if not is_cloud_path(path):
            Path(path).parent.mkdir(parents=True, exist_ok=True)

        format_name = config.get("format", "").lower()
        if format_name not in {"delta", "parquet", "csv", "json", "orc"}:
            raise ConfigurationError(f"Unsupported format: {format_name}")

        writer = self.writer_factory.get_writer(format_name)

        start = time.time()
        writer.write(df, path, config)
        logger.info(f"Traditional write completed in {time.time() - start:.2f}s")

    def _save_model_artifacts(self, node: Dict[str, Any], model_version: str) -> None:
        """Save model artifacts to registry."""
        registry_path = self._ctx_get("global_settings", {}).get("model_registry_path")
        if not registry_path:
            logger.warning("Model registry path not configured")
            return

        for artifact in node.get("model_artifacts", []):
            if not (artifact and isinstance(artifact, dict) and artifact.get("name")):
                continue

            try:
                artifact_path = Path(registry_path) / artifact["name"] / model_version
                if not is_cloud_path(str(artifact_path)):
                    artifact_path.mkdir(parents=True, exist_ok=True)

                metadata = {
                    "artifact": artifact["name"],
                    "version": model_version,
                    "node": node.get("name"),
                    "saved_at": datetime.now(timezone.utc)
                    .isoformat()
                    .replace("+00:00", "Z"),
                }

                (artifact_path / "metadata.json").write_text(
                    json.dumps(metadata, indent=2), encoding="utf-8"
                )
                logger.info(f"Artifact '{artifact['name']}' saved to: {artifact_path}")

            except Exception as e:
                logger.error(
                    f"Error saving artifact {artifact.get('name', 'unknown')}: {e}"
                )

    def _get_output_keys(self, node: Dict[str, Any]) -> List[str]:
        """Get and validate output keys from node."""
        output = node.get("output", [])
        if isinstance(output, str):
            output = [output]

        if not isinstance(output, list):
            raise ConfigurationError("node['output'] must be string or list of strings")

        result = []
        seen = set()
        for key in output:
            if not isinstance(key, str) or not key.strip():
                raise ConfigurationError(f"Invalid output key: {key}")
            key = key.strip()
            if key not in seen:
                seen.add(key)
                result.append(key)

        return result

    def _is_spark_dataframe(self, df: Any) -> bool:
        """Check if object is a Spark DataFrame."""
        return self.df_manager._is_spark_dataframe(df)
