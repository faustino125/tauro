"""
Copyright (c) 2025 Faustino Lopez Ramos. 
For licensing information, see the LICENSE file in the project root
"""
from typing import Any, Dict, List, Optional, Union

from loguru import logger  # type: ignore

from core.io.constants import DEFAULT_CSV_OPTIONS, WriteMode
from core.io.exceptions import ConfigurationError, WriteOperationError
from core.io.validators import ConfigValidator, DataValidator

DESTINATION_EMPTY_ERROR = "Destination path cannot be empty"


def normalize_partition_config(config: Dict[str, Any]) -> Dict[str, Any]:
    """
    Normalize partition configuration keys.
    """
    val = config.get("partition")
    if val is None:
        val = config.get("partition_col")
    if val is None:
        val = config.get("partition_columns")

    if val is not None:
        config["partition"] = val
        config["partition_col"] = (
            val[0] if isinstance(val, (list, tuple)) and len(val) == 1 else val
        )
        config["partition_columns"] = val if isinstance(val, list) else [val]
    return config


class SparkWriterMixin:
    """Mixin for Spark-based writers with enhanced write mode and schema handling."""

    def _configure_spark_writer(self, df: Any, config: Dict[str, Any]) -> Any:
        """Configure Spark DataFrame writer with write mode and schema options."""
        try:
            data_validator = DataValidator()
            data_validator.validate_dataframe(df)

            config = normalize_partition_config(config)

            write_mode = self._determine_write_mode(config)
            writer = df.write.format(self._get_format()).mode(write_mode)
            logger.debug(f"Writer configured with mode: {write_mode}")

            writer = self._apply_partition(writer, df, config, data_validator)
            writer = self._apply_overwrite_and_replacewhere(writer, config, write_mode)

            extra_options = config.get("options", {})
            for key, value in extra_options.items():
                writer = writer.option(key, value)
                logger.debug(f"Extra option applied: {key}={value}")

            return writer
        except Exception as e:
            raise WriteOperationError(f"Failed to configure Spark writer: {e}") from e

    def _determine_write_mode(self, config: Dict[str, Any]) -> str:
        """Determine the write mode."""
        write_mode = config.get("write_mode", WriteMode.OVERWRITE.value)
        valid_modes = [mode.value for mode in WriteMode]
        if write_mode not in valid_modes:
            logger.warning(
                f"Invalid write mode '{write_mode}'. Using '{WriteMode.OVERWRITE.value}'.",
            )
            return WriteMode.OVERWRITE.value
        return write_mode

    def _apply_partition(
        self,
        writer: Any,
        df: Any,
        config: Dict[str, Any],
        data_validator: DataValidator,
    ) -> Any:
        """Apply partitioning if configured and columns exist."""
        partition_columns = config.get("partition")
        if not partition_columns:
            return writer

        if isinstance(partition_columns, str):
            partition_columns = [partition_columns]
        elif not isinstance(partition_columns, list):
            raise ConfigurationError(
                f"Partition columns must be str or list, not {type(partition_columns)}"
            ) from None

        # Warn about potential over-partitioning
        MAX_RECOMMENDED_PARTITIONS = 5
        if len(partition_columns) > MAX_RECOMMENDED_PARTITIONS:
            logger.warning(
                f"Partitioning by {len(partition_columns)} columns may cause excessive small files. "
                f"Consider reducing to {MAX_RECOMMENDED_PARTITIONS} or fewer columns for better performance. "
                f"Current partitions: {partition_columns}"
            )

        data_validator.validate_columns_exist(df, partition_columns)
        writer = writer.partitionBy(*partition_columns)
        logger.debug(f"partitionBy applied: {partition_columns}")
        return writer

    def _apply_overwrite_and_replacewhere(
        self, writer: Any, config: Dict[str, Any], write_mode: str
    ) -> Any:
        """Apply overwriteSchema and replaceWhere when appropriate."""
        overwrite_schema = bool(
            config.get("overwrite_schema", self._get_default_overwrite_schema())
        )
        if overwrite_schema and self._supports_overwrite_schema():
            writer = writer.option("overwriteSchema", "true")
            logger.debug("overwriteSchema=true applied")

        if (
            str(config.get("overwrite_strategy", "")).lower() == "replacewhere"
            and write_mode == WriteMode.OVERWRITE.value
        ):
            writer = self._apply_replace_where_strategy(writer, config)

        return writer

    def _apply_replace_where_strategy(self, writer: Any, config: Dict[str, Any]) -> Any:
        """Apply replaceWhere or replace_predicate for Delta format."""
        if self._get_format() != "delta":
            raise ConfigurationError(
                "overwrite_strategy=replaceWhere is supported only for Delta"
            ) from None

        config = normalize_partition_config(config)
        predicate = config.get("replace_predicate")
        if predicate:
            writer = writer.option("replaceWhere", predicate).option("overwriteSchema", "false")
            logger.debug(f"replaceWhere applied with custom predicate: {predicate}")
            return writer

        partition_col = config.get("partition_col")
        start_date = config.get("start_date")
        end_date = config.get("end_date")
        if not all([partition_col, start_date, end_date]):
            missing = [
                k
                for k, v in {
                    "partition_col": partition_col,
                    "start_date": start_date,
                    "end_date": end_date,
                }.items()
                if not v
            ]
            raise ConfigurationError(f"replaceWhere requires: {', '.join(missing)}") from None

        cfg_validator = ConfigValidator()
        if not (
            cfg_validator.validate_date_format(start_date)
            and cfg_validator.validate_date_format(end_date)
        ):
            raise ConfigurationError(
                f"Invalid date format: {start_date} - {end_date}. Expected: YYYY-MM-DD"
            ) from None

        predicate = f"{partition_col} BETWEEN '{start_date}' AND '{end_date}'"
        writer = writer.option("replaceWhere", predicate).option("overwriteSchema", "false")
        logger.debug(f"replaceWhere applied: {predicate}")
        return writer

    def _get_format(self) -> str:
        """Get the writer format derived from the writer class name."""
        return self.__class__.__name__.replace("Writer", "").lower()

    def _supports_overwrite_schema(self) -> bool:
        """Return whether the format supports overwriteSchema."""
        return self._get_format() in ["delta", "parquet"]

    def _get_default_overwrite_schema(self) -> bool:
        """Default value for overwriteSchema option."""
        return self._get_format() == "delta"


class DeltaWriter(SparkWriterMixin):
    """Delta Lake writer with advanced partition and selective replace support."""

    def __init__(self, context: Any):
        self.context = context

    def write(self, df, destination: str, config: dict) -> None:
        """Write data to Delta Lake: full batch, incremental, or selective writes."""
        try:
            config = normalize_partition_config(config)
            writer = self._configure_spark_writer(df, config)
            options = config.get("options", {}) or {}
            for k, v in options.items():
                writer = writer.option(k, v)
            writer.save(destination)
        except ConfigurationError as e:
            raise WriteOperationError(f"Error configuring Spark writer: {e}") from e
        except Exception as e:
            raise WriteOperationError(f"Delta write error: {e}") from e


class ParquetWriter(SparkWriterMixin):
    """Writer for Parquet format."""

    def __init__(self, context: Any):
        self.context = context

    def write(self, data: Any, destination: str, config: Dict[str, Any]) -> None:
        """Write Parquet data to destination."""
        if not destination:
            raise ConfigurationError(DESTINATION_EMPTY_ERROR) from None
        config = normalize_partition_config(config)
        try:
            writer = self._configure_spark_writer(data, config)
            logger.info(f"Writing Parquet data to: {destination}")
            writer.save(destination)
            logger.success(f"Parquet data written successfully to: {destination}")
        except WriteOperationError:
            raise
        except Exception as e:
            raise WriteOperationError(f"Failed to write Parquet to {destination}: {e}") from e


class CSVWriter(SparkWriterMixin):
    """Writer for CSV format."""

    def __init__(self, context: Any):
        self.context = context

    def write(self, data: Any, destination: str, config: Dict[str, Any]) -> None:
        """Write CSV data to destination."""
        if not destination:
            raise ConfigurationError(DESTINATION_EMPTY_ERROR) from None
        config = normalize_partition_config(config)
        try:
            writer = self._configure_spark_writer(data, config)
            csv_options = {
                **DEFAULT_CSV_OPTIONS,
                "quote": '"',
                "escape": '"',
                **config.get("options", {}),
            }
            for key, value in csv_options.items():
                writer = writer.option(key, value)
            logger.info(f"Writing CSV data to: {destination}")
            writer.save(destination)
            logger.success(f"CSV data written successfully to: {destination}")
        except WriteOperationError:
            raise
        except Exception as e:
            raise WriteOperationError(f"Failed to write CSV to {destination}: {e}") from e


class JSONWriter(SparkWriterMixin):
    """Writer for JSON format."""

    def __init__(self, context: Any):
        self.context = context

    def write(self, data: Any, destination: str, config: Dict[str, Any]) -> None:
        """Write JSON data to destination."""
        if not destination:
            raise ConfigurationError(DESTINATION_EMPTY_ERROR) from None
        config = normalize_partition_config(config)
        try:
            writer = self._configure_spark_writer(data, config)
            logger.info(f"Writing JSON data to: {destination}")
            writer.save(destination)
            logger.success(f"JSON data written successfully to: {destination}")
        except WriteOperationError:
            raise
        except Exception as e:
            raise WriteOperationError(f"Failed to write JSON to {destination}: {e}") from e


class ORCWriter(SparkWriterMixin):
    """Writer for ORC format."""

    def __init__(self, context: Any):
        self.context = context

    def write(self, data: Any, destination: str, config: Dict[str, Any]) -> None:
        """Write ORC data to destination."""
        if not destination:
            raise ConfigurationError(DESTINATION_EMPTY_ERROR) from None
        config = normalize_partition_config(config)
        try:
            writer = self._configure_spark_writer(data, config)
            logger.info(f"Writing ORC data to: {destination}")
            writer.save(destination)
            logger.success(f"ORC data written successfully to: {destination}")
        except WriteOperationError:
            raise
        except Exception as e:
            raise WriteOperationError(f"Failed to write ORC to {destination}: {e}") from e
