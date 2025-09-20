from typing import Any, Dict

from loguru import logger  # type: ignore

from tauro.io.constants import DEFAULT_CSV_OPTIONS, WriteMode
from tauro.io.exceptions import ConfigurationError, WriteOperationError
from tauro.io.validators import ConfigValidator, DataValidator

DESTINATION_EMPTY_ERROR = "Destination path cannot be empty"


class SparkWriterMixin:
    """Mixin for Spark-based writers with enhanced write mode and schema handling."""

    def _configure_spark_writer(self, df: Any, config: Dict[str, Any]) -> Any:
        """Configure Spark DataFrame writer with write mode and schema options."""
        try:
            data_validator = DataValidator()
            data_validator.validate_dataframe(df)

            write_mode = self._determine_write_mode(config)
            writer = df.write.format(self._get_format()).mode(write_mode)
            logger.debug(f"Configured writer with mode: {write_mode}")

            writer = self._apply_partition(writer, df, config, data_validator)
            writer = self._apply_overwrite_and_replacewhere(writer, config, write_mode)

            extra_options = config.get("options", {})
            for key, value in extra_options.items():
                writer = writer.option(key, value)
                logger.debug(f"Applied option {key}={value}")

            return writer
        except Exception as e:
            raise WriteOperationError(f"Failed to configure Spark writer: {e}") from e

    def _determine_write_mode(self, config: Dict[str, Any]) -> str:
        """Determine and validate write mode from config, falling back to default."""
        try:
            write_mode = config.get("write_mode", WriteMode.OVERWRITE.value)
            valid_modes = [mode.value for mode in WriteMode]

            if write_mode not in valid_modes:
                logger.warning(
                    f"Invalid write mode '{write_mode}' specified. Available modes: {valid_modes}. "
                    f"Falling back to '{WriteMode.OVERWRITE.value}'."
                )
                return WriteMode.OVERWRITE.value
            return write_mode
        except Exception as e:
            raise ConfigurationError(f"Failed to determine write mode: {e}") from e

    def _apply_partition(
        self,
        writer: Any,
        df: Any,
        config: Dict[str, Any],
        data_validator: DataValidator,
    ) -> Any:
        """Apply partitioning to the writer if partition config is present."""
        try:
            partition_columns = config.get("partition")
            if not partition_columns:
                return writer

            if isinstance(partition_columns, str):
                partition_columns = [partition_columns]
            elif not isinstance(partition_columns, list):
                raise ConfigurationError(
                    f"Partition columns must be string or list, got: {type(partition_columns)}"
                ) from None

            data_validator.validate_columns_exist(df, partition_columns)
            writer = writer.partitionBy(*partition_columns)
            logger.debug(f"Applied partitionBy on columns: {partition_columns}")
            return writer
        except Exception as e:
            raise ConfigurationError(f"Failed to apply partitioning: {e}") from e

    def _apply_overwrite_and_replacewhere(
        self, writer: Any, config: Dict[str, Any], write_mode: str
    ) -> Any:
        """Handle overwrite schema option and replaceWhere overwrite strategy."""
        try:
            overwrite_schema = bool(
                config.get("overwrite_schema", self._get_default_overwrite_schema())
            )
            if overwrite_schema and self._supports_overwrite_schema():
                writer = writer.option("overwriteSchema", "true")
                logger.debug("Applied overwriteSchema=true")

            if (
                config.get("overwrite_strategy") == "replaceWhere"
                and write_mode == WriteMode.OVERWRITE.value
            ):
                writer = self._apply_replace_where_strategy(writer, config)

            return writer
        except Exception as e:
            raise ConfigurationError(f"Failed to apply overwrite options: {e}") from e

    def _apply_replace_where_strategy(self, writer: Any, config: Dict[str, Any]) -> Any:
        """Apply replaceWhere overwrite strategy with validation."""
        try:
            if self._get_format() != "delta":
                raise ConfigurationError(
                    "overwrite_strategy=replaceWhere is only supported for Delta format"
                ) from None

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
                raise ConfigurationError(
                    f"replaceWhere strategy requires: {', '.join(missing)}"
                ) from None

            cfg_validator = ConfigValidator()
            if not (
                cfg_validator.validate_date_format(start_date)
                and cfg_validator.validate_date_format(end_date)
            ):
                raise ConfigurationError(
                    f"Invalid date format for replaceWhere: {start_date} - {end_date}. "
                    "Expected format: YYYY-MM-DD"
                ) from None

            predicate = f"{partition_col} BETWEEN '{start_date}' AND '{end_date}'"
            writer = writer.option("replaceWhere", predicate).option(
                "overwriteSchema", "false"
            )
            logger.debug(f"Applied replaceWhere predicate: {predicate}")

            return writer
        except Exception as e:
            raise ConfigurationError(
                f"Failed to apply replaceWhere strategy: {e}"
            ) from e

    def _get_format(self) -> str:
        """Get format string for the writer."""
        return self.__class__.__name__.replace("Writer", "").lower()

    def _supports_overwrite_schema(self) -> bool:
        """Check if the format supports overwriteSchema option."""
        return self._get_format() in ["delta", "parquet"]

    def _get_default_overwrite_schema(self) -> bool:
        """Get default overwriteSchema value for the format."""
        return (
            self._get_format() == "delta"
        )  # Only Delta enables overwriteSchema by default


class DeltaWriter(SparkWriterMixin):
    """Writer for Delta format."""

    def __init__(self, context: Any):
        self.context = context

    def _validate_replacewhere_params(self, config: dict) -> None:
        """Ensure replaceWhere has partition_col, start_date, end_date."""
        if (
            not config.get("partition_col")
            or not config.get("start_date")
            or not config.get("end_date")
        ):
            raise ConfigurationError(
                "overwrite_strategy=replaceWhere requires partition_col, start_date and end_date"
            )

    def _apply_overwrite_and_replacewhere(self, writer, config: dict):
        """Apply write mode, overwriteSchema and (optionally) replaceWhere strategy."""
        try:
            mode = str(config.get("write_mode", "overwrite")).lower()
            writer = writer.mode(mode)

            if config.get("overwrite_schema", True):
                writer = writer.option("overwriteSchema", "true")
                logger.debug("Applied overwriteSchema=true")

            if str(config.get("overwrite_strategy", "")).lower() == "replacewhere":
                self._validate_replacewhere_params(config)
                partition_col = config["partition_col"]
                start_date = config["start_date"]
                end_date = config["end_date"]
                replace_expr = (
                    f"{partition_col} BETWEEN '{start_date}' AND '{end_date}'"
                )
                writer = writer.option("replaceWhere", replace_expr)
                logger.debug(f"Applied replaceWhere with expression: {replace_expr}")

            return writer
        except ConfigurationError:
            raise
        except Exception as e:
            raise ConfigurationError(f"Failed to apply overwrite options: {e}") from e

    def write(self, df, destination: str, config: dict) -> None:
        """Escritura Delta; ejemplo simplificado enfocándose en manejo de errores."""
        try:
            writer = df.write.format("delta")
            writer = self._apply_overwrite_and_replacewhere(writer, config)

            options = config.get("options", {}) or {}
            for k, v in options.items():
                writer = writer.option(k, v)

            writer.save(destination)
        except ConfigurationError as e:
            raise WriteOperationError(f"Failed to configure Spark writer: {e}") from e
        except Exception as e:
            raise WriteOperationError(f"Delta write failed: {e}") from e


class ParquetWriter(SparkWriterMixin):
    """Writer for Parquet format."""

    def __init__(self, context: Any):
        self.context = context

    def write(self, data: Any, destination: str, config: Dict[str, Any]) -> None:
        """Write Parquet data to destination."""
        if not destination:
            raise ConfigurationError(DESTINATION_EMPTY_ERROR) from None

        try:
            writer = self._configure_spark_writer(data, config)
            logger.info(f"Writing Parquet data to: {destination}")
            writer.save(destination)
            logger.success(f"Parquet data written successfully to: {destination}")
        except WriteOperationError:
            raise  # Re-raise WriteOperationError as-is
        except Exception as e:
            raise WriteOperationError(
                f"Failed to write Parquet to {destination}: {e}"
            ) from e


class CSVWriter(SparkWriterMixin):
    """Writer for CSV format."""

    def __init__(self, context: Any):
        self.context = context

    def write(self, data: Any, destination: str, config: Dict[str, Any]) -> None:
        """Write CSV data to destination."""
        if not destination:
            raise ConfigurationError(DESTINATION_EMPTY_ERROR) from None

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
            raise  # Re-raise WriteOperationError as-is
        except Exception as e:
            raise WriteOperationError(
                f"Failed to write CSV to {destination}: {e}"
            ) from e


class JSONWriter(SparkWriterMixin):
    """Writer for JSON format."""

    def __init__(self, context: Any):
        self.context = context

    def write(self, data: Any, destination: str, config: Dict[str, Any]) -> None:
        """Write JSON data to destination."""
        if not destination:
            raise ConfigurationError(DESTINATION_EMPTY_ERROR) from None

        try:
            writer = self._configure_spark_writer(data, config)
            logger.info(f"Writing JSON data to: {destination}")
            writer.save(destination)
            logger.success(f"JSON data written successfully to: {destination}")
        except WriteOperationError:
            raise  # Re-raise WriteOperationError as-is
        except Exception as e:
            raise WriteOperationError(
                f"Failed to write JSON to {destination}: {e}"
            ) from e


class ORCWriter(SparkWriterMixin):
    """Writer for ORC format."""

    def __init__(self, context: Any):
        self.context = context

    def write(self, data: Any, destination: str, config: Dict[str, Any]) -> None:
        """Write ORC data to destination."""
        if not destination:
            raise ConfigurationError(DESTINATION_EMPTY_ERROR) from None

        try:
            writer = self._configure_spark_writer(data, config)
            logger.info(f"Writing ORC data to: {destination}")
            writer.save(destination)
            logger.success(f"ORC data written successfully to: {destination}")
        except WriteOperationError:
            raise  # Re-raise WriteOperationError as-is
        except Exception as e:
            raise WriteOperationError(
                f"Failed to write ORC to {destination}: {e}"
            ) from e
