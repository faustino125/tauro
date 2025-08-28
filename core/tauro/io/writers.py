from typing import Any, Dict

from loguru import logger  # type: ignore

from tauro.io.constants import DEFAULT_CSV_OPTIONS, WriteMode
from tauro.io.exceptions import ConfigurationError, WriteOperationError
from tauro.io.factories import BaseWriter
from tauro.io.validators import ConfigValidator, DataValidator


class SparkWriterMixin:
    """Mixin for Spark-based writers with enhanced write mode and schema handling."""

    def _configure_spark_writer(self, df: Any, config: Dict[str, Any]) -> Any:
        """Configure Spark DataFrame writer with write mode and schema options."""
        data_validator = DataValidator()
        data_validator.validate_dataframe(df)

        write_mode = config.get("write_mode", WriteMode.OVERWRITE.value)
        if write_mode not in [mode.value for mode in WriteMode]:
            logger.warning(
                f"Invalid write mode '{write_mode}' specified. Falling back to '{WriteMode.OVERWRITE.value}'."
            )
            write_mode = WriteMode.OVERWRITE.value

        writer = df.write.format(self._get_format()).mode(write_mode)
        logger.debug(f"Configured writer with mode: {write_mode}")

        if partition_columns := config.get("partition"):
            partition_columns = (
                [partition_columns]
                if isinstance(partition_columns, str)
                else partition_columns
            )
            data_validator.validate_columns_exist(df, partition_columns)
            writer = writer.partitionBy(*partition_columns)
            logger.debug(f"Applied partitionBy on columns: {partition_columns}")

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
            part_col = config.get("partition_col")
            start = config.get("start_date")
            end = config.get("end_date")
            if part_col and start is not None and end is not None:
                predicate = f"{part_col} >= '{start}' AND {part_col} <= '{end}'"
                writer = writer.option("replaceWhere", predicate)
                logger.debug(f"Applied replaceWhere predicate: {predicate}")
            else:
                logger.warning(
                    "overwrite_strategy=replaceWhere configured but partition_col/start_date/end_date are missing; "
                    "skipping replaceWhere"
                )

        extra_options = config.get("options", {})
        for key, value in extra_options.items():
            writer = writer.option(key, value)
            logger.debug(f"Applied option {key}={value}")

        return writer

    def _apply_replace_where(self, writer: Any, config: Dict[str, Any]) -> Any:
        """Apply replaceWhere strategy for conditional overwrites."""
        partition_col = config.get("partition_col")
        start_date = config.get("start_date")
        end_date = config.get("end_date")

        if not all([partition_col, start_date, end_date]):
            raise ConfigurationError(
                "partition_col, start_date, and end_date are required for replaceWhere strategy"
            )

        config_validator = ConfigValidator()
        if not (
            config_validator.validate_date_format(start_date)
            and config_validator.validate_date_format(end_date)
        ):
            raise ConfigurationError(f"Invalid date format: {start_date} - {end_date}")

        writer = writer.option(
            "replaceWhere", f"{partition_col} BETWEEN '{start_date}' AND '{end_date}'"
        ).option("overwriteSchema", "false")
        logger.debug(
            f"Applied replaceWhere strategy for {partition_col} between {start_date} and {end_date}"
        )
        return writer

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


class DeltaWriter(BaseWriter, SparkWriterMixin):
    """Writer for Delta format."""

    def write(self, data: Any, destination: str, config: Dict[str, Any]) -> None:
        try:
            writer = self._configure_spark_writer(data, config)
            logger.info(f"Writing Delta data to: {destination}")
            writer.save(destination)
            logger.success(f"Delta data written successfully to: {destination}")
        except Exception as e:
            raise WriteOperationError(
                f"Failed to write Delta to {destination}: {e}"
            ) from e


class ParquetWriter(BaseWriter, SparkWriterMixin):
    """Writer for Parquet format."""

    def write(self, data: Any, destination: str, config: Dict[str, Any]) -> None:
        try:
            writer = self._configure_spark_writer(data, config)
            logger.info(f"Writing Parquet data to: {destination}")
            writer.save(destination)
            logger.success(f"Parquet data written successfully to: {destination}")
        except Exception as e:
            raise WriteOperationError(
                f"Failed to write Parquet to {destination}: {e}"
            ) from e


class CSVWriter(BaseWriter, SparkWriterMixin):
    """Writer for CSV format."""

    def write(self, data: Any, destination: str, config: Dict[str, Any]) -> None:
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
        except Exception as e:
            raise WriteOperationError(
                f"Failed to write CSV to {destination}: {e}"
            ) from e


class JSONWriter(BaseWriter, SparkWriterMixin):
    """Writer for JSON format."""

    def write(self, data: Any, destination: str, config: Dict[str, Any]) -> None:
        try:
            writer = self._configure_spark_writer(data, config)
            logger.info(f"Writing JSON data to: {destination}")
            writer.save(destination)
            logger.success(f"JSON data written successfully to: {destination}")
        except Exception as e:
            raise WriteOperationError(
                f"Failed to write JSON to {destination}: {e}"
            ) from e


class ORCWriter(BaseWriter, SparkWriterMixin):
    """Writer for ORC format."""

    def write(self, data: Any, destination: str, config: Dict[str, Any]) -> None:
        try:
            writer = self._configure_spark_writer(data, config)
            logger.info(f"Writing ORC data to: {destination}")
            writer.save(destination)
            logger.success(f"ORC data written successfully to: {destination}")
        except Exception as e:
            raise WriteOperationError(
                f"Failed to write ORC to {destination}: {e}"
            ) from e
