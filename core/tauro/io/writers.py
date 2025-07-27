from typing import Any, Dict

from loguru import logger  # type: ignore

from tauro.io.constants import DEFAULT_CSV_OPTIONS, WriteMode
from tauro.io.exceptions import WriteOperationError
from tauro.io.factories import BaseWriter
from tauro.io.validators import DataValidator


class SparkWriterMixin:
    """Mixin for Spark-based writers."""

    def _configure_spark_writer(self, df: Any, config: Dict[str, Any]) -> Any:
        """Configure Spark DataFrame writer."""
        data_validator = DataValidator()
        data_validator.validate_dataframe(df)

        write_mode = config.get("write_mode", WriteMode.OVERWRITE.value)
        if write_mode not in [mode.value for mode in WriteMode]:
            logger.warning(
                f"Write mode '{write_mode}' not recognized. Using 'overwrite'."
            )
            write_mode = WriteMode.OVERWRITE.value

        writer = df.write.format(self._get_format()).mode(write_mode)

        if partition_columns := config.get("partition"):
            data_validator.validate_columns_exist(
                df,
                (
                    [partition_columns]
                    if isinstance(partition_columns, str)
                    else partition_columns
                ),
            )
            writer = writer.partitionBy(
                *(
                    [partition_columns]
                    if isinstance(partition_columns, str)
                    else partition_columns
                )
            )

        return writer

    def _get_format(self) -> str:
        """Get format string for the writer."""
        return self.__class__.__name__.replace("Writer", "").lower()


class DeltaWriter(BaseWriter, SparkWriterMixin):
    """Writer for Delta format."""

    def write(self, data: Any, destination: str, config: Dict[str, Any]) -> None:
        """Write data in Delta format."""
        try:
            writer = self._configure_spark_writer(data, config)
            writer = writer.option("overwriteSchema", "true")

            if extra_options := config.get("options", {}):
                for key, value in extra_options.items():
                    writer = writer.option(key, value)

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
        """Write data in Parquet format."""
        try:
            writer = self._configure_spark_writer(data, config)

            if extra_options := config.get("options", {}):
                for key, value in extra_options.items():
                    writer = writer.option(key, value)

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
        """Write data in CSV format."""
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
        """Write data in JSON format."""
        try:
            writer = self._configure_spark_writer(data, config)

            if extra_options := config.get("options", {}):
                for key, value in extra_options.items():
                    writer = writer.option(key, value)

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
        """Write data in ORC format."""
        try:
            writer = self._configure_spark_writer(data, config)

            if extra_options := config.get("options", {}):
                for key, value in extra_options.items():
                    writer = writer.option(key, value)

            logger.info(f"Writing ORC data to: {destination}")
            writer.save(destination)
            logger.success(f"ORC data written successfully to: {destination}")
        except Exception as e:
            raise WriteOperationError(
                f"Failed to write ORC to {destination}: {e}"
            ) from e
