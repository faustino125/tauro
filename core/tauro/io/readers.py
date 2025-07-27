import pickle
from typing import Any, Dict

from loguru import logger  # type: ignore

from tauro.io.constants import DEFAULT_CSV_OPTIONS
from tauro.io.exceptions import ConfigurationError, ReadOperationError
from tauro.io.factories import BaseReader


class SparkReaderMixin:
    """Mixin for Spark-based readers."""

    def _spark_read(self, fmt: str, filepath: str, config: Dict[str, Any]) -> Any:
        """Generic Spark read method."""
        logger.info(f"Reading {fmt.upper()} data from: {filepath}")
        return (
            self.context.spark.read.options(**config.get("options", {}))
            .format(fmt)
            .load(filepath)
        )


class ParquetReader(BaseReader, SparkReaderMixin):
    """Reader for Parquet format."""

    def read(self, source: str, config: Dict[str, Any]) -> Any:
        """Read Parquet data."""
        try:
            return self._spark_read("parquet", source, config)
        except Exception as e:
            raise ReadOperationError(
                f"Failed to read Parquet from {source}: {e}"
            ) from e


class JSONReader(BaseReader, SparkReaderMixin):
    """Reader for JSON format."""

    def read(self, source: str, config: Dict[str, Any]) -> Any:
        """Read JSON data."""
        try:
            return self._spark_read("json", source, config)
        except Exception as e:
            raise ReadOperationError(f"Failed to read JSON from {source}: {e}") from e


class CSVReader(BaseReader, SparkReaderMixin):
    """Reader for CSV format."""

    def read(self, source: str, config: Dict[str, Any]) -> Any:
        """Read CSV data."""
        try:
            options = {**DEFAULT_CSV_OPTIONS, **config.get("options", {})}
            config_with_defaults = {**config, "options": options}
            return self._spark_read("csv", source, config_with_defaults)
        except Exception as e:
            raise ReadOperationError(f"Failed to read CSV from {source}: {e}") from e


class DeltaReader(BaseReader):
    """Reader for Delta Lake format."""

    def read(self, source: str, config: Dict[str, Any]) -> Any:
        """Read Delta data with time travel support."""
        try:
            reader = self.context.spark.read.options(
                **config.get("options", {})
            ).format("delta")

            if version := config.get("version"):
                return reader.option("versionAsOf", version).load(source)
            if timestamp := config.get("timestamp"):
                return reader.option("timestampAsOf", timestamp).load(source)
            return reader.load(source)
        except Exception as e:
            raise ReadOperationError(f"Failed to read Delta from {source}: {e}") from e


class PickleReader(BaseReader):
    """Reader for Pickle format."""

    def read(self, source: str, config: Dict[str, Any]) -> Any:
        """Read Pickle data."""
        try:
            use_pandas = config.get("use_pandas", False)

            if self.context.execution_mode == "local" or use_pandas:
                return self._read_local_pickle(source, config)
            else:
                return self._read_distributed_pickle(source, config)
        except Exception as e:
            raise ReadOperationError(f"Failed to read Pickle from {source}: {e}") from e

    def _read_local_pickle(self, source: str, config: Dict[str, Any]) -> Any:
        """Read pickle file locally."""
        import pandas as pd

        logger.info(f"Reading pickle file using local method: {source}")
        with open(source, "rb") as f:
            data = pickle.load(f)

        use_pandas = config.get("use_pandas", False)
        if isinstance(data, pd.DataFrame) and not use_pandas:
            return self.context.spark.createDataFrame(data)
        return data

    def _read_distributed_pickle(self, source: str, config: Dict[str, Any]) -> Any:
        """Read pickle file in distributed mode."""
        logger.info(f"Reading pickle file using distributed method: {source}")
        rdd = self.context.spark.sparkContext.binaryFiles(source).map(
            lambda x: pickle.loads(x[1])
        )

        to_dataframe = config.get("to_dataframe", True)
        return self.context.spark.createDataFrame(rdd) if to_dataframe else rdd


class AvroReader(BaseReader, SparkReaderMixin):
    """Reader for Avro format."""

    def read(self, source: str, config: Dict[str, Any]) -> Any:
        """Read Avro data."""
        try:
            return self._spark_read("avro", source, config)
        except Exception as e:
            raise ReadOperationError(f"Failed to read Avro from {source}: {e}") from e


class ORCReader(BaseReader, SparkReaderMixin):
    """Reader for ORC format."""

    def read(self, source: str, config: Dict[str, Any]) -> Any:
        """Read ORC data."""
        try:
            return self._spark_read("orc", source, config)
        except Exception as e:
            raise ReadOperationError(f"Failed to read ORC from {source}: {e}") from e


class XMLReader(BaseReader):
    """Reader for XML format."""

    def read(self, source: str, config: Dict[str, Any]) -> Any:
        """Read XML data."""
        try:
            row_tag = config.get("rowTag", "row")
            logger.info(f"Reading XML file with row tag '{row_tag}': {source}")
            return (
                self.context.spark.read.format("com.databricks.spark.xml")
                .option("rowTag", row_tag)
                .options(**config.get("options", {}))
                .load(source)
            )
        except Exception as e:
            raise ReadOperationError(f"Failed to read XML from {source}: {e}") from e


class QueryReader(BaseReader):
    """Reader for SQL queries."""

    def read(self, source: str, config: Dict[str, Any]) -> Any:
        """Execute SQL query."""
        try:
            query = config.get("query")
            if not query:
                raise ConfigurationError("Query format specified without SQL query")

            logger.info(f"Executing SQL query: {query[:100]}...")
            return self.context.spark.sql(query)
        except Exception as e:
            raise ReadOperationError(f"Failed to execute query: {e}") from e
