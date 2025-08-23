import pickle
from typing import Any, Dict

from loguru import logger  # type: ignore

from tauro.io.constants import DEFAULT_CSV_OPTIONS
from tauro.io.exceptions import ConfigurationError, ReadOperationError
from tauro.io.factories import BaseReader


class SparkReaderMixin:
    """Mixin para lectores Spark con acceso seguro al contexto."""

    def _get_spark(self) -> Any:
        ctx = getattr(self, "context", None)
        if isinstance(ctx, dict):
            return ctx.get("spark")
        return getattr(ctx, "spark", None)

    def _get_execution_mode(self) -> str:
        ctx = getattr(self, "context", None)
        mode = (
            ctx.get("execution_mode")
            if isinstance(ctx, dict)
            else getattr(ctx, "execution_mode", None)
        )
        if not mode:
            return ""
        mode = str(mode).lower()
        return "distributed" if mode == "databricks" else mode

    def _spark_read(self, fmt: str, filepath: str, config: Dict[str, Any]) -> Any:
        """Generic Spark read method."""
        spark = self._get_spark()
        logger.info(f"Reading {fmt.upper()} data from: {filepath}")
        return (
            spark.read.options(**config.get("options", {})).format(fmt).load(filepath)
        )


class ParquetReader(BaseReader, SparkReaderMixin):
    """Reader for Parquet format."""

    def read(self, source: str, config: Dict[str, Any]) -> Any:
        try:
            return self._spark_read("parquet", source, config)
        except Exception as e:
            raise ReadOperationError(
                f"Failed to read Parquet from {source}: {e}"
            ) from e


class JSONReader(BaseReader, SparkReaderMixin):
    """Reader for JSON format."""

    def read(self, source: str, config: Dict[str, Any]) -> Any:
        try:
            return self._spark_read("json", source, config)
        except Exception as e:
            raise ReadOperationError(f"Failed to read JSON from {source}: {e}") from e


class CSVReader(BaseReader, SparkReaderMixin):
    """Reader for CSV format."""

    def read(self, source: str, config: Dict[str, Any]) -> Any:
        try:
            options = {**DEFAULT_CSV_OPTIONS, **config.get("options", {})}
            config_with_defaults = {**config, "options": options}
            return self._spark_read("csv", source, config_with_defaults)
        except Exception as e:
            raise ReadOperationError(f"Failed to read CSV from {source}: {e}") from e


class DeltaReader(BaseReader, SparkReaderMixin):
    """Reader for Delta Lake format."""

    def read(self, source: str, config: Dict[str, Any]) -> Any:
        try:
            spark = self._get_spark()
            reader = spark.read.options(**config.get("options", {})).format("delta")
            if version := config.get("version"):
                return reader.option("versionAsOf", version).load(source)
            if timestamp := config.get("timestamp"):
                return reader.option("timestampAsOf", timestamp).load(source)
            return reader.load(source)
        except Exception as e:
            raise ReadOperationError(f"Failed to read Delta from {source}: {e}") from e


class PickleReader(BaseReader, SparkReaderMixin):
    """Reader for Pickle format."""

    def read(self, source: str, config: Dict[str, Any]) -> Any:
        try:
            use_pandas = config.get("use_pandas", False)
            mode = self._get_execution_mode()
            if mode == "local" or use_pandas:
                return self._read_local_pickle(source, config)
            else:
                return self._read_distributed_pickle(source, config)
        except Exception as e:
            raise ReadOperationError(f"Failed to read Pickle from {source}: {e}") from e

    def _read_local_pickle(self, source: str, config: Dict[str, Any]) -> Any:
        with open(source, "rb") as f:
            data = pickle.load(f)

        # Convert Pandas -> Spark si procede
        if not config.get("use_pandas", False):
            try:
                import pandas as pd  # type: ignore

                if isinstance(data, pd.DataFrame):
                    spark = self._get_spark()
                    return spark.createDataFrame(data)
            except Exception:
                pass
        return data

    def _read_distributed_pickle(self, source: str, config: Dict[str, Any]) -> Any:
        spark = self._get_spark()
        rdd = spark.sparkContext.binaryFiles(source).map(lambda x: pickle.loads(x[1]))  # type: ignore[attr-defined]
        to_dataframe = config.get("to_dataframe", True)
        return spark.createDataFrame(rdd) if to_dataframe else rdd


class AvroReader(BaseReader, SparkReaderMixin):
    """Reader for Avro format."""

    def read(self, source: str, config: Dict[str, Any]) -> Any:
        try:
            return self._spark_read("avro", source, config)
        except Exception as e:
            raise ReadOperationError(f"Failed to read Avro from {source}: {e}") from e


class ORCReader(BaseReader, SparkReaderMixin):
    """Reader for ORC format."""

    def read(self, source: str, config: Dict[str, Any]) -> Any:
        try:
            return self._spark_read("orc", source, config)
        except Exception as e:
            raise ReadOperationError(f"Failed to read ORC from {source}: {e}") from e


class XMLReader(BaseReader, SparkReaderMixin):
    """Reader for XML format."""

    def read(self, source: str, config: Dict[str, Any]) -> Any:
        try:
            row_tag = config.get("rowTag", "row")
            spark = self._get_spark()
            logger.info(f"Reading XML file with row tag '{row_tag}': {source}")
            return (
                spark.read.format("com.databricks.spark.xml")
                .option("rowTag", row_tag)
                .options(**config.get("options", {}))
                .load(source)
            )
        except Exception as e:
            raise ReadOperationError(f"Failed to read XML from {source}: {e}") from e


class QueryReader(BaseReader, SparkReaderMixin):
    """Reader for SQL queries."""

    def read(self, source: str, config: Dict[str, Any]) -> Any:
        try:
            query = config.get("query")
            if not query:
                raise ConfigurationError("Query format specified without SQL query")

            spark = self._get_spark()
            logger.info(f"Executing SQL query: {query[:100]}...")
            return spark.sql(query)
        except Exception as e:
            raise ReadOperationError(f"Failed to execute query: {e}") from e
