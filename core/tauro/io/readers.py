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
        """Generic Spark read method with Spark presence validation."""
        spark = self._get_spark()
        if spark is None:
            raise ReadOperationError(
                f"Spark session is not available in context; cannot read {fmt.upper()} from {filepath}"
            )
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
            if spark is None:
                raise ReadOperationError(
                    f"Spark session is not available in context; cannot read DELTA from {source}"
                )
            reader = spark.read.options(**config.get("options", {})).format("delta")
            version = config.get("versionAsOf") or config.get("version")
            timestamp = config.get("timestampAsOf") or config.get("timestamp")
            if version is not None:
                return reader.option("versionAsOf", version).load(source)
            if timestamp is not None:
                return reader.option("timestampAsOf", timestamp).load(source)
            return reader.load(source)
        except Exception as e:
            raise ReadOperationError(f"Failed to read Delta from {source}: {e}") from e


class PickleReader(BaseReader, SparkReaderMixin):
    """Reader for Pickle format."""

    def read(self, source: str, config: Dict[str, Any]) -> Any:
        try:
            if not bool(config.get("allow_untrusted_pickle", False)):
                raise ReadOperationError(
                    "Reading pickle requires allow_untrusted_pickle=True due to security risks (arbitrary code execution)."
                )

            use_pandas = bool(config.get("use_pandas", False))
            spark = self._get_spark()

            if spark is None or use_pandas:
                return self._read_local_pickle(source, config)

            return self._read_distributed_pickle(source, config)
        except Exception as e:
            raise ReadOperationError(f"Failed to read Pickle from {source}: {e}") from e

    def _read_local_pickle(self, source: str, config: Dict[str, Any]) -> Any:
        import os

        if not os.path.exists(source):
            raise ReadOperationError(f"Pickle file not found: {source}")

        try:
            with open(source, "rb") as f:
                data = pickle.load(f)
        except (IOError, OSError) as e:
            raise ReadOperationError(f"Failed to read pickle file {source}: {e}") from e

        if not config.get("use_pandas", False):
            try:
                import pandas as pd  # type: ignore

                if isinstance(data, pd.DataFrame):
                    spark = self._get_spark()
                    if spark is not None:
                        return spark.createDataFrame(data)
            except Exception:
                pass
        return data

    def _read_distributed_pickle(self, source: str, config: Dict[str, Any]) -> Any:
        spark = self._get_spark()
        to_dataframe = config.get("to_dataframe", True)
        try:
            bf_df = spark.read.format("binaryFile").load(source)
        except Exception as e:
            raise ReadOperationError(
                f"binaryFile datasource is unavailable; cannot read pickle(s): {e}"
            )

        try:
            rows = bf_df.select("content").collect()
        except Exception as e:
            raise ReadOperationError(f"Failed to read binary content: {e}")

        objects = []
        for r in rows:
            try:
                data = pickle.loads(bytes(r[0]))
            except Exception as e:
                raise ReadOperationError(f"Failed to unpickle object: {e}")
            objects.append(data)

        if not to_dataframe:
            return objects

        if not objects:
            raise ReadOperationError(
                "No pickled objects found to create a DataFrame; set to_dataframe=False to get a list."
            )

        first = objects[0]
        if not isinstance(first, (dict, tuple, list)):
            raise ReadOperationError(
                "Cannot create DataFrame from pickled objects; expected dict/tuple/list rows. Set to_dataframe=False."
            )
        return spark.createDataFrame(objects)


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
            if spark is None:
                raise ReadOperationError(
                    f"Spark session is not available in context; cannot read XML from {source}"
                )
            try:
                _ = spark._jvm.com.databricks.spark.xml
            except Exception:
                raise ConfigurationError(
                    "XML reader requires the com.databricks:spark-xml package. Install the jar or add --packages com.databricks:spark-xml:latest_2.12"
                )
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
            if not query or not query.strip():
                raise ConfigurationError(
                    "Query format specified without SQL query or query is empty"
                )

            spark = self._get_spark()
            if spark is None:
                raise ReadOperationError(
                    "Spark session is not available in context; cannot execute SQL query"
                )
            logger.info(f"Executing SQL query: {query[:100]}...")
            return spark.sql(query)
        except Exception as e:
            raise ReadOperationError(f"Failed to execute query: {e}") from e
