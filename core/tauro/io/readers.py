import pickle
from typing import Any, Dict

from loguru import logger  # type: ignore

from tauro.io.base import BaseIO
from tauro.io.constants import DEFAULT_CSV_OPTIONS
from tauro.io.exceptions import ConfigurationError, ReadOperationError
from tauro.io.sql import SQLSanitizer


class SparkReaderBase(BaseIO):
    """Base para lectores Spark con acceso unificado al contexto y utilidades comunes."""

    def __init__(self, context: Any):
        super().__init__(context)

    def _spark_read(self, fmt: str, filepath: str, config: Dict[str, Any]) -> Any:
        """Método genérico de lectura Spark con validación de presencia."""
        spark = self._ctx_spark()
        if spark is None:
            raise ReadOperationError(
                f"Spark session is not available in context; cannot read {fmt.upper()} from {filepath}"
            ) from None
        logger.info(f"Reading {fmt.upper()} data from: {filepath}")
        try:
            return (
                spark.read.options(**config.get("options", {}))
                .format(fmt)
                .load(filepath)
            )
        except Exception as e:
            raise ReadOperationError(
                f"Spark failed to read {fmt.upper()} from {filepath}: {e}"
            ) from e


class ParquetReader(SparkReaderBase):
    """Reader for Parquet format."""

    def read(self, source: str, config: Dict[str, Any]) -> Any:
        """Read Parquet data from source."""
        try:
            return self._spark_read("parquet", source, config)
        except ReadOperationError:
            raise
        except Exception as e:
            raise ReadOperationError(
                f"Failed to read Parquet from {source}: {e}"
            ) from e


class JSONReader(SparkReaderBase):
    """Reader for JSON format."""

    def read(self, source: str, config: Dict[str, Any]) -> Any:
        """Read JSON data from source."""
        try:
            return self._spark_read("json", source, config)
        except ReadOperationError:
            raise
        except Exception as e:
            raise ReadOperationError(f"Failed to read JSON from {source}: {e}") from e


class CSVReader(SparkReaderBase):
    """Reader for CSV format."""

    def read(self, source: str, config: Dict[str, Any]) -> Any:
        """Read CSV data from source."""
        try:
            options = {**DEFAULT_CSV_OPTIONS, **config.get("options", {})}
            config_with_defaults = {**config, "options": options}
            return self._spark_read("csv", source, config_with_defaults)
        except ReadOperationError:
            raise
        except Exception as e:
            raise ReadOperationError(f"Failed to read CSV from {source}: {e}") from e


class DeltaReader(SparkReaderBase):
    """Reader for Delta Lake format."""

    def read(self, source: str, config: Dict[str, Any]) -> Any:
        """Read Delta data from source."""
        try:
            spark = self._ctx_spark()
            if spark is None:
                raise ReadOperationError(
                    f"Spark session is not available in context; cannot read DELTA from {source}"
                ) from None

            reader = spark.read.options(**config.get("options", {})).format("delta")
            version = config.get("versionAsOf") or config.get("version")
            timestamp = config.get("timestampAsOf") or config.get("timestamp")

            if version is not None:
                return reader.option("versionAsOf", version).load(source)
            if timestamp is not None:
                return reader.option("timestampAsOf", timestamp).load(source)
            return reader.load(source)
        except ReadOperationError:
            raise
        except Exception as e:
            raise ReadOperationError(f"Failed to read Delta from {source}: {e}") from e


class PickleReader(SparkReaderBase):
    """Reader for Pickle format."""

    def read(self, source: str, config: Dict[str, Any]) -> Any:
        """Read Pickle data from source."""
        try:
            if not bool(config.get("allow_untrusted_pickle", False)):
                raise ReadOperationError(
                    "Reading pickle requires allow_untrusted_pickle=True due to security risks (arbitrary code execution)."
                ) from None

            use_pandas = bool(config.get("use_pandas", False))
            spark = self._ctx_spark()

            if spark is None or use_pandas:
                return self._read_local_pickle(source, config)

            return self._read_distributed_pickle(source, config)
        except ReadOperationError:
            raise
        except Exception as e:
            raise ReadOperationError(f"Failed to read Pickle from {source}: {e}") from e

    def _read_local_pickle(self, source: str, config: Dict[str, Any]) -> Any:
        """Read pickle file locally."""
        import os

        if not os.path.exists(source):
            raise ReadOperationError(f"Pickle file not found: {source}") from None

        try:
            with open(source, "rb") as f:
                data = pickle.load(f)
        except (IOError, OSError) as e:
            raise ReadOperationError(f"Failed to read pickle file {source}: {e}") from e

        if not config.get("use_pandas", False):
            try:
                import pandas as pd  # type: ignore

                if isinstance(data, pd.DataFrame):
                    spark = self._ctx_spark()
                    if spark is not None:
                        return spark.createDataFrame(data)
            except Exception:
                pass
        return data

    def _read_distributed_pickle(self, source: str, config: Dict[str, Any]) -> Any:
        """Read pickle files using Spark distributed processing.

        Nota: Para evitar OOM en el driver por collect(), se aplica por defecto un límite
        de registros cuando no se especifica 'max_records'. Puede desactivarse con max_records=0.
        """
        spark = self._ctx_spark()
        to_dataframe = config.get("to_dataframe", True)

        # Comportamiento por defecto más seguro: limitar si no se especifica
        try:
            raw_max = int(config.get("max_records", -1))
        except Exception:
            raw_max = -1

        if raw_max < 0:
            max_records = 10000  # límite por defecto seguro
            logger.warning(
                "No 'max_records' specified for distributed pickle read. "
                "Applying default limit of 10000 records to avoid driver OOM. "
                "Set config.max_records=0 to read all, or a positive integer to customize."
            )
        elif raw_max == 0:
            max_records = 0  # sin límite
        else:
            max_records = raw_max

        try:
            bf_df = spark.read.format("binaryFile").load(source)
            if max_records and max_records > 0:
                logger.warning(
                    f"Limiting distributed pickle read to {max_records} records to avoid driver OOM. "
                    "Override with config.max_records=0 to read all."
                )
                bf_df = bf_df.limit(max_records)
        except Exception as e:
            raise ReadOperationError(
                f"binaryFile datasource is unavailable; cannot read pickle(s): {e}"
            ) from e

        try:
            rows = bf_df.select("content").collect()
        except Exception as e:
            raise ReadOperationError(f"Failed to read binary content: {e}") from e

        objects = []
        for r in rows:
            try:
                data = pickle.loads(bytes(r[0]))
            except Exception as e:
                raise ReadOperationError(f"Failed to unpickle object: {e}") from e
            objects.append(data)

        if not to_dataframe:
            return objects

        if not objects:
            raise ReadOperationError(
                "No pickled objects found to create a DataFrame; set to_dataframe=False to get a list."
            ) from None

        first = objects[0]
        if not isinstance(first, (dict, tuple, list)):
            raise ReadOperationError(
                "Cannot create DataFrame from pickled objects; expected dict/tuple/list rows. Set to_dataframe=False."
            ) from None

        try:
            return spark.createDataFrame(objects)
        except Exception as e:
            raise ReadOperationError(
                f"Failed to create DataFrame from pickled objects: {e}"
            ) from e


class AvroReader(SparkReaderBase):
    """Reader for Avro format."""

    def read(self, source: str, config: Dict[str, Any]) -> Any:
        """Read Avro data from source."""
        try:
            return self._spark_read("avro", source, config)
        except ReadOperationError:
            raise
        except Exception as e:
            raise ReadOperationError(f"Failed to read Avro from {source}: {e}") from e


class ORCReader(SparkReaderBase):
    """Reader for ORC format."""

    def read(self, source: str, config: Dict[str, Any]) -> Any:
        """Read ORC data from source."""
        try:
            return self._spark_read("orc", source, config)
        except ReadOperationError:
            raise
        except Exception as e:
            raise ReadOperationError(f"Failed to read ORC from {source}: {e}") from e


class XMLReader(SparkReaderBase):
    """Reader for XML format."""

    def read(self, source: str, config: Dict[str, Any]) -> Any:
        """Read XML data from source."""
        try:
            row_tag = config.get("rowTag", "row")
            spark = self._ctx_spark()
            if spark is None:
                raise ReadOperationError(
                    f"Spark session is not available in context; cannot read XML from {source}"
                ) from None
            try:
                _ = spark._jvm.com.databricks.spark.xml
            except Exception as e:
                raise ConfigurationError(
                    "XML reader requires the com.databricks:spark-xml package. Install the jar or add --packages com.databricks:spark-xml:latest_2.12"
                ) from e
            logger.info(f"Reading XML file with row tag '{row_tag}': {source}")
            return (
                spark.read.format("com.databricks.spark.xml")
                .option("rowTag", row_tag)
                .options(**config.get("options", {}))
                .load(source)
            )
        except (ReadOperationError, ConfigurationError):
            raise
        except Exception as e:
            raise ReadOperationError(f"Failed to read XML from {source}: {e}") from e


class QueryReader(SparkReaderBase):
    """Reader for SQL queries."""

    def read(self, source: str, config: Dict[str, Any]) -> Any:
        """Execute SQL query and return results."""
        try:
            query = config.get("query")
            if not query or not query.strip():
                raise ConfigurationError(
                    "Query format specified without SQL query or query is empty"
                ) from None

            sanitized = SQLSanitizer.sanitize_query(query)

            spark = self._ctx_spark()
            if spark is None:
                raise ReadOperationError(
                    "Spark session is not available in context; cannot execute SQL query"
                ) from None
            logger.info(f"Executing SQL query: {sanitized[:100]}...")
            return spark.sql(sanitized)
        except (ReadOperationError, ConfigurationError):
            raise
        except Exception as e:
            raise ReadOperationError(f"Failed to execute query: {e}") from e
