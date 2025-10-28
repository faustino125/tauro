"""
Copyright (c) 2025 Faustino Lopez Ramos. 
For licensing information, see the LICENSE file in the project root
"""
import pickle
from typing import Any, Dict

from loguru import logger  # type: ignore

from tauro.core.io.base import BaseIO
from tauro.core.io.constants import DEFAULT_CSV_OPTIONS
from tauro.core.io.exceptions import ConfigurationError, ReadOperationError


class SparkReaderBase(BaseIO):
    """Base class for Spark readers providing unified context access and common utilities."""

    def __init__(self, context: Any):
        super().__init__(context)

    def _spark_read(self, fmt: str, filepath: str, config: Dict[str, Any]) -> Any:
        spark = self._ctx_spark()
        if spark is None:
            raise ReadOperationError(
                f"Spark session is not available in context; cannot read {fmt.upper()} from {filepath}"
            ) from None
        logger.info(f"Reading {fmt.upper()} data from: {filepath}")
        try:
            reader = spark.read.options(**config.get("options", {})).format(fmt)
            partition_filter = config.get("partition_filter")
            if partition_filter:
                df = reader.load(filepath)
                logger.info(f"Applying partition_filter: {partition_filter}")
                return df.where(partition_filter)
            else:
                return reader.load(filepath)
        except Exception as e:
            raise ReadOperationError(
                f"Spark failed to read {fmt.upper()} from {filepath}: {e}"
            ) from e


class ParquetReader(SparkReaderBase):
    def read(self, source: str, config: Dict[str, Any]) -> Any:
        try:
            return self._spark_read("parquet", source, config)
        except ReadOperationError:
            raise
        except Exception as e:
            raise ReadOperationError(f"Failed to read Parquet from {source}: {e}") from e


class JSONReader(SparkReaderBase):
    def read(self, source: str, config: Dict[str, Any]) -> Any:
        try:
            return self._spark_read("json", source, config)
        except ReadOperationError:
            raise
        except Exception as e:
            raise ReadOperationError(f"Failed to read JSON from {source}: {e}") from e


class CSVReader(SparkReaderBase):
    def read(self, source: str, config: Dict[str, Any]) -> Any:
        try:
            options = {**DEFAULT_CSV_OPTIONS, **config.get("options", {})}
            config_with_defaults = {**config, "options": options}
            return self._spark_read("csv", source, config_with_defaults)
        except ReadOperationError:
            raise
        except Exception as e:
            raise ReadOperationError(f"Failed to read CSV from {source}: {e}") from e


class DeltaReader(SparkReaderBase):
    def read(self, source: str, config: Dict[str, Any]) -> Any:
        try:
            spark = self._ctx_spark()
            if spark is None:
                raise ReadOperationError(
                    f"Spark session is not available in context; cannot read DELTA from {source}"
                ) from None

            reader = spark.read.options(**config.get("options", {})).format("delta")
            version = config.get("versionAsOf") or config.get("version")
            timestamp = config.get("timestampAsOf") or config.get("timestamp")

            partition_filter = config.get("partition_filter")

            if version is not None:
                df = reader.option("versionAsOf", version).load(source)
            elif timestamp is not None:
                df = reader.option("timestampAsOf", timestamp).load(source)
            else:
                df = reader.load(source)

            if partition_filter:
                logger.info(f"Applying partition_filter: {partition_filter}")
                return df.where(partition_filter)
            return df
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

        Note: To avoid OOM on the driver from collect(), a default record limit is applied
        when 'max_records' is not specified. This can be disabled with max_records=0.
        """
        spark = self._ctx_spark()
        to_dataframe = config.get("to_dataframe", True)

        # Default safer behavior: apply limit if not specified
        try:
            raw_max = int(config.get("max_records", -1))
        except Exception:
            raw_max = -1

        if raw_max < 0:
            max_records = 10000  # safe default limit
            logger.warning(
                "No 'max_records' specified for distributed pickle read. "
                "Applying default limit of 10000 records to avoid driver OOM. "
                "Set config.max_records=0 to read all, or a positive integer to customize."
            )
        elif raw_max == 0:
            max_records = 0  # no limit
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
            raise ReadOperationError(f"Failed to create DataFrame from pickled objects: {e}") from e


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


class QueryReader(BaseIO):
    """Reader for executing SQL queries in Spark."""

    def read(self, source: str, config: Dict[str, Any]) -> Any:
        """Execute SQL query and return results."""
        try:
            query = (config or {}).get("query")
            if not query or not str(query).strip():
                raise ConfigurationError(
                    "Query format specified without SQL query or query is empty"
                ) from None

            if not self._spark_available():
                raise ReadOperationError("Spark session is required to execute queries") from None

            sanitized = self.sanitize_sql_query(str(query))

            spark = self._ctx_spark()
            return spark.sql(sanitized)

        except ConfigurationError as e:
            raise ReadOperationError(str(e)) from e
        except Exception as e:
            raise ReadOperationError(f"Failed to execute query: {e}") from e
