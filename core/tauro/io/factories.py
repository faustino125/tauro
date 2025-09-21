"""
Copyright (c) 2025 Faustino Lopez Ramos. 
For licensing information, see the LICENSE file in the project root
"""
from typing import Any, Dict, Protocol

from tauro.io.constants import SupportedFormats
from tauro.io.exceptions import FormatNotSupportedError


class DataReader(Protocol):
    """Protocol for data readers."""

    def read(self, source: str, config: Dict[str, Any]) -> Any:
        """Read data from source."""
        ...


class DataWriter(Protocol):
    """Protocol for data writers."""

    def write(self, data: Any, destination: str, config: Dict[str, Any]) -> None:
        """Write data to destination."""
        ...


class ReaderFactory:
    """Factory for creating data readers."""

    def __init__(self, context: Any):
        self.context = context
        self._readers: Dict[str, DataReader] = {}
        self._register_readers()

    def _register_readers(self) -> None:
        """Register available readers."""
        from .readers import (
            AvroReader,
            CSVReader,
            DeltaReader,
            JSONReader,
            ORCReader,
            ParquetReader,
            PickleReader,
            QueryReader,
            XMLReader,
        )

        self._readers = {
            SupportedFormats.PARQUET.value: ParquetReader(self.context),
            SupportedFormats.JSON.value: JSONReader(self.context),
            SupportedFormats.CSV.value: CSVReader(self.context),
            SupportedFormats.DELTA.value: DeltaReader(self.context),
            SupportedFormats.PICKLE.value: PickleReader(self.context),
            SupportedFormats.AVRO.value: AvroReader(self.context),
            SupportedFormats.ORC.value: ORCReader(self.context),
            SupportedFormats.XML.value: XMLReader(self.context),
            SupportedFormats.QUERY.value: QueryReader(self.context),
        }

    def get_reader(self, format_name: str) -> DataReader:
        """Get reader for specified format."""
        format_key = format_name.lower()
        if format_key not in self._readers:
            raise FormatNotSupportedError(f"Format '{format_name}' not supported")
        return self._readers[format_key]


class WriterFactory:
    """Factory for creating data writers."""

    def __init__(self, context: Any):
        self.context = context
        self._writers: Dict[str, DataWriter] = {}
        self._register_writers()

    def _register_writers(self) -> None:
        """Register available writers."""
        from .writers import (
            CSVWriter,
            DeltaWriter,
            JSONWriter,
            ORCWriter,
            ParquetWriter,
        )

        self._writers = {
            SupportedFormats.DELTA.value: DeltaWriter(self.context),
            SupportedFormats.PARQUET.value: ParquetWriter(self.context),
            SupportedFormats.CSV.value: CSVWriter(self.context),
            SupportedFormats.JSON.value: JSONWriter(self.context),
            SupportedFormats.ORC.value: ORCWriter(self.context),
        }

    def get_writer(self, format_name: str) -> DataWriter:
        """Get writer for specified format."""
        format_key = format_name.lower()
        if format_key not in self._writers:
            raise FormatNotSupportedError(f"Format '{format_name}' not supported")
        return self._writers[format_key]
