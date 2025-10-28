import pytest
from unittest.mock import MagicMock
from tauro.io.factories import ReaderFactory, WriterFactory
from tauro.io.exceptions import FormatNotSupportedError


class TestReaderFactory:
    @pytest.fixture
    def factory(self):
        return ReaderFactory(MagicMock())

    @pytest.mark.parametrize(
        "fmt",
        ["parquet", "json", "csv", "delta", "pickle", "avro", "orc", "xml", "query"],
    )
    def test_get_reader_supported_formats(self, factory, fmt):
        reader = factory.get_reader(fmt)
        assert reader is not None

    def test_get_reader_unsupported_format(self, factory):
        with pytest.raises(FormatNotSupportedError):
            factory.get_reader("unsupported_format")

    @pytest.mark.parametrize("fmt", ["PARQUET", "parquet", "PaRqUeT"])
    def test_get_reader_case_insensitive(self, factory, fmt):
        reader = factory.get_reader(fmt)
        assert reader is not None


class TestWriterFactory:
    @pytest.fixture
    def factory(self):
        return WriterFactory(MagicMock())

    @pytest.mark.parametrize("fmt", ["delta", "parquet", "csv", "json", "orc"])
    def test_get_writer_supported_formats(self, factory, fmt):
        writer = factory.get_writer(fmt)
        assert writer is not None

    def test_get_writer_unsupported_format(self, factory):
        with pytest.raises(FormatNotSupportedError):
            factory.get_writer("unsupported_format")
