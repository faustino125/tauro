import pytest
from unittest.mock import MagicMock, patch
from tauro.io.factories import ReaderFactory, WriterFactory
from tauro.io.exceptions import FormatNotSupportedError


class TestReaderFactory:
    @pytest.fixture
    def factory(self):
        return ReaderFactory(MagicMock())

    def test_get_reader_supported_formats(self, factory):
        supported_formats = [
            "parquet",
            "json",
            "csv",
            "delta",
            "pickle",
            "avro",
            "orc",
            "xml",
            "query",
        ]

        for fmt in supported_formats:
            reader = factory.get_reader(fmt)
            assert reader is not None

    def test_get_reader_unsupported_format(self, factory):
        with pytest.raises(FormatNotSupportedError):
            factory.get_reader("unsupported_format")

    def test_get_reader_case_insensitive(self, factory):
        # Should work regardless of case
        reader1 = factory.get_reader("PARQUET")
        reader2 = factory.get_reader("parquet")
        assert reader1 is not None
        assert reader2 is not None


class TestWriterFactory:
    @pytest.fixture
    def factory(self):
        return WriterFactory(MagicMock())

    def test_get_writer_supported_formats(self, factory):
        supported_formats = ["delta", "parquet", "csv", "json", "orc"]

        for fmt in supported_formats:
            writer = factory.get_writer(fmt)
            assert writer is not None

    def test_get_writer_unsupported_format(self, factory):
        with pytest.raises(FormatNotSupportedError):
            factory.get_writer("unsupported_format")
