import pytest  # type: ignore
from core.streaming.readers import StreamingReaderFactory, KafkaStreamingReader
from core.streaming.writers import StreamingWriterFactory, ParquetStreamingWriter
from core.streaming.exceptions import StreamingFormatNotSupportedError, StreamingError


class DummyContext:
    def __init__(self):
        # minimal context required for reader_factory init
        self.spark = None
        self.format_policy = None


def test_reader_factory_get_reader_supported_and_unsupported():
    ctx = DummyContext()
    rf = StreamingReaderFactory(ctx)
    # kafka should be supported
    reader = rf.get_reader("kafka")
    assert isinstance(reader, KafkaStreamingReader)

    # unsupported format should raise StreamingFormatNotSupportedError
    with pytest.raises(StreamingFormatNotSupportedError):
        rf.get_reader("this_format_does_not_exist")


def test_kafka_reader_missing_required_options_raises():
    ctx = DummyContext()
    kr = KafkaStreamingReader(ctx)
    # Missing options (no kafka.bootstrap.servers) -> _validate_options should raise StreamError
    cfg = {"options": {}}
    with pytest.raises(StreamingError):
        kr.read_stream(cfg)


def test_writer_factory_and_parquet_writer_missing_path_raises():
    ctx = DummyContext()
    wf = StreamingWriterFactory(ctx)
    writer = wf.get_writer("parquet")
    # writer.write_stream expects path in config; supply empty config to provoke error
    fake_write_stream = object()
    with pytest.raises(StreamingError):
        writer.write_stream(fake_write_stream, {})
