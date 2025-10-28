import pytest
import time
from unittest import mock
from tauro.streaming.query_manager import StreamingQueryManager
from tauro.streaming.exceptions import StreamingError, StreamingQueryError
from tauro.streaming.constants import DEFAULT_STREAMING_CONFIG


class DummyContext:
    def __init__(self):
        self.format_policy = None
        self.spark = None
        self.global_settings = {}
        self.output_path = "/tmp/test_checkpoints"


@pytest.fixture
def sqm():
    ctx = DummyContext()
    sqm = StreamingQueryManager(ctx)
    return sqm


class _FakeWriteStream:
    """
    Minimal chainable fake for df.writeStream that supports:
      .outputMode(...).queryName(...).option(...).trigger(...)
    Methods return self so calls can be chained. The writer used in tests
    will be mocked to accept this object.
    """

    def outputMode(self, mode):
        self._output_mode = mode
        return self

    def queryName(self, name):
        self._query_name = name
        return self

    def option(self, key, value):
        # store option for potential assertions
        if not hasattr(self, "_options"):
            self._options = {}
        self._options[key] = value
        return self

    def trigger(self, **kwargs):
        # store trigger config
        self._trigger = kwargs
        return self


def test_default_streaming_config_not_mutated(sqm):
    # Keep original copy
    import copy

    orig = copy.deepcopy(DEFAULT_STREAMING_CONFIG)

    # Monkeypatch writer_factory.get_writer to avoid needing Spark writer internals
    fake_query = mock.MagicMock()
    fake_writer = mock.MagicMock()
    fake_writer.write_stream.return_value = fake_query

    sqm.writer_factory.get_writer = mock.MagicMock(return_value=fake_writer)

    # Create a fake DataFrame object with a writeStream providing chainable methods
    class FakeDataFrame:
        def __init__(self):
            self.writeStream = _FakeWriteStream()

    df = FakeDataFrame()

    node1 = {
        "name": "n1",
        "streaming": {"trigger": {"type": "processing_time", "interval": "2 seconds"}},
        "output": {"format": "console"},
    }
    # Should not raise
    sqm._configure_and_start_query(df, node1, "exec1", "pipeline1")

    node2 = {
        "name": "n2",
        "streaming": {"trigger": {"type": "processing_time", "interval": "5 seconds"}},
        "output": {"format": "console"},
    }
    sqm._configure_and_start_query(df, node2, "exec2", "pipeline1")

    # Ensure the global DEFAULT_STREAMING_CONFIG wasn't mutated
    assert DEFAULT_STREAMING_CONFIG == orig


def test_stop_query_removes_from_active(sqm):
    # Build a fake query object
    q = mock.MagicMock()
    # Simulate isActive attribute (callable or boolean)
    q.isActive = True

    # Simulate stop changing isActive to False
    def stop_side_effect():
        q.isActive = False

    q.stop.side_effect = stop_side_effect

    # Insert into active queries map
    key = "p:e:n"
    with sqm._active_queries_lock:
        sqm._active_queries[key] = {"query": q, "node_name": "n"}

    # Now stop the query
    res = sqm.stop_query(q, graceful=True, timeout_seconds=2.0)
    assert res is True
    # Ensure removed
    with sqm._active_queries_lock:
        assert key not in sqm._active_queries


def test_ensure_checkpoint_dir_raises_streaming_error_on_mkdir_failure(sqm, monkeypatch):
    # Force Path.mkdir to raise OSError
    import pathlib

    monkeypatch.setattr(pathlib.Path, "mkdir", mock.MagicMock(side_effect=OSError("no perm")))
    with pytest.raises(StreamingError):
        sqm._ensure_checkpoint_dir("/tmp/forbidden_path")
