import time
import logging
from unittest import mock

import pytest

streaming_pm_mod = pytest.importorskip(
    "tauro.streaming.pipeline_manager",
    reason="StreamingPipelineManager not available in tauro.streaming.pipeline_manager",
)
StreamingPipelineManager = getattr(streaming_pm_mod, "StreamingPipelineManager", None)
if StreamingPipelineManager is None:
    pytest.skip("StreamingPipelineManager class not found in tauro.streaming.pipeline_manager")

from tauro.streaming.validators import (
    StreamingValidator,
)  # direct import; importorskip above will have skipped if missing

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())


class DummyContext:
    """Minimal fake context used by StreamingPipelineManager tests."""

    def __init__(self):
        self.format_policy = None
        self.nodes_config = {}
        self.spark = None
        self.global_settings = {}


@pytest.fixture
def manager(monkeypatch):
    """Fixture that constructs a StreamingPipelineManager."""

    class DummyStreamingQuery:
        pass

    monkeypatch.setattr(streaming_pm_mod, "StreamingQuery", DummyStreamingQuery)

    ctx = DummyContext()
    mgr = StreamingPipelineManager(ctx, max_concurrent_pipelines=2, validator=StreamingValidator())

    mgr.query_manager = mock.MagicMock()
    fake_query = mock.MagicMock()
    fake_query.__class__ = DummyStreamingQuery
    fake_query.isActive = True
    fake_query.id = "fake-id"
    fake_query.name = "fake-query"

    mgr.query_manager.create_and_start_query.return_value = fake_query
    mgr.query_manager.stop_query.return_value = True
    return mgr


def test_start_and_stop_pipeline_flow(manager):
    """
    Start a streaming pipeline and then stop it gracefully.
    """
    logger.info("Test: start and stop pipeline flow")

    pipeline_name = "p1"
    pipeline_cfg = {
        "type": "streaming",
        "nodes": [
            {
                "name": "n1",
                "input": {
                    "format": "kafka",
                    "options": {"subscribe": "t", "kafka.bootstrap.servers": "b"},
                },
                "output": {"format": "console"},
            }
        ],
    }

    exec_id = manager.start_pipeline(pipeline_name, pipeline_cfg)
    logger.debug("Pipeline started, exec_id=%s", exec_id)
    assert isinstance(exec_id, str)

    time.sleep(0.1)

    ok = manager.stop_pipeline(exec_id, graceful=True, timeout_seconds=2.0)
    logger.debug("stop_pipeline returned: %s", ok)
    assert ok is True

    assert (
        manager.query_manager.stop_query.called
    ), "Expected StreamingPipelineManager to call query_manager.stop_query() when stopping a pipeline"

    status = manager.get_pipeline_status(exec_id)
    logger.debug("Pipeline status for %s: %s", exec_id, status)
    assert status is not None


def test_start_exceeding_max_concurrent_raises(manager):
    """
    Verify that starting more pipelines than max_concurrent_pipelines raises."""
    logger.info("Test: starting more pipelines than allowed should raise")

    pipeline_cfg = {
        "type": "streaming",
        "nodes": [
            {
                "name": "n1",
                "input": {
                    "format": "kafka",
                    "options": {"subscribe": "t", "kafka.bootstrap.servers": "b"},
                },
                "output": {"format": "console"},
            }
        ],
    }

    exec1 = manager.start_pipeline("p1", pipeline_cfg)
    logger.debug("Started pipeline p1, exec_id=%s", exec1)
    exec2 = manager.start_pipeline("p2", pipeline_cfg)
    logger.debug("Started pipeline p2, exec_id=%s", exec2)

    with pytest.raises(Exception):
        logger.debug("Attempting to start pipeline p3 which should exceed concurrency limits")
        manager.start_pipeline("p3", pipeline_cfg)
