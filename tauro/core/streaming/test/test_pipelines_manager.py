import pytest  # type: ignore
import time
from unittest import mock
from tauro.core.streaming.pipeline_manager import StreamingPipelineManager
from tauro.core.streaming.exceptions import StreamingPipelineError
from tauro.core.streaming.validators import StreamingValidator


class DummyContext:
    def __init__(self):
        self.format_policy = None
        self.nodes_config = {}
        self.spark = None


@pytest.fixture
def manager():
    ctx = DummyContext()
    mgr = StreamingPipelineManager(ctx, max_concurrent_pipelines=2, validator=StreamingValidator())
    # Replace query_manager with a mock to avoid starting real queries
    mgr.query_manager = mock.MagicMock()
    # create_and_start_query returns a mock streaming query object
    fake_query = mock.MagicMock()
    fake_query.isActive = True
    mgr.query_manager.create_and_start_query.return_value = fake_query
    # stop_query returns True by default
    mgr.query_manager.stop_query.return_value = True
    return mgr


def test_start_and_stop_pipeline_flow(manager):
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

    # Start pipeline (returns execution id)
    exec_id = manager.start_pipeline(pipeline_name, pipeline_cfg)
    assert isinstance(exec_id, str)

    # Allow a short time for the executor to schedule run (it will create queries via mocked query_manager)
    time.sleep(0.1)

    # Now stop pipeline gracefully
    ok = manager.stop_pipeline(exec_id, graceful=True, timeout_seconds=2.0)
    assert ok is True

    # After stopping, pipeline status should be 'stopped' or 'completed' (we check stopped/exists)
    status = manager.get_pipeline_status(exec_id)
    assert status is not None
    assert status["execution_id"] == exec_id


def test_start_pipeline_exceeding_maximum_raises(manager):
    # Fill running pipelines up to limit
    with manager._lock:
        for i in range(manager.max_concurrent_pipelines):
            manager._running_pipelines[f"id_{i}"] = {"pipeline_name": f"p{i}"}

    # Attempting to start a new pipeline should raise StreamingPipelineError
    with pytest.raises(StreamingPipelineError):
        manager.start_pipeline(
            "new_p",
            {
                "type": "streaming",
                "nodes": [
                    {
                        "input": {
                            "format": "kafka",
                            "options": {
                                "subscribe": "t",
                                "kafka.bootstrap.servers": "b",
                            },
                        },
                        "output": {"format": "console"},
                    }
                ],
            },
        )
