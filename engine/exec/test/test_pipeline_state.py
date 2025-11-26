import time

from tauro.core.exec.pipeline_state import (
    UnifiedPipelineState,
    NodeType,
    NodeStatus,
)


class FakeQueryHandle:
    def __init__(self):
        self.stopped = False

    def stop(self):
        self.stopped = True


def test_register_start_complete_node_and_notify_dependents():
    ups = UnifiedPipelineState()
    ups.register_node("batch1", NodeType.BATCH, [])
    ups.register_node("stream1", NodeType.STREAMING, ["batch1"])
    assert ups.get_node_status("batch1") == NodeStatus.PENDING
    started = ups.start_node_execution("batch1")
    assert started
    ups.complete_node_execution("batch1", output_path="/tmp/out")
    assert ups.get_batch_output_path("batch1") == "/tmp/out"
    # now stream1 should be ready
    assert ups.is_node_ready("stream1") is True


def test_fail_node_with_retries_and_circuit_breaker():
    ups = UnifiedPipelineState(circuit_breaker_threshold=1)
    ups.register_node("n1", NodeType.BATCH, [])
    # set low max_retries on the node object
    ups._nodes["n1"].max_retries = 0
    # first failure should set FAILED (since no retries allowed)
    ups.fail_node_execution("n1", "err1")
    assert ups.get_node_status("n1") == NodeStatus.FAILED
    # subsequent failures should trigger circuit breaker when threshold exceeded
    ups.register_node("n2", NodeType.BATCH, [])
    # call fail multiple times on n2 to increase failure count
    ups.fail_node_execution("n2", "e")
    # second exceed threshold (threshold=1)
    ups.fail_node_execution("n2", "e2")
    assert ups._circuit_open is True


def test_stop_dependent_streaming_nodes_by_handle_and_by_id(monkeypatch):
    ups = UnifiedPipelineState()
    # register batch and streaming nodes and cross-dependency
    ups.register_node("batch1", NodeType.BATCH, [])
    # Ensure the batch node will be marked FAILED immediately (no retries)
    # so that failure propagation stops dependent streaming queries synchronously.
    ups._nodes["batch1"].max_retries = 0

    ups.register_node("streamA", NodeType.STREAMING, ["batch1"])
    ups.register_node("streamB", NodeType.STREAMING, ["batch1"])

    # register streaming queries: one handle-like, one id-like
    handle = FakeQueryHandle()
    ups.register_streaming_query("streamA", handle)
    # streamB registered as id string; stopper will be configured to accept it
    ups.register_streaming_query("streamB", "qid-123")

    stopped = []

    def stopper(qid):
        stopped.append(qid)
        return True

    ups.set_streaming_stopper(stopper)

    # failing batch1 should stop both streaming nodes
    ups.fail_node_execution("batch1", "boom")
    # small sleep to allow any synchronous propagation to finish (should be immediate)
    time.sleep(0.01)

    # handle should have been stopped and id should have been requested
    assert handle.stopped is True
    assert "qid-123" in stopped
