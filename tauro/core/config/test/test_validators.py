import pytest
from tauro.config.validators import (
    ConfigValidator,
    PipelineValidator,
    FormatPolicy,
    MLValidator,
    StreamingValidator,
)
from tauro.config.exceptions import ConfigValidationError, PipelineValidationError
import tauro.config.validators as validators


def test_validate_required_keys_missing():
    with pytest.raises(ConfigValidationError) as exc:
        ConfigValidator.validate_required_keys({"a": 1}, ["a", "b"], "cfg")
    assert "Missing required keys" in str(exc.value)


def test_pipeline_validator_missing_node():
    pipelines = {"p": {"nodes": ["n1"]}}
    nodes = {}
    with pytest.raises(PipelineValidationError):
        PipelineValidator.validate_pipeline_nodes(pipelines, nodes)


def test_format_policy_compatibility():
    policy = FormatPolicy()
    assert policy.is_supported_input("kafka")
    assert policy.is_supported_output("parquet")
    assert policy.are_compatible("parquet", "file_stream") is True
    assert not policy.are_compatible("json", "kafka")


def test_ml_validator_strict_and_lenient(monkeypatch):
    mv = MLValidator()
    pipelines = {"m": {"nodes": ["n1"], "spark_config": {}}}
    nodes = {"n1": {"input": {}, "output": {}}}
    # strict should raise missing required fields for node
    with pytest.raises(ConfigValidationError):
        mv.validate_ml_pipeline_config(pipelines, nodes, strict=True)

    # lenient should not raise; capture warnings emitted via loguru by patching the module logger
    captured = []

    def fake_warning(msg, *args, **kwargs):
        captured.append(str(msg))

    # validators.logger is the loguru logger imported in the validators module
    monkeypatch.setattr(validators.logger, "warning", fake_warning)

    # Should not raise
    mv.validate_ml_pipeline_config(pipelines, nodes, strict=False)

    # Ensure that at least one warning about missing required fields was emitted
    joined = " ".join(captured).lower()
    assert "missing required fields" in joined or "missing recommended spark ml config" in joined


def test_streaming_validator_node_missing():
    sv = StreamingValidator()
    pipeline = {"name": "s", "nodes": ["n1"], "spark_config": {}}
    nodes = {}
    with pytest.raises(ConfigValidationError):
        sv.validate_streaming_pipeline_with_nodes(pipeline, nodes, strict=True)
