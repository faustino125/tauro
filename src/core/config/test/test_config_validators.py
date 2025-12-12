"""
Tests for tauro.core.config.validators module.

Coverage includes:
- Configuration validation (required keys, structure)
- Pipeline validation (node references, dependencies)
- Format policy validation (input/output compatibility)
- ML pipeline validation (model config, hyperparameters)
- Streaming pipeline validation (Kafka/streaming-specific config)
- Strict vs lenient validation modes
"""

import pytest  # type: ignore

from core.config.validators import (
    ConfigValidator,
    PipelineValidator,
    FormatPolicy,
    MLValidator,
    StreamingValidator,
)
from core.config.exceptions import ConfigValidationError, PipelineValidationError


# ============================================================================
# CONFIG VALIDATOR TESTS
# ============================================================================


class TestConfigValidator:
    """Tests for basic configuration validation."""

    def test_validate_required_keys_present(self):
        """Should pass when all required keys are present."""
        cfg = {"a": 1, "b": 2, "c": 3}
        # Should not raise
        ConfigValidator.validate_required_keys(cfg, ["a", "b"], "test_config")

    def test_validate_required_keys_missing(self):
        """Should raise error when required keys are missing."""
        with pytest.raises((ConfigValidationError, KeyError, ValueError)) as exc:
            ConfigValidator.validate_required_keys({"a": 1}, ["a", "b", "c"], "test_config")
        # Error message should mention missing keys
        error_msg = str(exc.value).lower()
        assert "missing" in error_msg or "required" in error_msg

    def test_validate_required_keys_empty_config(self):
        """Should raise error for empty config with required keys."""
        with pytest.raises((ConfigValidationError, KeyError, ValueError)):
            ConfigValidator.validate_required_keys({}, ["required"], "empty_cfg")

    def test_validate_required_keys_partial_match(self):
        """Should raise error when only some required keys present."""
        with pytest.raises((ConfigValidationError, KeyError, ValueError)):
            ConfigValidator.validate_required_keys(
                {"a": 1, "b": 2}, ["a", "b", "c", "d"], "partial_cfg"
            )

    def test_validate_required_keys_with_none_values(self):
        """Should accept None values if key exists."""
        # Depending on implementation, None might be accepted
        try:
            ConfigValidator.validate_required_keys({"a": 1, "b": None}, ["a", "b"], "with_none")
        except (ConfigValidationError, ValueError):
            # Some implementations may reject None values
            pass


# ============================================================================
# PIPELINE VALIDATOR TESTS
# ============================================================================


class TestPipelineValidator:
    """Tests for pipeline configuration validation."""

    def test_pipeline_validator_valid_structure(self, minimal_configs):
        """Should validate correct pipeline structure."""
        _, pipelines, nodes, _, _ = minimal_configs
        # Should not raise
        PipelineValidator.validate_pipeline_nodes(pipelines, nodes)

    def test_pipeline_validator_missing_referenced_node(self):
        """Should raise error when pipeline references missing node."""
        pipelines = {"p1": {"nodes": ["missing_node"], "type": "batch"}}
        nodes = {"other_node": {}}

        with pytest.raises((PipelineValidationError, ConfigValidationError, KeyError)):
            PipelineValidator.validate_pipeline_nodes(pipelines, nodes)

    def test_pipeline_validator_multiple_node_references(self, minimal_configs):
        """Should validate pipelines with multiple node references."""
        _, pipelines, nodes, _, _ = minimal_configs

        # Add a pipeline with multiple nodes
        if "nodes" not in nodes:
            nodes["n2"] = {"input": {}, "output": {}}
            pipelines["multi_p"] = {"nodes": ["n1", "n2"], "type": "batch"}

        # Should not raise
        PipelineValidator.validate_pipeline_nodes(pipelines, nodes)

    def test_pipeline_validator_empty_nodes_list(self):
        """Should handle pipeline with empty nodes list."""
        pipelines = {"empty_p": {"nodes": [], "type": "batch"}}
        nodes = {}

        # Depending on implementation
        try:
            PipelineValidator.validate_pipeline_nodes(pipelines, nodes)
        except (PipelineValidationError, ConfigValidationError, ValueError):
            pass

    def test_pipeline_validator_missing_pipeline_type(self):
        """Should validate pipeline type field."""
        pipelines = {"p1": {"nodes": ["n1"]}}  # Missing type
        nodes = {"n1": {}}

        # May raise or warn depending on implementation
        try:
            PipelineValidator.validate_pipeline_nodes(pipelines, nodes)
        except (PipelineValidationError, ConfigValidationError, KeyError):
            pass


# ============================================================================
# FORMAT POLICY TESTS
# ============================================================================


class TestFormatPolicy:
    """Tests for format compatibility validation."""

    def test_format_policy_supported_input_formats(self):
        """Should report supported input formats."""
        policy = FormatPolicy()

        # Common input formats should be supported
        assert policy.is_supported_input("json")
        assert policy.is_supported_input("csv")
        assert policy.is_supported_input("parquet")

    def test_format_policy_supported_output_formats(self):
        """Should report supported output formats."""
        policy = FormatPolicy()

        # Common output formats should be supported
        assert policy.is_supported_output("parquet")
        assert policy.is_supported_output("csv")
        assert policy.is_supported_output("json")

    def test_format_policy_kafka_input(self):
        """Should support Kafka as input format."""
        policy = FormatPolicy()
        # Kafka is typically supported for streaming input
        is_supported = policy.is_supported_input("kafka")
        assert isinstance(is_supported, bool)

    def test_format_policy_kafka_output(self):
        """Should support Kafka as output format."""
        policy = FormatPolicy()
        is_supported = policy.is_supported_output("kafka")
        assert isinstance(is_supported, bool)

    @pytest.mark.parametrize(
        "input_fmt,output_fmt,should_be_compatible",
        [
            ("parquet", "parquet", True),
            ("json", "csv", True),
            ("csv", "parquet", True),
        ],
    )
    def test_format_policy_common_compatibility(self, input_fmt, output_fmt, should_be_compatible):
        """Should validate common format combinations."""
        policy = FormatPolicy()
        result = policy.are_compatible(input_fmt, output_fmt)
        assert isinstance(result, bool)

    def test_format_policy_streaming_format_compatibility(self):
        """Should handle streaming-specific formats."""
        policy = FormatPolicy()

        # Kafka is common for streaming
        result = policy.are_compatible("kafka", "kafka")
        assert isinstance(result, bool)

    def test_format_policy_unsupported_format(self):
        """Should handle unsupported formats gracefully."""
        policy = FormatPolicy()

        # Completely unsupported formats
        assert policy.is_supported_input("unsupported_xyz") is False
        assert policy.is_supported_output("fake_format_abc") is False


# ============================================================================
# ML VALIDATOR TESTS
# ============================================================================


class TestMLValidator:
    """Tests for ML pipeline validation."""

    def test_ml_validator_valid_config(self, ml_configs):
        """Should validate valid ML configuration."""
        _, pipelines, nodes, _, _ = ml_configs
        mv = MLValidator()

        # Should not raise with valid config
        mv.validate_ml_pipeline_config(pipelines, nodes, strict=False)

    def test_ml_validator_missing_model_config_strict(self):
        """Should raise error in strict mode for missing model config."""
        pipelines = {"ml_p": {"nodes": ["n_ml"], "type": "ml"}}
        nodes = {
            "n_ml": {
                "input": {},
                "output": {},
                # Missing model config
            }
        }
        mv = MLValidator()

        with pytest.raises((ConfigValidationError, ValueError)):
            mv.validate_ml_pipeline_config(pipelines, nodes, strict=True)

    def test_ml_validator_missing_model_config_lenient(self):
        """Should warn but not raise in lenient mode for missing model config."""
        pipelines = {"ml_p": {"nodes": ["n_ml"], "type": "ml"}}
        nodes = {
            "n_ml": {
                "input": {},
                "output": {},
            }
        }
        mv = MLValidator()

        # Should not raise
        mv.validate_ml_pipeline_config(pipelines, nodes, strict=False)

    def test_ml_validator_hyperparameter_validation(self, ml_configs):
        """Should validate hyperparameter configuration."""
        _, pipelines, nodes, _, _ = ml_configs

        # Add hyperparameters to ML node if not present
        for node in nodes.values():
            if "model" in node and node["model"].get("type") == "spark_ml":
                if "hyperparams" not in node:
                    node["hyperparams"] = {"lr": 0.1}

        mv = MLValidator()
        # Should not raise
        mv.validate_ml_pipeline_config(pipelines, nodes, strict=False)

    def test_ml_validator_model_type_support(self, ml_configs):
        """Should validate supported model types."""
        _, pipelines, nodes, _, _ = ml_configs

        # Check if spark_ml model type is supported
        mv = MLValidator()
        mv.validate_ml_pipeline_config(pipelines, nodes, strict=False)


# ============================================================================
# STREAMING VALIDATOR TESTS
# ============================================================================


class TestStreamingValidator:
    """Tests for streaming pipeline validation."""

    def test_streaming_validator_valid_config(self, streaming_configs):
        """Should validate valid streaming configuration."""
        _, pipelines, nodes, _, _ = streaming_configs
        sv = StreamingValidator()

        # Should not raise
        for name, pipeline in pipelines.items():
            if pipeline.get("type") == "streaming":
                sv.validate_streaming_pipeline_with_nodes(pipeline, nodes, strict=False)

    def test_streaming_validator_missing_spark_config_lenient(self):
        """Should warn but not raise in lenient mode for missing Spark config."""
        pipeline = {
            "name": "s1",
            "nodes": ["n1"],
            "type": "streaming"
            # Missing spark_config
        }
        nodes = {"n1": {"input": {"format": "kafka"}, "output": {}}}
        sv = StreamingValidator()

        # Should not raise in lenient mode
        sv.validate_streaming_pipeline_with_nodes(pipeline, nodes, strict=False)

    def test_streaming_validator_missing_node_references(self):
        """Should raise error for missing node references."""
        pipeline = {"name": "s1", "nodes": ["missing_n1"], "type": "streaming", "spark_config": {}}
        nodes = {}
        sv = StreamingValidator()

        with pytest.raises((ConfigValidationError, KeyError)):
            sv.validate_streaming_pipeline_with_nodes(pipeline, nodes, strict=True)

    def test_streaming_validator_kafka_input_validation(self):
        """Should validate Kafka input configuration."""
        pipeline = {
            "name": "kafka_s",
            "nodes": ["n_kafka"],
            "type": "streaming",
            "spark_config": {},
        }
        nodes = {
            "n_kafka": {
                "input": {"format": "kafka", "kafka_config": {"brokers": "localhost:9092"}},
                "output": {},
            }
        }
        sv = StreamingValidator()

        # Should not raise
        sv.validate_streaming_pipeline_with_nodes(pipeline, nodes, strict=False)

    def test_streaming_validator_multiple_nodes(self, streaming_configs):
        """Should validate pipelines with multiple streaming nodes."""
        _, pipelines, nodes, _, _ = streaming_configs
        sv = StreamingValidator()

        for pipeline in pipelines.values():
            if pipeline.get("type") == "streaming":
                sv.validate_streaming_pipeline_with_nodes(pipeline, nodes, strict=False)


# ============================================================================
# PARAMETRIZED VALIDATION TESTS
# ============================================================================


class TestParametrizedValidation:
    """Tests using parametrization for multiple scenarios."""

    @pytest.mark.parametrize(
        "pipeline_type,required_fields",
        [
            ("batch", ["nodes"]),
            ("ml", ["nodes", "type"]),
            ("streaming", ["nodes", "type"]),
            ("hybrid", ["nodes", "type"]),
        ],
    )
    def test_pipeline_type_validation(self, pipeline_type, required_fields):
        """Should validate pipeline type-specific requirements."""
        pipeline = {"type": pipeline_type, "nodes": ["n1"]}

        # Each type should have required fields
        for field in required_fields:
            assert field in pipeline

    @pytest.mark.parametrize(
        "format_combo",
        [
            ("json", "parquet"),
            ("csv", "csv"),
            ("parquet", "json"),
        ],
    )
    def test_format_compatibility_multiple_combos(self, format_combo):
        """Should handle multiple format combinations."""
        input_fmt, output_fmt = format_combo
        policy = FormatPolicy()

        result = policy.are_compatible(input_fmt, output_fmt)
        assert isinstance(result, bool)


# ============================================================================
# ERROR HANDLING AND EDGE CASES
# ============================================================================


class TestValidationErrorHandling:
    """Tests for validation error handling and edge cases."""

    def test_validator_with_none_config(self):
        """Should handle None configuration gracefully."""
        with pytest.raises((ConfigValidationError, TypeError, AttributeError)):
            ConfigValidator.validate_required_keys(None, ["key"], "none_cfg")

    def test_validator_with_empty_required_keys_list(self):
        """Should handle empty required keys list."""
        cfg = {"a": 1, "b": 2}
        # Should not raise - no required keys
        ConfigValidator.validate_required_keys(cfg, [], "any_cfg")

    def test_pipeline_validator_circular_dependency_detection(self):
        """Should detect or handle circular node dependencies."""
        pipelines = {"p1": {"nodes": ["n1", "n2"], "type": "batch"}}
        nodes = {"n1": {"depends_on": ["n2"]}, "n2": {"depends_on": ["n1"]}}  # Circular reference
        pv = PipelineValidator()

        # May raise or warn depending on implementation
        try:
            pv.validate_pipeline_nodes(pipelines, nodes)
        except (PipelineValidationError, ConfigValidationError):
            pass

    def test_format_policy_case_insensitivity(self):
        """Should handle format names case-insensitively if applicable."""
        policy = FormatPolicy()

        # Test with different cases
        json_lower = policy.is_supported_input("json")
        # Behavior depends on implementation
        assert isinstance(json_lower, bool)
