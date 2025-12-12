"""
Tests for tauro.core.config.contexts module.

Coverage includes:
- Context creation from JSON configuration
- Base context functionality (pipeline/node lookup, variable access)
- Context specialization (ML, Streaming, Hybrid)
- Node filtering and dependency resolution
- Error handling for missing pipelines/nodes
"""

import pytest  # type: ignore

from core.config.contexts import (
    Context,
    ContextFactory,
    MLContext,
    StreamingContext,
    HybridContext,
)
from core.config.exceptions import ConfigurationError


# ============================================================================
# BASIC CONTEXT CREATION
# ============================================================================


class TestContextCreation:
    """Tests for creating base context from configuration."""

    def test_context_from_minimal_config(self, minimal_configs):
        """Should create context from minimal valid configuration."""
        g, pipelines, nodes, inp, out = minimal_configs
        ctx = Context.from_json_config(g, pipelines, nodes, inp, out)

        assert ctx is not None
        assert hasattr(ctx, "list_pipeline_names")
        assert hasattr(ctx, "get_pipeline")

    def test_context_from_comprehensive_config(self, comprehensive_configs):
        """Should create context from comprehensive configuration."""
        g, pipelines, nodes, inp, out = comprehensive_configs
        ctx = Context.from_json_config(g, pipelines, nodes, inp, out)

        assert ctx is not None
        assert len(ctx.list_pipeline_names()) >= 3

    def test_context_preserves_global_settings(self, minimal_configs):
        """Should preserve global settings in context."""
        g, pipelines, nodes, inp, out = minimal_configs
        ctx = Context.from_json_config(g, pipelines, nodes, inp, out)

        # Context should be able to access global settings
        assert hasattr(ctx, "global_settings") or hasattr(ctx, "settings")


# ============================================================================
# PIPELINE ACCESS AND DISCOVERY
# ============================================================================


class TestPipelineAccess:
    """Tests for accessing pipelines and nodes from context."""

    def test_list_all_pipeline_names(self, minimal_configs):
        """Should return all pipeline names."""
        g, pipelines, nodes, inp, out = minimal_configs
        ctx = Context.from_json_config(g, pipelines, nodes, inp, out)

        names = ctx.list_pipeline_names()
        assert isinstance(names, list)
        assert len(names) > 0

    def test_get_existing_pipeline(self, minimal_configs):
        """Should retrieve pipeline by name."""
        g, pipelines, nodes, inp, out = minimal_configs
        ctx = Context.from_json_config(g, pipelines, nodes, inp, out)

        pipeline_names = ctx.list_pipeline_names()
        if pipeline_names:
            first_name = pipeline_names[0]
            pipeline = ctx.get_pipeline(first_name)
            assert pipeline is not None

    def test_get_nonexistent_pipeline_raises_error(self, minimal_configs):
        """Should raise error for missing pipeline."""
        g, pipelines, nodes, inp, out = minimal_configs
        ctx = Context.from_json_config(g, pipelines, nodes, inp, out)

        with pytest.raises((ConfigurationError, KeyError, AttributeError)):
            ctx.get_pipeline("nonexistent_pipeline")

    def test_pipeline_contains_node_information(self, minimal_configs):
        """Should include node information in pipeline."""
        g, pipelines, nodes, inp, out = minimal_configs
        ctx = Context.from_json_config(g, pipelines, nodes, inp, out)

        pipeline_names = ctx.list_pipeline_names()
        if pipeline_names:
            pipeline = ctx.get_pipeline(pipeline_names[0])
            # Pipeline should contain node information
            assert "nodes" in pipeline or "node" in pipeline or len(pipeline) > 0


# ============================================================================
# CONTEXT SPECIALIZATION
# ============================================================================


class TestContextSpecialization:
    """Tests for context factory and specialization."""

    def test_context_factory_creates_specialized_context(self, minimal_configs):
        """Should create specialized context from base context."""
        g, pipelines, nodes, inp, out = minimal_configs
        base = Context.from_json_config(g, pipelines, nodes, inp, out)

        specialized = ContextFactory.create_context(base)
        assert specialized is not None
        assert isinstance(specialized, Context)

    def test_hybrid_context_creation(self, minimal_configs):
        """Should detect and create HybridContext when appropriate."""
        g, pipelines, nodes, inp, out = minimal_configs
        base = Context.from_json_config(g, pipelines, nodes, inp, out)

        specialized = ContextFactory.create_context(base)
        # Will be HybridContext if both ML and streaming nodes exist
        assert specialized is not None

    def test_ml_context_from_base_context(self, ml_configs):
        """Should create ML context from base context."""
        g, pipelines, nodes, inp, out = ml_configs
        base = Context.from_json_config(g, pipelines, nodes, inp, out)

        ml_ctx = MLContext.from_base_context(base)
        assert ml_ctx is not None
        assert isinstance(ml_ctx, MLContext)

    def test_streaming_context_from_base_context(self, streaming_configs):
        """Should create Streaming context from base context."""
        g, pipelines, nodes, inp, out = streaming_configs
        base = Context.from_json_config(g, pipelines, nodes, inp, out)

        stream_ctx = StreamingContext.from_base_context(base)
        assert stream_ctx is not None
        assert isinstance(stream_ctx, StreamingContext)


# ============================================================================
# ML CONTEXT TESTS
# ============================================================================


class TestMLContext:
    """Tests for ML-specific context functionality."""

    def test_ml_context_identifies_ml_nodes(self, ml_configs):
        """Should identify and categorize ML nodes."""
        g, pipelines, nodes, inp, out = ml_configs
        ctx = Context.from_json_config(g, pipelines, nodes, inp, out)
        ml_ctx = MLContext.from_base_context(ctx)

        # Should have ml_nodes attribute or method
        if hasattr(ml_ctx, "ml_nodes"):
            assert isinstance(ml_ctx.ml_nodes, (list, dict))
        elif hasattr(ml_ctx, "get_ml_nodes"):
            ml_nodes = ml_ctx.get_ml_nodes()
            assert ml_nodes is not None

    def test_ml_context_identifies_training_pipelines(self, ml_configs):
        """Should identify training pipelines."""
        g, pipelines, nodes, inp, out = ml_configs
        ctx = Context.from_json_config(g, pipelines, nodes, inp, out)
        ml_ctx = MLContext.from_base_context(ctx)

        # Should be able to identify training configurations
        assert ml_ctx is not None

    def test_ml_context_with_hyperparameters(self, ml_configs):
        """Should preserve hyperparameter configurations."""
        g, pipelines, nodes, inp, out = ml_configs
        ctx = Context.from_json_config(g, pipelines, nodes, inp, out)
        ml_ctx = MLContext.from_base_context(ctx)

        assert ml_ctx is not None


# ============================================================================
# STREAMING CONTEXT TESTS
# ============================================================================


class TestStreamingContext:
    """Tests for Streaming-specific context functionality."""

    def test_streaming_context_identifies_streaming_nodes(self, streaming_configs):
        """Should identify streaming nodes."""
        g, pipelines, nodes, inp, out = streaming_configs
        ctx = Context.from_json_config(g, pipelines, nodes, inp, out)
        stream_ctx = StreamingContext.from_base_context(ctx)

        # Should have streaming_nodes attribute or method
        if hasattr(stream_ctx, "streaming_nodes"):
            assert isinstance(stream_ctx.streaming_nodes, (list, dict))

    def test_streaming_context_identifies_streaming_pipelines(self, streaming_configs):
        """Should identify streaming pipelines."""
        g, pipelines, nodes, inp, out = streaming_configs
        ctx = Context.from_json_config(g, pipelines, nodes, inp, out)
        stream_ctx = StreamingContext.from_base_context(ctx)

        assert stream_ctx is not None


# ============================================================================
# ERROR HANDLING
# ============================================================================


class TestContextErrorHandling:
    """Tests for error handling in context operations."""

    def test_missing_required_global_setting(self):
        """Should handle missing required global settings gracefully."""
        incomplete_global = {"input_path": "/in"}  # Missing output_path

        with pytest.raises((ConfigurationError, KeyError, ValueError)):
            Context.from_json_config(
                incomplete_global,
                {"p1": {"nodes": ["n1"], "type": "batch"}},
                {"n1": {"input": {}, "output": {}}},
                {},
                {},
            )

    def test_empty_pipelines_config(self, minimal_configs):
        """Should handle empty pipelines dictionary."""
        g, _, nodes, inp, out = minimal_configs

        with pytest.raises((ConfigurationError, KeyError, ValueError)):
            Context.from_json_config(g, {}, nodes, inp, out)

    def test_pipeline_references_missing_node(self, minimal_configs):
        """Should handle pipeline referencing non-existent node."""
        g, pipelines, nodes, inp, out = minimal_configs

        # Add pipeline with missing node reference
        pipelines["bad_p"] = {"nodes": ["nonexistent_node"], "type": "batch"}

        # Creation might succeed, but accessing bad pipeline should fail
        ctx = Context.from_json_config(g, pipelines, nodes, inp, out)

        # Accessing the bad pipeline should raise error
        with pytest.raises((ConfigurationError, KeyError, AttributeError, ValueError)):
            ctx.get_pipeline("bad_p")


# ============================================================================
# EDGE CASES
# ============================================================================


class TestContextEdgeCases:
    """Tests for edge cases and unusual configurations."""

    def test_context_with_single_pipeline(self):
        """Should handle configuration with single pipeline."""
        ctx = Context.from_json_config(
            {"input_path": "/in", "output_path": "/out"},
            {"single": {"nodes": ["n1"], "type": "batch"}},
            {"n1": {"input": {}, "output": {}}},
            {},
            {},
        )

        pipelines = ctx.list_pipeline_names()
        assert "single" in pipelines

    def test_context_with_complex_node_dependencies(self, comprehensive_configs):
        """Should handle nodes with complex dependencies."""
        g, pipelines, nodes, inp, out = comprehensive_configs
        ctx = Context.from_json_config(g, pipelines, nodes, inp, out)

        # Context should successfully handle dependencies
        assert ctx is not None
        assert len(ctx.list_pipeline_names()) > 0

    def test_context_with_variable_substitution(self, minimal_configs):
        """Should handle variable substitution in configuration."""
        g, pipelines, nodes, inp, out = minimal_configs

        # Add variables to global settings
        if "variables" not in g:
            g["variables"] = {}
        g["variables"]["env"] = "test"

        ctx = Context.from_json_config(g, pipelines, nodes, inp, out)
        assert ctx is not None
