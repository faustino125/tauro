from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from tauro.config.context import Context


class ContextFactory:
    """Factory for creating specialized contexts with priority handling."""

    @staticmethod
    def create_context(base_context: "Context") -> "Context":
        """Create specialized context based on pipeline configurations."""
        pipeline_types = {
            name: pipeline.get("type", "batch")
            for name, pipeline in base_context.pipelines_config.items()
        }

        has_streaming = any(t == "streaming" for t in pipeline_types.values())
        has_ml = any(t == "ml" for t in pipeline_types.values())
        has_hybrid = any(t == "hybrid" for t in pipeline_types.values())

        if has_hybrid or (has_streaming and has_ml):
            from tauro.config.context_hierarchy import HybridContext
            return HybridContext(base_context)
        elif has_streaming:
            from tauro.config.streaming_context import StreamingContext
            return StreamingContext.from_base_context(base_context)
        elif has_ml:
            from tauro.config.ml_context import MLContext
            return MLContext.from_base_context(base_context)

        return base_context
