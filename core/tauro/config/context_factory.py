from tauro.config.context import Context
from tauro.config.streaming_context import StreamingContext
from tauro.config.ml_context import MLContext


class ContextFactory:
    """Factory for creating specialized contexts with priority handling."""

    @staticmethod
    def create_context(base_context: Context) -> Context:
        """Create specialized context based on pipeline configurations."""
        pipelines = base_context.pipelines_config.values()
        has_streaming = any(p.get("type") == "streaming" for p in pipelines)
        has_ml = any(p.get("type") == "ml" for p in pipelines)

        if has_streaming and has_ml:
            return HybridContext.from_base_context(base_context)
        elif has_streaming:
            return StreamingContext.from_base_context(base_context)
        elif has_ml:
            return MLContext.from_base_context(base_context)

        return base_context


class HybridContext(StreamingContext, MLContext):
    """Combined context for hybrid streaming/ML pipelines."""

    @classmethod
    def from_base_context(cls, base_context: Context) -> "HybridContext":
        return cls(
            global_settings=base_context.global_settings,
            pipelines_config=base_context.pipelines_config,
            nodes_config=base_context.nodes_config,
            input_config=base_context.input_config,
            output_config=base_context.output_config,
        )

    def _validate_hybrid_config(self):
        """Special validations for hybrid environments"""
        super()._validate_streaming_configurations()
        super()._validate_ml_configurations()

        # Validaciones adicionales específicas para híbridos
        for pipeline in self.pipelines_config.values():
            if pipeline.get("type") == "hybrid":
                self._validate_hybrid_pipeline(pipeline)
