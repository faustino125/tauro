from tauro.config.exceptions import ConfigValidationError
from tauro.config.context import Context


class HybridContext(Context):
    """Combined context for hybrid streaming/ML pipelines."""

    def __init__(self, base_context: Context):
        super().__init__(
            global_settings=base_context.global_settings,
            pipelines_config=base_context.pipelines_config,
            nodes_config=base_context.nodes_config,
            input_config=base_context.input_config,
            output_config=base_context.output_config,
        )

        self.base_context = base_context

        from tauro.config.streaming_context import StreamingContext
        from tauro.config.ml_context import MLContext

        self._streaming_ctx = StreamingContext.from_base_context(self)
        self._ml_ctx = MLContext.from_base_context(self)

        if hasattr(base_context, "spark"):
            self.spark = base_context.spark

        self._validate_hybrid_config()

    def _validate_hybrid_config(self):
        """Enhanced cross-validation for hybrid pipelines"""
        self._validate_cross_dependencies()

        hybrid_pipelines = {
            name: p
            for name, p in self.pipelines_config.items()
            if p.get("type") == "hybrid"
        }

        from .cross_validators import HybridValidator

        HybridValidator.validate_context(self)

    def _validate_cross_dependencies(self):
        """Validate dependencies between different context types"""
        errors = []

        for node_name, config in self.nodes_config.items():
            deps = config.get("dependencies", [])

            for dep in deps:
                dep_config = self.nodes_config.get(dep)
                if not dep_config:
                    continue

                if self._ml_ctx._is_compatible_node(
                    config
                ) and self._streaming_ctx._is_compatible_node(dep_config):
                    if not self._validate_ml_streaming_dependency(node_name, dep):
                        errors.append(
                            f"ML node '{node_name}' has incompatible dependency "
                            f"on streaming node '{dep}'"
                        )

                if self._streaming_ctx._is_compatible_node(
                    config
                ) and self._ml_ctx._is_compatible_node(dep_config):
                    if not self._validate_streaming_ml_dependency(node_name, dep):
                        errors.append(
                            f"Streaming node '{node_name}' has incompatible dependency "
                            f"on ML node '{dep}'"
                        )

        if errors:
            raise ConfigValidationError("\n".join(errors))

    def _validate_ml_streaming_dependency(
        self, ml_node: str, streaming_dep: str
    ) -> bool:
        """Validate ML -> Streaming dependency compatibility"""
        dep_config = self.nodes_config[streaming_dep]
        output_format = dep_config.get("output", {}).get("format", "")
        return output_format in ["delta", "parquet"]

    def _validate_streaming_ml_dependency(
        self, streaming_node: str, ml_dep: str
    ) -> bool:
        """Validate Streaming -> ML dependency compatibility"""
        dep_config = self.nodes_config[ml_dep]
        model_type = dep_config.get("model", {}).get("type", "")
        return model_type == "spark_ml"

    def _validate_hybrid_pipeline_structure(self, name: str, pipeline: dict):
        """Ensure hybrid pipelines contain both node types"""
        nodes = pipeline.get("nodes", [])

        has_streaming = any(
            self._streaming_ctx._is_compatible_node(self.nodes_config[n]) for n in nodes
        )
        has_ml = any(
            self._ml_ctx._is_compatible_node(self.nodes_config[n]) for n in nodes
        )
