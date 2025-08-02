from functools import lru_cache
from typing import Any, Dict
import logging
from tauro.config.context import Context
from tauro.config.exceptions import ConfigValidationError
from tauro.config.streaming_validators import StreamingValidator

logger = logging.getLogger(__name__)


class StreamingContext(Context):
    """Context for managing streaming pipelines and configurations."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.streaming_validator = StreamingValidator()
        self._streaming_nodes = (
            self.get_streaming_nodes()
        )  # Precalcular nodos streaming
        self._validate_streaming_configurations()

    @classmethod
    def from_base_context(cls, base_context: Context) -> "StreamingContext":
        """Create StreamingContext from base Context instance."""
        return cls(
            global_settings=base_context.global_settings,
            pipelines_config=base_context.pipelines_config,
            nodes_config=base_context.nodes_config,
            input_config=base_context.input_config,
            output_config=base_context.output_config,
        )

    @property
    @lru_cache(maxsize=1)
    def streaming_nodes(self) -> Dict[str, Dict[str, Any]]:
        """Cache streaming nodes for performance."""
        return self.get_streaming_nodes()

    @property
    @lru_cache(maxsize=1)
    def streaming_pipelines(self) -> Dict[str, Dict[str, Any]]:
        """Cache streaming pipelines for performance."""
        return self.get_streaming_pipelines()

    def get_streaming_nodes(self) -> Dict[str, Dict[str, Any]]:
        """Return precomputed streaming nodes."""
        return self._streaming_nodes

    def get_streaming_pipelines(self) -> Dict[str, Dict[str, Any]]:
        """Get all streaming pipelines from the configuration."""
        return {
            name: pipeline
            for name, pipeline in self.pipelines_config.items()
            if pipeline.get("type") == "streaming"
        }

    def _validate_streaming_configurations(self) -> None:
        """Validate streaming-specific configurations and dependencies."""
        streaming_pipelines = self.streaming_pipelines
        for pipeline_name, pipeline_config in streaming_pipelines.items():
            self.streaming_validator.validate_streaming_pipeline_config(pipeline_config)
        self.validate_streaming_node_dependencies()

    def validate_streaming_node_dependencies(self) -> None:
        """Validate streaming node dependencies and raise errors for incompatibilities."""
        streaming_nodes = self.streaming_nodes

        for node_name, node_config in streaming_nodes.items():
            dependencies = node_config.get("dependencies", [])

            for dep in dependencies:
                dep_name = dep if isinstance(dep, str) else str(dep)
                dep_config = self.nodes_config.get(dep_name)

                if not dep_config:
                    raise ConfigValidationError(
                        f"Streaming node '{node_name}' depends on missing node '{dep_name}'"
                    )

                if not self._is_streaming_node(dep_config):
                    raise ConfigValidationError(
                        f"Streaming node '{node_name}' depends on batch node '{dep_name}', which is incompatible"
                    )

    def _validate_pipeline_compatibility(
        self,
        batch_pipelines: Dict[str, Dict[str, Any]],
        streaming_pipelines: Dict[str, Dict[str, Any]],
    ) -> None:
        """Validate compatibility between all batch and streaming pipelines."""
        if not batch_pipelines or not streaming_pipelines:
            return

        for batch_name, batch_pipeline in batch_pipelines.items():
            for streaming_name, streaming_pipeline in streaming_pipelines.items():
                warnings = self.streaming_validator.validate_pipeline_compatibility(
                    batch_pipeline, streaming_pipeline
                )
                for warning in warnings:
                    logger.warning(
                        f"Compatibility issue between '{batch_name}' and '{streaming_name}': {warning}"
                    )

    def _is_streaming_node(self, node_config: Dict[str, Any]) -> bool:
        """Check if a node is streaming-compatible with improved validation."""
        input_conf = node_config.get("input", {})
        output_conf = node_config.get("output", {})

        input_format = (
            input_conf.get("format", "") if isinstance(input_conf, dict) else ""
        )
        output_format = (
            output_conf.get("format", "") if isinstance(output_conf, dict) else ""
        )

        return (
            input_format
            in self.streaming_validator.SUPPORTED_STREAMING_FORMATS["input"]
            or output_format
            in self.streaming_validator.SUPPORTED_STREAMING_FORMATS["output"]
        )
