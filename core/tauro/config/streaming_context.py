from functools import cached_property
from typing import Any, Dict
import logging
from tauro.config.validators import StreamingValidationStrategy
from tauro.config.context import Context
from tauro.config.exceptions import ConfigValidationError
from tauro.config.streaming_validators import StreamingValidator
from tauro.config.base_specialized_context import BaseSpecializedContext

logger = logging.getLogger(__name__)


class StreamingContext(BaseSpecializedContext):
    """Context for managing streaming pipelines and configurations."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.streaming_validator = StreamingValidator()
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

    @cached_property
    def streaming_nodes(self) -> Dict[str, Dict[str, Any]]:
        """Cache streaming nodes for performance."""
        return {
            name: node
            for name, node in self.nodes_config.items()
            if self._is_streaming_node(node)
        }

    def _create_validator(self):
        return StreamingValidationStrategy()

    def _get_specialized_nodes(self) -> Dict[str, Dict[str, Any]]:
        return {
            name: node
            for name, node in self.nodes_config.items()
            if self._is_compatible_node(node)
        }

    def _is_compatible_node(self, node_config: Dict[str, Any]) -> bool:
        input_conf = node_config.get("input", {})
        output_conf = node_config.get("output", {})

        input_format = (
            input_conf.get("format", "") if isinstance(input_conf, dict) else ""
        )
        output_format = (
            output_conf.get("format", "") if isinstance(output_conf, dict) else ""
        )

        return (
            input_format in self._validator.SUPPORTED_STREAMING_FORMATS["input"]
            or output_format in self._validator.SUPPORTED_STREAMING_FORMATS["output"]
        )

    def _get_context_type_name(self) -> str:
        return "Streaming"

    def _validate_configurations(self) -> None:
        """Validate streaming-specific configurations."""
        super()._validate_configurations()
        for pipeline_name, pipeline_config in self.streaming_pipelines.items():
            pipeline_with_name = {**pipeline_config, "name": pipeline_name}
            self._validator.validate_streaming_pipeline_config(pipeline_with_name)
            self._validator.validate_streaming_pipeline_with_nodes(
                pipeline_with_name, self.nodes_config
            )

    @cached_property
    def streaming_pipelines(self) -> Dict[str, Dict[str, Any]]:
        """Cache streaming pipelines for performance."""
        return {
            name: pipeline
            for name, pipeline in self.pipelines_config.items()
            if pipeline.get("type") == "streaming"
        }

    def get_streaming_nodes(self) -> Dict[str, Dict[str, Any]]:
        """Return streaming nodes (delegates to cached property)."""
        return self.streaming_nodes

    def get_streaming_pipelines(self) -> Dict[str, Dict[str, Any]]:
        """Get all streaming pipelines (delegates to cached property)."""
        return self.streaming_pipelines

    def _validate_streaming_configurations(self) -> None:
        """Validate streaming-specific configurations and dependencies."""
        for pipeline_name, pipeline_config in self.streaming_pipelines.items():
            pipeline_with_name = {**pipeline_config, "name": pipeline_name}
            self.streaming_validator.validate_streaming_pipeline_config(
                pipeline_with_name
            )

            self.streaming_validator.validate_streaming_pipeline_with_nodes(
                pipeline_with_name, self.nodes_config
            )

        self._validate_specialized_node_dependencies(
            self.streaming_nodes, "streaming", self._is_streaming_node
        )

    def _validate_specialized_node_dependencies(
        self,
        specialized_nodes: Dict[str, Dict[str, Any]],
        node_type: str,
        is_compatible_func,
    ) -> None:
        """Generic validation for specialized node dependencies."""
        for node_name, node_config in specialized_nodes.items():
            dependencies = node_config.get("dependencies", [])

            if not isinstance(dependencies, list):
                raise ConfigValidationError(
                    f"{node_type.title()} node '{node_name}' dependencies must be a list"
                )

            for dep in dependencies:
                dep_name = dep if isinstance(dep, str) else str(dep)
                dep_config = self.nodes_config.get(dep_name)

                if not dep_config:
                    raise ConfigValidationError(
                        f"{node_type.title()} node '{node_name}' depends on missing node '{dep_name}'"
                    )

                if not is_compatible_func(dep_config):
                    raise ConfigValidationError(
                        f"{node_type.title()} node '{node_name}' depends on incompatible node '{dep_name}'"
                    )

    def validate_streaming_node_dependencies(self) -> None:
        """Validate streaming node dependencies (maintained for backward compatibility)."""
        self._validate_specialized_node_dependencies(
            self.streaming_nodes, "streaming", self._is_streaming_node
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
