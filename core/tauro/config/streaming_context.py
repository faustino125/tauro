# tauro/config/streaming_context.py
from typing import Any, Dict, List, Optional, Union

from loguru import logger

from tauro.config.context import Context
from tauro.streaming.constants import PipelineType
from tauro.streaming.validators import StreamingValidator


class StreamingContext(Context):
    """Extended Context with streaming pipeline support."""

    def __init__(
        self,
        global_settings: Union[str, Dict],
        pipelines_config: Union[str, Dict],
        nodes_config: Union[str, Dict],
        input_config: Union[str, Dict],
        output_config: Union[str, Dict],
    ):
        """Initialize the streaming context."""
        super().__init__(
            global_settings, pipelines_config, nodes_config, input_config, output_config
        )

        self.streaming_validator = StreamingValidator()
        self._validate_streaming_configurations()

        logger.info("StreamingContext initialized with streaming support")

    def _validate_streaming_configurations(self) -> None:
        """Validate streaming-specific configurations."""

        # Check for streaming pipelines and validate them
        streaming_pipelines = self.get_streaming_pipelines()

        for pipeline_name, pipeline_config in streaming_pipelines.items():
            try:
                self.streaming_validator.validate_streaming_pipeline_config(
                    pipeline_config
                )
                logger.debug(
                    f"Streaming pipeline '{pipeline_name}' validated successfully"
                )
            except Exception as e:
                logger.error(
                    f"Validation failed for streaming pipeline '{pipeline_name}': {e}"
                )
                raise

        # Validate compatibility between batch and streaming pipelines
        batch_pipelines = self.get_batch_pipelines()
        self._validate_pipeline_compatibility(batch_pipelines, streaming_pipelines)

    def get_streaming_pipelines(self) -> Dict[str, Dict[str, Any]]:
        """Get all streaming pipelines."""
        streaming_pipelines = {}

        for name, pipeline in self.pipelines.items():
            pipeline_type = pipeline.get("type", PipelineType.BATCH.value)
            if pipeline_type in [
                PipelineType.STREAMING.value,
                PipelineType.HYBRID.value,
            ]:
                streaming_pipelines[name] = pipeline

        return streaming_pipelines

    def get_batch_pipelines(self) -> Dict[str, Dict[str, Any]]:
        """Get all batch pipelines."""
        batch_pipelines = {}

        for name, pipeline in self.pipelines.items():
            pipeline_type = pipeline.get("type", PipelineType.BATCH.value)
            if pipeline_type in [PipelineType.BATCH.value, PipelineType.HYBRID.value]:
                batch_pipelines[name] = pipeline

        return batch_pipelines

    def get_hybrid_pipelines(self) -> Dict[str, Dict[str, Any]]:
        """Get all hybrid pipelines."""
        hybrid_pipelines = {}

        for name, pipeline in self.pipelines.items():
            pipeline_type = pipeline.get("type", PipelineType.BATCH.value)
            if pipeline_type == PipelineType.HYBRID.value:
                hybrid_pipelines[name] = pipeline

        return hybrid_pipelines

    def get_pipeline_by_type(self, pipeline_type: str) -> Dict[str, Dict[str, Any]]:
        """Get pipelines filtered by type."""
        if pipeline_type == PipelineType.STREAMING.value:
            return self.get_streaming_pipelines()
        elif pipeline_type == PipelineType.BATCH.value:
            return self.get_batch_pipelines()
        elif pipeline_type == PipelineType.HYBRID.value:
            return self.get_hybrid_pipelines()
        else:
            return {}

    def get_streaming_nodes(self) -> Dict[str, Dict[str, Any]]:
        """Get all nodes configured for streaming."""
        streaming_nodes = {}

        for node_name, node_config in self.nodes_config.items():
            if self._is_streaming_node(node_config):
                streaming_nodes[node_name] = node_config

        return streaming_nodes

    def _is_streaming_node(self, node_config: Dict[str, Any]) -> bool:
        """Check if a node is configured for streaming."""

        # Check for streaming configuration
        if node_config.get("streaming"):
            return True

        # Check for streaming input formats
        input_config = node_config.get("input", {})
        if isinstance(input_config, dict):
            input_format = input_config.get("format", "")
            streaming_formats = [
                "kafka",
                "kinesis",
                "delta_stream",
                "file_stream",
                "socket",
                "rate",
            ]
            if input_format in streaming_formats:
                return True

        # Check for streaming output formats
        output_config = node_config.get("output", {})
        if isinstance(output_config, dict):
            output_format = output_config.get("format", "")
            if output_format in ["kafka", "memory", "console"]:
                return True

        return False

    def _validate_pipeline_compatibility(
        self,
        batch_pipelines: Dict[str, Dict[str, Any]],
        streaming_pipelines: Dict[str, Dict[str, Any]],
    ) -> None:
        """Validate compatibility between batch and streaming pipelines."""

        if not batch_pipelines or not streaming_pipelines:
            return

        # Use a sample batch and streaming pipeline for compatibility check
        sample_batch = next(iter(batch_pipelines.values()))
        sample_streaming = next(iter(streaming_pipelines.values()))

        warnings = self.streaming_validator.validate_pipeline_compatibility(
            sample_batch, sample_streaming
        )

        for warning in warnings:
            logger.warning(f"Pipeline compatibility warning: {warning}")

    def get_streaming_configuration_summary(self) -> Dict[str, Any]:
        """Get a summary of streaming configurations."""

        streaming_pipelines = self.get_streaming_pipelines()
        streaming_nodes = self.get_streaming_nodes()

        # Count different types of streaming sources/sinks
        source_formats = {}
        sink_formats = {}

        for node_config in streaming_nodes.values():
            # Count input formats
            input_config = node_config.get("input", {})
            if isinstance(input_config, dict):
                format_type = input_config.get("format")
                if format_type:
                    source_formats[format_type] = source_formats.get(format_type, 0) + 1

            # Count output formats
            output_config = node_config.get("output", {})
            if isinstance(output_config, dict):
                format_type = output_config.get("format")
                if format_type:
                    sink_formats[format_type] = sink_formats.get(format_type, 0) + 1

        summary = {
            "total_pipelines": len(self.pipelines),
            "streaming_pipelines": len(streaming_pipelines),
            "batch_pipelines": len(self.get_batch_pipelines()),
            "hybrid_pipelines": len(self.get_hybrid_pipelines()),
            "streaming_nodes": len(streaming_nodes),
            "streaming_sources": source_formats,
            "streaming_sinks": sink_formats,
            "pipeline_names": {
                "streaming": list(streaming_pipelines.keys()),
                "batch": list(self.get_batch_pipelines().keys()),
                "hybrid": list(self.get_hybrid_pipelines().keys()),
            },
        }

        return summary

    def validate_streaming_node_dependencies(self) -> List[str]:
        """Validate streaming node dependencies."""
        errors = []

        streaming_nodes = self.get_streaming_nodes()

        for node_name, node_config in streaming_nodes.items():
            dependencies = node_config.get("dependencies", [])

            for dep in dependencies:
                dep_name = dep if isinstance(dep, str) else str(dep)
                dep_config = self.nodes_config.get(dep_name)

                if not dep_config:
                    errors.append(
                        f"Streaming node '{node_name}' depends on missing node '{dep_name}'"
                    )
                    continue

                # Check if dependency is also streaming-compatible
                if not self._is_streaming_node(dep_config):
                    logger.warning(
                        f"Streaming node '{node_name}' depends on batch node '{dep_name}'. "
                        f"This may cause execution issues."
                    )

        return errors

    @classmethod
    def from_json_config(
        cls,
        global_settings: Dict[str, Any],
        pipelines_config: Dict[str, Any],
        nodes_config: Dict[str, Any],
        input_config: Dict[str, Any],
        output_config: Dict[str, Any],
    ) -> "StreamingContext":
        """Create StreamingContext instance directly from JSON/dictionary configurations."""
        return cls(
            global_settings=global_settings,
            pipelines_config=pipelines_config,
            nodes_config=nodes_config,
            input_config=input_config,
            output_config=output_config,
        )

    @classmethod
    def from_python_dsl(cls, python_module_path: str) -> "StreamingContext":
        """Create StreamingContext instance from a Python DSL module."""
        from ..dsl_loader import DSLLoader

        dsl_loader = DSLLoader()
        config_data = dsl_loader.load_from_module(python_module_path)

        return cls(
            global_settings=config_data["global_settings"],
            pipelines_config=config_data["pipelines_config"],
            nodes_config=config_data["nodes_config"],
            input_config=config_data["input_config"],
            output_config=config_data["output_config"],
        )
