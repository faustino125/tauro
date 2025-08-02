from typing import Any, Dict, List
from tauro.config.exceptions import ConfigValidationError


class StreamingValidator:
    """Validator for streaming-specific configurations."""

    SUPPORTED_STREAMING_FORMATS = {
        "input": ["kafka", "kinesis", "delta_stream", "file_stream", "socket", "rate"],
        "output": ["kafka", "memory", "console", "delta"],
    }

    def validate_streaming_pipeline_config(
        self, pipelines_config: Dict[str, Any], nodes_config: Dict[str, Any]
    ) -> None:
        """Validate a streaming pipeline configuration using global nodes."""
        for pipeline_name, pipeline in pipelines_config.items():
            for node_name in pipeline.get("nodes", []):
                if node_name not in nodes_config:
                    raise ConfigValidationError(
                        f"Node '{node_name}' in pipeline '{pipeline_name}' "
                        f"is not defined in global nodes configuration"
                    )
                node_config = nodes_config[node_name]
                self._validate_node_formats(node_config, node_name)

            spark_config = pipeline.get("spark_config", {})
            self._validate_spark_streaming_config(spark_config)

    def _validate_node_formats(
        self, node_config: Dict[str, Any], node_name: str
    ) -> None:
        """Validate input and output formats for a node in a streaming pipeline."""
        input_config = node_config.get("input", {})
        output_config = node_config.get("output", {})

        if isinstance(input_config, dict):
            input_format = input_config.get("format")
            if (
                input_format
                and input_format not in self.SUPPORTED_STREAMING_FORMATS["input"]
            ):
                raise ConfigValidationError(
                    f"Node '{node_name}' has unsupported streaming input format: {input_format}"
                )

        if isinstance(output_config, dict):
            output_format = output_config.get("format")
            if (
                output_format
                and output_format not in self.SUPPORTED_STREAMING_FORMATS["output"]
            ):
                raise ConfigValidationError(
                    f"Node '{node_name}' has unsupported streaming output format: {output_format}"
                )

    def _validate_spark_streaming_config(self, spark_config: Dict[str, Any]) -> None:
        """Validate Spark configurations for streaming."""
        required_configs = [
            "spark.streaming.backpressure.enabled",
            "spark.streaming.receiver.maxRate",
        ]
        for config in required_configs:
            if config not in spark_config:
                raise ConfigValidationError(
                    f"Missing required Spark streaming config: {config}"
                )

    def validate_pipeline_compatibility(
        self, batch_pipeline: Dict[str, Any], streaming_pipeline: Dict[str, Any]
    ) -> List[str]:
        """Validate compatibility between batch and streaming pipelines."""
        warnings = []

        batch_nodes = set(batch_pipeline.get("nodes", []))
        streaming_nodes = set(streaming_pipeline.get("nodes", []))
        common_nodes = batch_nodes.intersection(streaming_nodes)

        for node in common_nodes:
            node_config = batch_pipeline["nodes"].get(
                node, streaming_pipeline["nodes"].get(node)
            )
            if node_config:
                output_format = node_config.get("output", {}).get("format")
                if output_format not in self.SUPPORTED_STREAMING_FORMATS["output"]:
                    warnings.append(
                        f"Node '{node}' used in both batch and streaming pipelines has incompatible output format: {output_format}"
                    )

        return warnings
