from typing import Any, Dict, List

from tauro.config.exceptions import ConfigValidationError, PipelineValidationError


class ConfigValidator:
    """Validator for configuration data."""

    @staticmethod
    def validate_required_keys(
        config: Dict[str, Any],
        required_keys: List[str],
        config_name: str = "configuration",
    ) -> None:
        """Validate that all required keys are present in the configuration."""
        missing_keys = [key for key in required_keys if key not in config]
        if missing_keys:
            available_keys = list(config.keys())[:10]
            raise ConfigValidationError(
                f"Missing required keys in {config_name}: {', '.join(missing_keys)}\n"
                f"Available keys: {', '.join(available_keys)}"
                f"{'...' if len(config) > 10 else ''}"
            )

    @staticmethod
    def validate_type(
        config: Any, expected_type: type, config_name: str = "configuration"
    ) -> None:
        """Validate that configuration is of the expected type."""
        if not isinstance(config, expected_type):
            raise ConfigValidationError(
                f"{config_name} must be of type {expected_type.__name__}, got {type(config).__name__}"
            )


class PipelineValidator:
    """Validator for pipeline configurations."""

    @staticmethod
    def validate_pipeline_nodes(
        pipelines: Dict[str, Any], nodes_config: Dict[str, Any]
    ) -> None:
        """Validate that all referenced nodes exist in the configuration."""
        missing_nodes = []
        for pipeline_name, pipeline in pipelines.items():
            for node in pipeline.get("nodes", []):
                if node not in nodes_config:
                    missing_nodes.append(f"{node} (in pipeline '{pipeline_name}')")

        if missing_nodes:
            raise PipelineValidationError(f"Missing nodes: {', '.join(missing_nodes)}")
