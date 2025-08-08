from functools import cached_property
from typing import Any, Dict, List, Optional
import logging

from tauro.config.context import Context
from tauro.config.exceptions import ConfigValidationError
from tauro.config.ml_validators import MLValidator

logger = logging.getLogger(__name__)


class MLContext(Context):
    """Context for managing machine learning pipelines and configurations."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.ml_validator = MLValidator()
        self._validate_ml_configurations()

    @classmethod
    def from_base_context(cls, base_context: Context) -> "MLContext":
        """Create MLContext from base Context instance."""
        return cls(
            global_settings=base_context.global_settings,
            pipelines_config=base_context.pipelines_config,
            nodes_config=base_context.nodes_config,
            input_config=base_context.input_config,
            output_config=base_context.output_config,
        )

    @cached_property
    def ml_nodes(self) -> Dict[str, Dict[str, Any]]:
        """Return nodes configured for machine learning."""
        return {
            name: node
            for name, node in self.nodes_config.items()
            if self._is_ml_node(node)
        }

    @cached_property
    def ml_pipelines(self) -> Dict[str, Dict[str, Any]]:
        """Get all ML pipelines from the configuration."""
        return {
            name: pipeline
            for name, pipeline in self.pipelines_config.items()
            if pipeline.get("type") == "ml"
        }

    def get_ml_nodes(self) -> Dict[str, Dict[str, Any]]:
        """Return nodes configured for machine learning."""
        return {
            name: node
            for name, node in self.nodes_config.items()
            if self._is_ml_node(node)
        }

    def _validate_ml_configurations(self) -> None:
        """Validate ML-specific configurations and dependencies."""
        self.ml_validator.validate_ml_pipeline_config(
            self.ml_pipelines, self.nodes_config
        )

        self.validate_ml_node_dependencies()

        batch_pipelines = {
            name: pipeline
            for name, pipeline in self.pipelines_config.items()
            if pipeline.get("type") == "batch"
        }
        self._validate_pipeline_compatibility(batch_pipelines, self.ml_pipelines)

    def validate_ml_node_dependencies(self) -> None:
        """Validate ML node dependencies and raise errors for incompatibilities."""
        ml_nodes = self.ml_nodes

        for node_name, node_config in ml_nodes.items():
            dependencies = node_config.get("dependencies", [])
            if not isinstance(dependencies, list):
                raise ConfigValidationError(
                    f"ML node '{node_name}' dependencies must be a list"
                )

            for dep in dependencies:
                if not isinstance(dep, str):
                    raise ConfigValidationError(
                        f"ML node '{node_name}' dependency must be string: {dep}"
                    )

                dep_config = self.nodes_config.get(dep)
                if not dep_config:
                    raise ConfigValidationError(
                        f"ML node '{node_name}' depends on missing node '{dep}'"
                    )
                if not self._is_ml_node(dep_config):
                    raise ConfigValidationError(
                        f"ML node '{node_name}' depends on non-ML node '{dep}'"
                    )

    def _is_ml_node(self, node_config: Dict[str, Any]) -> bool:
        """Check if a node is ML-compatible."""
        return (
            "model" in node_config
            or "hyperparams" in node_config
            or "metrics" in node_config
        )

    def _validate_pipeline_compatibility(
        self,
        batch_pipelines: Dict[str, Dict[str, Any]],
        ml_pipelines: Dict[str, Dict[str, Any]],
    ) -> None:
        """Validate compatibility between batch and ML pipelines."""
        if not batch_pipelines or not ml_pipelines:
            return

        for batch_name, batch_pipeline in batch_pipelines.items():
            for ml_name, ml_pipeline in ml_pipelines.items():
                warnings = self.ml_validator.validate_pipeline_compatibility(
                    batch_pipeline, ml_pipeline
                )
                for warning in warnings:
                    logger.warning(
                        f"Compatibility issue between batch pipeline '{batch_name}' "
                        f"and ML pipeline '{ml_name}': {warning}"
                    )

    def validate_hybrid_pipeline(
        self, pipeline_name: str, pipeline: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Validación especializada para pipelines híbridos en contexto ML."""
        validation_result = {"is_valid": True, "errors": [], "warnings": []}

        ml_nodes = self.get_ml_nodes()
        for node in pipeline.get("nodes", []):
            if node in ml_nodes:
                self.ml_validator._validate_node_config(ml_nodes[node], node)

        batch_pipelines = {
            name: p
            for name, p in self.pipelines_config.items()
            if p.get("type") == "batch"
        }

        corresponding_batch_pipeline = batch_pipelines.get(pipeline_name)

        if corresponding_batch_pipeline:
            warnings = self.ml_validator.validate_pipeline_compatibility(
                corresponding_batch_pipeline, pipeline
            )
            validation_result["warnings"].extend(warnings)

        self.validate_ml_node_dependencies()

        validation_result["is_valid"] = len(validation_result["errors"]) == 0
        return validation_result


class ModelRegistry:
    def __init__(self):
        self._models: Dict[str, Any] = {}

    def register_model(self, model_name: str, model: Any) -> None:
        if model_name in self._models:
            logger.warning(f"Overwriting existing model: {model_name}")
        self._models[model_name] = model
        logger.info(f"Model '{model_name}' registered successfully")

    def get_model(self, model_name: str) -> Optional[Any]:
        model = self._models.get(model_name)
        if model is None:
            logger.warning(f"Model '{model_name}' not found in registry")
        return model

    def list_models(self) -> List[str]:
        return list(self._models.keys())
