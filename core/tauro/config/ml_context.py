from functools import cached_property
from typing import Any, Dict, List, Optional
import logging

from tauro.config.validators import MLValidationStrategy
from tauro.config.session import SparkSessionFactory
from tauro.config.context import Context
from tauro.config.exceptions import ConfigValidationError
from tauro.config.base_specialized_context import BaseSpecializedContext


logger = logging.getLogger(__name__)


class MLContext(BaseSpecializedContext):
    """Context for managing machine learning pipelines and configurations."""

    def __init__(self, *args, **kwargs):
        existing_spark = kwargs.pop("spark_session", None)
        super().__init__(*args, **kwargs)

        self.spark = (
            existing_spark
            if existing_spark
            else SparkSessionFactory.get_session(
                self.execution_mode, ml_config=self._get_spark_ml_config()
            )
        )

    @classmethod
    def from_base_context(cls, base_context: Context) -> "MLContext":
        return cls(
            global_settings=base_context.global_settings,
            pipelines_config=base_context.pipelines_config,
            nodes_config=base_context.nodes_config,
            input_config=base_context.input_config,
            output_config=base_context.output_config,
            spark_session=base_context.spark,
        )

    @cached_property
    def ml_nodes(self) -> Dict[str, Dict[str, Any]]:
        """Return nodes configured for machine learning."""
        return {
            name: node
            for name, node in self.nodes_config.items()
            if self._is_ml_node(node)
        }

    def _create_validator(self):
        return MLValidationStrategy()

    def _get_specialized_nodes(self) -> Dict[str, Dict[str, Any]]:
        return {
            name: node
            for name, node in self.nodes_config.items()
            if self._is_compatible_node(node)
        }

    def _is_compatible_node(self, node_config: Dict[str, Any]) -> bool:
        return (
            "model" in node_config
            or "hyperparams" in node_config
            or "metrics" in node_config
        )

    def _get_context_type_name(self) -> str:
        return "ML"

    def _validate_configurations(self) -> None:
        """Validate ML-specific configurations and dependencies."""
        super()._validate_configurations()
        self._validator.validate_ml_pipeline_config(
            self.ml_pipelines, self.nodes_config
        )

        batch_pipelines = {
            name: pipeline
            for name, pipeline in self.pipelines_config.items()
            if pipeline.get("type") == "batch"
        }
        self._validate_pipeline_compatibility(batch_pipelines, self.ml_pipelines)

    @cached_property
    def ml_pipelines(self) -> Dict[str, Dict[str, Any]]:
        """Get all ML pipelines from the configuration."""
        return {
            name: pipeline
            for name, pipeline in self.pipelines_config.items()
            if pipeline.get("type") == "ml"
        }

    def get_ml_nodes(self) -> Dict[str, Dict[str, Any]]:
        """Return nodes configured for machine learning (delegates to cached property)."""
        return self.ml_nodes

    def _validate_ml_configurations(self) -> None:
        """Validate ML-specific configurations and dependencies."""
        self.ml_validator.validate_ml_pipeline_config(
            self.ml_pipelines, self.nodes_config
        )

        self._validate_specialized_node_dependencies(
            self.ml_nodes, "ML", self._is_ml_node
        )

        batch_pipelines = {
            name: pipeline
            for name, pipeline in self.pipelines_config.items()
            if pipeline.get("type") == "batch"
        }
        self._validate_pipeline_compatibility(batch_pipelines, self.ml_pipelines)

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
                    f"{node_type} node '{node_name}' dependencies must be a list"
                )

            for dep in dependencies:
                if not isinstance(dep, str):
                    raise ConfigValidationError(
                        f"{node_type} node '{node_name}' dependency must be string: {dep}"
                    )

                dep_config = self.nodes_config.get(dep)
                if not dep_config:
                    raise ConfigValidationError(
                        f"{node_type} node '{node_name}' depends on missing node '{dep}'"
                    )

                if not is_compatible_func(dep_config):
                    raise ConfigValidationError(
                        f"{node_type} node '{node_name}' depends on incompatible node '{dep}'"
                    )

    def validate_ml_node_dependencies(self) -> None:
        """Validate ML node dependencies (maintained for backward compatibility)."""
        self._validate_specialized_node_dependencies(
            self.ml_nodes, "ML", self._is_ml_node
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

        for node_name in pipeline.get("nodes", []):
            if node_name in self.ml_nodes:
                try:
                    self.ml_validator._validate_node_config(
                        self.ml_nodes[node_name], node_name
                    )
                except ConfigValidationError as e:
                    validation_result["errors"].append(str(e))

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

        try:
            self.validate_ml_node_dependencies()
        except ConfigValidationError as e:
            validation_result["errors"].append(str(e))

        validation_result["is_valid"] = len(validation_result["errors"]) == 0
        return validation_result


class ModelRegistry:
    """Registry for managing ML models."""

    def __init__(self):
        self._models: Dict[str, Any] = {}

    def register_model(self, model_name: str, model: Any) -> None:
        """Register a model in the registry."""
        if model_name in self._models:
            logger.warning(f"Overwriting existing model: {model_name}")
        self._models[model_name] = model
        logger.info(f"Model '{model_name}' registered successfully")

    def get_model(self, model_name: str) -> Optional[Any]:
        """Get a model from the registry."""
        model = self._models.get(model_name)
        if model is None:
            logger.warning(f"Model '{model_name}' not found in registry")
        return model

    def list_models(self) -> List[str]:
        """List all registered models."""
        return list(self._models.keys())

    def unregister_model(self, model_name: str) -> bool:
        """Remove a model from the registry."""
        if model_name in self._models:
            del self._models[model_name]
            logger.info(f"Model '{model_name}' unregistered successfully")
            return True
        else:
            logger.warning(f"Model '{model_name}' not found for unregistration")
            return False

    def clear_registry(self) -> None:
        """Clear all models from the registry."""
        count = len(self._models)
        self._models.clear()
        logger.info(f"Registry cleared. Removed {count} models")
