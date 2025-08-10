import logging
from abc import ABC, abstractmethod
from typing import Any, Dict, Callable
from tauro.config.context import Context
from tauro.config.exceptions import ConfigValidationError

logger = logging.getLogger(__name__)


class BaseSpecializedContext(Context, ABC):
    """Abstract base class for specialized contexts"""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._validator = self._create_validator()
        try:
            self._validate_configurations()
        except ConfigValidationError as e:
            logger.error(f"Configuration validation failed: {str(e)}")
            raise

    @abstractmethod
    def _create_validator(self):
        """Create context-specific validator"""
        pass

    @abstractmethod
    def _get_specialized_nodes(self) -> Dict[str, Dict[str, Any]]:
        """Get nodes compatible with this context"""
        pass

    @abstractmethod
    def _is_compatible_node(self, node_config: Dict[str, Any]) -> bool:
        """Check node compatibility"""
        pass

    @abstractmethod
    def _get_context_type_name(self) -> str:
        """Get human-readable context name"""
        pass

    def _validate_configurations(self) -> None:
        """Template method for context-specific validation"""
        self._validate_specialized_node_dependencies()

    def _validate_specialized_node_dependencies(self) -> None:
        """Validate dependencies between specialized nodes"""
        specialized_nodes = self._get_specialized_nodes()
        context_type = self._get_context_type_name()

        for node_name, node_config in specialized_nodes.items():
            dependencies = node_config.get("dependencies", [])

            if not isinstance(dependencies, list):
                raise ConfigValidationError(
                    f"{context_type} node '{node_name}' dependencies must be a list"
                )

            for dep in dependencies:
                # Validación unificada: dependencias deben ser strings
                if not isinstance(dep, str):
                    raise ConfigValidationError(
                        f"{context_type} node '{node_name}' dependency must be string, got {type(dep).__name__}"
                    )

                dep_config = self.nodes_config.get(dep)

                if not dep_config:
                    raise ConfigValidationError(
                        f"{context_type} node '{node_name}' depends on missing node '{dep}'"
                    )

                if not self._is_compatible_node(dep_config):
                    raise ConfigValidationError(
                        f"{context_type} node '{node_name}' depends on incompatible node '{dep}'"
                    )

    def validate_pipeline_compatibility(
        self,
        pipelines_a: Dict[str, Dict[str, Any]],
        pipelines_b: Dict[str, Dict[str, Any]],
        compatibility_validator: Callable[[Dict, Dict], list],
    ) -> None:
        """Generic pipeline compatibility validation"""
        if not pipelines_a or not pipelines_b:
            return

        for name_a, pipeline_a in pipelines_a.items():
            for name_b, pipeline_b in pipelines_b.items():
                warnings = compatibility_validator(pipeline_a, pipeline_b)
                for warning in warnings:
                    logger.warning(
                        f"Compatibility issue between '{name_a}' and '{name_b}': {warning}"
                    )

    @classmethod
    def from_base_context(cls, base_context: Context):
        """Create specialized context from base Context"""
        return cls(
            global_settings=base_context.global_settings,
            pipelines_config=base_context.pipelines_config,
            nodes_config=base_context.nodes_config,
            input_config=base_context.input_config,
            output_config=base_context.output_config,
        )
