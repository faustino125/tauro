from abc import ABC, abstractmethod
from functools import cached_property
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Union

from loguru import logger  # type: ignore

from tauro.config.exceptions import ConfigLoadError, ConfigValidationError
from tauro.config.interpolator import VariableInterpolator
from tauro.config.loaders import (
    ConfigLoaderFactory,
    DSLConfigLoader,
    PythonConfigLoader,
)
from tauro.config.session import SparkSessionFactory
from tauro.config.validators import (
    ConfigValidator,
    FormatPolicy,
    HybridValidator,
    MLValidator,
    PipelineValidator,
    StreamingValidator,
)


class PipelineManager:
    """Manages pipeline configurations and operations."""

    def __init__(self, pipelines_config: Dict[str, Any], nodes_config: Dict[str, Any]):
        self.pipelines_config = pipelines_config
        self.nodes_config = nodes_config
        self._validator = PipelineValidator()

    @cached_property
    def pipelines(self) -> Dict[str, Dict[str, Any]]:
        """Return all loaded and validated pipeline configurations."""
        self._validator.validate_pipeline_nodes(
            self.pipelines_config, self.nodes_config
        )
        return {
            name: self._generate_pipeline_config(name, contents)
            for name, contents in self.pipelines_config.items()
        }

    def _generate_pipeline_config(
        self, name: str, contents: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Generate complete configuration for a specific pipeline."""
        return {
            "nodes": [
                {"name": node_name, **self.nodes_config[node_name]}
                for node_name in contents.get("nodes", [])
            ],
            "inputs": contents.get("inputs", []),
            "outputs": contents.get("outputs", []),
            "type": contents.get("type", "batch"),
            "spark_config": contents.get("spark_config", {}),
        }

    def get_pipeline(self, name: str) -> Optional[Dict[str, Any]]:
        """Get a specific pipeline configuration by name."""
        return self.pipelines.get(name)

    def list_pipeline_names(self) -> List[str]:
        """Get a list of all pipeline names."""
        return list(self.pipelines.keys())


class Context:
    """Context for managing configuration-based pipelines with ML/Streaming support."""

    REQUIRED_GLOBAL_SETTINGS = ["input_path", "output_path", "mode"]

    def __init__(
        self,
        global_settings: Union[str, Dict],
        pipelines_config: Union[str, Dict],
        nodes_config: Union[str, Dict],
        input_config: Union[str, Dict],
        output_config: Union[str, Dict],
    ):
        """Initialize the context with configuration sources."""
        self._config_loader = ConfigLoaderFactory()
        self._validator = ConfigValidator()
        self._interpolator = VariableInterpolator()

        self._load_configurations(
            global_settings,
            pipelines_config,
            nodes_config,
            input_config,
            output_config,
        )

        self.format_policy = FormatPolicy(self.global_settings.get("format_policy", {}))

        self.spark = SparkSessionFactory.get_session(
            self.execution_mode, ml_config=self._get_spark_ml_config()
        )
        self._process_configurations()

        self._pipeline_manager = PipelineManager(
            self.pipelines_config, self.nodes_config
        )

    def _load_configurations(
        self,
        global_settings: Union[str, Dict],
        pipelines_config: Union[str, Dict],
        nodes_config: Union[str, Dict],
        input_config: Union[str, Dict],
        output_config: Union[str, Dict],
    ) -> None:
        """Load all configuration sources and validate global settings."""
        self.global_settings = self._load_and_validate_config(
            global_settings, "global settings"
        )
        self._validator.validate_required_keys(
            self.global_settings, self.REQUIRED_GLOBAL_SETTINGS, "global settings"
        )

        self.execution_mode = self.global_settings.get("mode", "databricks")
        self.input_path = self.global_settings.get("input_path")
        self.output_path = self.global_settings.get("output_path")

        self.pipelines_config = self._load_and_validate_config(
            pipelines_config, "pipelines config"
        )
        self.nodes_config = self._load_and_validate_config(nodes_config, "nodes config")
        self.input_config = self._load_and_validate_config(input_config, "input config")
        self.output_config = self._load_and_validate_config(
            output_config, "output config"
        )

    def _load_and_validate_config(
        self, source: Union[str, Dict], config_name: str
    ) -> Dict[str, Any]:
        """Load configuration from source with validation."""
        source_info = source if isinstance(source, str) else type(source).__name__

        try:
            config = self._config_loader.load_config(source)
            self._validator.validate_type(config, dict, config_name)
            return config
        except (ConfigLoadError, ConfigValidationError) as e:
            logger.error(f"Error in {config_name} from {source_info}: {e}")
            raise
        except Exception as e:
            logger.exception(f"Unexpected error in {config_name} from {source_info}")
            raise ConfigLoadError(f"Unexpected error: {str(e)}") from e

    def _process_configurations(self) -> None:
        """Process and prepare configurations after loading."""
        self.layer = self.global_settings.get("layer", "").lower()
        try:
            self._interpolator.interpolate_config_paths(
                self.input_config, self.global_settings
            )
            self._interpolator.interpolate_config_paths(
                self.output_config, self.global_settings
            )
        except Exception:
            logger.debug("Path interpolation skipped due to error", exc_info=True)

    def _get_spark_ml_config(self) -> Dict[str, Any]:
        """Extract Spark ML configuration from global settings."""
        return self.global_settings.get("spark_config", {})

    def _interpolate_input_paths(self) -> None:
        """Interpolate variables in input/output data paths."""
        variables = {"input_path": self.input_path, "output_path": self.output_path}
        self._interpolator.interpolate_config_paths(self.input_config, variables)
        self._interpolator.interpolate_config_paths(self.output_config, variables)

    @property
    def pipelines(self) -> Dict[str, Dict[str, Any]]:
        """Return all loaded and validated pipeline configurations (expanded nodes)."""
        return self._pipeline_manager.pipelines

    def get_pipeline(self, name: str) -> Optional[Dict[str, Any]]:
        """Get a specific pipeline configuration by name."""
        return self._pipeline_manager.get_pipeline(name)

    def list_pipeline_names(self) -> List[str]:
        """Get a list of all pipeline names."""
        return self._pipeline_manager.list_pipeline_names()

    def get_pipeline_ml_config(self, pipeline_name: str) -> Dict[str, Any]:
        """Get ML-specific configuration for a pipeline (base implementation)."""
        pipeline = self.pipelines_config.get(pipeline_name, {})
        return {
            "model_version": pipeline.get("model_version", self.default_model_version),
            "hyperparams": self._merge_hyperparams(pipeline.get("hyperparams", {})),
            "description": pipeline.get("description", ""),
        }

    @property
    def default_model_version(self) -> Optional[str]:
        return (self.global_settings or {}).get("default_model_version")

    def get_node_ml_config(self, node_name: str) -> Dict[str, Any]:
        """Get ML-specific configuration for a node."""
        node = self.nodes_config.get(node_name, {})
        return {
            "hyperparams": node.get("hyperparams", {}),
            "metrics": node.get("metrics", []),
            "description": node.get("description", ""),
        }

    def _merge_hyperparams(
        self, pipeline_hyperparams: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Merge default hyperparams with pipeline-specific ones."""
        merged = self.default_hyperparams.copy()
        merged.update(pipeline_hyperparams)
        return merged

    @property
    def default_hyperparams(self) -> Dict[str, Any]:
        return dict((self.global_settings or {}).get("default_hyperparams", {}) or {})

    @property
    def is_ml_layer(self) -> bool:
        """Check if this is an ML layer."""
        return self.layer == "ml"

    @classmethod
    def from_json_config(
        cls,
        global_settings: Dict[str, Any],
        pipelines_config: Dict[str, Any],
        nodes_config: Dict[str, Any],
        input_config: Dict[str, Any],
        output_config: Dict[str, Any],
    ) -> "Context":
        """Create Context instance directly from JSON/dictionary configurations."""
        return cls(
            global_settings=global_settings,
            pipelines_config=pipelines_config,
            nodes_config=nodes_config,
            input_config=input_config,
            output_config=output_config,
        )

    @classmethod
    def from_dsl(cls, path: str) -> "Context":
        """Create Context instance from a DSL file (.dsl|.tdsl) or Python module (.py) that returns a monolithic config dict with the 5 required sections."""
        p = Path(path)
        if p.suffix.lower() == ".py":
            loader = PythonConfigLoader()
            config_data = loader.load(p)
        else:
            loader = DSLConfigLoader()
            config_data = loader.load(p)

        # Esperamos un dict monolítico con las secciones requeridas
        required_keys = [
            "global_settings",
            "pipelines_config",
            "nodes_config",
            "input_config",
            "output_config",
        ]
        missing = [k for k in required_keys if k not in config_data]
        if missing:
            raise ConfigValidationError(
                f"DSL/Python config is missing required sections: {', '.join(missing)}"
            )

        return cls(
            global_settings=config_data["global_settings"],
            pipelines_config=config_data["pipelines_config"],
            nodes_config=config_data["nodes_config"],
            input_config=config_data["input_config"],
            output_config=config_data["output_config"],
        )


class BaseSpecializedContext(Context, ABC):
    """Abstract base class for specialized contexts (ML/Streaming)."""

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
        compatibility_validator: Callable[[Dict, Dict], List[str]],
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
        return {
            name: node
            for name, node in self.nodes_config.items()
            if self._is_compatible_node(node)
        }

    def _create_validator(self):
        return MLValidator()

    def _get_specialized_nodes(self) -> Dict[str, Dict[str, Any]]:
        return self.ml_nodes

    def _is_compatible_node(self, node_config: Dict[str, Any]) -> bool:
        return (
            "model" in node_config
            or "hyperparams" in node_config
            or "metrics" in node_config
        )

    def _get_context_type_name(self) -> str:
        return "ML"

    def _validate_configurations(self) -> None:
        super()._validate_configurations()
        # Validación de pipelines ML
        self._validator.validate_ml_pipeline_config(
            self.ml_pipelines, self.nodes_config
        )
        # Compatibilidad con pipelines batch
        batch_pipelines = {
            name: pipeline
            for name, pipeline in self.pipelines_config.items()
            if pipeline.get("type") == "batch"
        }
        self._validate_pipeline_compatibility(batch_pipelines, self.ml_pipelines)

    @cached_property
    def ml_pipelines(self) -> Dict[str, Dict[str, Any]]:
        return {
            name: pipeline
            for name, pipeline in self.pipelines_config.items()
            if pipeline.get("type") == "ml"
        }

    def validate_ml_node_dependencies(self) -> None:
        super()._validate_specialized_node_dependencies()

    def _validate_pipeline_compatibility(
        self,
        batch_pipelines: Dict[str, Dict[str, Any]],
        ml_pipelines: Dict[str, Dict[str, Any]],
    ) -> None:
        if not batch_pipelines or not ml_pipelines:
            return

        for batch_name, batch_pipeline in batch_pipelines.items():
            for ml_name, ml_pipeline in ml_pipelines.items():
                warnings = self._validator.validate_pipeline_compatibility(
                    batch_pipeline, ml_pipeline, self.nodes_config
                )
                for warning in warnings:
                    logger.warning(
                        f"Compatibility issue between batch pipeline '{batch_name}' "
                        f"and ML pipeline '{ml_name}': {warning}"
                    )


class StreamingContext(BaseSpecializedContext):
    """Context for managing streaming pipelines and configurations."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._validate_streaming_configurations()

    @classmethod
    def from_base_context(cls, base_context: Context) -> "StreamingContext":
        return cls(
            global_settings=base_context.global_settings,
            pipelines_config=base_context.pipelines_config,
            nodes_config=base_context.nodes_config,
            input_config=base_context.input_config,
            output_config=base_context.output_config,
        )

    @cached_property
    def streaming_nodes(self) -> Dict[str, Dict[str, Any]]:
        return {
            name: node
            for name, node in self.nodes_config.items()
            if self._is_streaming_node(node)
        }

    def _create_validator(self):
        # Inyectar la política de formatos del contexto
        return StreamingValidator(self.format_policy)

    def _get_specialized_nodes(self) -> Dict[str, Dict[str, Any]]:
        return self.streaming_nodes

    def _is_compatible_node(self, node_config: Dict[str, Any]) -> bool:
        return self._is_streaming_node(node_config)

    def _get_context_type_name(self) -> str:
        return "Streaming"

    def _validate_configurations(self) -> None:
        super()._validate_configurations()
        for pipeline_name, pipeline_config in self.streaming_pipelines.items():
            pipeline_with_name = {**pipeline_config, "name": pipeline_name}
            self._validator.validate_streaming_pipeline_config(pipeline_with_name)
            self._validator.validate_streaming_pipeline_with_nodes(
                pipeline_with_name, self.nodes_config
            )

    @cached_property
    def streaming_pipelines(self) -> Dict[str, Dict[str, Any]]:
        return {
            name: pipeline
            for name, pipeline in self.pipelines_config.items()
            if pipeline.get("type") == "streaming"
        }

    def _validate_streaming_configurations(self) -> None:
        for pipeline_name, pipeline_config in self.streaming_pipelines.items():
            pipeline_with_name = {**pipeline_config, "name": pipeline_name}
            self._validator.validate_streaming_pipeline_config(pipeline_with_name)
            self._validator.validate_streaming_pipeline_with_nodes(
                pipeline_with_name, self.nodes_config
            )
        super()._validate_specialized_node_dependencies()

    def validate_streaming_node_dependencies(self) -> None:
        super()._validate_specialized_node_dependencies()

    def _validate_pipeline_compatibility(
        self,
        batch_pipelines: Dict[str, Dict[str, Any]],
        streaming_pipelines: Dict[str, Dict[str, Any]],
    ) -> None:
        if not batch_pipelines or not streaming_pipelines:
            return

        for batch_name, batch_pipeline in batch_pipelines.items():
            for streaming_name, streaming_pipeline in streaming_pipelines.items():
                warnings = self._validator.validate_pipeline_compatibility(
                    batch_pipeline, streaming_pipeline, self.nodes_config
                )
                for warning in warnings:
                    logger.warning(
                        f"Compatibility issue between '{batch_name}' and '{streaming_name}': {warning}"
                    )

    def _is_streaming_node(self, node_config: Dict[str, Any]) -> bool:
        input_conf = node_config.get("input", {})
        output_conf = node_config.get("output", {})

        input_format = (
            input_conf.get("format", "") if isinstance(input_conf, dict) else ""
        )
        output_format = (
            output_conf.get("format", "") if isinstance(output_conf, dict) else ""
        )

        return self._validator.policy.is_supported_input(
            input_format
        ) or self._validator.policy.is_supported_output(output_format)


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

        self._streaming_ctx = StreamingContext.from_base_context(self)
        self._ml_ctx = MLContext.from_base_context(self)

        if hasattr(base_context, "spark"):
            self.spark = base_context.spark

        self._validate_hybrid_config()

    def _validate_hybrid_config(self):
        """Enhanced cross-validation for hybrid pipelines"""
        HybridValidator.validate_context(self)


class ContextFactory:
    """Factory for creating specialized contexts with priority handling."""

    @staticmethod
    def create_context(base_context: Context) -> Context:
        """Create specialized context based on pipeline configurations."""
        pipeline_types = {
            name: pipeline.get("type", "batch")
            for name, pipeline in base_context.pipelines_config.items()
        }

        has_streaming = any(t == "streaming" for t in pipeline_types.values())
        has_ml = any(t == "ml" for t in pipeline_types.values())
        has_hybrid = any(t == "hybrid" for t in pipeline_types.values())

        if has_hybrid or (has_streaming and has_ml):
            return HybridContext(base_context)
        elif has_streaming:
            return StreamingContext.from_base_context(base_context)
        elif has_ml:
            return MLContext.from_base_context(base_context)

        return base_context
