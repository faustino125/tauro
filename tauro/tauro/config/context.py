from typing import Any, Dict, List, Optional, Union

from loguru import logger  # type: ignore

from tauro.config.configuration import PipelineManager
from tauro.config.interpolator import VariableInterpolator
from tauro.config.loaders import ConfigLoaderFactory
from tauro.config.session import SparkSessionFactory
from tauro.config.validators import ConfigValidator


class Context:
    """Context for managing configuration-based pipelines with enhanced ML support."""

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

        self.spark = SparkSessionFactory.create_session(
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
        try:
            config = self._config_loader.load_config(source)
            self._validator.validate_type(config, dict, config_name)
            return config
        except Exception as e:
            source_info = source if isinstance(source, str) else type(source).__name__
            logger.error(f"Error loading {config_name} from {source_info}: {str(e)}")
            raise

    def _process_configurations(self) -> None:
        """Process and prepare configurations after loading."""
        self.layer = self.global_settings.get("layer", "").lower()
        self.input_path = self.global_settings.get("input_path", "")
        self.output_path = self.global_settings.get("output_path", "")

        self.project_name = self.global_settings.get("project_name", "")
        self.default_model_version = self.global_settings.get(
            "default_model_version", "latest"
        )
        self.default_hyperparams = self.global_settings.get("default_hyperparams", {})

        self._interpolate_input_paths()

    def _get_spark_ml_config(self) -> Dict[str, Any]:
        """Extract Spark ML configuration from global settings."""
        return self.global_settings.get("spark_config", {})

    def _interpolate_input_paths(self) -> None:
        """Interpolate variables in input data paths."""
        variables = {"input_path": self.input_path, "output_path": self.output_path}
        self._interpolator.interpolate_config_paths(self.input_config, variables)
        self._interpolator.interpolate_config_paths(self.output_config, variables)

    @property
    def pipelines(self) -> Dict[str, Dict[str, Any]]:
        """Return all loaded and validated pipeline configurations."""
        return self._pipeline_manager.pipelines

    def get_pipeline(self, name: str) -> Optional[Dict[str, Any]]:
        """Get a specific pipeline configuration by name."""
        return self._pipeline_manager.get_pipeline(name)

    def list_pipeline_names(self) -> List[str]:
        """Get a list of all pipeline names."""
        return self._pipeline_manager.list_pipeline_names()

    def get_pipeline_ml_config(self, pipeline_name: str) -> Dict[str, Any]:
        """Get ML-specific configuration for a pipeline."""
        pipeline = self.pipelines_config.get(pipeline_name, {})
        return {
            "model_version": pipeline.get("model_version", self.default_model_version),
            "hyperparams": self._merge_hyperparams(pipeline.get("hyperparams", {})),
            "description": pipeline.get("description", ""),
        }

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
    def from_python_dsl(cls, python_module_path: str) -> "Context":
        """Create Context instance from a Python DSL module."""
        from .dsl_loader import DSLLoader

        dsl_loader = DSLLoader()
        config_data = dsl_loader.load_from_module(python_module_path)

        return cls(
            global_settings=config_data["global_settings"],
            pipelines_config=config_data["pipelines_config"],
            nodes_config=config_data["nodes_config"],
            input_config=config_data["input_config"],
            output_config=config_data["output_config"],
        )
