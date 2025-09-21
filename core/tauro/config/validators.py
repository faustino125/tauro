"""
Copyright (c) 2025 Faustino Lopez Ramos. 
For licensing information, see the LICENSE file in the project root
"""
from typing import Any, Dict, List, Optional
from loguru import logger  # type: ignore

from tauro.config.exceptions import ConfigValidationError, PipelineValidationError


class ConfigValidator:
    """Validator for configuration data."""

    @staticmethod
    def validate_required_keys(
        config: Dict[str, Any],
        required_keys: List[str],
        config_name: str = "configuration",
    ) -> None:
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
        missing_nodes = []
        for pipeline_name, pipeline in pipelines.items():
            for node in pipeline.get("nodes", []):
                if node not in nodes_config:
                    missing_nodes.append(f"{node} (in pipeline '{pipeline_name}')")

        if missing_nodes:
            raise PipelineValidationError(f"Missing nodes: {', '.join(missing_nodes)}")


class FormatPolicy:
    """
    Centraliza formatos soportados y compatibilidades entre batch y streaming,
    permitiendo overrides desde configuración.
    """

    DEFAULT_SUPPORTED_INPUTS = [
        "kafka",
        "kinesis",
        "delta_stream",
        "file_stream",
        "socket",
        "rate",
        "memory",
    ]

    DEFAULT_SUPPORTED_OUTPUTS = [
        "kafka",
        "memory",
        "console",
        "delta",
        "parquet",
        "json",
        "csv",
    ]

    # Compatibilidad batch_output_format -> streaming_input_format permitido
    DEFAULT_COMPATIBILITY_MAP = {
        "parquet": ["file_stream"],
        "delta": [
            "delta_stream"
        ],  # o "file_stream" si se prefiere lectura por archivos
        "json": ["file_stream"],
        "csv": ["file_stream"],
        "kafka": ["kafka"],
    }

    # Formatos de entrada streaming que requieren checkpoint
    DEFAULT_CHECKPOINT_REQUIRED_INPUTS = ["kafka", "kinesis", "delta_stream"]

    def __init__(self, overrides: Optional[Dict[str, Any]] = None):
        overrides = overrides or {}
        self.supported_inputs = overrides.get(
            "supported_inputs", self.DEFAULT_SUPPORTED_INPUTS
        )
        self.supported_outputs = overrides.get(
            "supported_outputs", self.DEFAULT_SUPPORTED_OUTPUTS
        )
        self.compatibility_map = overrides.get(
            "compatibility_map", self.DEFAULT_COMPATIBILITY_MAP
        )
        self.checkpoint_required_inputs = overrides.get(
            "checkpoint_required_inputs", self.DEFAULT_CHECKPOINT_REQUIRED_INPUTS
        )

    def is_supported_input(self, fmt: Optional[str]) -> bool:
        return bool(fmt) and fmt in self.supported_inputs

    def is_supported_output(self, fmt: Optional[str]) -> bool:
        return bool(fmt) and fmt in self.supported_outputs

    def are_compatible(
        self, batch_output_fmt: Optional[str], streaming_input_fmt: Optional[str]
    ) -> bool:
        if not batch_output_fmt or not streaming_input_fmt:
            return False
        allowed = self.compatibility_map.get(batch_output_fmt, [])
        return streaming_input_fmt in allowed

    def get_supported_input_formats(self) -> List[str]:
        return list(self.supported_inputs)

    def get_supported_output_formats(self) -> List[str]:
        return list(self.supported_outputs)


class MLValidator:
    """Validator for machine learning-specific configurations."""

    SUPPORTED_MODEL_TYPES = ["spark_ml", "sklearn", "tensorflow", "pytorch"]
    REQUIRED_NODE_FIELDS = ["model", "input", "output"]

    def validate_ml_pipeline_config(
        self,
        pipelines_config: Dict[str, Any],
        nodes_config: Dict[str, Any],
        *,
        strict: bool = True,
    ) -> None:
        for pipeline_name, pipeline in pipelines_config.items():
            for node_name in pipeline.get("nodes", []):
                if node_name not in nodes_config:
                    raise ConfigValidationError(
                        f"Node '{node_name}' in pipeline '{pipeline_name}' "
                        f"is not defined in global nodes configuration"
                    )
                node_config = nodes_config[node_name]
                self._validate_node_config(node_config, node_name, strict=strict)
            self._validate_spark_ml_config(
                pipeline.get("spark_config", {}), strict=strict
            )

    def _validate_node_config(
        self, node_config: Dict[str, Any], node_name: str, *, strict: bool = True
    ) -> None:
        missing_fields = [
            field for field in self.REQUIRED_NODE_FIELDS if field not in node_config
        ]
        if missing_fields:
            msg = f"ML node '{node_name}' is missing required fields: {', '.join(missing_fields)}"
            if strict:
                raise ConfigValidationError(msg)
            logger.warning(msg)

        model_config = node_config.get("model", {})
        model_type = model_config.get("type")
        if model_type and model_type not in self.SUPPORTED_MODEL_TYPES:
            msg = (
                f"Node '{node_name}' has unsupported model type: {model_type}. "
                f"Supported: {', '.join(self.SUPPORTED_MODEL_TYPES)}"
            )
            if strict:
                raise ConfigValidationError(msg)
            logger.warning(msg)

        hyperparams = node_config.get("hyperparams", {})
        if hyperparams and not isinstance(hyperparams, dict):
            msg = f"Node '{node_name}' has invalid hyperparams format; expected dict, got {type(hyperparams).__name__}"
            if strict:
                raise ConfigValidationError(msg)
            logger.warning(msg)

        metrics = node_config.get("metrics", [])
        if metrics and not isinstance(metrics, list):
            msg = f"Node '{node_name}' has invalid metrics format; expected list, got {type(metrics).__name__}"
            if strict:
                raise ConfigValidationError(msg)
            logger.warning(msg)

    def _validate_spark_ml_config(
        self, spark_config: Dict[str, Any], *, strict: bool = True
    ) -> None:
        required_configs = [
            "spark.ml.pipeline.cacheStorageLevel",
            "spark.ml.feature.pipeline.enabled",
        ]
        for config in required_configs:
            if config not in spark_config:
                msg = f"Missing recommended Spark ML config: {config}"
                if strict:
                    raise ConfigValidationError(msg)
                logger.warning(msg)

    def validate_pipeline_compatibility(
        self,
        batch_pipeline: Dict[str, Any],
        ml_pipeline: Dict[str, Any],
        nodes_config: Dict[str, Any],
    ) -> List[str]:
        warnings: List[str] = []

        batch_nodes = set(batch_pipeline.get("nodes", []))
        ml_nodes = set(ml_pipeline.get("nodes", []))
        common_nodes = batch_nodes.intersection(ml_nodes)

        for node in common_nodes:
            node_config = nodes_config.get(node, {})

            if "model" in node_config:
                warnings.append(
                    f"Node '{node}' used in both batch and ML pipelines contains ML-specific model configuration"
                )
            output_format = node_config.get("output", {}).get("format", "")
            if output_format and output_format not in ["parquet", "delta"]:
                warnings.append(
                    f"Node '{node}' used in both batch and ML pipelines has incompatible output format: {output_format}"
                )

        return warnings


class StreamingValidator:
    """Validator for streaming-specific configurations."""

    def __init__(self, format_policy: Optional[FormatPolicy] = None) -> None:
        self.policy = format_policy or FormatPolicy()

    def validate_streaming_pipeline_config(
        self, pipeline_config: Dict[str, Any], *, strict: bool = True
    ) -> None:
        spark_config = pipeline_config.get("spark_config", {})
        self._validate_spark_streaming_config(spark_config, strict=strict)

    def validate_streaming_pipeline_with_nodes(
        self,
        pipeline_config: Dict[str, Any],
        nodes_config: Dict[str, Any],
        *,
        strict: bool = True,
    ) -> None:
        pipeline_name = pipeline_config.get("name", "unnamed_pipeline")

        for node_name in pipeline_config.get("nodes", []):
            if node_name not in nodes_config:
                raise ConfigValidationError(
                    f"Node '{node_name}' in pipeline '{pipeline_name}' "
                    f"is not defined in global nodes configuration"
                )
            node_config = nodes_config[node_name]
            self._validate_node_formats(node_config, node_name, strict=strict)

        self.validate_streaming_pipeline_config(pipeline_config, strict=strict)

    def _validate_node_formats(
        self, node_config: Dict[str, Any], node_name: str, *, strict: bool = True
    ) -> None:
        input_config = node_config.get("input", {})
        output_config = node_config.get("output", {})

        if isinstance(input_config, dict):
            input_format = input_config.get("format")
            if input_format and not self.policy.is_supported_input(input_format):
                msg = (
                    f"Node '{node_name}' has unsupported streaming input format: {input_format}. "
                    f"Supported: {self.policy.get_supported_input_formats()}"
                )
                if strict:
                    raise ConfigValidationError(msg)
                logger.warning(msg)

        if isinstance(output_config, dict):
            output_format = output_config.get("format")
            if output_format and not self.policy.is_supported_output(output_format):
                msg = (
                    f"Node '{node_name}' has unsupported streaming output format: {output_format}. "
                    f"Supported: {self.policy.get_supported_output_formats()}"
                )
                if strict:
                    raise ConfigValidationError(msg)
                logger.warning(msg)

    def _validate_spark_streaming_config(
        self, spark_config: Dict[str, Any], *, strict: bool = True
    ) -> None:
        required_configs = [
            "spark.streaming.backpressure.enabled",
            "spark.streaming.receiver.maxRate",
        ]
        for config in required_configs:
            if config not in spark_config:
                msg = f"Missing recommended Spark streaming config: {config}"
                if strict:
                    raise ConfigValidationError(msg)
                logger.warning(msg)

    def validate_pipeline_compatibility(
        self,
        batch_pipeline: Dict[str, Any],
        streaming_pipeline: Dict[str, Any],
        nodes_config: Dict[str, Any],
    ) -> List[str]:
        warnings: List[str] = []

        batch_nodes = set(batch_pipeline.get("nodes", []))
        streaming_nodes = set(streaming_pipeline.get("nodes", []))
        common_nodes = batch_nodes.intersection(streaming_nodes)

        for node in common_nodes:
            node_config = nodes_config.get(node, {})
            output_format = node_config.get("output", {}).get("format")
            if output_format and not self.policy.is_supported_output(output_format):
                warnings.append(
                    f"Node '{node}' used in both batch and streaming pipelines has incompatible output format: {output_format}"
                )

        return warnings


class CrossValidator:
    """Cross-validation for dependencies between different types of nodes."""

    @staticmethod
    def _get_node_type(config: dict) -> str:
        input_cfg = config.get("input", {})
        if isinstance(input_cfg, dict) and "format" in input_cfg:
            return "streaming"
        return "ml"

    @staticmethod
    def _check_ml_node_dependency(
        node_name: str, dep_name: str, dep_config: dict, errors: List[str]
    ) -> None:
        if CrossValidator._get_node_type(dep_config) == "streaming":
            output_format = dep_config.get("output", {}).get("format")
            if output_format not in ["delta", "parquet"]:
                errors.append(
                    f"ML node '{node_name}' requires delta/parquet output from streaming node '{dep_name}', got {output_format}"
                )

    @staticmethod
    def _check_streaming_node_dependency(
        node_name: str, dep_name: str, dep_config: dict, errors: List[str]
    ) -> None:
        if CrossValidator._get_node_type(dep_config) == "ml":
            model_type = dep_config.get("model", {}).get("type")
            if model_type != "spark_ml":
                errors.append(
                    f"Streaming node '{node_name}' requires spark_ml model from ML node '{dep_name}', got {model_type}"
                )

    @staticmethod
    def validate_hybrid_dependencies(nodes_config: dict) -> None:
        errors: List[str] = []

        for node_name, config in nodes_config.items():
            deps = config.get("dependencies", [])
            node_type = CrossValidator._get_node_type(config)
            for dep in deps:
                dep_config = nodes_config.get(dep, {})
                if node_type == "ml":
                    CrossValidator._check_ml_node_dependency(
                        node_name, dep, dep_config, errors
                    )
                elif node_type == "streaming":
                    CrossValidator._check_streaming_node_dependency(
                        node_name, dep, dep_config, errors
                    )

        if errors:
            raise ConfigValidationError("\n".join(errors))


class HybridValidator:
    @staticmethod
    def validate_context(context) -> None:
        """Centralized hybrid validation"""
        errors: List[str] = []

        # Dependencias cruzadas entre nodos
        try:
            CrossValidator.validate_hybrid_dependencies(context.nodes_config)
        except ConfigValidationError as e:
            errors.append(str(e))

        # Estructura de pipelines híbridos: deben tener ambos tipos
        hybrid_pipelines = {
            name: p
            for name, p in context.pipelines_config.items()
            if p.get("type") == "hybrid"
        }

        for name, pipeline in hybrid_pipelines.items():
            nodes = pipeline.get("nodes", [])
            has_streaming = any(
                context._streaming_ctx._is_compatible_node(context.nodes_config[n])
                for n in nodes
                if n in context.nodes_config
            )
            has_ml = any(
                context._ml_ctx._is_compatible_node(context.nodes_config[n])
                for n in nodes
                if n in context.nodes_config
            )
            if not (has_streaming and has_ml):
                errors.append(
                    f"Hybrid pipeline '{name}' must contain both streaming and ML nodes"
                )

        if errors:
            raise ConfigValidationError("\n".join(errors))
