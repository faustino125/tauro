from typing import Any, Dict, List
from tauro.config.exceptions import ConfigValidationError


class MLValidator:
    """Validator for machine learning-specific configurations."""

    SUPPORTED_MODEL_TYPES = ["spark_ml", "sklearn", "tensorflow", "pytorch"]
    REQUIRED_NODE_FIELDS = ["model", "input", "output"]

    def validate_ml_pipeline_config(
        self, pipelines_config: Dict[str, Any], nodes_config: Dict[str, Any]
    ) -> None:
        """Validate an ML pipeline configuration."""
        for pipeline_name, pipeline in pipelines_config.items():
            for node_name in pipeline.get("nodes", []):
                if node_name not in nodes_config:
                    raise ConfigValidationError(
                        f"Node '{node_name}' in pipeline '{pipeline_name}' "
                        f"is not defined in global nodes configuration"
                    )
                node_config = nodes_config[node_name]
                self._validate_node_config(node_config, node_name)
            self._validate_spark_ml_config(pipeline.get("spark_config", {}))

    def _validate_node_config(
        self, node_config: Dict[str, Any], node_name: str
    ) -> None:
        """Validate ML node configuration."""
        missing_fields = [
            field for field in self.REQUIRED_NODE_FIELDS if field not in node_config
        ]
        if missing_fields:
            raise ConfigValidationError(
                f"ML node '{node_name}' is missing required fields: {', '.join(missing_fields)}"
            )

        model_config = node_config.get("model", {})
        model_type = model_config.get("type")
        if model_type and model_type not in self.SUPPORTED_MODEL_TYPES:
            raise ConfigValidationError(
                f"Node '{node_name}' has unsupported model type: {model_type}. Supported: {', '.join(self.SUPPORTED_MODEL_TYPES)}"
            )

        hyperparams = node_config.get("hyperparams", {})
        if hyperparams and not isinstance(hyperparams, dict):
            raise ConfigValidationError(
                f"Node '{node_name}' has invalid hyperparams format; expected dict, got {type(hyperparams).__name__}"
            )

        metrics = node_config.get("metrics", [])
        if metrics and not isinstance(metrics, list):
            raise ConfigValidationError(
                f"Node '{node_name}' has invalid metrics format; expected list, got {type(metrics).__name__}"
            )

    def _validate_spark_ml_config(self, spark_config: Dict[str, Any]) -> None:
        """Validate Spark configurations for ML pipelines."""
        required_configs = [
            "spark.ml.pipeline.cacheStorageLevel",
            "spark.ml.feature.pipeline.enabled",
        ]
        for config in required_configs:
            if config not in spark_config:
                raise ConfigValidationError(
                    f"Missing required Spark ML config: {config}"
                )

    def validate_pipeline_compatibility(
        self,
        batch_pipeline: Dict[str, Dict[str, Any]],
        ml_pipeline: Dict[str, Dict[str, Any]],
    ) -> List[str]:
        """Validate compatibility between batch and ML pipelines."""
        warnings = []
        batch_nodes = set(batch_pipeline.get("nodes", []))
        ml_nodes = set(ml_pipeline.get("nodes", []))
        common_nodes = batch_nodes.intersection(ml_nodes)

        for node in common_nodes:
            node_config = batch_pipeline.get("nodes_config", {}).get(
                node, ml_pipeline.get("nodes_config", {}).get(node, {})
            )
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
