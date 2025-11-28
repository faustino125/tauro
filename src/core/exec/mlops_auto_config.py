from typing import Any, Dict, List, Optional

from loguru import logger


class MLOpsAutoConfigurator:
    """Auto-configures MLOps based on context and node patterns."""

    # Node name patterns that indicate ML workload
    ML_NODE_PATTERNS = [
        "train",
        "model",
        "predict",
        "evaluate",
        "hyperparameter",
        "feature_engineering",
        "feature_selection",
        "cross_validation",
        "grid_search",
        "fit",
        "score",
        "experiment",
    ]

    # Function name patterns that indicate ML libraries
    ML_FUNCTION_PATTERNS = [
        "sklearn",
        "xgboost",
        "lightgbm",
        "tensorflow",
        "pytorch",
        "keras",
        "train_model",
        "fit_model",
        "build_model",
        "tune_model",
        "cross_validate",
        "grid_search",
    ]

    # ML-related input/output patterns
    ML_IO_PATTERNS = ["model", "weights", "checkpoint", "hyperparams", "metrics"]

    @classmethod
    def should_enable_mlops(cls, node_config: Dict[str, Any]) -> bool:
        """
        Detect if a node needs MLOps automatically.
        """
        # Explicit ML configuration
        if "ml" in node_config:
            return True

        # Explicit opt-out
        if node_config.get("mlops_enabled") is False:
            return False

        # Check node name
        node_name = node_config.get("name", "").lower()
        if any(pattern in node_name for pattern in cls.ML_NODE_PATTERNS):
            logger.debug(f"MLOps auto-enabled for node '{node_name}' (pattern match in name)")
            return True

        # Check function name
        function = node_config.get("function", "").lower()
        if any(pattern in function for pattern in cls.ML_FUNCTION_PATTERNS):
            logger.debug(f"MLOps auto-enabled for node '{node_name}' (pattern match in function)")
            return True

        # Check for ML-related hyperparameters
        if "hyperparams" in node_config:
            logger.debug(f"MLOps auto-enabled for node '{node_name}' (has hyperparams)")
            return True

        # Check for ML-related metrics
        if "metrics" in node_config:
            logger.debug(f"MLOps auto-enabled for node '{node_name}' (has metrics)")
            return True

        # Check input/output patterns
        inputs = node_config.get("input", [])
        outputs = node_config.get("output", [])

        for io_item in [*inputs, *outputs]:
            io_str = str(io_item).lower()
            if any(pattern in io_str for pattern in cls.ML_IO_PATTERNS):
                logger.debug(
                    f"MLOps auto-enabled for node '{node_name}' (ML pattern in I/O: {io_str})"
                )
                return True

        # Default: no MLOps needed
        return False

    @classmethod
    def get_default_ml_config(cls, node_config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Generate sensible ML defaults for a node.
        """
        node_name = node_config.get("name", "unnamed_node")

        return {
            "experiment": {
                "name": f"experiment_{node_name}",
                "auto_track": True,
                "auto_log_params": True,
                "auto_log_metrics": True,
            },
            "hyperparams": {},  # Use function's default parameters
            "metrics": ["auto"],  # Auto-detect from function return
            "model": {
                "version": "auto",  # Semantic versioning
                "auto_register": True,
                "tags": {"node": node_name},
            },
        }

    @classmethod
    def merge_ml_config(
        cls,
        node_config: Dict[str, Any],
        pipeline_ml_config: Dict[str, Any],
        global_ml_config: Dict[str, Any],
        ml_info: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Merge ML configurations with proper precedence.
        """
        # Start with auto-generated defaults
        merged = cls.get_default_ml_config(node_config)

        # Apply global config
        if global_ml_config:
            merged = cls._deep_merge(merged, global_ml_config)

        # Apply ml_info (from file or context)
        if ml_info:
            merged = cls._deep_merge(merged, ml_info)

        # Apply pipeline-level config
        if pipeline_ml_config:
            merged = cls._deep_merge(merged, pipeline_ml_config)

        # Apply node-level config (highest priority)
        node_ml_config = node_config.get("ml", {})
        if node_ml_config:
            merged = cls._deep_merge(merged, node_ml_config)

        return merged

    @staticmethod
    def _deep_merge(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
        """
        Deep merge two dictionaries.
        """
        result = base.copy()

        for key, value in override.items():
            if key in result and isinstance(result[key], dict) and isinstance(value, dict):
                result[key] = MLOpsAutoConfigurator._deep_merge(result[key], value)
            else:
                result[key] = value

        return result

    @classmethod
    def detect_pipeline_ml_nodes(cls, nodes_config: Dict[str, Dict[str, Any]]) -> List[str]:
        """
        Detect which nodes in a pipeline need MLOps.
        """
        ml_nodes = []

        for node_name, node_config in nodes_config.items():
            # Add name to config for detection
            config_with_name = {**node_config, "name": node_name}

            if cls.should_enable_mlops(config_with_name):
                ml_nodes.append(node_name)

        return ml_nodes

    @classmethod
    def should_init_mlops_for_pipeline(
        cls, nodes_config: Dict[str, Dict[str, Any]], global_settings: Dict[str, Any]
    ) -> bool:
        """
        Determine if MLOps should be initialized for a pipeline.
        """
        # Check global override
        mlops_global = global_settings.get("mlops", {})

        # Explicit disable
        if mlops_global.get("enabled") is False:
            logger.info("MLOps disabled by global settings")
            return False

        # Explicit enable
        if mlops_global.get("enabled") is True:
            logger.info("MLOps enabled by global settings")
            return True

        # Auto-detect: check if any node needs MLOps
        ml_nodes = cls.detect_pipeline_ml_nodes(nodes_config)

        if ml_nodes:
            logger.info(f"MLOps auto-enabled for pipeline (ML nodes detected: {ml_nodes})")
            return True

        logger.debug("MLOps not needed for this pipeline (no ML nodes detected)")
        return False
