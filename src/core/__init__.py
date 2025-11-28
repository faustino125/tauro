from typing import Any
import importlib

_CONFIG_CONTEXTS = "core.config.contexts"
_CONFIG_SESSION = "core.config.session"
_CONFIG_VALIDATORS = "core.config.validators"
_CONFIG_PROVIDERS = "core.config.providers"
_MLOPS_CONFIG = "core.mlops.config"
_MLOPS_REGISTRY = "core.mlops.model_registry"
_MLOPS_TRACKING = "core.mlops.experiment_tracking"
_EXEC_MODULE = "core.exec"

_module_mapping = {
    # Config
    "Context": _CONFIG_CONTEXTS,
    "MLContext": _CONFIG_CONTEXTS,
    "StreamingContext": _CONFIG_CONTEXTS,
    "HybridContext": _CONFIG_CONTEXTS,
    "ContextFactory": _CONFIG_CONTEXTS,
    "SparkSessionFactory": _CONFIG_SESSION,
    "FormatPolicy": _CONFIG_VALIDATORS,
    "ActiveConfigRecord": _CONFIG_PROVIDERS,
    "IConfigRepository": _CONFIG_PROVIDERS,
    # MLOps
    "MLOpsContext": _MLOPS_CONFIG,
    "init_mlops": _MLOPS_CONFIG,
    "get_mlops_context": _MLOPS_CONFIG,
    "ModelRegistry": _MLOPS_REGISTRY,
    "ExperimentTracker": _MLOPS_TRACKING,
    # Exec
    "BatchExecutor": _EXEC_MODULE,
    "StreamingExecutor": _EXEC_MODULE,
    "HybridExecutor": _EXEC_MODULE,
    "PipelineExecutor": _EXEC_MODULE,
}


def __getattr__(name: str) -> Any:
    if name in _module_mapping:
        module = importlib.import_module(_module_mapping[name])
        return getattr(module, name)

    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = [
    # Config
    "Context",
    "MLContext",
    "StreamingContext",
    "HybridContext",
    "ContextFactory",
    "SparkSessionFactory",
    "FormatPolicy",
    "ActiveConfigRecord",
    "IConfigRepository",
    # MLOps
    "MLOpsContext",
    "init_mlops",
    "get_mlops_context",
    "ModelRegistry",
    "ExperimentTracker",
    # Exec
    "BatchExecutor",
    "StreamingExecutor",
    "HybridExecutor",
    "PipelineExecutor",
]
