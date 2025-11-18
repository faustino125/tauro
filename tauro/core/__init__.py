"""
Tauro Core Module - Data Pipeline Framework Core.

Includes:
- Configuration management (contexts, loaders, validators)
- Execution layer (executors, validators, dependency resolution)
- I/O operations
- Streaming operations
- MLOps layer (Model Registry, Experiment Tracking)
"""

from typing import Any


def __getattr__(name: str) -> Any:
    # Config module
    if (
        name == "Context"
        or name == "MLContext"
        or name == "StreamingContext"
        or name == "HybridContext"
        or name == "ContextFactory"
    ):
        from tauro.core.config import contexts

        return getattr(contexts, name)

    if name == "SparkSessionFactory":
        from tauro.core.config import session

        return getattr(session, name)

    if name == "FormatPolicy":
        from tauro.core.config.validators import FormatPolicy

        return FormatPolicy

    if name == "ActiveConfigRecord" or name == "IConfigRepository":
        from tauro.core.config import providers

        return getattr(providers, name)

    # MLOps module
    if name == "MLOpsContext":
        from tauro.core.mlops.config import MLOpsContext

        return MLOpsContext

    if name == "init_mlops" or name == "get_mlops_context":
        from tauro.core.mlops.config import init_mlops, get_mlops_context

        return init_mlops if name == "init_mlops" else get_mlops_context

    if name == "ModelRegistry":
        from tauro.core.mlops.model_registry import ModelRegistry

        return ModelRegistry

    if name == "ExperimentTracker":
        from tauro.core.mlops.experiment_tracking import ExperimentTracker

        return ExperimentTracker

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
]
