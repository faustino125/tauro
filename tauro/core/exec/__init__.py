"""
Execution layer for Tauro data pipelines.

Includes:
- Pipeline executors (Batch, Streaming, Hybrid)
- Node execution with ML support
- Command pattern for operations
- Dependency resolution
- MLOps integration
"""

from typing import Any


def __getattr__(name: str) -> Any:
    if name == "PipelineExecutor":
        from tauro.core.exec.executor import PipelineExecutor

        return PipelineExecutor
    elif name == "BaseExecutor":
        from tauro.core.exec.executor import BaseExecutor

        return BaseExecutor
    elif name == "BatchExecutor":
        from tauro.core.exec.executor import BatchExecutor

        return BatchExecutor
    elif name == "StreamingExecutor":
        from tauro.core.exec.executor import StreamingExecutor

        return StreamingExecutor
    elif name == "HybridExecutor":
        from tauro.core.exec.executor import HybridExecutor

        return HybridExecutor
    elif name == "NodeExecutor":
        from tauro.core.exec.node_executor import NodeExecutor

        return NodeExecutor
    elif name == "Command":
        from tauro.core.exec.commands import Command

        return Command
    elif name == "NodeCommand":
        from tauro.core.exec.commands import NodeCommand

        return NodeCommand
    elif name == "MLNodeCommand":
        from tauro.core.exec.commands import MLNodeCommand

        return MLNodeCommand
    elif name == "MLOpsExecutorIntegration":
        from tauro.core.exec.mlops_integration import MLOpsExecutorIntegration

        return MLOpsExecutorIntegration
    elif name == "MLOpsExecutorMixin":
        from tauro.core.exec.mlops_executor_mixin import MLOpsExecutorMixin

        return MLOpsExecutorMixin
    elif name == "MLInfoConfigLoader":
        from tauro.core.exec.mlops_integration import MLInfoConfigLoader

        return MLInfoConfigLoader
    elif name == "DependencyResolver":
        from tauro.core.exec.dependency_resolver import DependencyResolver

        return DependencyResolver
    elif name == "PipelineValidator":
        from tauro.core.exec.pipeline_validator import PipelineValidator

        return PipelineValidator
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = [
    # Executors
    "PipelineExecutor",
    "BaseExecutor",
    "BatchExecutor",
    "StreamingExecutor",
    "HybridExecutor",
    # Node execution
    "NodeExecutor",
    # Commands
    "Command",
    "NodeCommand",
    "MLNodeCommand",
    # MLOps integration
    "MLOpsExecutorIntegration",
    "MLOpsExecutorMixin",
    "MLInfoConfigLoader",
    # Utilities
    "DependencyResolver",
    "PipelineValidator",
]
