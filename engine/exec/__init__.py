"""Tauro Execution Engine public API.
This module re-exports the most commonly used execution components for convenience.
"""

# Commands
from engine.exec.commands import (
    Command,
    NodeCommand,
    MLNodeCommand,
    ExperimentCommand,
    NodeFunction,
)

# Dependency Resolution
from engine.exec.dependency_resolver import DependencyResolver

# Executors
from engine.exec.executor import (
    BaseExecutor,
    BatchExecutor,
    StreamingExecutor,
    HybridExecutor,
    PipelineExecutor,
)

# Node Execution
from engine.exec.node_executor import (
    NodeExecutor,
    ThreadSafeExecutionState,
)

# MLflow Integration (if available)
try:
    from engine.exec.mlflow_node_executor import (
        MLflowNodeExecutor,
        create_mlflow_executor,
    )

    MLFLOW_AVAILABLE = True
except ImportError:
    MLFLOW_AVAILABLE = False
    MLflowNodeExecutor = None
    create_mlflow_executor = None

# ML Validation
from engine.exec.ml_node_validator import MLNodeValidator

# MLOps Integration
from engine.exec.mlops_auto_config import MLOpsAutoConfigurator
from engine.exec.mlops_executor_mixin import MLOpsExecutorMixin
from engine.exec.mlops_integration import (
    MLOpsExecutorIntegration,
    MLInfoConfigLoader,
)

# Pipeline State Management
from engine.exec.pipeline_state import (
    NodeStatus,
    NodeType,
    NodeExecutionInfo,
    CircuitBreakerState,
    CircuitBreaker,
    UnifiedPipelineState,
)

# Pipeline Validation
from engine.exec.pipeline_validator import PipelineValidator

# Resilience
from engine.exec.resilience import RetryPolicy

# Resource Management
from engine.exec.resource_pool import (
    ResourceHandle,
    ResourcePool,
    get_default_resource_pool,
    reset_default_resource_pool,
)

# Utilities
from engine.exec.utils import (
    normalize_dependencies,
    extract_dependency_name,
    extract_pipeline_nodes,
    get_node_dependencies,
)

__all__ = [
    # Commands
    "Command",
    "NodeCommand",
    "MLNodeCommand",
    "ExperimentCommand",
    "NodeFunction",
    # Dependency Resolution
    "DependencyResolver",
    # Executors
    "BaseExecutor",
    "BatchExecutor",
    "StreamingExecutor",
    "HybridExecutor",
    "PipelineExecutor",
    # Node Execution
    "NodeExecutor",
    "ThreadSafeExecutionState",
    # MLflow Integration
    "MLflowNodeExecutor",
    "create_mlflow_executor",
    "MLFLOW_AVAILABLE",
    # ML Validation
    "MLNodeValidator",
    # MLOps Integration
    "MLOpsAutoConfigurator",
    "MLOpsExecutorMixin",
    "MLOpsExecutorIntegration",
    "MLInfoConfigLoader",
    # Pipeline State Management
    "NodeStatus",
    "NodeType",
    "NodeExecutionInfo",
    "CircuitBreakerState",
    "CircuitBreaker",
    "UnifiedPipelineState",
    # Pipeline Validation
    "PipelineValidator",
    # Resilience
    "RetryPolicy",
    # Resource Management
    "ResourceHandle",
    "ResourcePool",
    "get_default_resource_pool",
    "reset_default_resource_pool",
    # Utilities
    "normalize_dependencies",
    "extract_dependency_name",
    "extract_pipeline_nodes",
    "get_node_dependencies",
]
