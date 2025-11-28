"""Tauro Execution Engine public API.
This module re-exports the most commonly used execution components for convenience.
"""

# Commands
from core.exec.commands import (
    Command,
    NodeCommand,
    MLNodeCommand,
    ExperimentCommand,
    NodeFunction,
)

# Dependency Resolution
from core.exec.dependency_resolver import DependencyResolver

# Executors
from core.exec.executor import (
    BaseExecutor,
    BatchExecutor,
    StreamingExecutor,
    HybridExecutor,
    PipelineExecutor,
)

# Node Execution
from core.exec.node_executor import (
    NodeExecutor,
    ThreadSafeExecutionState,
)

# MLflow Integration (if available)
try:
    from core.exec.mlflow_node_executor import (
        MLflowNodeExecutor,
        create_mlflow_executor,
    )

    MLFLOW_AVAILABLE = True
except ImportError:
    MLFLOW_AVAILABLE = False
    MLflowNodeExecutor = None
    create_mlflow_executor = None

# ML Validation
from core.exec.ml_node_validator import MLNodeValidator

# MLOps Integration
from core.exec.mlops_auto_config import MLOpsAutoConfigurator
from core.exec.mlops_executor_mixin import MLOpsExecutorMixin
from core.exec.mlops_integration import (
    MLOpsExecutorIntegration,
    MLInfoConfigLoader,
)

# Pipeline State Management
from core.exec.pipeline_state import (
    NodeStatus,
    NodeType,
    NodeExecutionInfo,
    CircuitBreakerState,
    CircuitBreaker,
    UnifiedPipelineState,
)

# Pipeline Validation
from core.exec.pipeline_validator import PipelineValidator

# Resilience
from core.exec.resilience import RetryPolicy

# Resource Management
from core.exec.resource_pool import (
    ResourceHandle,
    ResourcePool,
    get_default_resource_pool,
    reset_default_resource_pool,
)

# Utilities
from core.exec.utils import (
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
