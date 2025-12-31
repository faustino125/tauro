from __future__ import annotations

from importlib import import_module, metadata
from importlib.metadata import PackageNotFoundError
from typing import Any, Dict

import sys as _sys

try:  # pragma: no cover - metadata not available in editable installs
    __version__ = metadata.version("tauro")
except PackageNotFoundError:  # pragma: no cover
    __version__ = "0.1.4"

__description__ = "Tauro - Data pipeline framework for batch, streaming, hybrid and ML workflows"


def about() -> Dict[str, str]:
    """Return basic package metadata for introspection and UX helpers."""

    return {
        "name": "tauro",
        "version": __version__,
        "description": __description__,
        "homepage": "https://github.com/faustino125/tauro",
    }


_CLI_MODULE = "src.core.cli"
_CONFIG_MODULE = "src.core.config"
_EXEC_MODULE = "src.core.exec"
_IO_MODULE = "src.core.io"
_MLOPS_MODULE = "src.core.mlops"
_STREAMING_MODULE = "src.core.streaming"
_VIRTUALIZATION_MODULE = "src.core.virtualization"

_SUBMODULE_ALIASES = {
    "cli": _CLI_MODULE,
    "config": _CONFIG_MODULE,
    "exec": _EXEC_MODULE,
    "execution": _EXEC_MODULE,
    "io": _IO_MODULE,
    "mlops": _MLOPS_MODULE,
    "streaming": _STREAMING_MODULE,
    "virtualization": _VIRTUALIZATION_MODULE,
}

_SYMBOL_EXPORTS = {
    # CLI facade
    "UnifiedCLI": (_CLI_MODULE, "UnifiedCLI"),
    "TauroCLI": (_CLI_MODULE, "TauroCLI"),
    "main": (_CLI_MODULE, "main"),
    "ConfigDiscovery": (_CLI_MODULE, "ConfigDiscovery"),
    "ConfigManager": (_CLI_MODULE, "ConfigManager"),
    "ContextInitializer": (_CLI_MODULE, "ContextInitializer"),
    "CLIPipelineExecutor": (_CLI_MODULE, "CLIPipelineExecutor"),
    # Config contexts and helpers
    "Context": (_CONFIG_MODULE, "Context"),
    "MLContext": (_CONFIG_MODULE, "MLContext"),
    "StreamingContext": (_CONFIG_MODULE, "StreamingContext"),
    "HybridContext": (_CONFIG_MODULE, "HybridContext"),
    "ContextFactory": (_CONFIG_MODULE, "ContextFactory"),
    "ContextLoader": (_CONFIG_MODULE, "ContextLoader"),
    "SparkSessionFactory": (_CONFIG_MODULE, "SparkSessionFactory"),
    "SparkSessionManager": (_CONFIG_MODULE, "SparkSessionManager"),
    "ConfigLoader": (_CONFIG_MODULE, "ConfigLoader"),
    "YamlConfigLoader": (_CONFIG_MODULE, "YamlConfigLoader"),
    "JsonConfigLoader": (_CONFIG_MODULE, "JsonConfigLoader"),
    "PythonConfigLoader": (_CONFIG_MODULE, "PythonConfigLoader"),
    "DSLConfigLoader": (_CONFIG_MODULE, "DSLConfigLoader"),
    "ConfigLoaderFactory": (_CONFIG_MODULE, "ConfigLoaderFactory"),
    "VariableInterpolator": (_CONFIG_MODULE, "VariableInterpolator"),
    "ConfigValidator": (_CONFIG_MODULE, "ConfigValidator"),
    "FormatPolicy": (_CONFIG_MODULE, "FormatPolicy"),
    # Execution layer
    "BaseExecutor": (_EXEC_MODULE, "BaseExecutor"),
    "BatchExecutor": (_EXEC_MODULE, "BatchExecutor"),
    "StreamingExecutor": (_EXEC_MODULE, "StreamingExecutor"),
    "HybridExecutor": (_EXEC_MODULE, "HybridExecutor"),
    "PipelineExecutor": (_EXEC_MODULE, "PipelineExecutor"),
    "DependencyResolver": (_EXEC_MODULE, "DependencyResolver"),
    "NodeExecutor": (_EXEC_MODULE, "NodeExecutor"),
    "PipelineValidator": (_EXEC_MODULE, "PipelineValidator"),
    "RetryPolicy": (_EXEC_MODULE, "RetryPolicy"),
    # IO layer
    "InputLoader": (_IO_MODULE, "InputLoader"),
    "SequentialLoadingStrategy": (_IO_MODULE, "SequentialLoadingStrategy"),
    "ReaderFactory": (_IO_MODULE, "ReaderFactory"),
    "WriterFactory": (_IO_MODULE, "WriterFactory"),
    "DataOutputManager": (_IO_MODULE, "DataOutputManager"),
    "UnityCatalogManager": (_IO_MODULE, "UnityCatalogManager"),
    "UnityCatalogConfig": (_IO_MODULE, "UnityCatalogConfig"),
    "SupportedFormats": (_IO_MODULE, "SupportedFormats"),
    "WriteMode": (_IO_MODULE, "WriteMode"),
    # Streaming helpers
    "StreamingPipelineManager": (_STREAMING_MODULE, "StreamingPipelineManager"),
    "StreamingQueryManager": (_STREAMING_MODULE, "StreamingQueryManager"),
    # MLOps facade
    "MLOpsContext": (_MLOPS_MODULE, "MLOpsContext"),
    "MLOpsConfig": (_MLOPS_MODULE, "MLOpsConfig"),
    "init_mlops": (_MLOPS_MODULE, "init_mlops"),
    "get_mlops_context": (_MLOPS_MODULE, "get_mlops_context"),
    "ModelRegistry": (_MLOPS_MODULE, "ModelRegistry"),
    "ExperimentTracker": (_MLOPS_MODULE, "ExperimentTracker"),
    # Virtualization layer
    "VirtualDataLayer": (_VIRTUALIZATION_MODULE, "VirtualDataLayer"),
    "VirtualTable": (_VIRTUALIZATION_MODULE, "VirtualTable"),
    "SourceType": (_VIRTUALIZATION_MODULE, "SourceType"),
    "CacheStrategy": (_VIRTUALIZATION_MODULE, "CacheStrategy"),
    "EncryptionConfig": (_VIRTUALIZATION_MODULE, "EncryptionConfig"),
    "SchemaRegistry": (_VIRTUALIZATION_MODULE, "SchemaRegistry"),
    "TableStatistics": (_VIRTUALIZATION_MODULE, "TableStatistics"),
    # Virtualization security
    "SecurityEnforcer": (_VIRTUALIZATION_MODULE, "SecurityEnforcer"),
    "TableSecurityPolicy": (_VIRTUALIZATION_MODULE, "TableSecurityPolicy"),
    "FieldSecurityPolicy": (_VIRTUALIZATION_MODULE, "FieldSecurityPolicy"),
    "AccessLevel": (_VIRTUALIZATION_MODULE, "AccessLevel"),
    "Operation": (_VIRTUALIZATION_MODULE, "Operation"),
    "AuditLog": (_VIRTUALIZATION_MODULE, "AuditLog"),
    # Virtualization federation
    "FederationEngine": (_VIRTUALIZATION_MODULE, "FederationEngine"),
    "QueryOptimizer": (_VIRTUALIZATION_MODULE, "QueryOptimizer"),
    "QueryPlan": (_VIRTUALIZATION_MODULE, "QueryPlan"),
    "Predicate": (_VIRTUALIZATION_MODULE, "Predicate"),
    "PredicateOperator": (_VIRTUALIZATION_MODULE, "PredicateOperator"),
    "ExecutionStrategy": (_VIRTUALIZATION_MODULE, "ExecutionStrategy"),
    "QueryStatistics": (_VIRTUALIZATION_MODULE, "QueryStatistics"),
    # Virtualization readers
    "VirtualReaderFactory": (_VIRTUALIZATION_MODULE, "VirtualReaderFactory"),
    "VirtualDataSourceReader": (_VIRTUALIZATION_MODULE, "VirtualDataSourceReader"),
    "FilesystemVirtualReader": (_VIRTUALIZATION_MODULE, "FilesystemVirtualReader"),
    "DatabaseVirtualReader": (_VIRTUALIZATION_MODULE, "DatabaseVirtualReader"),
    "DataWarehouseVirtualReader": (_VIRTUALIZATION_MODULE, "DataWarehouseVirtualReader"),
    # Context
    "VirtualContext": (_CONFIG_MODULE, "VirtualContext"),
}

_ALL_EXPORTS = {
    "__version__",
    "__description__",
    "about",
    *_SUBMODULE_ALIASES.keys(),
    *_SYMBOL_EXPORTS.keys(),
}

__all__ = list(_ALL_EXPORTS)
__all__.sort()
__all__ = tuple(__all__)


def __getattr__(name: str) -> Any:
    """Lazy loader for both submodules and curated public symbols."""

    if name in _SUBMODULE_ALIASES:
        module = import_module(_SUBMODULE_ALIASES[name])
        globals()[name] = module
        return module

    if name in _SYMBOL_EXPORTS:
        module_path, attribute = _SYMBOL_EXPORTS[name]
        module = import_module(module_path)
        value = getattr(module, attribute)
        globals()[name] = value
        return value

    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def __dir__() -> list[str]:  # pragma: no cover - introspection helper
    return sorted(__all__)


# Register runtime alias so ``import tauro`` succeeds after the first import of ``core``.
_sys.modules.setdefault("tauro", _sys.modules[__name__])
