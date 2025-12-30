"""Tauro Config public API.
This module re-exports the most commonly used Config components for convenience.
"""

# Exceptions
from .exceptions import (
    ConfigurationError,
    ConfigLoadError,
    ConfigValidationError,
    PipelineValidationError,
    ConfigRepositoryError,
    ActiveConfigNotFound,
)

# Loaders
from .loaders import (
    ConfigLoader,
    YamlConfigLoader,
    JsonConfigLoader,
    PythonConfigLoader,
    DSLConfigLoader,
    ConfigLoaderFactory,
)

# Interpolator
from .interpolator import VariableInterpolator

# Validators
from .validators import (
    ConfigValidator,
    PipelineValidator,
    FormatPolicy,
    MLValidator,
    StreamingValidator,
    CrossValidator,
    HybridValidator,
)

# Session Management
from .session import SparkSessionFactory, SparkSessionManager

# Context and Context Management
from .contexts import (
    Context,
    PipelineManager,
    BaseSpecializedContext,
    MLContext,
    StreamingContext,
    HybridContext,
    VirtualContext,
    ContextFactory,
)

# Context Loader
from .context_loader import ContextLoader

# Providers
from .providers import (
    IConfigRepository,
    ActiveConfigRecord,
)

__all__ = [
    # Exceptions
    "ConfigurationError",
    "ConfigLoadError",
    "ConfigValidationError",
    "PipelineValidationError",
    "ConfigRepositoryError",
    "ActiveConfigNotFound",
    # Loaders
    "ConfigLoader",
    "YamlConfigLoader",
    "JsonConfigLoader",
    "PythonConfigLoader",
    "DSLConfigLoader",
    "ConfigLoaderFactory",
    # Interpolator
    "VariableInterpolator",
    # Validators
    "ConfigValidator",
    "PipelineValidator",
    "FormatPolicy",
    "MLValidator",
    "StreamingValidator",
    "CrossValidator",
    "HybridValidator",
    # Session Management
    "SparkSessionFactory",
    "SparkSessionManager",
    # Context and Context Management
    "Context",
    "PipelineManager",
    "BaseSpecializedContext",
    "MLContext",
    "StreamingContext",
    "HybridContext",
    "VirtualContext",
    "ContextFactory",
    # Context Loader
    "ContextLoader",
    # Providers
    "IConfigRepository",
    "ActiveConfigRecord",
]
