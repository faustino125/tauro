"""Tauro Config public API.
This module re-exports the most commonly used Config components for convenience.
"""

# Exceptions
from core.config.exceptions import (
    ConfigurationError,
    ConfigLoadError,
    ConfigValidationError,
    PipelineValidationError,
    ConfigRepositoryError,
    ActiveConfigNotFound,
)

# Loaders
from core.config.loaders import (
    ConfigLoader,
    YamlConfigLoader,
    JsonConfigLoader,
    PythonConfigLoader,
    DSLConfigLoader,
    ConfigLoaderFactory,
)

# Interpolator
from core.config.interpolator import VariableInterpolator

# Validators
from core.config.validators import (
    ConfigValidator,
    PipelineValidator,
    FormatPolicy,
    MLValidator,
    StreamingValidator,
    CrossValidator,
    HybridValidator,
)

# Session Management
from core.config.session import SparkSessionFactory

# Context and Context Management
from core.config.contexts import (
    Context,
    PipelineManager,
    BaseSpecializedContext,
    MLContext,
    StreamingContext,
    HybridContext,
    ContextFactory,
)

# Context Loader
from core.config.context_loader import ContextLoader

# Providers
from core.config.providers import (
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
    # Context and Context Management
    "Context",
    "PipelineManager",
    "BaseSpecializedContext",
    "MLContext",
    "StreamingContext",
    "HybridContext",
    "ContextFactory",
    # Context Loader
    "ContextLoader",
    # Providers
    "IConfigRepository",
    "ActiveConfigRecord",
]
