"""
Copyright (c) 2025 Faustino Lopez Ramos.
For licensing information, see the LICENSE file in the project root

Common module: Shared utilities and definitions across CLI, API, and Core
"""

from tauro.common.constants import (  # noqa: F401
    CanonicalEnvironment,
    EnvironmentCategory,
    DEFAULT_ENVIRONMENTS,
    ENV_ALIASES,
    FALLBACK_CHAINS,
    get_fallback_chain,
    is_valid_environment,
)

from tauro.common.config_interface import (  # noqa: F401
    ConfigProvider,
    FileBasedConfigProvider,
)

__all__ = [
    "CanonicalEnvironment",
    "EnvironmentCategory",
    "DEFAULT_ENVIRONMENTS",
    "ENV_ALIASES",
    "FALLBACK_CHAINS",
    "get_fallback_chain",
    "is_valid_environment",
    "ConfigProvider",
    "FileBasedConfigProvider",
]
