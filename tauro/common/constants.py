"""
Copyright (c) 2025 Faustino Lopez Ramos.
For licensing information, see the LICENSE file in the project root

Canonical environment definitions and constants.
Single source of truth for environment names across CLI, API, and Core.
"""

from enum import Enum
from typing import List, Dict


class CanonicalEnvironment(str, Enum):
    """
    Official canonical environments supported by Tauro.

    This is the single source of truth for environment definitions.
    All subsystems (CLI, API, Core) must use these canonical names.
    """

    BASE = "base"  # Base configuration (CLI only)
    DEV = "dev"  # Development environment
    SANDBOX = "sandbox"  # Sandbox/testing environment
    STAGING = "staging"  # Staging/pre-production
    PROD = "prod"  # Production environment

    def __str__(self) -> str:
        return self.value

    @classmethod
    def is_local(cls, env: str) -> bool:
        """Check if environment is local (development-friendly)"""
        normalized = str(env).lower()
        return normalized in [cls.BASE.value, cls.DEV.value, cls.SANDBOX.value]

    @classmethod
    def is_remote(cls, env: str) -> bool:
        """Check if environment is remote (deployment)"""
        normalized = str(env).lower()
        return normalized in [cls.STAGING.value, cls.PROD.value]

    @classmethod
    def is_testing(cls, env: str) -> bool:
        """Check if environment is for testing"""
        normalized = str(env).lower()
        return normalized in [cls.DEV.value, cls.SANDBOX.value]

    @classmethod
    def is_safe(cls, env: str) -> bool:
        """Check if environment is safe (non-production)"""
        normalized = str(env).lower()
        return normalized in [cls.BASE.value, cls.DEV.value, cls.SANDBOX.value, cls.STAGING.value]


class EnvironmentCategory:
    """
    Categorization of environments for different use cases.
    Helps determine behavior, fallback chains, and restrictions.
    """

    # Local development environments (can use file-based storage)
    LOCAL = [
        CanonicalEnvironment.BASE.value,
        CanonicalEnvironment.DEV.value,
        CanonicalEnvironment.SANDBOX.value,
    ]

    # Remote deployment environments (require external services)
    REMOTE = [
        CanonicalEnvironment.STAGING.value,
        CanonicalEnvironment.PROD.value,
    ]

    # Testing environments (allow experimental changes)
    TESTING = [
        CanonicalEnvironment.DEV.value,
        CanonicalEnvironment.SANDBOX.value,
    ]

    # Safe environments (non-production, allow rollback)
    SAFE = [
        CanonicalEnvironment.BASE.value,
        CanonicalEnvironment.DEV.value,
        CanonicalEnvironment.SANDBOX.value,
        CanonicalEnvironment.STAGING.value,
    ]

    # Dangerous environments (production, restricted access)
    DANGEROUS = [
        CanonicalEnvironment.PROD.value,
    ]


# Default environments that every Tauro project should support
DEFAULT_ENVIRONMENTS = [
    CanonicalEnvironment.BASE.value,
    CanonicalEnvironment.DEV.value,
    CanonicalEnvironment.SANDBOX.value,
    CanonicalEnvironment.PROD.value,
]

# User-friendly aliases that map to canonical names
ENV_ALIASES: Dict[str, str] = {
    "development": CanonicalEnvironment.DEV.value,
    "prod": CanonicalEnvironment.PROD.value,
    "production": CanonicalEnvironment.PROD.value,
    "staging": CanonicalEnvironment.STAGING.value,
    "test": CanonicalEnvironment.SANDBOX.value,
    "testing": CanonicalEnvironment.SANDBOX.value,
}

# Fallback chains for each environment
FALLBACK_CHAINS: Dict[str, List[str]] = {
    CanonicalEnvironment.BASE.value: [CanonicalEnvironment.BASE.value],
    CanonicalEnvironment.DEV.value: [
        CanonicalEnvironment.DEV.value,
        CanonicalEnvironment.BASE.value,
    ],
    CanonicalEnvironment.SANDBOX.value: [
        CanonicalEnvironment.SANDBOX.value,
        CanonicalEnvironment.BASE.value,
    ],
    CanonicalEnvironment.STAGING.value: [
        CanonicalEnvironment.STAGING.value,
        CanonicalEnvironment.PROD.value,  # Can fallback to prod config as template
        CanonicalEnvironment.BASE.value,
    ],
    CanonicalEnvironment.PROD.value: [
        CanonicalEnvironment.PROD.value,
        CanonicalEnvironment.BASE.value,
    ],
}


def get_fallback_chain(env: str) -> List[str]:
    """
    Get the fallback chain for an environment.

    Args:
        env: Environment name (should be normalized)

    Returns:
        List of environments to try in order (fallback priority)

    Raises:
        ValueError: If environment is not recognized
    """
    normalized = str(env).lower()

    if normalized not in FALLBACK_CHAINS:
        raise ValueError(
            f"Unknown environment '{env}'. "
            f"Valid environments: {', '.join(DEFAULT_ENVIRONMENTS)}"
        )

    return FALLBACK_CHAINS[normalized]


def is_valid_environment(env: str) -> bool:
    """
    Check if an environment name is valid (canonical or alias).

    Args:
        env: Environment name to check

    Returns:
        True if valid (canonical or alias), False otherwise
    """
    if not isinstance(env, str):
        return False

    normalized = env.strip().lower()

    # Check canonical environments
    if normalized in [e.value for e in CanonicalEnvironment]:
        return True

    # Check aliases
    if normalized in ENV_ALIASES:
        return True

    # Check sandbox pattern: sandbox_<developer>
    if normalized == "sandbox" or normalized.startswith("sandbox_"):
        return True

    return False


__all__ = [
    "CanonicalEnvironment",
    "EnvironmentCategory",
    "DEFAULT_ENVIRONMENTS",
    "ENV_ALIASES",
    "FALLBACK_CHAINS",
    "get_fallback_chain",
    "is_valid_environment",
]
