"""
Copyright (c) 2025 Faustino Lopez Ramos.
For licensing information, see the LICENSE file in the project root

Configuration provider interface and base implementations.
Unified interface for environment-aware configuration across subsystems.
"""

from abc import ABC, abstractmethod
from typing import Dict, Any, List, Optional
from pathlib import Path
from loguru import logger

from tauro.common.constants import (
    CanonicalEnvironment,
    get_fallback_chain,
    is_valid_environment,
)


class ConfigProvider(ABC):
    """
    Abstract base class for configuration providers.

    This interface ensures consistent behavior across CLI, API, and Core subsystems
    when handling environment-specific configurations.
    """

    @abstractmethod
    def validate_environment(self, env: str) -> bool:
        """
        Validate if environment is allowed/recognized.

        Args:
            env: Environment name (may be alias or normalized)

        Returns:
            True if environment is valid, False otherwise
        """
        pass

    @abstractmethod
    def get_normalized_env(self, env: str) -> Optional[str]:
        """
        Normalize environment name (aliases, case, sanitization).

        Examples:
            "production" -> "prod"
            "DEV" -> "dev"
            "sandbox_@alice!" -> "sandbox_alice"
            "SANDBOX_MARIA" -> "sandbox_maria"

        Args:
            env: Raw environment name

        Returns:
            Normalized environment name or None if invalid
        """
        pass

    @abstractmethod
    def get_fallback_chain(self, env: str) -> List[str]:
        """
        Get fallback chain for environment (ordered preference list).

        Examples:
            "dev" -> ["dev", "base"]
            "sandbox_alice" -> ["sandbox_alice", "sandbox", "base"]
            "prod" -> ["prod", "base"]

        Args:
            env: Normalized environment name

        Returns:
            List of environments to try in fallback order

        Raises:
            ValueError: If environment is unknown
        """
        pass

    @abstractmethod
    def get_config(self, env: str) -> Dict[str, Any]:
        """
        Get configuration for a specific environment.

        This should:
        1. Normalize the environment name
        2. Validate it's allowed
        3. Follow fallback chain to find config
        4. Merge base + environment configs
        5. Return final configuration

        Args:
            env: Environment name (may be raw or normalized)

        Returns:
            Dictionary with configuration for the environment

        Raises:
            ValueError: If environment is invalid
            FileNotFoundError: If required config files missing
            IOError: If can't read config files
        """
        pass

    @abstractmethod
    def list_environments(self) -> List[str]:
        """
        List all available/configured environments.

        Returns:
            List of environment names
        """
        pass

    def is_local_environment(self, env: str) -> bool:
        """
        Check if environment is local (development-friendly).

        Local environments:
        - base, dev, sandbox
        """
        normalized = self.get_normalized_env(env)
        if not normalized:
            return False
        return normalized in [
            CanonicalEnvironment.BASE.value,
            CanonicalEnvironment.DEV.value,
            CanonicalEnvironment.SANDBOX.value,
        ]

    def is_remote_environment(self, env: str) -> bool:
        """
        Check if environment is remote (requires deployment).

        Remote environments:
        - staging, prod
        """
        normalized = self.get_normalized_env(env)
        if not normalized:
            return False
        return normalized in [
            CanonicalEnvironment.STAGING.value,
            CanonicalEnvironment.PROD.value,
        ]

    def is_safe_environment(self, env: str) -> bool:
        """
        Check if environment is safe (non-production).

        Safe environments: base, dev, sandbox, staging
        """
        normalized = self.get_normalized_env(env)
        if not normalized:
            return False
        return normalized != CanonicalEnvironment.PROD.value

    def is_production_environment(self, env: str) -> bool:
        """
        Check if environment is production.

        Production: only prod
        """
        normalized = self.get_normalized_env(env)
        if not normalized:
            return False
        return normalized == CanonicalEnvironment.PROD.value


class FileBasedConfigProvider(ConfigProvider):
    """
    Base implementation for file-based configuration providers.

    Handles common operations:
    - Environment validation and normalization
    - Fallback chain resolution
    - File existence checking
    - Basic logging
    """

    def __init__(self, base_path: Path = Path(".")):
        """
        Initialize file-based config provider.

        Args:
            base_path: Root path for configuration files
        """
        self.base_path = Path(base_path)
        logger.debug(f"Initialized {self.__class__.__name__} with base_path: {self.base_path}")

    def validate_environment(self, env: str) -> bool:
        """
        Validate environment is recognized.

        Uses canonical environment definitions.
        """
        if not isinstance(env, str):
            return False
        return is_valid_environment(env)

    def get_normalized_env(self, env: str) -> Optional[str]:
        """
        Normalize environment name.

        Default implementation validates but doesn't transform.
        Subclasses should override for alias mapping.
        """
        if not self.validate_environment(env):
            return None
        return str(env).strip().lower()

    def get_fallback_chain(self, env: str) -> List[str]:
        """
        Get fallback chain using canonical definitions.
        """
        normalized = self.get_normalized_env(env)
        if not normalized:
            raise ValueError(f"Invalid environment: {env}")

        # Handle sandbox_<developer> pattern
        if normalized.startswith("sandbox_"):
            return [
                normalized,  # sandbox_alice
                "sandbox",  # sandbox (generic)
                "base",  # base (fallback)
            ]

        try:
            return get_fallback_chain(normalized)
        except ValueError:
            # Unknown environment, create simple chain
            return [normalized, "base"]

    def get_config(self, env: str) -> Dict[str, Any]:
        """
        Get configuration for environment.

        Subclasses must implement actual loading logic.
        """
        raise NotImplementedError(f"{self.__class__.__name__} must implement get_config()")

    def list_environments(self) -> List[str]:
        """
        List available environments.

        Subclasses can override to provide actual list.
        Default returns canonical environments.
        """
        return list(CanonicalEnvironment.__members__.keys())


__all__ = [
    "ConfigProvider",
    "FileBasedConfigProvider",
]
