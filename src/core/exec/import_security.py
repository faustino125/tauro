"""
Copyright (c) 2025 Faustino Lopez Ramos.
For licensing information, see the LICENSE file in the project root
"""
import importlib
import importlib.util
import os
import re
import sys
from contextlib import contextmanager
from functools import lru_cache
from pathlib import Path
from typing import Any, List, Optional, Set

from loguru import logger  # type: ignore


class ModuleImportError(Exception):
    """Exception raised when module import fails security validation."""

    pass


class SecureModuleImporter:
    """
    Secure module importer with strict validation and cleanup.
    """

    # Default allowed module prefixes (can be customized per instance)
    DEFAULT_ALLOWED_PREFIXES = [
        "core.",
        "nodes.",
        "custom_nodes.",
        "src.",
        "lib.",
        "pipelines.",
        "transformations.",
        "data.",
    ]

    # Patterns that are never allowed (security)
    FORBIDDEN_PATTERNS = [
        r"\.\.",  # Parent directory traversal
        r"^/",  # Absolute paths
        r"^[A-Z]:",  # Windows drive letters
        r"__import__",  # Direct import manipulation
        r"eval",  # Code evaluation
        r"exec",  # Code execution
    ]

    # Maximum module path length to prevent DoS
    MAX_MODULE_PATH_LENGTH = 256

    def __init__(
        self,
        allowed_prefixes: Optional[List[str]] = None,
        additional_search_paths: Optional[List[Path]] = None,
        strict_mode: bool = False,
    ):
        """
        Initialize secure module importer.
        """
        self.allowed_prefixes = allowed_prefixes or self.DEFAULT_ALLOWED_PREFIXES
        self.strict_mode = strict_mode
        self._module_cache: dict = {}
        self._validated_paths: Set[str] = set()

        # Validate and store additional search paths
        self.additional_search_paths = []
        if additional_search_paths:
            for path in additional_search_paths:
                validated = self._validate_search_path(path)
                if validated:
                    self.additional_search_paths.append(validated)

    def _validate_search_path(self, path: Path) -> Optional[Path]:
        """
        Validate that a search path is safe to add to sys.path.
        """
        try:
            resolved_path = path.resolve(strict=False)

            # Check if path exists and is a directory
            if not resolved_path.exists() or not resolved_path.is_dir():
                logger.debug(f"Search path does not exist or is not a directory: {path}")
                return None

            path_str = str(resolved_path)
            if ".." in path_str:
                logger.warning(f"Search path contains parent directory traversal: {path}")
                return None

            # Check read permissions
            if not os.access(resolved_path, os.R_OK):
                logger.warning(f"Search path is not readable: {path}")
                return None

            return resolved_path

        except (ValueError, OSError, RuntimeError) as e:
            logger.warning(f"Error validating search path {path}: {e}")
            return None

    def validate_module_path(self, module_path: str) -> bool:
        """
        Validate that a module path is safe to import.
        """
        # Check length to prevent DoS
        if len(module_path) > self.MAX_MODULE_PATH_LENGTH:
            raise ModuleImportError(
                f"Module path exceeds maximum length ({self.MAX_MODULE_PATH_LENGTH}): {module_path}"
            )

        # Check for empty or whitespace-only paths
        if not module_path or not module_path.strip():
            raise ModuleImportError("Module path cannot be empty")

        # Check for forbidden patterns
        for pattern in self.FORBIDDEN_PATTERNS:
            if re.search(pattern, module_path):
                raise ModuleImportError(
                    f"Module path contains forbidden pattern '{pattern}': {module_path}"
                )

        # Validate format: only alphanumeric, underscores, and dots
        if not re.match(r"^[a-zA-Z0-9_\.]+$", module_path):
            raise ModuleImportError(
                f"Module path contains invalid characters (only [a-zA-Z0-9_.] allowed): {module_path}"
            )

        # Check against whitelist ONLY in strict mode
        if self.strict_mode:
            if not any(module_path.startswith(prefix) for prefix in self.allowed_prefixes):
                raise ModuleImportError(
                    f"Module path '{module_path}' not in whitelist. "
                    f"Allowed prefixes: {', '.join(self.allowed_prefixes)}"
                )
        else:
            # In non-strict mode, just log a warning if not in whitelist
            if not any(module_path.startswith(prefix) for prefix in self.allowed_prefixes):
                logger.debug(
                    f"Module '{module_path}' not in default prefixes (allowed in non-strict mode)"
                )

        return True

    @contextmanager
    def temporary_sys_path(self, additional_paths: Optional[List[str]] = None):
        """
        Context manager to temporarily add paths to sys.path with guaranteed cleanup.
        """
        original_sys_path = sys.path.copy()
        added_paths = []

        try:
            # Add validated additional paths
            if additional_paths:
                for path_str in additional_paths:
                    path = Path(path_str)
                    validated = self._validate_search_path(path)
                    if validated and str(validated) not in sys.path:
                        sys.path.insert(0, str(validated))
                        added_paths.append(str(validated))
                        logger.debug(f"Temporarily added to sys.path: {validated}")

            # Add pre-configured search paths
            for path in self.additional_search_paths:
                if str(path) not in sys.path:
                    sys.path.insert(0, str(path))
                    added_paths.append(str(path))

            yield

        finally:
            # Guaranteed cleanup: restore original sys.path
            sys.path[:] = original_sys_path
            if added_paths:
                logger.debug(f"Cleaned up {len(added_paths)} temporary sys.path entries")

    @lru_cache(maxsize=128)
    def import_module(self, module_path: str) -> Any:
        """
        Securely import a module with validation and caching.
        """
        # Validate module path
        self.validate_module_path(module_path)

        # Check cache first
        if module_path in self._module_cache:
            logger.debug(f"Using cached module: {module_path}")
            return self._module_cache[module_path]

        # Try standard import first
        try:
            module = importlib.import_module(module_path)
            self._module_cache[module_path] = module
            logger.debug(f"Successfully imported module: {module_path}")
            return module

        except ImportError as e:
            logger.debug(f"Standard import failed for '{module_path}': {e}")

            # Try with additional search paths
            return self._import_with_fallback(module_path, e)

    def _import_with_fallback(self, module_path: str, original_error: ImportError) -> Any:
        """
        Attempt import with additional search paths.
        """
        if not self.additional_search_paths:
            raise ModuleImportError(
                f"Cannot import module '{module_path}': {original_error}"
            ) from original_error

        with self.temporary_sys_path():
            try:
                module = importlib.import_module(module_path)
                self._module_cache[module_path] = module
                logger.info(f"Successfully imported '{module_path}' using fallback paths")
                return module

            except ImportError as e:
                logger.error(f"Failed to import '{module_path}' even with fallback paths: {e}")
                raise ModuleImportError(
                    f"Cannot import module '{module_path}' after trying all search paths. "
                    f"Original error: {original_error}. Fallback error: {e}"
                ) from e

    def get_function_from_module(self, module_path: str, function_name: str) -> Any:
        """
        Securely import a module and extract a specific function.
        """
        # Validate function name format
        if not re.match(r"^[a-zA-Z_]\w*$", function_name):
            raise ModuleImportError(
                f"Invalid function name format: {function_name}. "
                "Must start with letter or underscore, followed by alphanumeric/underscore."
            )

        # Import module
        module = self.import_module(module_path)

        # Extract function
        if not hasattr(module, function_name):
            available_functions = [
                name
                for name in dir(module)
                if callable(getattr(module, name)) and not name.startswith("_")
            ]
            raise ModuleImportError(
                f"Function '{function_name}' not found in module '{module_path}'. "
                f"Available functions: {', '.join(available_functions[:10])}"
                + (
                    f" (and {len(available_functions) - 10} more)"
                    if len(available_functions) > 10
                    else ""
                )
            )

        func = getattr(module, function_name)

        # Verify it's callable
        if not callable(func):
            raise ModuleImportError(
                f"'{function_name}' in module '{module_path}' is not callable (type: {type(func).__name__})"
            )

        logger.debug(f"Successfully loaded function '{function_name}' from '{module_path}'")
        return func

    def clear_cache(self) -> None:
        """Clear the module import cache."""
        self._module_cache.clear()
        # Clear LRU cache
        self.import_module.cache_clear()
        logger.debug("Module import cache cleared")

    def get_cache_info(self) -> dict:
        """
        Get information about the import cache.
        """
        cache_info = self.import_module.cache_info()
        return {
            "cached_modules": len(self._module_cache),
            "cache_hits": cache_info.hits,
            "cache_misses": cache_info.misses,
            "cache_size": cache_info.currsize,
            "max_cache_size": cache_info.maxsize,
        }


# Global instance with default configuration
_default_importer: Optional[SecureModuleImporter] = None


def get_default_importer() -> SecureModuleImporter:
    """
    Get the default global SecureModuleImporter instance.
    """
    global _default_importer
    if _default_importer is None:
        _default_importer = SecureModuleImporter()
    return _default_importer


def configure_default_importer(
    allowed_prefixes: Optional[List[str]] = None,
    additional_search_paths: Optional[List[Path]] = None,
    strict_mode: bool = True,
) -> None:
    """
    Configure the default global importer.
    """
    global _default_importer
    _default_importer = SecureModuleImporter(
        allowed_prefixes=allowed_prefixes,
        additional_search_paths=additional_search_paths,
        strict_mode=strict_mode,
    )
    logger.info("Default module importer configured")
