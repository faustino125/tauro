"""
Copyright (c) 2025 Faustino Lopez Ramos.
For licensing information, see the LICENSE file in the project root
"""
from functools import wraps
from typing import Callable, Optional
from fastapi import Header
from loguru import logger
import warnings


class APIVersion:
    """API version information"""

    def __init__(self, major: int, minor: int, patch: int = 0):
        self.major = major
        self.minor = minor
        self.patch = patch

    @property
    def version_string(self) -> str:
        """Get semantic version string"""
        return f"{self.major}.{self.minor}.{self.patch}"

    def __str__(self) -> str:
        return self.version_string

    def __eq__(self, other):
        if isinstance(other, str):
            return self.version_string == other
        return self.major == other.major and self.minor == other.minor and self.patch == other.patch

    def __lt__(self, other):
        if isinstance(other, str):
            major, minor, patch = map(int, other.split("."))
            other = APIVersion(major, minor, patch)
        return (self.major, self.minor, self.patch) < (
            other.major,
            other.minor,
            other.patch,
        )

    def __le__(self, other):
        return self == other or self < other

    def __gt__(self, other):
        return other > self

    def __ge__(self, other):
        return other >= self or self == other


CURRENT_VERSION = APIVersion(1, 0, 0)


def deprecated(
    version: str,
    replacement: Optional[str] = None,
    message: Optional[str] = None,
):
    """
    Decorator to mark endpoints as deprecated.

    Usage:
        @router.get("/old-endpoint")
        @deprecated(version="1.0.0", replacement="/new-endpoint", message="Use new-endpoint instead")
        async def old_endpoint():
            ...
    """

    def decorator(func: Callable) -> Callable:
        @wraps(func)
        async def async_wrapper(*args, **kwargs):
            # Log deprecation warning
            warning_msg = f"Endpoint {func.__name__} is deprecated since v{version}. "
            if replacement:
                warning_msg += f"Use {replacement} instead. "
            if message:
                warning_msg += message

            logger.warning(warning_msg)
            warnings.warn(warning_msg, DeprecationWarning, stacklevel=2)

            return await func(*args, **kwargs)

        @wraps(func)
        def sync_wrapper(*args, **kwargs):
            warning_msg = f"Endpoint {func.__name__} is deprecated since v{version}. "
            if replacement:
                warning_msg += f"Use {replacement} instead. "
            if message:
                warning_msg += message

            logger.warning(warning_msg)
            warnings.warn(warning_msg, DeprecationWarning, stacklevel=2)

            return func(*args, **kwargs)

        # Mark the wrapper with deprecation info
        async_wrapper._deprecated = True
        async_wrapper._deprecated_version = version
        async_wrapper._replacement = replacement
        sync_wrapper._deprecated = True
        sync_wrapper._deprecated_version = version
        sync_wrapper._replacement = replacement

        # Return appropriate wrapper
        import asyncio

        return async_wrapper if asyncio.iscoroutinefunction(func) else sync_wrapper

    return decorator


def versioned(
    introduced: str,
    modified: Optional[str] = None,
    removed: Optional[str] = None,
):
    """
    Decorator to mark endpoints with version information.

    Usage:
        @router.get("/endpoint")
        @versioned(introduced="1.0.0", modified="1.2.0")
        async def versioned_endpoint():
            ...
    """

    def decorator(func: Callable) -> Callable:
        @wraps(func)
        async def async_wrapper(*args, **kwargs):
            # Optionally check API version from header
            # This is informational only
            return await func(*args, **kwargs)

        @wraps(func)
        def sync_wrapper(*args, **kwargs):
            return func(*args, **kwargs)

        # Mark the wrapper with version info
        async_wrapper._api_versions = {
            "introduced": introduced,
            "modified": modified,
            "removed": removed,
        }
        sync_wrapper._api_versions = {
            "introduced": introduced,
            "modified": modified,
            "removed": removed,
        }

        import asyncio

        return async_wrapper if asyncio.iscoroutinefunction(func) else sync_wrapper

    return decorator


def version_header(api_version: str = Header(default="1.0.0")) -> str:
    """
    Dependency to extract API version from header.

    Usage:
        @router.get("/endpoint")
        async def endpoint(version: str = Depends(version_header)):
            ...
    """
    return api_version


# =============================================================================
# Version Compatibility Checks
# =============================================================================


def is_version_supported(version: str, min_version: str, max_version: Optional[str] = None) -> bool:
    """
    Check if version is supported.

    Args:
        version: Version to check
        min_version: Minimum supported version
        max_version: Maximum supported version (None = current)

    Returns:
        True if version is supported
    """
    v = APIVersion(*map(int, version.split(".")))
    min_v = APIVersion(*map(int, min_version.split(".")))

    if v < min_v:
        return False

    if max_version:
        max_v = APIVersion(*map(int, max_version.split(".")))
        if v > max_v:
            return False

    return True


# =============================================================================
# Exports
# =============================================================================

__all__ = [
    "APIVersion",
    "CURRENT_VERSION",
    "deprecated",
    "versioned",
    "version_header",
    "is_version_supported",
]
