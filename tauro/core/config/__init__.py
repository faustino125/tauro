"""
Lazy re-export for backward compatibility:
from tauro.config import Context, SparkSessionFactory
without importing heavy submodules at import-time.
"""

from typing import Any


def __getattr__(name: str) -> Any:
    if (
        name == "Context"
        or name == "MLContext"
        or name == "StreamingContext"
        or name == "HybridContext"
        or name == "ContextFactory"
    ):
        from . import contexts

        return getattr(contexts, name)
    if name == "SparkSessionFactory":
        from . import session

        return getattr(session, name)
    if name == "FormatPolicy":
        from .validators import FormatPolicy

        return FormatPolicy
    if name == "ActiveConfigRecord" or name == "IConfigRepository":
        from . import providers

        return getattr(providers, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = [
    "Context",
    "MLContext",
    "StreamingContext",
    "HybridContext",
    "ContextFactory",
    "SparkSessionFactory",
    "FormatPolicy",
    "ActiveConfigRecord",
    "IConfigRepository",
]
