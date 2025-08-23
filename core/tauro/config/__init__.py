"""
Re-export perezoso para mantener compatibilidad:
from tauro.config import Context, SparkSessionFactory
sin importar submódulos pesados en import-time.
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
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = [
    "Context",
    "MLContext",
    "StreamingContext",
    "HybridContext",
    "ContextFactory",
    "SparkSessionFactory",
    "FormatPolicy",
]
