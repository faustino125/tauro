"""Routes package"""

from .pipelines import router as pipelines_router
from .scheduling import router as scheduling_router
from .monitoring import router as monitoring_router

__all__ = [
    "pipelines_router",
    "scheduling_router",
    "monitoring_router",
]
