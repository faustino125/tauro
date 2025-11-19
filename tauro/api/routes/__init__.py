"""Routes package"""

from .pipelines import router as pipelines_router
from .scheduling import router as scheduling_router
from .monitoring import router as monitoring_router
from .projects import router as projects_router
from .runs import router as runs_router
from .logs import router as logs_router
from .configs import (
    configs_router,
    config_versions_router,
)

__all__ = [
    "pipelines_router",
    "scheduling_router",
    "monitoring_router",
    "projects_router",
    "runs_router",
    "logs_router",
    "configs_router",
    "config_versions_router",
]
