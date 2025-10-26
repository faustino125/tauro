from __future__ import annotations

from typing import Any, Dict, Optional

from fastapi import APIRouter, Depends, Query, status
from loguru import logger

from tauro.api.core import get_config_service, get_current_settings, validate_identifier
from tauro.api.core.responses import (
    APIResponse,
    success_response,
    error_response,
)
from tauro.api.schemas import (
    ConfigContextResponse,
    ConfigVersionMetadataResponse,
)
from tauro.config.exceptions import ActiveConfigNotFound, ConfigRepositoryError

projects_router = APIRouter(prefix="/projects", tags=["projects"])
configs_router = APIRouter(prefix="/projects/{project_id}/config", tags=["config"])
config_versions_router = APIRouter(
    prefix="/projects/{project_id}/config/versions", tags=["config-versions"]
)

ENVIRONMENT_QUERY_DESCRIPTION = "Environment name"
ERROR_CONFIG_SERVICE_UNAVAILABLE = "ConfigService not available"


def _ensure_service(settings, service) -> Any:
    if settings.config_source != "mongo":
        raise error_response(
            code="NOT_IMPLEMENTED",
            message="Configuration endpoints are only available when CONFIG_SOURCE=mongo",
        )
    if service is None:
        raise error_response(
            code="SERVICE_UNAVAILABLE", message=ERROR_CONFIG_SERVICE_UNAVAILABLE
        )
    return service


@projects_router.get("/{project_id}", response_model=APIResponse)
async def get_project_details(
    project_id: str,
    settings=Depends(get_current_settings),
    service=Depends(get_config_service),
):
    try:
        project_id = validate_identifier(project_id, "project_id")
        service = _ensure_service(settings, service)

        project = service.get_project(project_id)
        if not project:
            return error_response(
                code="PROJECT_NOT_FOUND", message=f"Project '{project_id}' not found"
            )

        logger.debug("Project metadata fetched", extra={"project_id": project_id})
        response: Dict[str, Any] = dict(project)
        response.setdefault("id", project_id)
        return success_response(response)

    except Exception as e:
        logger.error(f"Error getting project details: {e}")
        return error_response(
            code="INTERNAL_ERROR",
            message="Error retrieving project details",
            details={"error": str(e)},
        )


@configs_router.get("/context", response_model=APIResponse)
async def get_active_context(
    project_id: str,
    environment: Optional[str] = Query(
        default=None, description=ENVIRONMENT_QUERY_DESCRIPTION
    ),
    settings=Depends(get_current_settings),
    service=Depends(get_config_service),
):
    try:
        project_id = validate_identifier(project_id, "project_id")
        env = environment or settings.environment
        service = _ensure_service(settings, service)

        bundle = service.get_active_context(project_id, env)
        logger.debug(
            "Active context resolved",
            extra={
                "project_id": project_id,
                "environment": env,
                "version": bundle.version,
            },
        )

        context_data = ConfigContextResponse(
            project_id=bundle.project_id,
            environment=bundle.environment,
            version=bundle.version,
            global_settings=bundle.global_settings,
            pipelines_config=bundle.pipelines_config,
            nodes_config=bundle.nodes_config,
            input_config=bundle.input_config,
            output_config=bundle.output_config,
        )
        return success_response(context_data)

    except ActiveConfigNotFound as exc:
        logger.warning(f"Active config not found: {exc}")
        return error_response(code="CONFIG_NOT_FOUND", message=str(exc))
    except ConfigRepositoryError as exc:
        logger.error(f"Error loading active context: {exc}")
        return error_response(
            code="CONFIG_REPO_ERROR",
            message="Failed to load configuration context",
            details={"error": str(exc)},
        )
    except Exception as e:
        logger.error(f"Error getting active context: {e}")
        return error_response(
            code="INTERNAL_ERROR",
            message="Error retrieving active context",
            details={"error": str(e)},
        )


@config_versions_router.get("/active", response_model=APIResponse)
async def get_active_version_metadata(
    project_id: str,
    environment: Optional[str] = Query(
        default=None, description=ENVIRONMENT_QUERY_DESCRIPTION
    ),
    settings=Depends(get_current_settings),
    service=Depends(get_config_service),
):
    try:
        project_id = validate_identifier(project_id, "project_id")
        env = environment or settings.environment
        service = _ensure_service(settings, service)

        metadata = service.get_active_version_metadata(project_id, env)
        logger.debug(
            "Active version metadata fetched",
            extra={
                "project_id": project_id,
                "environment": env,
                "version": metadata.get("version"),
            },
        )
        version_metadata = ConfigVersionMetadataResponse(**metadata)
        return success_response(version_metadata)

    except ActiveConfigNotFound as exc:
        logger.warning(f"Active config not found: {exc}")
        return error_response(code="CONFIG_NOT_FOUND", message=str(exc))
    except ConfigRepositoryError as exc:
        logger.error(f"Error retrieving version metadata: {exc}")
        return error_response(
            code="CONFIG_REPO_ERROR",
            message="Failed to load configuration metadata",
            details={"error": str(exc)},
        )
    except Exception as e:
        logger.error(f"Error getting active version metadata: {e}")
        return error_response(
            code="INTERNAL_ERROR",
            message="Error retrieving version metadata",
            details={"error": str(e)},
        )
