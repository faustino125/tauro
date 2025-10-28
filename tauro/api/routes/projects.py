"""
Copyright (c) 2025 Faustino Lopez Ramos.
For licensing information, see the LICENSE file in the project root
"""
from typing import Optional
from uuid import UUID

# Third-party
from fastapi import APIRouter, Depends, Query, status
from loguru import logger

# Local
from tauro.api.core.deps import get_project_service
from tauro.api.core.responses import (
    APIResponse,
    ListResponse,
    error_response,
    list_response,
    success_response,
)
from tauro.api.schemas.models import (
    ProjectCreate,
    ProjectUpdate,
)
from tauro.api.services.project_service import (
    InvalidProjectError,
    ProjectAlreadyExistsError,
    ProjectNotFoundError,
    ProjectService,
)

# Constants
DEFAULT_USER = "user@example.com"


# Router configuration
router = APIRouter(
    prefix="/projects",
    tags=["projects"],
    responses={
        404: {"description": "Project not found"},
        409: {"description": "Project already exists"},
    },
)


# =============================================================================
# CRUD Endpoints
# =============================================================================


@router.post("", response_model=APIResponse, status_code=status.HTTP_201_CREATED)
async def create_project(
    data: ProjectCreate,
    service: ProjectService = Depends(get_project_service),
    current_user: str = DEFAULT_USER,
):
    """
    Create a new project.
    """
    try:
        logger.info(f"Creating project: {data.name} (user: {current_user})")
        project = await service.create_project(data, current_user)
        logger.info(f"Project created: {project.id}")
        return success_response(project)

    except ProjectAlreadyExistsError as e:
        logger.warning(f"Project creation failed: {e}")
        return error_response(code="PROJECT_EXISTS", message=str(e), details={"name": data.name})

    except InvalidProjectError as e:
        logger.warning(f"Invalid project data: {e}")
        return error_response(code="INVALID_PROJECT", message=str(e))

    except Exception as e:
        logger.error(f"Error creating project: {e}", exc_info=True)
        return error_response(
            code="INTERNAL_ERROR",
            message="Failed to create project",
            details={"error": str(e)},
        )


@router.get("", response_model=ListResponse)
async def list_projects(
    skip: int = Query(0, ge=0),
    limit: int = Query(50, ge=1, le=100),
    status_filter: Optional[str] = Query(None, alias="status"),
    service: ProjectService = Depends(get_project_service),
):
    """
    List all projects with optional filtering.
    """
    try:
        logger.debug(f"Listing projects: skip={skip}, limit={limit}, status={status_filter}")

        filters = {}
        if status_filter:
            filters["status"] = status_filter

        projects, total = await service.list_projects(offset=skip, limit=limit, **filters)

        logger.debug(f"Found {len(projects)} projects (total: {total})")

        return list_response(projects, total, limit, skip)

    except Exception as e:
        logger.error(f"Error listing projects: {e}", exc_info=True)
        return error_response(
            code="INTERNAL_ERROR",
            message="Failed to list projects",
            details={"error": str(e)},
        )


@router.get("/{project_id}", response_model=APIResponse)
async def get_project(
    project_id: str,
    service: ProjectService = Depends(get_project_service),
):
    """
    Get a specific project by ID.
    """
    try:
        logger.debug(f"Getting project: {project_id}")

        project = await service.read_project(project_id)
        logger.debug(f"Project retrieved: {project.name}")
        return success_response(project)

    except ProjectNotFoundError as e:
        logger.warning(f"Project not found: {e}")
        return error_response(
            code="PROJECT_NOT_FOUND", message=str(e), details={"project_id": project_id}
        )

    except Exception as e:
        logger.error(f"Error getting project: {e}", exc_info=True)
        return error_response(
            code="INTERNAL_ERROR",
            message="Failed to get project",
            details={"error": str(e)},
        )


@router.put("/{project_id}", response_model=APIResponse)
async def update_project(
    project_id: str,
    data: ProjectUpdate,
    service: ProjectService = Depends(get_project_service),
    current_user: str = DEFAULT_USER,
):
    """
    Update a project.
    """
    try:
        logger.info(f"Updating project: {project_id} (user: {current_user})")

        project = await service.update_project(
            project_id, data.dict(exclude_unset=True), current_user
        )
        logger.info(f"Project updated: {project_id}")
        return success_response(project)

    except ProjectNotFoundError as e:
        logger.warning(f"Project not found: {e}")
        return error_response(
            code="PROJECT_NOT_FOUND", message=str(e), details={"project_id": project_id}
        )

    except InvalidProjectError as e:
        logger.warning(f"Invalid project data: {e}")
        return error_response(code="INVALID_PROJECT", message=str(e))

    except Exception as e:
        logger.error(f"Error updating project: {e}", exc_info=True)
        return error_response(
            code="INTERNAL_ERROR",
            message="Failed to update project",
            details={"error": str(e)},
        )


@router.delete("/{project_id}", response_model=APIResponse, status_code=status.HTTP_200_OK)
async def delete_project(
    project_id: str,
    service: ProjectService = Depends(get_project_service),
):
    """
    Delete a project (soft delete - marks as archived).
    """
    try:
        logger.info(f"Deleting project: {project_id}")

        success = await service.delete_project(project_id)

        if success:
            logger.info(f"Project deleted: {project_id}")
            return success_response({"message": f"Project {project_id} deleted"})
        else:
            return error_response(
                code="PROJECT_NOT_FOUND", message=f"Project {project_id} not found"
            )

    except ProjectNotFoundError as e:
        logger.warning(f"Project not found: {e}")
        return error_response(code="PROJECT_NOT_FOUND", message=str(e))

    except Exception as e:
        logger.error(f"Error deleting project: {e}", exc_info=True)
        return error_response(
            code="INTERNAL_ERROR",
            message="Failed to delete project",
            details={"error": str(e)},
        )


@router.post(
    "/{project_id}/duplicate",
    response_model=APIResponse,
    status_code=status.HTTP_201_CREATED,
)
async def duplicate_project(
    project_id: str,
    new_name: str = Query(..., min_length=1, max_length=255),
    service: ProjectService = Depends(get_project_service),
    current_user: str = DEFAULT_USER,
):
    """
    Duplicate an existing project with a new name.
    """
    try:
        logger.info(f"Duplicating project: {project_id} → {new_name} (user: {current_user})")

        # Validate UUID format
        try:
            UUID(project_id)
        except ValueError:
            return error_response(
                code="INVALID_PROJECT_ID",
                message=f"Invalid project ID format: {project_id}",
            )

        project = await service.duplicate_project(project_id, new_name, current_user)

        logger.info(f"Project duplicated: {project.id}")
        return success_response(project)

    except ProjectNotFoundError as e:
        logger.warning(f"Source project not found: {e}")
        return error_response(
            code="PROJECT_NOT_FOUND", message=f"Source project '{project_id}' not found"
        )

    except ProjectAlreadyExistsError as e:
        logger.warning(f"Duplicate project creation failed: {e}")
        return error_response(code="PROJECT_EXISTS", message=f"Project '{new_name}' already exists")

    except InvalidProjectError as e:
        logger.warning(f"Invalid project data: {e}")
        return error_response(code="INVALID_PROJECT", message=str(e))

    except Exception as e:
        logger.error(f"Error duplicating project: {e}", exc_info=True)
        return error_response(
            code="INTERNAL_ERROR",
            message="Failed to duplicate project",
            details={"error": str(e)},
        )


__all__ = ["router"]
