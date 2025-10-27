"""
Configuration Versioning REST Endpoints

Endpoints for managing configuration versions, history, promotions, and rollbacks:

GET  /api/v1/config-versions              - List versions with filters
GET  /api/v1/config-versions/{id}         - Get specific version
POST /api/v1/config-versions/{id}/promote - Promote to environment
POST /api/v1/config-versions/{id}/rollback - Rollback to previous version
GET  /api/v1/projects/{id}/versions       - List project version history
GET  /api/v1/config-versions/compare      - Compare two versions
GET  /api/v1/config-versions/stats        - Get version statistics
"""

from typing import Optional
from uuid import UUID

from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel, Field
from loguru import logger

from core.api.services.config_version_service import (
    ConfigVersionService,
    ConfigVersionResponse,
    EnvironmentType,
    PromotionStatus,
    VersionNotFoundError,
    PromotionFailedError,
    RollbackFailedError,
)
from core.api.core.deps import get_config_version_service
from core.api.core.responses import (
    APIResponse,
    ListResponse,
    success_response,
    error_response,
    list_response,
)


# ============================================================================
# Constants
# ============================================================================

VERSION_NOT_FOUND = "Version not found"


# ============================================================================
# Request/Response Models
# ============================================================================


class PromoteVersionRequest(BaseModel):
    """Request to promote a configuration version"""

    target_environment: EnvironmentType = Field(..., description="Target environment")
    approved_by: str = Field(..., description="User approving promotion")
    approval_reason: Optional[str] = Field(None, description="Optional approval notes")

    class Config:
        use_enum_values = True


class RollbackVersionRequest(BaseModel):
    """Request to rollback to previous version"""

    target_version: int = Field(..., description="Version number to rollback to")
    reason: str = Field(..., min_length=1, description="Reason for rollback")
    rolled_back_by: str = Field(..., description="User performing rollback")

    class Config:
        use_enum_values = True


class VersionComparisonResponse(BaseModel):
    """Response from version comparison"""

    from_version: int
    to_version: int
    change_count: int
    changes_summary: str


class VersionStatisticsResponse(BaseModel):
    """Statistics about version history"""

    total_versions: int
    versions_by_status: dict
    top_contributors: dict
    analysis_period_days: int
    analysis_start_date: str
    analysis_end_date: str


# ============================================================================
# Router
# ============================================================================

router = APIRouter(prefix="/api/v1/config-versions", tags=["configuration-versions"])


# ============================================================================
# LIST - List configuration versions
# ============================================================================


@router.get(
    "",
    response_model=ListResponse,
    summary="List configuration versions",
    description="Retrieve configuration versions with optional filtering",
)
async def list_versions(
    project_id: UUID = Query(..., description="Filter by project ID"),
    pipeline_id: Optional[UUID] = Query(None, description="Filter by pipeline ID"),
    promotion_status: Optional[str] = Query(
        None, description="Filter by promotion status"
    ),
    limit: int = Query(50, ge=1, le=500, description="Maximum results"),
    offset: int = Query(0, ge=0, description="Pagination offset"),
    service: ConfigVersionService = Depends(get_config_version_service),
) -> ListResponse:
    """
    List configuration versions with optional filtering.
    """
    try:
        status = None
        if promotion_status:
            status = PromotionStatus(promotion_status)

        versions = await service.list_versions(
            project_id=project_id,
            pipeline_id=pipeline_id,
            promotion_status=status,
            limit=limit,
            offset=offset,
        )

        logger.info(f"Retrieved {len(versions)} versions for project {project_id}")

        return list_response(versions, len(versions), limit, offset)

    except Exception as e:
        logger.error(f"Failed to list versions: {e}")
        return error_response(
            code="INTERNAL_ERROR",
            message="Failed to list versions",
            details={"error": str(e)},
        )


# ============================================================================
# READ - Get a specific version
# ============================================================================


@router.get(
    "/{version_id}",
    response_model=APIResponse,
    summary="Get configuration version",
    description="Retrieve a specific configuration version with full snapshot",
)
async def get_version(
    version_id: UUID = Query(..., description="Version ID"),
    service: ConfigVersionService = Depends(get_config_version_service),
) -> APIResponse:
    """
    Get a specific configuration version by ID.
    """
    try:
        version = await service.get_version(version_id=version_id, verify_hash=True)

        logger.info(f"Retrieved version {version_id}")

        return success_response(version)

    except VersionNotFoundError:
        logger.warning(f"Version {version_id} not found")
        return error_response(code="VERSION_NOT_FOUND", message=VERSION_NOT_FOUND)

    except Exception as e:
        logger.error(f"Failed to get version {version_id}: {e}")
        return error_response(
            code="INTERNAL_ERROR",
            message="Failed to retrieve version",
            details={"error": str(e)},
        )


# ============================================================================
# PROMOTE - Promote version to environment
# ============================================================================


@router.post(
    "/{version_id}/promote",
    response_model=APIResponse,
    summary="Promote configuration to environment",
    description="Promote a configuration version to next environment (dev→staging→prod)",
)
async def promote_version(
    version_id: UUID,
    request: PromoteVersionRequest,
    service: ConfigVersionService = Depends(get_config_version_service),
) -> APIResponse:
    """
    Promote a configuration version to target environment.
    """
    try:
        promoted = await service.promote_version(
            version_id=version_id,
            target_environment=request.target_environment,
            approved_by=request.approved_by,
            approval_reason=request.approval_reason,
        )

        logger.info(
            f"Version {version_id} promoted to {request.target_environment} "
            f"by {request.approved_by}"
        )

        return success_response(promoted)

    except VersionNotFoundError:
        logger.warning(f"Version {version_id} not found")
        return error_response(code="VERSION_NOT_FOUND", message=VERSION_NOT_FOUND)

    except PromotionFailedError as e:
        logger.error(f"Promotion failed for version {version_id}: {e}")
        return error_response(code="PROMOTION_FAILED", message=str(e))

    except Exception as e:
        logger.error(f"Promotion error: {e}")
        return error_response(
            code="INTERNAL_ERROR", message="Promotion failed", details={"error": str(e)}
        )


# ============================================================================
# ROLLBACK - Rollback to previous version
# ============================================================================


@router.post(
    "/{version_id}/rollback",
    response_model=APIResponse,
    summary="Rollback to previous version",
    description="Create a rollback version reverting to a previous configuration",
)
async def rollback_version(
    version_id: UUID,
    request: RollbackVersionRequest,
    service: ConfigVersionService = Depends(get_config_version_service),
) -> APIResponse:
    """
    Create a rollback to a previous configuration version.
    """
    try:
        # Get current version to extract project_id
        current_version = await service.get_version(
            version_id=version_id, verify_hash=False
        )

        rollback = await service.rollback(
            project_id=current_version.project_id,
            target_version=request.target_version,
            reason=request.reason,
            rolled_back_by=request.rolled_back_by,
            pipeline_id=current_version.pipeline_id,
        )

        logger.info(
            f"Rollback executed: project {current_version.project_id} "
            f"to version {request.target_version} by {request.rolled_back_by}"
        )

        return success_response(rollback)

    except VersionNotFoundError:
        logger.warning("Target version not found")
        return error_response(code="VERSION_NOT_FOUND", message=VERSION_NOT_FOUND)

    except RollbackFailedError as e:
        logger.error(f"Rollback failed: {e}")
        return error_response(code="ROLLBACK_FAILED", message=str(e))

    except Exception as e:
        logger.error(f"Rollback error: {e}")
        return error_response(
            code="INTERNAL_ERROR", message="Rollback failed", details={"error": str(e)}
        )


# ============================================================================
# COMPARE - Compare two versions
# ============================================================================


@router.get(
    "/compare",
    response_model=APIResponse,
    summary="Compare two versions",
    description="Generate diff between two configuration versions",
)
async def compare_versions(
    from_version_id: UUID = Query(..., description="Source version ID"),
    to_version_id: UUID = Query(..., description="Target version ID"),
    service: ConfigVersionService = Depends(get_config_version_service),
) -> APIResponse:
    """
    Compare two configuration versions and generate diff.
    """
    try:
        comparison = await service.compare_versions(
            from_version_id=from_version_id, to_version_id=to_version_id
        )

        logger.info(
            f"Comparison: version {comparison.from_version} → {comparison.to_version}"
        )

        return success_response(
            VersionComparisonResponse(
                from_version=comparison.from_version,
                to_version=comparison.to_version,
                change_count=len(comparison.changes),
                changes_summary=comparison.summary,
            )
        )

    except VersionNotFoundError:
        return error_response(code="VERSION_NOT_FOUND", message=VERSION_NOT_FOUND)

    except Exception as e:
        logger.error(f"Comparison failed: {e}")
        return error_response(
            code="INTERNAL_ERROR",
            message="Comparison failed",
            details={"error": str(e)},
        )


# ============================================================================
# STATS - Get version statistics
# ============================================================================


@router.get(
    "/stats",
    response_model=APIResponse,
    summary="Get version statistics",
    description="Retrieve statistics about configuration version history",
)
async def get_version_stats(
    project_id: UUID = Query(..., description="Project ID"),
    pipeline_id: Optional[UUID] = Query(None, description="Pipeline ID (optional)"),
    days: int = Query(30, ge=1, le=365, description="Analysis period in days"),
    service: ConfigVersionService = Depends(get_config_version_service),
) -> APIResponse:
    """
    Get statistics about configuration version history.
    """
    try:
        stats = await service.get_version_stats(
            project_id=project_id, pipeline_id=pipeline_id, days=days
        )

        logger.info(f"Statistics retrieved for project {project_id}")

        return success_response(
            VersionStatisticsResponse(
                total_versions=stats["total_versions"],
                versions_by_status=stats["versions_by_status"],
                top_contributors=stats["top_contributors"],
                analysis_period_days=stats["analysis_period_days"],
                analysis_start_date=stats["analysis_start_date"],
                analysis_end_date=stats["analysis_end_date"],
            )
        )

    except Exception as e:
        logger.error(f"Failed to get stats: {e}")
        return error_response(
            code="INTERNAL_ERROR",
            message="Failed to retrieve statistics",
            details={"error": str(e)},
        )


# ============================================================================
# Project-specific version endpoints
# ============================================================================


@router.get(
    "/projects/{project_id}/versions",
    response_model=ListResponse,
    summary="List project version history",
    description="Get complete version history for a project",
)
async def list_project_versions(
    project_id: UUID,
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
    service: ConfigVersionService = Depends(get_config_version_service),
) -> ListResponse:
    """
    Get version history for a specific project.
    """
    try:
        versions = await service.list_versions(
            project_id=project_id, limit=limit, offset=offset
        )

        logger.info(f"Retrieved version history for project {project_id}")

        return list_response(versions, len(versions), limit, offset)

    except Exception as e:
        logger.error(f"Failed to retrieve project versions: {e}")
        return error_response(
            code="INTERNAL_ERROR",
            message="Failed to retrieve versions",
            details={"error": str(e)},
        )


__all__ = ["router"]
