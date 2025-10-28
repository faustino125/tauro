from typing import Any, Optional, Dict, List
from datetime import datetime, timezone
from pydantic import BaseModel, Field
from enum import Enum

RESPONSE_TIMESTAMP_DESC = "Response timestamp"
EXAMPLE_TIMESTAMP = "2025-01-15T10:30:00Z"


class ResponseStatus(str, Enum):
    """Response status enum"""

    SUCCESS = "success"
    ERROR = "error"
    ACCEPTED = "accepted"


class ErrorDetail(BaseModel):
    """Error detail information"""

    code: str = Field(..., description="Error code (e.g., RESOURCE_NOT_FOUND)")
    message: str = Field(..., description="Human-readable error message")
    details: Optional[Dict[str, Any]] = Field(default=None, description="Additional error details")


class APIResponse(BaseModel):
    """Standard API response for success or error"""

    status: ResponseStatus
    data: Optional[Any] = Field(default=None, description="Response data (null for errors)")
    error: Optional[ErrorDetail] = Field(
        default=None, description="Error details (null for success)"
    )
    timestamp: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc),
        description=RESPONSE_TIMESTAMP_DESC,
    )

    class Config:
        json_schema_extra = {
            "examples": [
                {
                    "status": "success",
                    "data": None,
                    "error": None,
                    "timestamp": EXAMPLE_TIMESTAMP,
                },
                {
                    "status": "error",
                    "data": None,
                    "error": {
                        "code": "PROJECT_NOT_FOUND",
                        "message": "Project not found",
                        "details": {"project_id": "proj-123"},
                    },
                    "timestamp": EXAMPLE_TIMESTAMP,
                },
            ]
        }


class AsyncResponse(BaseModel):
    """Response for async operations (202 Accepted)"""

    status: ResponseStatus = ResponseStatus.ACCEPTED
    task_id: str = Field(..., description="Task ID for polling")
    status_url: str = Field(..., description="URL to check status")
    message: str = Field(
        default="Operation accepted and processing",
        description="Human-readable message",
    )
    timestamp: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc),
        description=RESPONSE_TIMESTAMP_DESC,
    )

    class Config:
        json_schema_extra = {
            "example": {
                "status": "accepted",
                "task_id": "run-abc123",
                "message": "Pipeline execution started in background",
                "timestamp": EXAMPLE_TIMESTAMP,
            }
        }


class PaginationInfo(BaseModel):
    """Pagination information for list responses"""

    total: int = Field(..., description="Total number of items")
    limit: int = Field(..., description="Items per page")
    offset: int = Field(..., description="Current offset")
    has_next: bool = Field(..., description="Is there a next page?")
    has_previous: bool = Field(..., description="Is there a previous page?")


class ListResponse(BaseModel):
    """Response for list endpoints"""

    status: ResponseStatus = ResponseStatus.SUCCESS
    data: List[Any] = Field(..., description="List of items")
    pagination: PaginationInfo = Field(..., description="Pagination info")
    timestamp: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc),
        description=RESPONSE_TIMESTAMP_DESC,
    )

    class Config:
        json_schema_extra = {
            "example": {
                "status": "success",
                "data": [{"id": "item-1"}, {"id": "item-2"}],
                "pagination": {
                    "total": 100,
                    "limit": 50,
                    "offset": 0,
                    "has_next": True,
                    "has_previous": False,
                },
                "timestamp": EXAMPLE_TIMESTAMP,
            }
        }


# =============================================================================
# Helper Functions
# =============================================================================


def success_response(data: Any) -> APIResponse:
    """Create success response"""
    return APIResponse(status=ResponseStatus.SUCCESS, data=data, error=None)


def error_response(
    code: str, message: str, details: Optional[Dict[str, Any]] = None
) -> APIResponse:
    """Create error response"""
    return APIResponse(
        status=ResponseStatus.ERROR,
        data=None,
        error=ErrorDetail(code=code, message=message, details=details),
    )


def async_response(task_id: str, status_url: str, message: str = "") -> AsyncResponse:
    """Create async response"""
    return AsyncResponse(
        task_id=task_id,
        status_url=status_url,
        message=message or "Operation accepted and processing",
    )


def list_response(items: List[Any], total: int, limit: int, offset: int) -> ListResponse:
    """Create list response"""
    return ListResponse(
        data=items,
        pagination=PaginationInfo(
            total=total,
            limit=limit,
            offset=offset,
            has_next=(offset + limit) < total,
            has_previous=offset > 0,
        ),
    )
