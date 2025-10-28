"""
Copyright (c) 2025 Faustino Lopez Ramos.
For licensing information, see the LICENSE file in the project root
"""

import re
from fastapi import HTTPException, status

VALID_ID_PATTERN = re.compile(r"^[a-zA-Z0-9_\-\.]+$")


def validate_identifier(
    value: str,
    name: str = "identifier",
    max_length: int = 255,
) -> str:
    """
    Validate path parameter identifiers.

    Ensures IDs only contain safe characters (alphanumeric, underscore, hyphen, dot).
    Prevents path traversal (../) and other injection attacks.

    Args:
        value: The identifier to validate
        name: Description of what we're validating (for error messages)
        max_length: Maximum allowed length (default 255)

    Returns:
        The validated ID string

    Raises:
        HTTPException: If the identifier is invalid
    """
    if not value:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"{name.capitalize()} cannot be empty",
        )

    if len(value) > max_length:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"{name.capitalize()} exceeds maximum length of {max_length}",
        )

    if not VALID_ID_PATTERN.match(value):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid {name}: only alphanumeric characters, underscore, hyphen, and dot are allowed",
        )

    return value


def validate_project_id(project_id: str) -> str:
    """Convenience function to validate project_id parameter"""
    return validate_identifier(project_id, name="project_id")


def validate_pipeline_id(pipeline_id: str) -> str:
    """Convenience function to validate pipeline_id parameter"""
    return validate_identifier(pipeline_id, name="pipeline_id")


def validate_schedule_id(schedule_id: str) -> str:
    """Convenience function to validate schedule_id parameter"""
    return validate_identifier(schedule_id, name="schedule_id")


def validate_run_id(run_id: str) -> str:
    """Convenience function to validate run_id parameter"""
    return validate_identifier(run_id, name="run_id")


# =============================================================================
# Exports
# =============================================================================

__all__ = [
    "VALID_ID_PATTERN",
    "validate_identifier",
    "validate_project_id",
    "validate_pipeline_id",
    "validate_schedule_id",
    "validate_run_id",
]
