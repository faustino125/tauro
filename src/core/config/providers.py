"""
Copyright (c) 2025 Faustino Lopez Ramos.
For licensing information, see the LICENSE file in the project root
"""
from __future__ import annotations
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Dict, Optional


@dataclass
class ActiveConfigRecord:
    """
    Data class representing an active configuration version record.

    This is returned by repository implementations when fetching
    the active configuration for a project/environment.
    """

    project_id: str
    environment: str
    version: str
    document: Dict[str, Any]
    metadata: Optional[Dict[str, Any]] = None
    release: Dict[str, Any] = field(default_factory=dict)
    environment_variables: Optional[Dict[str, Any]] = None
    created_at: Optional[str] = None
    updated_at: Optional[str] = None


class IConfigRepository(ABC):
    """
    Abstract interface for configuration repository implementations.

    Implementations should provide methods to:
    - Retrieve project metadata
    - Get active configuration versions
    - Close/cleanup resources
    """

    @abstractmethod
    def get_project(self, project_id: str) -> Optional[Dict[str, Any]]:
        """
        Retrieve project metadata by project ID.

        Args:
            project_id: Unique project identifier

        Returns:
            Project metadata dictionary or None if not found
        """
        pass

    @abstractmethod
    def get_active_version(self, project_id: str, environment: str) -> ActiveConfigRecord:
        """
        Get the active configuration version for a project/environment.

        Args:
            project_id: Unique project identifier
            environment: Environment name (dev, prod, etc.)

        Returns:
            ActiveConfigRecord with configuration data

        Raises:
            ActiveConfigNotFound: If no active version exists
            ConfigRepositoryError: If there's an error accessing the repository
        """
        pass

    @abstractmethod
    def close(self) -> None:
        """
        Close repository connections and cleanup resources.

        Should be called when the repository is no longer needed.
        """
        pass
