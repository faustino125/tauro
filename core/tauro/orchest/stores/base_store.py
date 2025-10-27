"""
Base store implementation providing common database operations.

This module provides:
- Common initialization and connection management
- Helper methods for MongoDB operations
- Type definitions and abstract interfaces
- Health checking and monitoring
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, Optional, List
import threading
from datetime import datetime, timezone
import json

from loguru import logger  # type: ignore
from tauro.orchest.models import PipelineRun, TaskRun, RunState


class BaseStore(ABC):
    """
    Abstract base class for store implementations.

    Provides:
    - Connection management
    - Health checks
    - Type safety
    - Common patterns
    """

    def __init__(self, context: Optional[Any] = None):
        """
        Initialize base store.

        Args:
            context: Tauro execution context
        """
        self.context = context
        self._lock = threading.RLock()

    @staticmethod
    def _parse_datetime(value: Optional[str]) -> Optional[datetime]:
        """Parse ISO datetime string to datetime object."""
        if not value:
            return None
        try:
            return datetime.fromisoformat(value)
        except (ValueError, TypeError):
            try:
                return datetime.strptime(value, "%Y-%m-%dT%H:%M:%S.%f%z")
            except (ValueError, TypeError):
                return None

    @staticmethod
    def _format_datetime(value: Optional[datetime]) -> Optional[str]:
        """Format datetime object to ISO string."""
        if not value:
            return None
        return value.isoformat()

    @staticmethod
    def _parse_json(value: Optional[str]) -> Dict[str, Any]:
        """Parse JSON string to dictionary."""
        if not value:
            return {}
        try:
            if isinstance(value, str):
                return json.loads(value)
            elif isinstance(value, dict):
                return value
            return {}
        except (json.JSONDecodeError, TypeError):
            return {}

    @staticmethod
    def _format_json(value: Dict[str, Any]) -> str:
        """Format dictionary to JSON string."""
        return json.dumps(value or {})

    def health_check(self) -> Dict[str, Any]:
        """
        Perform health check on store.

        Returns:
            Dictionary with health status
        """
        return {
            "healthy": True,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "checks": {},
        }

    def close(self) -> None:
        """Close connections and cleanup resources."""
        pass

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()


class PipelineRunStore(BaseStore):
    """Interface for PipelineRun operations."""

    @abstractmethod
    def create_pipeline_run(
        self, pipeline_id: str, params: Optional[Dict[str, Any]] = None
    ) -> PipelineRun:
        """Create a new pipeline run."""
        pass

    @abstractmethod
    def get_pipeline_run(self, run_id: str) -> Optional[PipelineRun]:
        """Get a pipeline run by ID."""
        pass

    @abstractmethod
    def update_pipeline_run_state(
        self,
        run_id: str,
        new_state: RunState,
        started_at: Optional[datetime] = None,
        finished_at: Optional[datetime] = None,
        error: Optional[str] = None,
    ) -> None:
        """Update pipeline run state."""
        pass

    @abstractmethod
    def list_pipeline_runs(
        self,
        pipeline_id: Optional[str] = None,
        state: Optional[RunState] = None,
        limit: int = 100,
        offset: int = 0,
        created_after: Optional[datetime] = None,
        created_before: Optional[datetime] = None,
    ) -> List[PipelineRun]:
        """List pipeline runs with optional filtering."""
        pass


class TaskRunStore(BaseStore):
    """Interface for TaskRun operations."""

    @abstractmethod
    def create_task_run(self, pipeline_run_id: str, task_id: str) -> TaskRun:
        """Create a new task run."""
        pass

    @abstractmethod
    def update_task_run_state(
        self,
        task_run_id: str,
        new_state: RunState,
        try_number: Optional[int] = None,
        started_at: Optional[datetime] = None,
        finished_at: Optional[datetime] = None,
        error: Optional[str] = None,
        log_uri: Optional[str] = None,
    ) -> None:
        """Update task run state."""
        pass

    @abstractmethod
    def list_task_runs(
        self, pipeline_run_id: str, state: Optional[RunState] = None
    ) -> List[TaskRun]:
        """List task runs for a pipeline run."""
        pass
