"""Serializers and Deserializers for MongoDB Documents

Converts between Pydantic models and MongoDB documents.
"""

from typing import Dict, Any, Optional
from datetime import datetime, timezone
from uuid import UUID
from core.api.schemas.models import (
    ProjectCreate,
    ProjectResponse,
    RunResponse,
    ScheduleResponse,
    ConfigVersionResponse,
)
from core.api.db.models import (
    ProjectDocument,
    PipelineRunDocument,
    ScheduleDocument,
    ConfigVersionDocument,
)


class ProjectSerializer:
    """Serializes/deserializes Project models"""

    @staticmethod
    def to_document(
        project: ProjectCreate,
        project_id: UUID,
        created_by: str,
    ) -> ProjectDocument:
        """
        Convert ProjectCreate to MongoDB document

        Args:
            project: ProjectCreate instance
            project_id: Generated UUID
            created_by: User email

        Returns:
            ProjectDocument for MongoDB
        """
        now = datetime.now(timezone.utc)

        doc: ProjectDocument = {
            "id": str(project_id),
            "name": project.name,
            "description": project.description or "",
            "global_settings": project.global_settings.dict(),
            "pipelines": [p.dict() for p in (project.pipelines or [])],
            "created_at": now,
            "updated_at": now,
            "created_by": created_by,
            "tags": project.tags or {},
            "status": project.status.value if project.status else "active",
        }

        return doc

    @staticmethod
    def from_document(doc: Dict[str, Any]) -> ProjectResponse:
        """
        Convert MongoDB document to ProjectResponse

        Args:
            doc: MongoDB document

        Returns:
            ProjectResponse instance
        """
        return ProjectResponse(
            id=UUID(doc["id"]),
            name=doc["name"],
            description=doc.get("description"),
            global_settings=doc.get("global_settings", {}),
            pipelines=doc.get("pipelines", []),
            tags=doc.get("tags", {}),
            status=doc.get("status", "active"),
            created_at=doc["created_at"],
            updated_at=doc["updated_at"],
            created_by=doc["created_by"],
            run_count=doc.get("run_count", 0),
        )

    @staticmethod
    def update_document(
        doc: ProjectDocument,
        update_data: Dict[str, Any],
    ) -> ProjectDocument:
        """
        Update ProjectDocument with new data

        Args:
            doc: Existing ProjectDocument
            update_data: Data to update

        Returns:
            Updated ProjectDocument
        """
        doc["updated_at"] = datetime.now(timezone.utc)

        if "description" in update_data:
            doc["description"] = update_data["description"]

        if "global_settings" in update_data:
            doc["global_settings"] = update_data["global_settings"]

        if "pipelines" in update_data:
            doc["pipelines"] = update_data["pipelines"]

        if "tags" in update_data:
            doc["tags"] = update_data["tags"]

        if "status" in update_data:
            doc["status"] = update_data["status"]

        return doc


class RunSerializer:
    """Serializes/deserializes Run models"""

    @staticmethod
    def to_document(
        project_id: UUID,
        pipeline_id: UUID,
        params: Dict[str, Any],
        priority: str,
        tags: Dict[str, str],
        run_id: UUID,
        created_by: str,
    ) -> PipelineRunDocument:
        """
        Create PipelineRunDocument

        Args:
            project_id: Project UUID
            pipeline_id: Pipeline UUID
            params: Run parameters
            priority: Priority level
            tags: Run tags
            run_id: Run UUID
            created_by: User email

        Returns:
            PipelineRunDocument
        """
        doc: PipelineRunDocument = {
            "id": str(run_id),
            "project_id": str(project_id),
            "pipeline_id": str(pipeline_id),
            "state": "PENDING",
            "params": params,
            "priority": priority,
            "created_at": datetime.now(timezone.utc),
            "created_by": created_by,
            "tags": tags,
            "progress": {
                "total_tasks": 0,
                "completed_tasks": 0,
                "failed_tasks": 0,
                "skipped_tasks": 0,
            },
            "retry_count": 0,
        }

        return doc

    @staticmethod
    def from_document(doc: Dict[str, Any]) -> RunResponse:
        """
        Convert MongoDB document to RunResponse

        Args:
            doc: MongoDB document

        Returns:
            RunResponse instance
        """
        return RunResponse(
            id=UUID(doc["id"]),
            project_id=UUID(doc["project_id"]),
            pipeline_id=UUID(doc["pipeline_id"]),
            state=doc.get("state", "PENDING"),
            params=doc.get("params", {}),
            priority=doc.get("priority", "normal"),
            tags=doc.get("tags", {}),
            created_at=doc["created_at"],
            started_at=doc.get("started_at"),
            ended_at=doc.get("ended_at"),
            created_by=doc["created_by"],
            error=doc.get("error"),
            retry_count=doc.get("retry_count", 0),
        )


class ScheduleSerializer:
    """Serializes/deserializes Schedule models"""

    @staticmethod
    def to_document(
        project_id: UUID,
        pipeline_id: UUID,
        kind: str,
        expression: str,
        enabled: bool,
        max_concurrency: int,
        timeout_seconds: Optional[int],
        retry_policy: Optional[Dict[str, Any]],
        tags: Dict[str, str],
        schedule_id: UUID,
        created_by: str,
    ) -> ScheduleDocument:
        """
        Create ScheduleDocument

        Args:
            project_id: Project UUID
            pipeline_id: Pipeline UUID
            kind: Schedule kind (CRON or INTERVAL)
            expression: Cron/interval expression
            enabled: Whether enabled
            max_concurrency: Max concurrent runs
            timeout_seconds: Timeout for runs
            retry_policy: Retry policy
            tags: Schedule tags
            schedule_id: Schedule UUID
            created_by: User email

        Returns:
            ScheduleDocument
        """
        now = datetime.now(timezone.utc)

        doc: ScheduleDocument = {
            "id": str(schedule_id),
            "project_id": str(project_id),
            "pipeline_id": str(pipeline_id),
            "kind": kind,
            "expression": expression,
            "enabled": enabled,
            "max_concurrency": max_concurrency,
            "timeout_seconds": timeout_seconds,
            "retry_policy": retry_policy,
            "created_at": now,
            "updated_at": now,
            "created_by": created_by,
            "tags": tags,
        }

        return doc

    @staticmethod
    def from_document(doc: Dict[str, Any]) -> ScheduleResponse:
        """
        Convert MongoDB document to ScheduleResponse

        Args:
            doc: MongoDB document

        Returns:
            ScheduleResponse instance
        """
        return ScheduleResponse(
            id=UUID(doc["id"]),
            project_id=UUID(doc["project_id"]),
            pipeline_id=UUID(doc["pipeline_id"]),
            kind=doc["kind"],
            expression=doc["expression"],
            enabled=doc.get("enabled", True),
            max_concurrency=doc.get("max_concurrency", 1),
            timeout_seconds=doc.get("timeout_seconds"),
            retry_policy=doc.get("retry_policy"),
            tags=doc.get("tags", {}),
            created_at=doc["created_at"],
            updated_at=doc["updated_at"],
            created_by=doc["created_by"],
            next_run_at=doc.get("next_run_at"),
            last_run_at=doc.get("last_run_at"),
            last_run_id=UUID(doc["last_run_id"]) if doc.get("last_run_id") else None,
        )


class ConfigVersionSerializer:
    """Serializes/deserializes ConfigVersion models"""

    @staticmethod
    def to_document(
        project_id: UUID,
        pipeline_id: UUID,
        version_number: int,
        config_hash: str,
        config_snapshot: Dict[str, Any],
        changes: Dict[str, Any],
        created_by: str,
        change_reason: Optional[str] = None,
    ) -> ConfigVersionDocument:
        """
        Create ConfigVersionDocument

        Args:
            project_id: Project UUID
            pipeline_id: Pipeline UUID
            version_number: Version number
            config_hash: SHA256 hash of config
            config_snapshot: Full config snapshot
            changes: Changes made in this version
            created_by: User email
            change_reason: Reason for change

        Returns:
            ConfigVersionDocument
        """
        doc: ConfigVersionDocument = {
            "project_id": str(project_id),
            "pipeline_id": str(pipeline_id),
            "version_number": version_number,
            "config_hash": config_hash,
            "config_snapshot": config_snapshot,
            "changes": changes,
            "created_at": datetime.now(timezone.utc),
            "created_by": created_by,
            "change_reason": change_reason,
            "promoted": False,
        }

        return doc

    @staticmethod
    def from_document(doc: Dict[str, Any]) -> ConfigVersionResponse:
        """
        Convert MongoDB document to ConfigVersionResponse

        Args:
            doc: MongoDB document

        Returns:
            ConfigVersionResponse instance
        """
        return ConfigVersionResponse(
            version_number=doc["version_number"],
            config_hash=doc["config_hash"],
            changes=doc["changes"],
            created_at=doc["created_at"],
            created_by=doc["created_by"],
            change_reason=doc.get("change_reason"),
            promoted=doc.get("promoted", False),
            promoted_at=doc.get("promoted_at"),
        )
