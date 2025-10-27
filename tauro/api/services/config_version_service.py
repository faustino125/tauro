import hashlib
import json
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List
from uuid import UUID, uuid4
from enum import Enum

from pydantic import BaseModel, Field, validator
from loguru import logger


# ============================================================================
# Enums and Constants
# ============================================================================


class EnvironmentType(str, Enum):
    """Supported deployment environments"""

    DEV = "dev"
    STAGING = "staging"
    PRODUCTION = "prod"


class ChangeType(str, Enum):
    """Types of configuration changes"""

    CREATED = "created"
    UPDATED = "updated"
    DELETED = "deleted"
    PROMOTED = "promoted"
    ROLLED_BACK = "rolled_back"


class PromotionStatus(str, Enum):
    """Status of configuration promotion"""

    PENDING = "pending"
    APPROVED = "approved"
    PROMOTED = "promoted"
    FAILED = "failed"


# ============================================================================
# Data Models
# ============================================================================


class ConfigChange(BaseModel):
    """Represents a single configuration change"""

    field_path: str = Field(..., description="Path to changed field (dot notation)")
    old_value: Any = Field(None, description="Previous value")
    new_value: Any = Field(..., description="New value")
    change_type: ChangeType = Field(default=ChangeType.UPDATED)

    class Config:
        use_enum_values = True


class ConfigSnapshot(BaseModel):
    """Complete configuration snapshot at a point in time"""

    project_config: Dict[str, Any] = Field(
        ..., description="Full project configuration"
    )
    pipeline_configs: Dict[str, Dict[str, Any]] = Field(
        default_factory=dict, description="Pipeline configurations by pipeline_id"
    )
    metadata: Dict[str, Any] = Field(
        default_factory=dict, description="Additional metadata"
    )

    def calculate_hash(self) -> str:
        """Calculate SHA256 hash of snapshot for integrity verification"""
        snapshot_json = json.dumps(self.dict(), sort_keys=True)
        return hashlib.sha256(snapshot_json.encode()).hexdigest()


class ConfigVersionCreate(BaseModel):
    """Request to create a new configuration version"""

    project_id: UUID
    pipeline_id: Optional[UUID] = None
    snapshot: ConfigSnapshot
    changes: List[ConfigChange] = Field(default_factory=list)
    change_reason: str = Field(..., min_length=1, max_length=500)
    created_by: str = Field(..., description="User creating version")
    tags: Dict[str, str] = Field(default_factory=dict)

    class Config:
        use_enum_values = True


class ConfigVersionResponse(BaseModel):
    """Configuration version response"""

    id: UUID
    version_number: int
    project_id: UUID
    pipeline_id: Optional[UUID]
    snapshot_hash: str
    changes: List[ConfigChange]
    change_reason: str
    created_at: datetime
    created_by: str
    promoted_to: Optional[EnvironmentType] = None
    promoted_at: Optional[datetime] = None
    promoted_by: Optional[str] = None
    tags: Dict[str, str]
    promotion_status: PromotionStatus = PromotionStatus.PENDING

    class Config:
        use_enum_values = True


class VersionComparisonResult(BaseModel):
    """Result of comparing two configuration versions"""

    from_version: int
    to_version: int
    changes: List[ConfigChange]
    timestamp_diff: timedelta
    summary: str


class ConfigVersionPromotionRequest(BaseModel):
    """Request to promote a configuration version"""

    source_version_id: UUID
    target_environment: EnvironmentType
    approval_reason: Optional[str] = None
    approved_by: Optional[str] = None

    class Config:
        use_enum_values = True


# ============================================================================
# Exceptions
# ============================================================================


class ConfigVersionError(Exception):
    """Base exception for config versioning errors"""

    pass


class VersionNotFoundError(ConfigVersionError):
    """Version not found in repository"""

    pass


class RollbackFailedError(ConfigVersionError):
    """Rollback operation failed"""

    pass


class PromotionFailedError(ConfigVersionError):
    """Promotion operation failed"""

    pass


class SnapshotIntegrityError(ConfigVersionError):
    """Snapshot integrity check failed"""

    pass


# ============================================================================
# ConfigVersionService
# ============================================================================


class ConfigVersionService:
    """
    Service for managing configuration versions with full audit trail.

    Features:
    - Version tracking with automatic numbering
    - Snapshot storage with integrity verification
    - Change history with detailed metadata
    - Rollback capability with validation
    - Promotion across environments
    - Comparison and diff capabilities

    Example:
        >>> service = ConfigVersionService(db)
        >>>
        >>> # Create a version
        >>> version = await service.create_version(
        ...     project_id=proj_id,
        ...     snapshot=ConfigSnapshot(project_config={...}),
        ...     change_reason="Performance optimization",
        ...     created_by="user@example.com"
        ... )
        >>>
        >>> # List all versions for a project
        >>> versions = await service.list_versions(project_id=proj_id)
        >>>
        >>> # Rollback to previous version
        >>> rolled_back = await service.rollback(
        ...     project_id=proj_id,
        ...     target_version=version.version_number - 1,
        ...     reason="Performance regression detected"
        ... )
        >>>
        >>> # Promote to production
        >>> promoted = await service.promote_version(
        ...     version_id=version.id,
        ...     target_environment=EnvironmentType.PRODUCTION,
        ...     approved_by="admin@example.com"
        ... )
    """

    def __init__(self, db, collection_name: str = "config_versions"):
        """
        Initialize ConfigVersionService.

        Args:
            db: MongoDB AsyncClient or database instance
            collection_name: Name of MongoDB collection for version storage
        """
        self.db = db
        self.collection_name = collection_name
        self.collection = db[collection_name]

        logger.info(
            f"ConfigVersionService initialized with collection: {collection_name}"
        )

    async def _ensure_indexes(self) -> None:
        """Ensure required indexes exist on collection"""
        try:
            # Composite index for project + pipeline + version
            await self.collection.create_index(
                [("project_id", 1), ("pipeline_id", 1), ("version_number", -1)]
            )

            # Index for quick lookups by project
            await self.collection.create_index([("project_id", 1)])

            # Index for filtering by created_at
            await self.collection.create_index([("created_at", -1)])

            # Index for promotion tracking
            await self.collection.create_index([("promoted_to", 1)])

            # TTL index (keep versions for 1 year by default)
            await self.collection.create_index(
                [("created_at", 1)], expireAfterSeconds=31536000  # 1 year
            )

            logger.debug("Indexes ensured on config_versions collection")
        except Exception as e:
            logger.warning(f"Index creation failed (may already exist): {e}")

    async def create_version(
        self,
        project_id: UUID,
        snapshot: ConfigSnapshot,
        change_reason: str,
        created_by: str,
        pipeline_id: Optional[UUID] = None,
        changes: Optional[List[ConfigChange]] = None,
        tags: Optional[Dict[str, str]] = None,
    ) -> ConfigVersionResponse:
        """
        Create a new configuration version with full snapshot and metadata.

        Args:
            project_id: ID of the project
            snapshot: ConfigSnapshot with current state
            change_reason: Human-readable reason for change
            created_by: User creating the version
            pipeline_id: Optional specific pipeline ID
            changes: List of ConfigChange detailing what changed
            tags: Optional metadata tags

        Returns:
            ConfigVersionResponse with version details

        Raises:
            ConfigVersionError: If version creation fails

        Example:
            >>> version = await service.create_version(
            ...     project_id=UUID("12345..."),
            ...     snapshot=ConfigSnapshot(
            ...         project_config={"name": "etl_v2", "max_nodes": 16},
            ...         pipeline_configs={"pipe1": {"nodes": 8}}
            ...     ),
            ...     change_reason="Increased parallelism",
            ...     created_by="alice@example.com"
            ... )
        """
        await self._ensure_indexes()

        try:
            # Calculate next version number
            version_number = await self._get_next_version_number(
                project_id, pipeline_id
            )

            # Calculate snapshot hash for integrity
            snapshot_hash = snapshot.calculate_hash()

            # Prepare version document
            version_doc = {
                "id": uuid4(),
                "version_number": version_number,
                "project_id": str(project_id),
                "pipeline_id": str(pipeline_id) if pipeline_id else None,
                "snapshot": snapshot.dict(),
                "snapshot_hash": snapshot_hash,
                "changes": [change.dict() for change in (changes or [])],
                "change_reason": change_reason,
                "created_at": datetime.now(datetime.timezone.utc),
                "created_by": created_by,
                "promoted_to": None,
                "promoted_at": None,
                "promoted_by": None,
                "promotion_status": PromotionStatus.PENDING.value,
                "tags": tags or {},
                "rollback_from_version": None,
            }

            # Insert version
            await self.collection.insert_one(version_doc)
            version_id = version_doc["id"]

            logger.info(
                f"Version {version_number} created for project {project_id} "
                f"by {created_by}: {change_reason}"
            )

            return ConfigVersionResponse(
                id=version_id,
                version_number=version_number,
                project_id=project_id,
                pipeline_id=pipeline_id,
                snapshot_hash=snapshot_hash,
                changes=changes or [],
                change_reason=change_reason,
                created_at=version_doc["created_at"],
                created_by=created_by,
                tags=tags or {},
            )

        except Exception as e:
            logger.error(f"Failed to create version for project {project_id}: {e}")
            raise ConfigVersionError(f"Version creation failed: {str(e)}")

    async def list_versions(
        self,
        project_id: UUID,
        pipeline_id: Optional[UUID] = None,
        limit: int = 50,
        offset: int = 0,
        promotion_status: Optional[PromotionStatus] = None,
    ) -> List[ConfigVersionResponse]:
        """
        List configuration versions with optional filtering.

        Args:
            project_id: Filter by project ID
            pipeline_id: Optional filter by pipeline ID
            limit: Maximum number of versions to return
            offset: Pagination offset
            promotion_status: Filter by promotion status

        Returns:
            List of ConfigVersionResponse sorted by version number (descending)

        Example:
            >>> versions = await service.list_versions(
            ...     project_id=proj_id,
            ...     promotion_status=PromotionStatus.PROMOTED,
            ...     limit=10
            ... )
        """
        await self._ensure_indexes()

        try:
            # Build query filter
            query = {"project_id": str(project_id)}

            if pipeline_id:
                query["pipeline_id"] = str(pipeline_id)

            if promotion_status:
                query["promotion_status"] = promotion_status.value

            # Query with pagination
            cursor = (
                self.collection.find(query)
                .sort("version_number", -1)
                .skip(offset)
                .limit(limit)
            )

            versions = []
            async for doc in cursor:
                versions.append(self._doc_to_response(doc))

            logger.debug(f"Retrieved {len(versions)} versions for project {project_id}")

            return versions

        except Exception as e:
            logger.error(f"Failed to list versions for project {project_id}: {e}")
            raise ConfigVersionError(f"List versions failed: {str(e)}")

    async def get_version(
        self, version_id: UUID, verify_hash: bool = True
    ) -> ConfigVersionResponse:
        """
        Get a specific version by ID with optional integrity verification.

        Args:
            version_id: Version ID to retrieve
            verify_hash: Verify snapshot integrity

        Returns:
            ConfigVersionResponse with full snapshot

        Raises:
            VersionNotFoundError: If version not found
            SnapshotIntegrityError: If hash verification fails

        Example:
            >>> version = await service.get_version(version_id)
        """
        try:
            doc = await self.collection.find_one({"id": version_id})

            if not doc:
                raise VersionNotFoundError(f"Version {version_id} not found")

            # Verify snapshot integrity if requested
            if verify_hash:
                snapshot = ConfigSnapshot(**doc["snapshot"])
                calculated_hash = snapshot.calculate_hash()

                if calculated_hash != doc["snapshot_hash"]:
                    raise SnapshotIntegrityError(
                        f"Snapshot hash mismatch for version {version_id}"
                    )

                logger.debug(f"Snapshot integrity verified for version {version_id}")

            return self._doc_to_response(doc)

        except VersionNotFoundError:
            raise
        except SnapshotIntegrityError:
            raise
        except Exception as e:
            logger.error(f"Failed to get version {version_id}: {e}")
            raise ConfigVersionError(f"Get version failed: {str(e)}")

    async def rollback(
        self,
        project_id: UUID,
        target_version: int,
        reason: str,
        rolled_back_by: str,
        pipeline_id: Optional[UUID] = None,
    ) -> ConfigVersionResponse:
        """
        Rollback configuration to a previous version.

        Args:
            project_id: Project to rollback
            target_version: Version number to rollback to
            reason: Reason for rollback
            rolled_back_by: User performing rollback
            pipeline_id: Optional specific pipeline

        Returns:
            ConfigVersionResponse of the newly created rollback version

        Raises:
            VersionNotFoundError: If target version not found
            RollbackFailedError: If rollback fails

        Example:
            >>> rolled_back = await service.rollback(
            ...     project_id=proj_id,
            ...     target_version=3,
            ...     reason="Performance regression detected",
            ...     rolled_back_by="ops@example.com"
            ... )
        """
        try:
            # Find target version
            query = {"project_id": str(project_id), "version_number": target_version}
            if pipeline_id:
                query["pipeline_id"] = str(pipeline_id)

            target_doc = await self.collection.find_one(query)

            if not target_doc:
                raise VersionNotFoundError(f"Target version {target_version} not found")

            # Get snapshot from target version
            target_snapshot = ConfigSnapshot(**target_doc["snapshot"])

            # Create rollback version
            rollback_change = ConfigChange(
                field_path="*",
                change_type=ChangeType.ROLLED_BACK,
                old_value=f"version_{target_doc['version_number']}",
            )

            # Create new version marked as rollback
            rollback_doc = {
                "id": uuid4(),
                "version_number": await self._get_next_version_number(
                    project_id, pipeline_id
                ),
                "project_id": str(project_id),
                "pipeline_id": str(pipeline_id) if pipeline_id else None,
                "snapshot": target_snapshot.dict(),
                "snapshot_hash": target_snapshot.calculate_hash(),
                "changes": [rollback_change.dict()],
                "change_reason": f"Rollback from version {target_version}: {reason}",
                "created_at": datetime.now(datetime.timezone.utc),
                "created_by": rolled_back_by,
                "promoted_to": None,
                "promoted_at": None,
                "promoted_by": None,
                "promotion_status": PromotionStatus.PENDING.value,
                "tags": {"rollback_from": str(target_version)},
                "rollback_from_version": target_version,
            }

            await self.collection.insert_one(rollback_doc)

            logger.info(
                f"Rollback completed: project {project_id} to version "
                f"{target_version} by {rolled_back_by}"
            )

            return ConfigVersionResponse(
                id=rollback_doc["id"],
                version_number=rollback_doc["version_number"],
                project_id=project_id,
                pipeline_id=pipeline_id,
                snapshot_hash=rollback_doc["snapshot_hash"],
                changes=[rollback_change],
                change_reason=rollback_doc["change_reason"],
                created_at=rollback_doc["created_at"],
                created_by=rolled_back_by,
                tags=rollback_doc["tags"],
            )

        except (VersionNotFoundError, RollbackFailedError):
            raise
        except Exception as e:
            logger.error(f"Rollback failed for project {project_id}: {e}")
            raise RollbackFailedError(f"Rollback operation failed: {str(e)}")

    async def promote_version(
        self,
        version_id: UUID,
        target_environment: EnvironmentType,
        approved_by: str,
        approval_reason: Optional[str] = None,
    ) -> ConfigVersionResponse:
        """
        Promote a configuration version to next environment.

        Args:
            version_id: Version to promote
            target_environment: Target environment (dev, staging, prod)
            approved_by: User approving promotion
            approval_reason: Optional approval notes

        Returns:
            Updated ConfigVersionResponse with promotion info

        Raises:
            VersionNotFoundError: If version not found
            PromotionFailedError: If promotion fails

        Example:
            >>> promoted = await service.promote_version(
            ...     version_id=version.id,
            ...     target_environment=EnvironmentType.PRODUCTION,
            ...     approved_by="admin@example.com",
            ...     approval_reason="All tests passed"
            ... )
        """
        try:
            # Get version (ensure it exists)
            await self.get_version(version_id, verify_hash=False)

            # Update promotion status
            update_result = await self.collection.update_one(
                {"id": version_id},
                {
                    "$set": {
                        "promoted_to": target_environment.value,
                        "promoted_at": datetime.now(datetime.timezone.utc),
                        "promoted_by": approved_by,
                        "promotion_status": PromotionStatus.PROMOTED.value,
                        "promotion_reason": approval_reason,
                    }
                },
            )

            if update_result.matched_count == 0:
                raise VersionNotFoundError(f"Version {version_id} not found")

            logger.info(
                f"Version {version_id} promoted to {target_environment.value} "
                f"by {approved_by}"
            )

            # Retrieve updated document
            updated_doc = await self.collection.find_one({"id": version_id})
            return self._doc_to_response(updated_doc)

        except VersionNotFoundError:
            raise
        except Exception as e:
            logger.error(f"Promotion failed for version {version_id}: {e}")
            raise PromotionFailedError(f"Promotion operation failed: {str(e)}")

    async def compare_versions(
        self, from_version_id: UUID, to_version_id: UUID
    ) -> VersionComparisonResult:
        """
        Compare two configuration versions and generate diff.

        Args:
            from_version_id: Source version
            to_version_id: Target version

        Returns:
            VersionComparisonResult with changes between versions

        Example:
            >>> comparison = await service.compare_versions(
            ...     from_version_id=version1.id,
            ...     to_version_id=version2.id
            ... )
            >>> print(comparison.summary)
        """
        try:
            from_version = await self.get_version(from_version_id, verify_hash=False)
            to_version = await self.get_version(to_version_id, verify_hash=False)

            # Compare snapshots using to_version.changes as reference
            changes = to_version.changes

            timestamp_diff = to_version.created_at - from_version.created_at

            # Generate summary
            change_count = len(changes)
            summary = (
                f"Comparison from version {from_version.version_number} to "
                f"{to_version.version_number}: {change_count} changes, "
                f"time difference: {timestamp_diff.total_seconds()}s"
            )

            return VersionComparisonResult(
                from_version=from_version.version_number,
                to_version=to_version.version_number,
                changes=changes,
                timestamp_diff=timestamp_diff,
                summary=summary,
            )

        except Exception as e:
            logger.error(
                f"Comparison failed between {from_version_id} and "
                f"{to_version_id}: {e}"
            )
            raise ConfigVersionError(f"Version comparison failed: {str(e)}")

    async def get_version_stats(
        self, project_id: UUID, pipeline_id: Optional[UUID] = None, days: int = 30
    ) -> Dict[str, Any]:
        """
        Get statistics about version history.

        Args:
            project_id: Project to analyze
            pipeline_id: Optional specific pipeline
            days: Number of days to analyze

        Returns:
            Dictionary with version statistics

        Example:
            >>> stats = await service.get_version_stats(project_id)
            >>> print(f"Total versions: {stats['total_versions']}")
            >>> print(f"Most changed fields: {stats['most_changed_fields']}")
        """
        try:
            cutoff_date = datetime.now(datetime.timezone.utc) - timedelta(days=days)

            query = {"project_id": str(project_id), "created_at": {"$gte": cutoff_date}}
            if pipeline_id:
                query["pipeline_id"] = str(pipeline_id)

            total_versions = await self.collection.count_documents(query)

            # Count by promotion status
            stats_by_status = {}
            for status in PromotionStatus:
                count = await self.collection.count_documents(
                    {**query, "promotion_status": status.value}
                )
                stats_by_status[status.value] = count

            # Find most active contributors
            cursor = self.collection.aggregate(
                [
                    {"$match": query},
                    {"$group": {"_id": "$created_by", "count": {"$sum": 1}}},
                    {"$sort": {"count": -1}},
                    {"$limit": 10},
                ]
            )

            contributors = {}
            async for doc in cursor:
                contributors[doc["_id"]] = doc["count"]

            return {
                "project_id": str(project_id),
                "pipeline_id": str(pipeline_id) if pipeline_id else None,
                "total_versions": total_versions,
                "versions_by_status": stats_by_status,
                "top_contributors": contributors,
                "analysis_period_days": days,
                "analysis_start_date": cutoff_date.isoformat(),
                "analysis_end_date": datetime.now(datetime.timezone.utc).isoformat(),
            }

        except Exception as e:
            logger.error(f"Failed to get stats for project {project_id}: {e}")
            raise ConfigVersionError(f"Stats retrieval failed: {str(e)}")

    # ========================================================================
    # Helper Methods
    # ========================================================================

    async def _get_next_version_number(
        self, project_id: UUID, pipeline_id: Optional[UUID] = None
    ) -> int:
        """Get next version number for project/pipeline"""
        query = {"project_id": str(project_id)}
        if pipeline_id:
            query["pipeline_id"] = str(pipeline_id)

        last_version = await self.collection.find_one(
            query, sort=[("version_number", -1)]
        )

        if last_version:
            return last_version["version_number"] + 1

        return 1

    def _doc_to_response(self, doc: Dict[str, Any]) -> ConfigVersionResponse:
        """Convert MongoDB document to ConfigVersionResponse"""
        changes = [ConfigChange(**change) for change in doc.get("changes", [])]

        return ConfigVersionResponse(
            id=doc["id"],
            version_number=doc["version_number"],
            project_id=UUID(doc["project_id"]),
            pipeline_id=UUID(doc["pipeline_id"]) if doc.get("pipeline_id") else None,
            snapshot_hash=doc["snapshot_hash"],
            changes=changes,
            change_reason=doc["change_reason"],
            created_at=doc["created_at"],
            created_by=doc["created_by"],
            promoted_to=doc.get("promoted_to"),
            promoted_at=doc.get("promoted_at"),
            promoted_by=doc.get("promoted_by"),
            tags=doc.get("tags", {}),
            promotion_status=doc.get("promotion_status", PromotionStatus.PENDING.value),
        )
