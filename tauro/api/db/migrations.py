"""MongoDB Migrations and Index Management for Tauro API

This module handles MongoDB collection creation and index management using
a versioning system. Each migration is versioned and tracked in the _migrations
collection to ensure consistency and enable rollbacks.

Usage:
    runner = MigrationRunner(db)
    await runner.run_migrations()
"""

from abc import ABC, abstractmethod
from datetime import datetime, timezone
from motor.motor_asyncio import AsyncDatabase  # type: ignore
from loguru import logger
from typing import List, Optional


class Migration(ABC):
    """Base class for all migrations"""

    version: int
    description: str

    @abstractmethod
    async def up(self, db: AsyncDatabase) -> None:
        """Execute migration (upgrade)"""
        pass

    @abstractmethod
    async def down(self, db: AsyncDatabase) -> None:
        """Rollback migration (downgrade)"""
        pass


class Migration001Initial(Migration):
    """Initial migration: create collections and indexes"""

    version = 1
    description = "Create initial collections and indexes"

    async def up(self, db: AsyncDatabase) -> None:
        """Create initial schema"""
        logger.info("Running migration 001: Initial setup")

        # Create collections
        collections = [
            "projects",
            "pipeline_runs",
            "task_runs",
            "schedules",
            "config_versions",
        ]

        for collection_name in collections:
            try:
                await db.create_collection(collection_name)
                logger.info(f"✓ Created collection: {collection_name}")
            except Exception:
                logger.debug(f"Collection {collection_name} already exists")

        # Create indexes
        await self._create_indexes(db)
        logger.info("✓ Migration 001 completed")

    async def down(self, db: AsyncDatabase) -> None:
        """Rollback initial setup"""
        logger.info("Rolling back migration 001")

        collections = [
            "projects",
            "pipeline_runs",
            "task_runs",
            "schedules",
            "config_versions",
        ]

        for collection_name in collections:
            try:
                await db.drop_collection(collection_name)
                logger.info(f"✓ Dropped collection: {collection_name}")
            except Exception:
                pass

    async def _create_indexes(self, db: AsyncDatabase) -> None:
        """Create all required indexes"""

        # Projects indexes
        projects = db["projects"]
        await projects.create_index([("name", 1)], unique=True)
        await projects.create_index([("created_at", -1)])
        await projects.create_index([("status", 1)])
        await projects.create_index([("created_by", 1)])
        logger.debug("✓ Created indexes for 'projects'")

        # Pipeline runs indexes
        runs = db["pipeline_runs"]
        await runs.create_index([("project_id", 1), ("pipeline_id", 1)])
        await runs.create_index([("state", 1)])
        await runs.create_index([("created_at", -1)])
        await runs.create_index(
            [("created_at", 1)],
            expireAfterSeconds=7776000,  # 90 days TTL
        )
        logger.debug("✓ Created indexes for 'pipeline_runs'")

        # Task runs indexes
        tasks = db["task_runs"]
        await tasks.create_index([("pipeline_run_id", 1)])
        await tasks.create_index([("state", 1)])
        await tasks.create_index(
            [("created_at", 1)],
            expireAfterSeconds=7776000,  # 90 days TTL
        )
        logger.debug("✓ Created indexes for 'task_runs'")

        # Schedules indexes
        schedules = db["schedules"]
        await schedules.create_index([("project_id", 1)])
        await schedules.create_index([("pipeline_id", 1)])
        await schedules.create_index([("enabled", 1)])
        await schedules.create_index([("next_run_at", 1)])
        logger.debug("✓ Created indexes for 'schedules'")

        # Config versions indexes
        configs = db["config_versions"]
        await configs.create_index([("project_id", 1), ("pipeline_id", 1)])
        await configs.create_index([("version_number", 1)])
        await configs.create_index([("created_at", -1)])
        logger.debug("✓ Created indexes for 'config_versions'")


class MigrationRunner:
    """Executes database migrations with versioning"""

    def __init__(self, db: AsyncDatabase):
        self.db = db
        self.migrations_collection = db["_migrations"]

    async def get_current_version(self) -> int:
        """Get current migration version"""
        async for doc in (
            self.migrations_collection.find().sort("version", -1).limit(1)
        ):
            return doc["version"]
        return 0

    async def run_migrations(self) -> None:
        """Run pending migrations"""
        logger.info("🔄 Starting migration runner")

        current_version = await self.get_current_version()
        logger.info(f"Current schema version: {current_version}")

        all_migrations = self._get_all_migrations()
        pending = [m for m in all_migrations if m.version > current_version]

        if not pending:
            logger.info("✓ Database is up to date")
            return

        logger.info(f"Found {len(pending)} pending migration(s)")

        for migration in pending:
            try:
                logger.info(f"📦 Migration {migration.version}: {migration.description}")
                await migration.up(self.db)

                # Record migration
                await self.migrations_collection.insert_one(
                    {
                        "version": migration.version,
                        "description": migration.description,
                        "timestamp": datetime.now(timezone.utc),
                        "status": "success",
                    }
                )

                logger.info(f"✓ Migration {migration.version} completed")

            except Exception as e:
                logger.error(
                    f"✗ Migration {migration.version} failed: {e}", exc_info=True
                )

                # Record failure
                await self.migrations_collection.insert_one(
                    {
                        "version": migration.version,
                        "description": migration.description,
                        "timestamp": datetime.now(timezone.utc),
                        "status": "failed",
                        "error": str(e),
                    }
                )

                raise RuntimeError(
                    f"Migration {migration.version} failed: {e}. "
                    f"Database may be in an inconsistent state."
                )

    @staticmethod
    def _get_all_migrations() -> List[Migration]:
        """Get all migrations in order"""
        return [
            Migration001Initial(),
            # Add more migrations here as they're created
            # IMPORTANT: Never modify existing migrations, always add new ones
        ]


# Legacy class for backwards compatibility
class MigrationManager:
    """Manages MongoDB migrations and index creation (deprecated - use MigrationRunner)"""

    def __init__(self, db: AsyncDatabase):
        """
        Initialize migration manager

        Args:
            db: Motor AsyncDatabase instance
        """
        self.db = db

    async def create_indexes(self) -> None:
        """Create all required indexes"""
        logger.info("Creating MongoDB indexes...")

        try:
            await self._create_projects_indexes()
            await self._create_pipeline_runs_indexes()
            await self._create_task_runs_indexes()
            await self._create_schedules_indexes()
            await self._create_config_versions_indexes()

            logger.info("All indexes created successfully")
        except Exception as e:
            logger.error(f"Error creating indexes: {e}")
            raise

    async def _create_projects_indexes(self) -> None:
        """Create indexes for projects collection"""
        logger.debug("Creating indexes for 'projects' collection")

        collection = self.db["projects"]

        # Unique index on name
        await collection.create_index("name", unique=True, name="idx_name_unique")

        # Index on created_at for sorting
        await collection.create_index("created_at", name="idx_created_at")

        # Index on status for filtering
        await collection.create_index("status", name="idx_status")

        # Compound index for listing
        await collection.create_index(
            [("status", 1), ("created_at", -1)], name="idx_status_created"
        )

        logger.debug("Projects indexes created")

    async def _create_pipeline_runs_indexes(self) -> None:
        """Create indexes for pipeline_runs collection"""
        logger.debug("Creating indexes for 'pipeline_runs' collection")

        collection = self.db["pipeline_runs"]

        # Compound index on project_id and pipeline_id
        await collection.create_index(
            [("project_id", 1), ("pipeline_id", 1)],
            name="idx_project_pipeline",
        )

        # Index on state
        await collection.create_index("state", name="idx_state")

        # Index on created_at for sorting (descending)
        await collection.create_index([("created_at", -1)], name="idx_created_at_desc")

        # TTL index: expire documents after 90 days
        await collection.create_index(
            "created_at",
            expireAfterSeconds=7776000,  # 90 days
            name="idx_ttl_90days",
        )

        # Compound index for queries
        await collection.create_index(
            [("project_id", 1), ("state", 1), ("created_at", -1)],
            name="idx_project_state_created",
        )

        logger.debug("Pipeline runs indexes created")

    async def _create_task_runs_indexes(self) -> None:
        """Create indexes for task_runs collection"""
        logger.debug("Creating indexes for 'task_runs' collection")

        collection = self.db["task_runs"]

        # Index on pipeline_run_id
        await collection.create_index("pipeline_run_id", name="idx_pipeline_run_id")

        # Index on state
        await collection.create_index("state", name="idx_state")

        # TTL index: expire after 90 days
        await collection.create_index(
            "created_at",
            expireAfterSeconds=7776000,  # 90 days
            name="idx_ttl_90days",
        )

        # Compound index for queries
        await collection.create_index(
            [("pipeline_run_id", 1), ("state", 1)],
            name="idx_pipeline_run_state",
        )

        logger.debug("Task runs indexes created")

    async def _create_schedules_indexes(self) -> None:
        """Create indexes for schedules collection"""
        logger.debug("Creating indexes for 'schedules' collection")

        collection = self.db["schedules"]

        # Index on project_id
        await collection.create_index("project_id", name="idx_project_id")

        # Index on pipeline_id
        await collection.create_index("pipeline_id", name="idx_pipeline_id")

        # Index on enabled status
        await collection.create_index("enabled", name="idx_enabled")

        # Index on next_run_at for scheduler queries
        await collection.create_index("next_run_at", name="idx_next_run_at")

        # Compound index for scheduler
        await collection.create_index(
            [("enabled", 1), ("next_run_at", 1)],
            name="idx_enabled_next_run",
        )

        logger.debug("Schedules indexes created")

    async def _create_config_versions_indexes(self) -> None:
        """Create indexes for config_versions collection"""
        logger.debug("Creating indexes for 'config_versions' collection")

        collection = self.db["config_versions"]

        # Compound index on project_id and pipeline_id
        await collection.create_index(
            [("project_id", 1), ("pipeline_id", 1)],
            name="idx_project_pipeline",
        )

        # Index on version_number
        await collection.create_index(
            [("project_id", 1), ("version_number", -1)],
            name="idx_project_version",
        )

        # Index on created_at
        await collection.create_index([("created_at", -1)], name="idx_created_at_desc")

        logger.debug("Config versions indexes created")

    async def seed_initial_data(self) -> None:
        """Seed initial data if collections are empty"""
        logger.info("Checking if seed data is needed...")

        try:
            # Check if projects collection has any data
            projects_count = await self.db["projects"].count_documents({})

            if projects_count == 0:
                logger.info("Seeding initial data...")

                # Example project (optional)
                # Uncomment if you want to seed a sample project
                # sample_project = {
                #     "id": str(uuid4()),
                #     "name": "sample_project",
                #     "description": "Sample project for testing",
                #     "status": "active",
                #     "created_at": datetime.utcnow(),
                #     "updated_at": datetime.utcnow(),
                #     "created_by": "system",
                #     "global_settings": {},
                #     "pipelines": [],
                #     "tags": {"source": "seed"}
                # }
                # await self.db["projects"].insert_one(sample_project)
                # logger.info("Sample project created")

                logger.info("No seed data needed at this time")
            else:
                logger.info(f"Database already has {projects_count} project(s)")

        except Exception as e:
            logger.error(f"Error seeding initial data: {e}")
            raise

    async def verify_collections(self) -> List[str]:
        """
        Verify that all required collections exist

        Returns:
            List of collection names
        """
        logger.info("Verifying required collections...")

        required_collections = [
            "projects",
            "pipeline_runs",
            "task_runs",
            "schedules",
            "config_versions",
        ]

        existing_collections = await self.db.list_collection_names()

        missing_collections = [
            c for c in required_collections if c not in existing_collections
        ]

        if missing_collections:
            logger.warning(
                f"Missing collections: {missing_collections}. "
                f"They will be created when first document is inserted."
            )
        else:
            logger.info("All required collections exist")

        return existing_collections

    async def run_all_migrations(self) -> None:
        """Run all migrations"""
        logger.info("Running all migrations...")

        try:
            await self.verify_collections()
            await self.create_indexes()
            await self.seed_initial_data()

            logger.info("All migrations completed successfully")
        except Exception as e:
            logger.error(f"Migration failed: {e}")
            raise


async def init_database(db: AsyncDatabase) -> None:
    """
    Initialize database with all migrations

    Args:
        db: Motor AsyncDatabase instance
    """
    manager = MigrationManager(db)
    await manager.run_all_migrations()
