"""
Copyright (c) 2025 Faustino Lopez Ramos.
For licensing information, see the LICENSE file in the project root
"""
from loguru import logger
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from uuid import UUID, uuid4

# Local
from tauro.api.db.models import ProjectDocument
from tauro.api.schemas.models import (
    PipelineConfig,
    ProjectCreate,
    ProjectResponse,
)
from tauro.api.schemas.project_validators import ProjectValidator
from tauro.api.schemas.serializers import ProjectSerializer


class ProjectNotFoundError(Exception):
    """Project not found in database"""

    pass


class ProjectAlreadyExistsError(Exception):
    """Project with that name already exists"""

    pass


class InvalidProjectError(Exception):
    """Invalid project data"""

    pass


class ProjectService:
    """
    Project management service.
    """

    def __init__(self, db: Any):
        """
        Initialize the service with a MongoDB instance.

        Args:
            db: AsyncDatabase instance from Motor (Motor 3.x+)
        """
        self.db = db
        self.projects_collection = db["projects"]
        self.pipeline_runs_collection = db["pipeline_runs"]
        self.schedules_collection = db["schedules"]
        self.validator = ProjectValidator()
        self.serializer = ProjectSerializer()

    async def create_project(
        self,
        project_data: ProjectCreate,
        created_by: str,
    ) -> ProjectResponse:
        """
        Create a new project.
        """
        try:
            # Validate input data
            self.validator.validate_project_creation(project_data)

            # Verify that the name is unique
            existing = await self.projects_collection.find_one({"name": project_data.name})
            if existing:
                raise ProjectAlreadyExistsError(
                    f"Project with name '{project_data.name}' already exists"
                )

            # Serialize to MongoDB document
            project_id = uuid4()
            project_doc: ProjectDocument = {
                "id": str(project_id),
                "name": project_data.name,
                "description": project_data.description,
                "global_settings": project_data.global_settings.dict(),
                "pipelines": [p.dict() for p in (project_data.pipelines or [])],
                "created_at": datetime.now(timezone.utc),
                "updated_at": datetime.now(timezone.utc),
                "created_by": created_by,
                "status": "active",
                "tags": project_data.tags or {},
            }

            # Insert into MongoDB
            await self.projects_collection.insert_one(project_doc)
            logger.info(f"Project created: {project_id} by {created_by}")

            # Return as response
            return ProjectResponse(**project_doc)

        except ProjectAlreadyExistsError:
            raise
        except Exception as e:
            logger.error(f"Error creating project: {str(e)}")
            raise InvalidProjectError(f"Error creating project: {str(e)}")

    async def read_project(self, project_id: str) -> ProjectResponse:
        """
        Read a project by ID.
        """
        project_doc = await self.projects_collection.find_one({"id": project_id})

        if not project_doc:
            raise ProjectNotFoundError(f"Project {project_id} not found")

        # Remove MongoDB _id before converting to Pydantic
        project_doc.pop("_id", None)
        return ProjectResponse(**project_doc)

    async def update_project(
        self,
        project_id: str,
        update_data: Dict[str, Any],
        updated_by: str,
    ) -> ProjectResponse:
        """
        Update an existing project.
        """
        try:
            # Verify that the project exists
            project_doc = await self.projects_collection.find_one({"id": project_id})
            if not project_doc:
                raise ProjectNotFoundError(f"Project {project_id} not found")

            # Validate name changes (must be unique)
            if "name" in update_data and update_data["name"] != project_doc["name"]:
                existing = await self.projects_collection.find_one({"name": update_data["name"]})
                if existing:
                    raise InvalidProjectError(
                        f"Project with name '{update_data['name']}' already exists"
                    )

            # Prepare update
            update_dict = {
                **update_data,
                "updated_at": datetime.now(timezone.utc),
            }

            # Update in MongoDB
            await self.projects_collection.update_one(
                {"id": project_id},
                {"$set": update_dict},
            )

            logger.info(
                f"Project {project_id} updated by {updated_by}. "
                f"Fields: {', '.join(update_dict.keys())}"
            )

            # Return updated project
            return await self.read_project(project_id)

        except (ProjectNotFoundError, InvalidProjectError):
            raise
        except Exception as e:
            logger.error(f"Error updating project {project_id}: {str(e)}")
            raise InvalidProjectError(f"Error updating project: {str(e)}")

    async def delete_project(self, project_id: str) -> bool:
        """
        Delete a project (soft delete: mark as deleted).
        """
        # Verify it exists
        project_doc = await self.projects_collection.find_one({"id": project_id})
        if not project_doc:
            raise ProjectNotFoundError(f"Project {project_id} not found")

        # Soft delete: mark as archived
        result = await self.projects_collection.update_one(
            {"id": project_id},
            {
                "$set": {
                    "status": "archived",
                    "updated_at": datetime.now(timezone.utc),
                }
            },
        )

        logger.info(f"Project {project_id} deleted (archived)")
        return result.modified_count > 0

    async def list_projects(
        self,
        status: Optional[str] = None,
        tags: Optional[Dict[str, str]] = None,
        limit: int = 50,
        offset: int = 0,
        created_by: Optional[str] = None,
    ) -> tuple[List[ProjectResponse], int]:
        """
        List projects with optional filters.
        """
        # Build filter
        query = {}

        if status:
            query["status"] = status

        if created_by:
            query["created_by"] = created_by

        if tags:
            for key, value in tags.items():
                query[f"tags.{key}"] = value

        # Count total
        total = await self.projects_collection.count_documents(query)

        # Get page
        cursor = (
            self.projects_collection.find(query).sort("created_at", -1).skip(offset).limit(limit)
        )

        projects = []
        async for doc in cursor:
            doc.pop("_id", None)
            projects.append(ProjectResponse(**doc))

        logger.debug(
            f"Listed {len(projects)} projects with filters: "
            f"status={status}, tags={tags}, offset={offset}"
        )

        return projects, total

    async def get_project_stats(self, project_id: str) -> Dict[str, Any]:
        """
        Get project statistics.
        """
        # Verify it exists
        project = await self.read_project(project_id)

        # Count runs
        total_runs = await self.pipeline_runs_collection.count_documents({"project_id": project_id})

        # Runs by state
        run_states = await self.pipeline_runs_collection.aggregate(
            [
                {"$match": {"project_id": project_id}},
                {"$group": {"_id": "$state", "count": {"$sum": 1}}},
            ]
        ).to_list(None)

        runs_by_state = {item["_id"]: item["count"] for item in run_states}

        # Count active schedules
        active_schedules = await self.schedules_collection.count_documents(
            {"project_id": project_id, "enabled": True}
        )

        # Pipelines
        num_pipelines = len(project.pipelines or [])

        logger.info(f"Stats retrieved for project {project_id}")

        return {
            "project_id": project_id,
            "name": project.name,
            "total_runs": total_runs,
            "runs_by_state": runs_by_state,
            "num_pipelines": num_pipelines,
            "active_schedules": active_schedules,
            "created_at": project.created_at,
            "updated_at": project.updated_at,
        }

    async def add_pipeline(
        self,
        project_id: str,
        pipeline_config: PipelineConfig,
    ) -> ProjectResponse:
        """
        Add a new pipeline to a project.
        """
        # Verify it exists
        project = await self.read_project(project_id)

        # Validate configuration
        try:
            from tauro.api.schemas.project_validators import PipelineValidator

            validator = PipelineValidator()
            validator.validate_pipeline(pipeline_config)
        except Exception as e:
            raise InvalidProjectError(f"Invalid pipeline: {str(e)}")

        # Verify that no pipeline exists with the same name
        existing_pipelines = project.pipelines or []
        if any(p.name == pipeline_config.name for p in existing_pipelines):
            raise InvalidProjectError(f"Pipeline with name '{pipeline_config.name}' already exists")

        # Add pipeline
        pipelines_updated = [p.dict() for p in existing_pipelines]
        pipelines_updated.append(pipeline_config.dict())

        # Update
        return await self.update_project(
            project_id,
            {"pipelines": pipelines_updated},
            updated_by="system",
        )

    async def remove_pipeline(
        self,
        project_id: str,
        pipeline_id: str,
    ) -> ProjectResponse:
        """
        Remove a pipeline from a project.
        """
        # Verify it exists
        project = await self.read_project(project_id)

        # Find and remove pipeline
        pipelines = project.pipelines or []
        pipeline_found = False

        for i, p in enumerate(pipelines):
            if str(p.id) == pipeline_id:
                pipelines.pop(i)
                pipeline_found = True
                break

        if not pipeline_found:
            raise InvalidProjectError(f"Pipeline {pipeline_id} not found in project")

        # Update
        return await self.update_project(
            project_id,
            {"pipelines": [p.dict() for p in pipelines]},
            updated_by="system",
        )

    async def duplicate_project(
        self,
        project_id: str,
        new_name: str,
        created_by: str,
    ) -> ProjectResponse:
        """
        Duplicate an existing project.
        """
        # Get original project
        original = await self.read_project(project_id)

        # Create data for new project
        project_data = ProjectCreate(
            name=new_name,
            description=f"Copy of {original.name}",
            global_settings=original.global_settings,
            pipelines=original.pipelines,
            tags=original.tags,
        )

        # Create new project
        return await self.create_project(project_data, created_by)
