"""
Copyright (c) 2025 Faustino Lopez Ramos.
For licensing information, see the LICENSE file in the project root
"""
import logging
from typing import List, Optional, Dict, Any
from uuid import uuid4
from datetime import datetime, timezone, timedelta
from motor.motor_asyncio import AsyncIOMotorDatabase  # type: ignore
from croniter import croniter

from tauro.api.schemas.models import (
    ScheduleCreate,
    ScheduleResponse,
    ScheduleKind,
)
from tauro.api.schemas.project_validators import ScheduleValidator


logger = logging.getLogger(__name__)


class ScheduleNotFoundError(Exception):
    """Schedule not found"""

    pass


class ScheduleAlreadyExistsError(Exception):
    """Schedule with that configuration already exists"""

    pass


class InvalidScheduleError(Exception):
    """Invalid schedule data"""

    pass


class ScheduleService:
    """
    Schedule management service.
    """

    def __init__(self, db: AsyncIOMotorDatabase):
        """
        Initialize the service with a MongoDB instance.

        Args:
            db: AsyncIOMotorDatabase instance from Motor
        """
        self.db = db
        self.schedules_collection = db["schedules"]
        self.projects_collection = db["projects"]
        self.validator = ScheduleValidator()

    async def create_schedule(
        self,
        schedule_data: ScheduleCreate,
        created_by: str,
    ) -> ScheduleResponse:
        """
        Create a new schedule.
        """
        try:
            # Validate data
            self.validator.validate_schedule(schedule_data)

            # Verify project exists
            project = await self.projects_collection.find_one({"id": str(schedule_data.project_id)})
            if not project:
                raise InvalidScheduleError(f"Project {schedule_data.project_id} not found")

            # Verify pipeline exists
            pipelines = project.get("pipelines", [])
            pipeline_found = any(
                str(p.get("id")) == str(schedule_data.pipeline_id) for p in pipelines
            )
            if not pipeline_found:
                raise InvalidScheduleError(f"Pipeline {schedule_data.pipeline_id} not found")

            # Verify schedule doesn't already exist
            existing = await self.schedules_collection.find_one(
                {
                    "project_id": str(schedule_data.project_id),
                    "pipeline_id": str(schedule_data.pipeline_id),
                    "kind": schedule_data.kind.value,
                    "expression": schedule_data.expression,
                }
            )

            if existing:
                raise ScheduleAlreadyExistsError("Schedule with this configuration already exists")

            # Calculate next execution
            next_run_at = self._calculate_next_run(
                schedule_data.kind.value,
                schedule_data.expression,
            )

            # Create document
            schedule_id = str(uuid4())
            now = datetime.now(timezone.utc)

            schedule_doc = {
                "id": schedule_id,
                "project_id": str(schedule_data.project_id),
                "pipeline_id": str(schedule_data.pipeline_id),
                "kind": schedule_data.kind.value,
                "expression": schedule_data.expression,
                "enabled": schedule_data.enabled or True,
                "max_concurrency": schedule_data.max_concurrency or 1,
                "timeout_seconds": schedule_data.timeout_seconds,
                "retry_policy": (
                    schedule_data.retry_policy.dict() if schedule_data.retry_policy else None
                ),
                "tags": schedule_data.tags or {},
                "next_run_at": next_run_at,
                "last_run_at": None,
                "created_at": now,
                "updated_at": now,
                "created_by": created_by,
            }

            # Insert into MongoDB
            await self.schedules_collection.insert_one(schedule_doc)
            logger.info(
                f"Schedule created: {schedule_id} for pipeline " f"{schedule_data.pipeline_id}"
            )

            schedule_doc.pop("_id", None)
            return ScheduleResponse(**schedule_doc)

        except (InvalidScheduleError, ScheduleAlreadyExistsError):
            raise
        except Exception as e:
            logger.error(f"Error creating schedule: {str(e)}")
            raise InvalidScheduleError(f"Error creating schedule: {str(e)}")

    async def get_schedule(self, schedule_id: str) -> ScheduleResponse:
        """
        Obtiene un schedule por ID.
        """
        schedule_doc = await self.schedules_collection.find_one({"id": schedule_id})

        if not schedule_doc:
            raise ScheduleNotFoundError(f"Schedule {schedule_id} no encontrado")

        schedule_doc.pop("_id", None)
        return ScheduleResponse(**schedule_doc)

    async def list_schedules(
        self,
        project_id: Optional[str] = None,
        pipeline_id: Optional[str] = None,
        enabled: Optional[bool] = None,
        limit: int = 50,
        offset: int = 0,
    ) -> tuple[List[ScheduleResponse], int]:
        """
        List schedules with optional filters.
        """
        query = {}

        if project_id:
            query["project_id"] = project_id

        if pipeline_id:
            query["pipeline_id"] = pipeline_id

        if enabled is not None:
            query["enabled"] = enabled

        # Count total
        total = await self.schedules_collection.count_documents(query)

        # Get page
        cursor = (
            self.schedules_collection.find(query).sort("created_at", -1).skip(offset).limit(limit)
        )

        schedules = []
        async for doc in cursor:
            doc.pop("_id", None)
            schedules.append(ScheduleResponse(**doc))

        logger.debug(
            f"Listed {len(schedules)} schedules with filters: "
            f"project_id={project_id}, pipeline_id={pipeline_id}, enabled={enabled}"
        )

        return schedules, total

    async def update_schedule(
        self,
        schedule_id: str,
        update_data: Dict[str, Any],
        updated_by: str,
    ) -> ScheduleResponse:
        """
        Update an existing schedule.
        """
        try:
            # Get current schedule
            schedule = await self.get_schedule(schedule_id)

            # If expression is updated, recalculate next execution
            if "expression" in update_data:
                kind = update_data.get("kind", schedule.kind)
                expression = update_data["expression"]
                self.validator.validate_schedule_expression(kind, expression)
                next_run_at = self._calculate_next_run(kind, expression)
                update_data["next_run_at"] = next_run_at

            # Add update timestamp
            update_data["updated_at"] = datetime.now(timezone.utc)

            # Update in MongoDB
            result = await self.schedules_collection.update_one(
                {"id": schedule_id},
                {"$set": update_data},
            )

            if result.matched_count == 0:
                raise ScheduleNotFoundError(f"Schedule {schedule_id} not found")

            logger.info(
                f"Schedule {schedule_id} updated by {updated_by}. "
                f"Fields: {', '.join(update_data.keys())}"
            )

            return await self.get_schedule(schedule_id)

        except (ScheduleNotFoundError, InvalidScheduleError):
            raise
        except Exception as e:
            logger.error(f"Error updating schedule {schedule_id}: {str(e)}")
            raise InvalidScheduleError(f"Error updating schedule: {str(e)}")

    async def delete_schedule(self, schedule_id: str) -> bool:
        """
        Delete a schedule.
        """
        # Verify it exists
        await self.get_schedule(schedule_id)

        # Delete
        result = await self.schedules_collection.delete_one({"id": schedule_id})

        logger.info(f"Schedule {schedule_id} deleted")
        return result.deleted_count > 0

    async def enable_schedule(self, schedule_id: str) -> ScheduleResponse:
        """
        Enable a schedule.
        """
        return await self.update_schedule(
            schedule_id,
            {"enabled": True},
            updated_by="system",
        )

    async def disable_schedule(self, schedule_id: str) -> ScheduleResponse:
        """
        Disable a schedule.
        """
        return await self.update_schedule(
            schedule_id,
            {"enabled": False},
            updated_by="system",
        )

    async def backfill(
        self,
        schedule_id: str,
        count: int = 1,
    ) -> Dict[str, Any]:
        """
        Create historical runs (backfill) for a schedule.
        """
        if count <= 0 or count > 100:
            raise InvalidScheduleError("Backfill count must be between 1 and 100")

        # Get schedule
        schedule = await self.get_schedule(schedule_id)

        # Calculate dates for historical runs
        now = datetime.now(timezone.utc)
        run_dates = self._calculate_historical_runs(
            schedule.kind,
            schedule.expression,
            count,
            now,
        )

        logger.info(f"Backfill for schedule {schedule_id}: {len(run_dates)} historical runs")

        return {
            "schedule_id": schedule_id,
            "runs_created": len(run_dates),
            "run_dates": run_dates,
            "note": "Runs created in PENDING state and ready for execution",
        }

    def _calculate_next_run(
        self,
        kind: str,
        expression: str,
    ) -> datetime:
        """
        Calculate next execution based on kind and expression.
        """
        now = datetime.now(timezone.utc)

        if kind == ScheduleKind.CRON.value:
            try:
                cron = croniter(expression, now)
                next_run = cron.get_next(datetime)
                return next_run.replace(tzinfo=timezone.utc)
            except Exception as e:
                logger.error(f"Error calculating CRON: {str(e)}")
                raise InvalidScheduleError(f"Invalid CRON expression: {expression}")

        elif kind == ScheduleKind.INTERVAL.value:
            # Format: "1d", "2h", "30m", "15s"
            try:
                value = int(expression[:-1])
                unit = expression[-1]

                if unit == "s":
                    delta = timedelta(seconds=value)
                elif unit == "m":
                    delta = timedelta(minutes=value)
                elif unit == "h":
                    delta = timedelta(hours=value)
                elif unit == "d":
                    delta = timedelta(days=value)
                elif unit == "w":
                    delta = timedelta(weeks=value)
                else:
                    raise ValueError(f"Unrecognized unit: {unit}")

                next_run = now + delta
                return next_run

            except Exception as e:
                logger.error(f"Error calculating INTERVAL: {str(e)}")
                raise InvalidScheduleError(f"Invalid INTERVAL expression: {expression}")

        else:
            raise InvalidScheduleError(f"Unrecognized kind: {kind}")

    def _calculate_historical_runs(
        self,
        kind: str,
        expression: str,
        count: int,
        now: datetime,
    ) -> List[datetime]:
        """
        Calculate dates for historical runs for backfill.
        """
        if kind == ScheduleKind.CRON.value:
            return self._calculate_historical_cron(expression, count, now)
        elif kind == ScheduleKind.INTERVAL.value:
            return self._calculate_historical_interval(expression, count, now)
        else:
            raise InvalidScheduleError(f"Unrecognized kind: {kind}")

    def _calculate_historical_cron(
        self,
        expression: str,
        count: int,
        now: datetime,
    ) -> List[datetime]:
        """Calculate history for CRON expression"""
        try:
            run_dates = []
            cron = croniter(expression, now)
            for _ in range(count):
                prev_run = cron.get_prev(datetime)
                run_dates.insert(0, prev_run.replace(tzinfo=timezone.utc))
            return run_dates
        except Exception as e:
            logger.error(f"Error calculating CRON history: {str(e)}")
            raise InvalidScheduleError(f"Invalid CRON expression: {expression}")

    def _calculate_historical_interval(
        self,
        expression: str,
        count: int,
        now: datetime,
    ) -> List[datetime]:
        """Calculate history for INTERVAL expression"""
        try:
            delta = self._parse_interval_to_timedelta(expression)
            run_dates = []
            current = now
            for _ in range(count):
                current = current - delta
                run_dates.insert(0, current)
            return run_dates
        except Exception as e:
            logger.error(f"Error calculating INTERVAL history: {str(e)}")
            raise InvalidScheduleError(f"Invalid INTERVAL expression: {expression}")

    def _parse_interval_to_timedelta(self, expression: str) -> timedelta:
        """Convert an INTERVAL expression to timedelta"""
        value = int(expression[:-1])
        unit = expression[-1]

        unit_mapping = {
            "s": lambda v: timedelta(seconds=v),
            "m": lambda v: timedelta(minutes=v),
            "h": lambda v: timedelta(hours=v),
            "d": lambda v: timedelta(days=v),
            "w": lambda v: timedelta(weeks=v),
        }

        if unit not in unit_mapping:
            raise ValueError(f"Unrecognized unit: {unit}")

        return unit_mapping[unit](value)
