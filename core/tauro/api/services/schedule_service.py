"""
ScheduleService: Gestión de schedules (ejecuciones programadas)

Este servicio implementa la lógica de negocio para crear, actualizar y gestionar
schedules que disparan ejecuciones periódicas de pipelines.
"""

import logging
from typing import List, Optional, Dict, Any
from uuid import uuid4
from datetime import datetime, timezone, timedelta
from motor.motor_asyncio import AsyncDatabase  # type: ignore
from croniter import croniter

from tauro.api.schemas.models import (
    ScheduleCreate,
    ScheduleResponse,
    ScheduleKind,
)
from tauro.api.schemas.project_validators import ScheduleValidator


logger = logging.getLogger(__name__)


class ScheduleNotFoundError(Exception):
    """Schedule no encontrado"""

    pass


class ScheduleAlreadyExistsError(Exception):
    """Schedule con esa configuración ya existe"""

    pass


class InvalidScheduleError(Exception):
    """Datos de schedule inválidos"""

    pass


class ScheduleService:
    """
    Servicio de gestión de schedules.

    Responsabilidades:
    - CRUD de schedules (Create, Read, Update, Delete)
    - Validación de expresiones CRON e INTERVAL
    - Cálculo de próximas ejecuciones
    - Enable/disable de schedules
    - Backfill (crear runs históricos)
    """

    def __init__(self, db: AsyncDatabase):
        """
        Inicializa el servicio con una instancia de MongoDB.

        Args:
            db: AsyncDatabase instance de Motor
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
        Crea un nuevo schedule.

        Args:
            schedule_data: Datos del schedule a crear
            created_by: Usuario que crea el schedule (email o ID)

        Returns:
            ScheduleResponse con el schedule creado

        Raises:
            InvalidScheduleError: Si los datos no son válidos
            ScheduleAlreadyExistsError: Si ya existe un schedule similar
        """
        try:
            # Validar datos
            self.validator.validate_schedule(schedule_data)

            # Verificar que el proyecto existe
            project = await self.projects_collection.find_one(
                {"id": str(schedule_data.project_id)}
            )
            if not project:
                raise InvalidScheduleError(
                    f"Proyecto {schedule_data.project_id} no encontrado"
                )

            # Verificar que el pipeline existe
            pipelines = project.get("pipelines", [])
            pipeline_found = any(
                str(p.get("id")) == str(schedule_data.pipeline_id) for p in pipelines
            )
            if not pipeline_found:
                raise InvalidScheduleError(
                    f"Pipeline {schedule_data.pipeline_id} no encontrado"
                )

            # Verificar que no existe un schedule similar
            existing = await self.schedules_collection.find_one(
                {
                    "project_id": str(schedule_data.project_id),
                    "pipeline_id": str(schedule_data.pipeline_id),
                    "kind": schedule_data.kind.value,
                    "expression": schedule_data.expression,
                }
            )

            if existing:
                raise ScheduleAlreadyExistsError(
                    "Ya existe un schedule con esta configuración"
                )

            # Calcular próxima ejecución
            next_run_at = self._calculate_next_run(
                schedule_data.kind.value,
                schedule_data.expression,
            )

            # Crear documento
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
                    schedule_data.retry_policy.dict()
                    if schedule_data.retry_policy
                    else None
                ),
                "tags": schedule_data.tags or {},
                "next_run_at": next_run_at,
                "last_run_at": None,
                "created_at": now,
                "updated_at": now,
                "created_by": created_by,
            }

            # Insertar en MongoDB
            await self.schedules_collection.insert_one(schedule_doc)
            logger.info(
                f"Schedule creado: {schedule_id} para pipeline "
                f"{schedule_data.pipeline_id}"
            )

            schedule_doc.pop("_id", None)
            return ScheduleResponse(**schedule_doc)

        except (InvalidScheduleError, ScheduleAlreadyExistsError):
            raise
        except Exception as e:
            logger.error(f"Error al crear schedule: {str(e)}")
            raise InvalidScheduleError(f"Error al crear schedule: {str(e)}")

    async def get_schedule(self, schedule_id: str) -> ScheduleResponse:
        """
        Obtiene un schedule por ID.

        Args:
            schedule_id: ID del schedule

        Returns:
            ScheduleResponse con los datos del schedule

        Raises:
            ScheduleNotFoundError: Si el schedule no existe
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
        Lista schedules con filtros opcionales.

        Args:
            project_id: Filtrar por proyecto
            pipeline_id: Filtrar por pipeline
            enabled: Filtrar por estado (habilitado/deshabilitado)
            limit: Número máximo de resultados
            offset: Número de resultados a saltar

        Returns:
            Tupla de (lista de schedules, total de registros)
        """
        # Construir filtro
        query = {}

        if project_id:
            query["project_id"] = project_id

        if pipeline_id:
            query["pipeline_id"] = pipeline_id

        if enabled is not None:
            query["enabled"] = enabled

        # Contar total
        total = await self.schedules_collection.count_documents(query)

        # Obtener página
        cursor = (
            self.schedules_collection.find(query)
            .sort("created_at", -1)
            .skip(offset)
            .limit(limit)
        )

        schedules = []
        async for doc in cursor:
            doc.pop("_id", None)
            schedules.append(ScheduleResponse(**doc))

        logger.debug(
            f"Listados {len(schedules)} schedules con filtros: "
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
        Actualiza un schedule existente.

        Args:
            schedule_id: ID del schedule
            update_data: Diccionario con campos a actualizar
            updated_by: Usuario que actualiza

        Returns:
            ScheduleResponse actualizado

        Raises:
            ScheduleNotFoundError: Si el schedule no existe
            InvalidScheduleError: Si los datos no son válidos
        """
        try:
            # Obtener schedule actual
            schedule = await self.get_schedule(schedule_id)

            # Si se actualiza la expresión, recalcular próxima ejecución
            if "expression" in update_data:
                kind = update_data.get("kind", schedule.kind)
                expression = update_data["expression"]
                self.validator.validate_schedule_expression(kind, expression)
                next_run_at = self._calculate_next_run(kind, expression)
                update_data["next_run_at"] = next_run_at

            # Agregar timestamp de actualización
            update_data["updated_at"] = datetime.now(timezone.utc)

            # Actualizar en MongoDB
            result = await self.schedules_collection.update_one(
                {"id": schedule_id},
                {"$set": update_data},
            )

            if result.matched_count == 0:
                raise ScheduleNotFoundError(f"Schedule {schedule_id} no encontrado")

            logger.info(
                f"Schedule {schedule_id} actualizado por {updated_by}. "
                f"Campos: {', '.join(update_data.keys())}"
            )

            return await self.get_schedule(schedule_id)

        except (ScheduleNotFoundError, InvalidScheduleError):
            raise
        except Exception as e:
            logger.error(f"Error al actualizar schedule {schedule_id}: {str(e)}")
            raise InvalidScheduleError(f"Error al actualizar schedule: {str(e)}")

    async def delete_schedule(self, schedule_id: str) -> bool:
        """
        Elimina un schedule.

        Args:
            schedule_id: ID del schedule a eliminar

        Returns:
            True si se eliminó exitosamente

        Raises:
            ScheduleNotFoundError: Si el schedule no existe
        """
        # Verificar que existe
        await self.get_schedule(schedule_id)

        # Eliminar
        result = await self.schedules_collection.delete_one({"id": schedule_id})

        logger.info(f"Schedule {schedule_id} eliminado")
        return result.deleted_count > 0

    async def enable_schedule(self, schedule_id: str) -> ScheduleResponse:
        """
        Habilita un schedule.

        Args:
            schedule_id: ID del schedule

        Returns:
            ScheduleResponse actualizado

        Raises:
            ScheduleNotFoundError: Si el schedule no existe
        """
        return await self.update_schedule(
            schedule_id,
            {"enabled": True},
            updated_by="system",
        )

    async def disable_schedule(self, schedule_id: str) -> ScheduleResponse:
        """
        Deshabilita un schedule.

        Args:
            schedule_id: ID del schedule

        Returns:
            ScheduleResponse actualizado

        Raises:
            ScheduleNotFoundError: Si el schedule no existe
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
        Crea runs históricos (backfill) para un schedule.

        Este método simula ejecuciones pasadas del schedule.
        Útil para llenar histórico o recuperar de fallos.

        Args:
            schedule_id: ID del schedule
            count: Número de runs a crear hacia el pasado

        Returns:
            Diccionario con información de backfill (runs_created, start_date, end_date)

        Raises:
            ScheduleNotFoundError: Si el schedule no existe
            InvalidScheduleError: Si count es inválido
        """
        if count <= 0 or count > 100:
            raise InvalidScheduleError("El conteo de backfill debe estar entre 1 y 100")

        # Obtener schedule
        schedule = await self.get_schedule(schedule_id)

        # Calcular fechas para runs históricos
        now = datetime.now(timezone.utc)
        run_dates = self._calculate_historical_runs(
            schedule.kind,
            schedule.expression,
            count,
            now,
        )

        logger.info(
            f"Backfill para schedule {schedule_id}: "
            f"{len(run_dates)} runs históricos"
        )

        return {
            "schedule_id": schedule_id,
            "runs_created": len(run_dates),
            "run_dates": run_dates,
            "note": "Runs creados en estado PENDING y listos para ejecución",
        }

    def _calculate_next_run(
        self,
        kind: str,
        expression: str,
    ) -> datetime:
        """
        Calcula la próxima ejecución según el kind y expression.

        Args:
            kind: "CRON" o "INTERVAL"
            expression: Expresión CRON o INTERVAL

        Returns:
            Próximo datetime de ejecución
        """
        now = datetime.now(timezone.utc)

        if kind == ScheduleKind.CRON.value:
            try:
                cron = croniter(expression, now)
                next_run = cron.get_next(datetime)
                return next_run.replace(tzinfo=timezone.utc)
            except Exception as e:
                logger.error(f"Error al calcular CRON: {str(e)}")
                raise InvalidScheduleError(f"Expresión CRON inválida: {expression}")

        elif kind == ScheduleKind.INTERVAL.value:
            # Formato: "1d", "2h", "30m", "15s"
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
                    raise ValueError(f"Unidad no reconocida: {unit}")

                next_run = now + delta
                return next_run

            except Exception as e:
                logger.error(f"Error al calcular INTERVAL: {str(e)}")
                raise InvalidScheduleError(f"Expresión INTERVAL inválida: {expression}")

        else:
            raise InvalidScheduleError(f"Kind no reconocido: {kind}")

    def _calculate_historical_runs(
        self,
        kind: str,
        expression: str,
        count: int,
        now: datetime,
    ) -> List[datetime]:
        """
        Calcula fechas de runs históricos para backfill.

        Args:
            kind: "CRON" o "INTERVAL"
            expression: Expresión CRON o INTERVAL
            count: Número de runs históricos
            now: Fecha actual de referencia

        Returns:
            Lista de datetimes históricos
        """
        if kind == ScheduleKind.CRON.value:
            return self._calculate_historical_cron(expression, count, now)
        elif kind == ScheduleKind.INTERVAL.value:
            return self._calculate_historical_interval(expression, count, now)
        else:
            raise InvalidScheduleError(f"Kind no reconocido: {kind}")

    def _calculate_historical_cron(
        self,
        expression: str,
        count: int,
        now: datetime,
    ) -> List[datetime]:
        """Calcula histórico para CRON expression"""
        try:
            run_dates = []
            cron = croniter(expression, now)
            for _ in range(count):
                prev_run = cron.get_prev(datetime)
                run_dates.insert(0, prev_run.replace(tzinfo=timezone.utc))
            return run_dates
        except Exception as e:
            logger.error(f"Error al calcular histórico CRON: {str(e)}")
            raise InvalidScheduleError(f"Expresión CRON inválida: {expression}")

    def _calculate_historical_interval(
        self,
        expression: str,
        count: int,
        now: datetime,
    ) -> List[datetime]:
        """Calcula histórico para INTERVAL expression"""
        try:
            delta = self._parse_interval_to_timedelta(expression)
            run_dates = []
            current = now
            for _ in range(count):
                current = current - delta
                run_dates.insert(0, current)
            return run_dates
        except Exception as e:
            logger.error(f"Error al calcular histórico INTERVAL: {str(e)}")
            raise InvalidScheduleError(f"Expresión INTERVAL inválida: {expression}")

    def _parse_interval_to_timedelta(self, expression: str) -> timedelta:
        """Convierte una expresión INTERVAL a timedelta"""
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
            raise ValueError(f"Unidad no reconocida: {unit}")

        return unit_mapping[unit](value)
