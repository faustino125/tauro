"""
ProjectService: Gestión de proyectos (CRUD + estadísticas)

Este servicio implementa la lógica de negocio para la gestión completa de proyectos.
Interactúa con MongoDB a través de la capa de persistencia.
"""

import logging
from typing import List, Optional, Dict, Any
from uuid import UUID, uuid4
from datetime import datetime, timezone
from motor.motor_asyncio import AsyncDatabase  # type: ignore

from tauro.api.schemas.models import (
    ProjectCreate,
    ProjectResponse,
    ProjectUpdate,
    GlobalSettings,
    PipelineConfig,
)
from tauro.api.schemas.serializers import ProjectSerializer
from tauro.api.db.models import ProjectDocument
from tauro.api.schemas.project_validators import ProjectValidator


logger = logging.getLogger(__name__)


class ProjectNotFoundError(Exception):
    """Proyecto no encontrado en la base de datos"""

    pass


class ProjectAlreadyExistsError(Exception):
    """Proyecto con ese nombre ya existe"""

    pass


class InvalidProjectError(Exception):
    """Datos de proyecto inválidos"""

    pass


class ProjectService:
    """
    Servicio de gestión de proyectos.

    Responsabilidades:
    - CRUD de proyectos (Create, Read, Update, Delete)
    - Validaciones de negocio
    - Manejo de pipelines dentro de proyectos
    - Estadísticas y reporting
    """

    def __init__(self, db: AsyncDatabase):
        """
        Inicializa el servicio con una instancia de MongoDB.

        Args:
            db: AsyncDatabase instance de Motor
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
        Crea un nuevo proyecto.

        Args:
            project_data: Datos del proyecto a crear
            created_by: Usuario que crea el proyecto (email o ID)

        Returns:
            ProjectResponse con el proyecto creado

        Raises:
            ProjectAlreadyExistsError: Si ya existe un proyecto con ese nombre
            InvalidProjectError: Si los datos no son válidos
        """
        try:
            # Validar datos de entrada
            self.validator.validate_project_creation(project_data)

            # Verificar que el nombre sea único
            existing = await self.projects_collection.find_one(
                {"name": project_data.name}
            )
            if existing:
                raise ProjectAlreadyExistsError(
                    f"Ya existe un proyecto con el nombre '{project_data.name}'"
                )

            # Serializar a documento de MongoDB
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

            # Insertar en MongoDB
            await self.projects_collection.insert_one(project_doc)
            logger.info(f"Proyecto creado: {project_id} por {created_by}")

            # Retornar como response
            return ProjectResponse(**project_doc)

        except ProjectAlreadyExistsError:
            raise
        except Exception as e:
            logger.error(f"Error al crear proyecto: {str(e)}")
            raise InvalidProjectError(f"Error al crear proyecto: {str(e)}")

    async def read_project(self, project_id: str) -> ProjectResponse:
        """
        Lee un proyecto por ID.

        Args:
            project_id: ID del proyecto

        Returns:
            ProjectResponse con los datos del proyecto

        Raises:
            ProjectNotFoundError: Si el proyecto no existe
        """
        project_doc = await self.projects_collection.find_one({"id": project_id})

        if not project_doc:
            raise ProjectNotFoundError(f"Proyecto {project_id} no encontrado")

        # Remover el _id de MongoDB antes de convertir a Pydantic
        project_doc.pop("_id", None)
        return ProjectResponse(**project_doc)

    async def update_project(
        self,
        project_id: str,
        update_data: Dict[str, Any],
        updated_by: str,
    ) -> ProjectResponse:
        """
        Actualiza un proyecto existente.

        Args:
            project_id: ID del proyecto a actualizar
            update_data: Diccionario con campos a actualizar
            updated_by: Usuario que realiza la actualización

        Returns:
            ProjectResponse con los datos actualizados

        Raises:
            ProjectNotFoundError: Si el proyecto no existe
            InvalidProjectError: Si los datos no son válidos
        """
        try:
            # Verificar que el proyecto existe
            project_doc = await self.projects_collection.find_one({"id": project_id})
            if not project_doc:
                raise ProjectNotFoundError(f"Proyecto {project_id} no encontrado")

            # Validar cambios de nombre (debe ser único)
            if "name" in update_data and update_data["name"] != project_doc["name"]:
                existing = await self.projects_collection.find_one(
                    {"name": update_data["name"]}
                )
                if existing:
                    raise InvalidProjectError(
                        f"Ya existe un proyecto con el nombre '{update_data['name']}'"
                    )

            # Preparar actualización
            update_dict = {
                **update_data,
                "updated_at": datetime.now(timezone.utc),
            }

            # Actualizar en MongoDB
            await self.projects_collection.update_one(
                {"id": project_id},
                {"$set": update_dict},
            )

            logger.info(
                f"Proyecto {project_id} actualizado por {updated_by}. "
                f"Campos: {', '.join(update_dict.keys())}"
            )

            # Retornar proyecto actualizado
            return await self.read_project(project_id)

        except (ProjectNotFoundError, InvalidProjectError):
            raise
        except Exception as e:
            logger.error(f"Error al actualizar proyecto {project_id}: {str(e)}")
            raise InvalidProjectError(f"Error al actualizar proyecto: {str(e)}")

    async def delete_project(self, project_id: str) -> bool:
        """
        Elimina un proyecto (soft delete: marca como deleted).

        Args:
            project_id: ID del proyecto a eliminar

        Returns:
            True si se eliminó exitosamente

        Raises:
            ProjectNotFoundError: Si el proyecto no existe
        """
        # Verificar que existe
        project_doc = await self.projects_collection.find_one({"id": project_id})
        if not project_doc:
            raise ProjectNotFoundError(f"Proyecto {project_id} no encontrado")

        # Soft delete: marcar como archived
        result = await self.projects_collection.update_one(
            {"id": project_id},
            {
                "$set": {
                    "status": "archived",
                    "updated_at": datetime.now(timezone.utc),
                }
            },
        )

        logger.info(f"Proyecto {project_id} eliminado (archived)")
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
        Lista proyectos con filtros opcionales.

        Args:
            status: Filtrar por estado (active, archived, draft)
            tags: Filtrar por tags (key:value pairs)
            limit: Número máximo de resultados
            offset: Número de resultados a saltar
            created_by: Filtrar por usuario creador

        Returns:
            Tupla de (lista de proyectos, total de registros)
        """
        # Construir filtro
        query = {}

        if status:
            query["status"] = status

        if created_by:
            query["created_by"] = created_by

        if tags:
            for key, value in tags.items():
                query[f"tags.{key}"] = value

        # Contar total
        total = await self.projects_collection.count_documents(query)

        # Obtener página
        cursor = (
            self.projects_collection.find(query)
            .sort("created_at", -1)
            .skip(offset)
            .limit(limit)
        )

        projects = []
        async for doc in cursor:
            doc.pop("_id", None)
            projects.append(ProjectResponse(**doc))

        logger.debug(
            f"Listados {len(projects)} proyectos con filtros: "
            f"status={status}, tags={tags}, offset={offset}"
        )

        return projects, total

    async def get_project_stats(self, project_id: str) -> Dict[str, Any]:
        """
        Obtiene estadísticas del proyecto.

        Args:
            project_id: ID del proyecto

        Returns:
            Diccionario con estadísticas

        Raises:
            ProjectNotFoundError: Si el proyecto no existe
        """
        # Verificar que existe
        project = await self.read_project(project_id)

        # Contar runs
        total_runs = await self.pipeline_runs_collection.count_documents(
            {"project_id": project_id}
        )

        # Runs por estado
        run_states = await self.pipeline_runs_collection.aggregate(
            [
                {"$match": {"project_id": project_id}},
                {"$group": {"_id": "$state", "count": {"$sum": 1}}},
            ]
        ).to_list(None)

        runs_by_state = {item["_id"]: item["count"] for item in run_states}

        # Contar schedules habilitados
        active_schedules = await self.schedules_collection.count_documents(
            {"project_id": project_id, "enabled": True}
        )

        # Pipelines
        num_pipelines = len(project.pipelines or [])

        logger.info(f"Stats recuperadas para proyecto {project_id}")

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
        Agrega un nuevo pipeline a un proyecto.

        Args:
            project_id: ID del proyecto
            pipeline_config: Configuración del pipeline

        Returns:
            ProjectResponse actualizado

        Raises:
            ProjectNotFoundError: Si el proyecto no existe
            InvalidProjectError: Si la configuración es inválida
        """
        # Verificar que existe
        project = await self.read_project(project_id)

        # Validar configuración
        try:
            from tauro.api.schemas.project_validators import PipelineValidator

            validator = PipelineValidator()
            validator.validate_pipeline(pipeline_config)
        except Exception as e:
            raise InvalidProjectError(f"Pipeline inválido: {str(e)}")

        # Verificar que no existe un pipeline con el mismo nombre
        existing_pipelines = project.pipelines or []
        if any(p.name == pipeline_config.name for p in existing_pipelines):
            raise InvalidProjectError(
                f"Ya existe un pipeline con el nombre '{pipeline_config.name}'"
            )

        # Agregar pipeline
        pipelines_updated = [p.dict() for p in existing_pipelines]
        pipelines_updated.append(pipeline_config.dict())

        # Actualizar
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
        Elimina un pipeline de un proyecto.

        Args:
            project_id: ID del proyecto
            pipeline_id: ID del pipeline a eliminar

        Returns:
            ProjectResponse actualizado

        Raises:
            ProjectNotFoundError: Si el proyecto no existe
            InvalidProjectError: Si el pipeline no existe
        """
        # Verificar que existe
        project = await self.read_project(project_id)

        # Buscar y eliminar pipeline
        pipelines = project.pipelines or []
        pipeline_found = False

        for i, p in enumerate(pipelines):
            if str(p.id) == pipeline_id:
                pipelines.pop(i)
                pipeline_found = True
                break

        if not pipeline_found:
            raise InvalidProjectError(
                f"Pipeline {pipeline_id} no encontrado en proyecto"
            )

        # Actualizar
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
        Duplica un proyecto existente.

        Args:
            project_id: ID del proyecto a duplicar
            new_name: Nombre del nuevo proyecto
            created_by: Usuario que crea el proyecto

        Returns:
            ProjectResponse con el nuevo proyecto

        Raises:
            ProjectNotFoundError: Si el proyecto no existe
            ProjectAlreadyExistsError: Si el nuevo nombre ya existe
        """
        # Obtener proyecto original
        original = await self.read_project(project_id)

        # Crear datos para nuevo proyecto
        project_data = ProjectCreate(
            name=new_name,
            description=f"Copia de {original.name}",
            global_settings=original.global_settings,
            pipelines=original.pipelines,
            tags=original.tags,
        )

        # Crear nuevo proyecto
        return await self.create_project(project_data, created_by)
