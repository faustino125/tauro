from __future__ import annotations
from typing import List, Dict
from fastapi import APIRouter, Depends

from tauro.cli.execution import PipelineExecutor  # ya existente en CLI
from tauro.config.contexts import Context

from tauro.api.auth import get_current_user
from tauro.api.deps import get_context
from tauro.api.config import ApiSettings

router = APIRouter(prefix="/pipelines", tags=["pipelines"])


@router.get("", response_model=List[str])
def list_pipelines(
    user: str = Depends(get_current_user),
    context: Context = Depends(get_context),
):
    # Reusa CLI executor para listar
    # Nota: PipelineExecutor aquí es el de CLI (no confundir con exec.executor.PipelineExecutor)
    executor = PipelineExecutor(context, None)
    return sorted(executor.list_pipelines())


@router.get("/{pipeline_id}", response_model=Dict)
def get_pipeline_info(
    pipeline_id: str,
    user: str = Depends(get_current_user),
    context: Context = Depends(get_context),
):
    executor = PipelineExecutor(context, None)
    info = executor.get_pipeline_info(pipeline_id)
    return info or {}
