"""
Copyright (c) 2025 Faustino Lopez Ramos.
For licensing information, see the LICENSE file in the project root
"""
from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple

from loguru import logger

from tauro.core.config.exceptions import ConfigRepositoryError
from tauro.core.config.providers import ActiveConfigRecord, IConfigRepository


@dataclass
class ContextBundle:
    """In-memory representation of a configuration bundle ready to build a Context."""

    project_id: str
    environment: str
    version: str
    global_settings: Dict[str, Any]
    pipelines_config: Dict[str, Any]
    nodes_config: Dict[str, Any]
    input_config: Dict[str, Any]
    output_config: Dict[str, Any]
    metadata: Dict[str, Any]
    release: Dict[str, Any]

    def as_tuple(
        self,
    ) -> Tuple[Dict[str, Any], Dict[str, Any], Dict[str, Any], Dict[str, Any], Dict[str, Any]]:
        return (
            self.global_settings,
            self.pipelines_config,
            self.nodes_config,
            self.input_config,
            self.output_config,
        )


class ConfigService:
    """High-level service that orchestrates configuration reads using a repository backend."""

    def __init__(
        self,
        repository: IConfigRepository,
        *,
        default_environment: Optional[str] = None,
    ) -> None:
        self._repository = repository
        self._default_environment = default_environment or "dev"

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def get_project(self, project_id: str) -> Optional[Dict[str, Any]]:
        return self._repository.get_project(project_id)

    def get_active_context(
        self, project_id: str, environment: Optional[str] = None
    ) -> ContextBundle:
        env = self._resolve_environment(environment)
        record = self._repository.get_active_version(project_id, env)
        return self._build_context_bundle(record)

    def get_active_version_metadata(
        self, project_id: str, environment: Optional[str] = None
    ) -> Dict[str, Any]:
        bundle = self.get_active_context(project_id, environment)
        return {
            "project_id": bundle.project_id,
            "environment": bundle.environment,
            "version": bundle.version,
            "metadata": bundle.metadata,
            "release": bundle.release,
        }

    def load_context_dicts(
        self, project_id: str, environment: Optional[str] = None
    ) -> Tuple[Dict[str, Any], Dict[str, Any], Dict[str, Any], Dict[str, Any], Dict[str, Any]]:
        bundle = self.get_active_context(project_id, environment)
        return bundle.as_tuple()

    def close(self) -> None:
        try:
            self._repository.close()
        except Exception as exc:  # pragma: no cover - defensive
            logger.debug(f"Error closing repository: {exc}")

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _resolve_environment(self, environment: Optional[str]) -> str:
        if environment and environment.strip():
            return environment
        return self._default_environment

    def _build_context_bundle(self, record: ActiveConfigRecord) -> ContextBundle:
        document = record.document or {}
        environment = record.environment
        overrides_catalog = document.get("environment_overrides", {}) or {}
        env_overrides = overrides_catalog.get(environment, {})

        sections = {
            "global_settings": self._ensure_dict(document.get("global_settings")),
            "pipelines_config": self._ensure_dict(document.get("pipelines_config")),
            "nodes_config": self._ensure_dict(document.get("nodes_config")),
            "input_config": self._ensure_dict(document.get("input_config")),
            "output_config": self._ensure_dict(document.get("output_config")),
        }

        merged_sections = {
            name: self._apply_overrides(section, env_overrides.get(name))
            for name, section in sections.items()
        }

        global_settings = merged_sections["global_settings"]
        global_settings.setdefault("environment", environment)
        global_settings.setdefault("project_id", record.project_id)

        if record.environment_variables:
            global_settings = self._apply_overrides(
                global_settings,
                {"variables": record.environment_variables},
            )

        merged_sections["global_settings"] = global_settings

        metadata = dict(record.metadata or {})
        metadata.setdefault("version", record.version)

        return ContextBundle(
            project_id=record.project_id,
            environment=environment,
            version=record.version,
            global_settings=global_settings,
            pipelines_config=merged_sections["pipelines_config"],
            nodes_config=merged_sections["nodes_config"],
            input_config=merged_sections["input_config"],
            output_config=merged_sections["output_config"],
            metadata=metadata,
            release=record.release,
        )

    @staticmethod
    def _ensure_dict(value: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        if isinstance(value, dict):
            return value
        if value is None:
            return {}
        raise ConfigRepositoryError(f"Expected dict but received {type(value).__name__}")

    @staticmethod
    def _apply_overrides(
        base: Dict[str, Any], overrides: Optional[Dict[str, Any]]
    ) -> Dict[str, Any]:
        if not overrides:
            return base
        result = deepcopy(base)
        ConfigService._deep_update(result, overrides)
        return result

    @staticmethod
    def _deep_update(target: Dict[str, Any], updates: Dict[str, Any]) -> None:
        for key, value in updates.items():
            if key in target and isinstance(target[key], dict) and isinstance(value, dict):
                ConfigService._deep_update(target[key], value)
            else:
                target[key] = value
