import importlib.util
import json
import sys
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, Union

import yaml  # type: ignore

from tauro.config.exceptions import ConfigLoadError


class ConfigLoader:
    """Abstract base class for configuration loaders."""

    def can_load(self, source: Union[str, Path]) -> bool:
        raise NotImplementedError

    def load(self, source: Union[str, Path]) -> Dict[str, Any]:
        raise NotImplementedError


class YamlConfigLoader(ConfigLoader):
    def can_load(self, source: Union[str, Path]) -> bool:
        if isinstance(source, str):
            source = Path(source)
        return source.suffix.lower() in (".yaml", ".yml")

    def load(self, source: Union[str, Path]) -> Dict[str, Any]:
        try:
            with Path(source).open("r", encoding="utf-8") as file:
                return yaml.safe_load(file) or {}
        except yaml.YAMLError as e:
            raise ConfigLoadError(f"Invalid YAML in {source}: {str(e)}") from e
        except Exception as e:
            raise ConfigLoadError(f"Error loading YAML file {source}: {str(e)}") from e


class JsonConfigLoader(ConfigLoader):
    def can_load(self, source: Union[str, Path]) -> bool:
        if isinstance(source, str):
            source = Path(source)
        return source.suffix.lower() == ".json"

    def load(self, source: Union[str, Path]) -> Dict[str, Any]:
        try:
            with Path(source).open("r", encoding="utf-8") as file:
                return json.load(file) or {}
        except json.JSONDecodeError as e:
            raise ConfigLoadError(f"Invalid JSON in {source}: {str(e)}") from e
        except Exception as e:
            raise ConfigLoadError(f"Error loading JSON file {source}: {str(e)}") from e


class PythonConfigLoader(ConfigLoader):
    def can_load(self, source: Union[str, Path]) -> bool:
        if isinstance(source, str):
            source = Path(source)
        return source.suffix.lower() == ".py"

    @lru_cache(maxsize=32)
    def _load_module(self, path: Path):
        module_name = path.stem
        spec = importlib.util.spec_from_file_location(module_name, path)

        if not spec or not spec.loader:
            raise ConfigLoadError(f"Could not load Python module: {path}")

        module = importlib.util.module_from_spec(spec)
        sys.modules[module_name] = module

        try:
            spec.loader.exec_module(module)
        except Exception as e:
            raise ConfigLoadError(f"Error executing module {path}: {str(e)}") from e

        return module

    def load(self, source: Union[str, Path]) -> Dict[str, Any]:
        path = Path(source)
        module = self._load_module(path)

        if not hasattr(module, "config"):
            raise ConfigLoadError(f"Python module {path} must define 'config' variable")

        return module.config


class ConfigLoaderFactory:
    """Factory for creating appropriate configuration loaders."""

    def __init__(self):
        self._loaders = [
            YamlConfigLoader(),
            JsonConfigLoader(),
            PythonConfigLoader(),
        ]

    def get_loader(self, source: Union[str, Path]) -> ConfigLoader:
        for loader in self._loaders:
            if loader.can_load(source):
                return loader

        if isinstance(source, str):
            source = Path(source)
        raise ConfigLoadError(f"Unsupported config format: {source.suffix}")

    def load_config(self, source: Union[str, Dict, Path]) -> Dict[str, Any]:
        if isinstance(source, dict):
            return source

        path = Path(source)
        if not path.exists():
            raise ConfigLoadError(f"Config source not found: {source}")

        loader = self.get_loader(path)
        return loader.load(path)


class DSLLoader:
    """Loader para Domain-Specific Language (DSL)."""

    REQUIRED_VARIABLES = [
        "global_settings",
        "pipelines_config",
        "nodes_config",
        "input_config",
        "output_config",
    ]

    PY_EXTS = {".py"}
    YAML_DSL_EXTS = {".tdsl", ".tdsl.yml", ".tdsl.yaml"}

    def load(self, dsl_path: Union[str, Path]) -> Dict[str, Any]:
        path = Path(dsl_path)
        if not path.exists():
            raise ConfigLoadError(f"DSL file not found: {dsl_path}")

        suffix = path.suffix.lower()
        # soportar doble sufijo .tdsl.yml y .tdsl.yaml
        if (
            suffix in {".yml", ".yaml"}
            and path.name.lower().endswith(".tdsl.yml")
            or path.name.lower().endswith(".tdsl.yaml")
        ):
            return self._load_yaml_dsl(path)
        if suffix in self.PY_EXTS:
            return self._load_python_dsl(path)
        if suffix in self.YAML_DSL_EXTS:
            return self._load_yaml_dsl(path)

        # fallback: permitir también .yml/.yaml si contienen la sección 'dsl: true'
        if suffix in {".yml", ".yaml"}:
            data = self._read_yaml(path)
            if isinstance(data, dict) and data.get("dsl", False):
                return self._map_yaml_dsl_to_config(data)
            raise ConfigLoadError(
                f"YAML provided is not marked as DSL (dsl: true) or uses unsupported extension for DSL: {path}"
            )

        raise ConfigLoadError(f"Unsupported DSL format: {path.suffix}")

    def _load_python_dsl(self, path: Path) -> Dict[str, Any]:
        # Cargar un módulo .py que define variables REQUERIDAS
        module = self._load_module_unique(path)
        self._validate_module_variables(module, str(path))
        return {
            var_name: getattr(module, var_name) for var_name in self.REQUIRED_VARIABLES
        }

    def _load_yaml_dsl(self, path: Path) -> Dict[str, Any]:
        data = self._read_yaml(path)
        if not isinstance(data, dict):
            raise ConfigLoadError(f"YAML DSL must be a mapping at root: {path}")
        return self._map_yaml_dsl_to_config(data)

    def _read_yaml(self, path: Path) -> Dict[str, Any]:
        try:
            with path.open("r", encoding="utf-8") as f:
                return yaml.safe_load(f) or {}
        except yaml.YAMLError as e:
            raise ConfigLoadError(f"Invalid YAML in DSL file {path}: {str(e)}") from e
        except Exception as e:
            raise ConfigLoadError(f"Error reading DSL file {path}: {str(e)}") from e

    def _map_yaml_dsl_to_config(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Mapea el DSL YAML a la estructura estándar de configuración.
        Estructura DSL esperada (ver DSL_SPEC.md):
          global: {}
          nodes: {}
          pipelines: {}
          inputs: {}
          outputs: {}
        """
        global_settings = data.get("global", {})
        nodes_config = data.get("nodes", {})
        pipelines_section = data.get("pipelines", {})
        input_config = data.get("inputs", {})
        output_config = data.get("outputs", {})

        # Validaciones mínimas de forma
        if not isinstance(global_settings, dict):
            raise ConfigLoadError("DSL 'global' section must be a mapping")
        if not isinstance(nodes_config, dict):
            raise ConfigLoadError("DSL 'nodes' section must be a mapping")
        if not isinstance(pipelines_section, dict):
            raise ConfigLoadError("DSL 'pipelines' section must be a mapping")
        if not isinstance(input_config, dict):
            raise ConfigLoadError("DSL 'inputs' section must be a mapping")
        if not isinstance(output_config, dict):
            raise ConfigLoadError("DSL 'outputs' section must be a mapping")

        # Normalización de pipelines -> pipelines_config
        pipelines_config: Dict[str, Any] = {}
        for p_name, p_body in pipelines_section.items():
            if not isinstance(p_body, dict):
                raise ConfigLoadError(f"Pipeline '{p_name}' body must be a mapping")
            p_type = p_body.get("type", "batch")
            nodes = p_body.get("nodes", [])
            spark_config = p_body.get("spark", {})
            inputs = p_body.get("inputs", [])
            outputs = p_body.get("outputs", [])

            if not isinstance(nodes, list):
                raise ConfigLoadError(f"Pipeline '{p_name}' nodes must be a list")

            pipelines_config[p_name] = {
                "type": p_type,
                "nodes": nodes,
                "spark_config": spark_config,
                "inputs": inputs,
                "outputs": outputs,
            }

        # Ensamblar la salida estándar
        return {
            "global_settings": global_settings,
            "pipelines_config": pipelines_config,
            "nodes_config": nodes_config,
            "input_config": input_config,
            "output_config": output_config,
        }

    def _load_module_unique(self, path: Path):
        """Carga segura de módulo .py para DSL Python con un nombre único"""
        import uuid

        unique_name = f"{path.stem}_{uuid.uuid4().hex}"
        spec = importlib.util.spec_from_file_location(unique_name, path)

        if not spec or not spec.loader:
            raise ConfigLoadError(f"Could not load Python DSL module: {path}")

        module = importlib.util.module_from_spec(spec)
        sys.modules[unique_name] = module

        try:
            spec.loader.exec_module(module)
            return module
        except Exception as e:
            if unique_name in sys.modules:
                del sys.modules[unique_name]
            raise ConfigLoadError(f"Error executing DSL module {path}: {str(e)}") from e

    def _validate_module_variables(self, module, module_path: str) -> None:
        missing_vars = [
            var for var in self.REQUIRED_VARIABLES if not hasattr(module, var)
        ]
        if missing_vars:
            raise ConfigLoadError(
                f"Python DSL module {module_path} must define: {', '.join(missing_vars)}"
            )
