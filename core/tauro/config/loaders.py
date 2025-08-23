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
        raise ConfigLoadError(f"No supported loader for source: {source}")

    def load_config(self, source: Union[str, Dict, Path]) -> Dict[str, Any]:
        """Load configuration from dict or file path using appropriate loader."""
        if isinstance(source, dict):
            return source
        return self.get_loader(source).load(source)
