import importlib.util
import json
import sys
from abc import ABC, abstractmethod
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, Union

import yaml  # type: ignore

from tauro.config.exceptions import ConfigLoadError


class ConfigLoader(ABC):
    """Abstract base class for configuration loaders."""

    @abstractmethod
    def can_load(self, source: Union[str, Path]) -> bool:
        """Check if this loader can handle the given source."""
        pass

    @abstractmethod
    def load(self, source: Union[str, Path]) -> Dict[str, Any]:
        """Load configuration from the source."""
        pass


class YamlConfigLoader(ConfigLoader):
    """Loader for YAML configuration files."""

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
    """Loader for JSON configuration files."""

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
    """Loader for Python module configuration files."""

    def can_load(self, source: Union[str, Path]) -> bool:
        """Check if this loader can handle the given source."""
        if isinstance(source, str):
            source = Path(source)
        return source.suffix.lower() == ".py"

    @lru_cache(maxsize=32)
    def _load_module(self, path: Path):
        """Cached module loader."""
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
        """Get the appropriate loader for the given source."""
        for loader in self._loaders:
            if loader.can_load(source):
                return loader

        if isinstance(source, str):
            source = Path(source)
        raise ConfigLoadError(f"Unsupported config format: {source.suffix}")

    def load_config(self, source: Union[str, Dict, Path]) -> Dict[str, Any]:
        """Load configuration from various sources."""
        if isinstance(source, dict):
            return source

        path = Path(source)
        if not path.exists():
            raise ConfigLoadError(f"Config source not found: {source}")

        loader = self.get_loader(path)
        return loader.load(path)
