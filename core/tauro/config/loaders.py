from __future__ import annotations

import importlib.util
import json
import re
import sys
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, List, Union

try:
    import yaml  # type: ignore
except Exception:  # pragma: no cover - yaml is optional, handled in load()
    yaml = None  # type: ignore

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
        if yaml is None:
            raise ConfigLoadError("PyYAML not installed. Run: pip install PyYAML")
        try:
            with Path(source).open("r", encoding="utf-8") as file:
                return yaml.safe_load(file) or {}
        except yaml.YAMLError as e:  # type: ignore
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

        if not isinstance(module.config, dict):
            raise ConfigLoadError(f"'config' in {path} must be a dict")
        return module.config


class DSLConfigLoader(ConfigLoader):
    """Loader for Tauro's simple hierarchical DSL."""

    SECTION_RE = re.compile(r"^\[(?P<name>.+?)\]\s*$")

    def can_load(self, source: Union[str, Path]) -> bool:
        if isinstance(source, str):
            source = Path(source)
        suffix = source.suffix.lower()
        return suffix in (".dsl", ".tdsl")

    def load(self, source: Union[str, Path]) -> Dict[str, Any]:
        path = Path(source)
        if not path.exists():
            raise ConfigLoadError(f"File not found: {path}")

        result: Dict[str, Any] = {}
        current_path: List[str] = []

        def ensure_section(root: Dict[str, Any], parts: List[str]) -> Dict[str, Any]:
            node = root
            for p in parts:
                if p not in node or not isinstance(node[p], dict):
                    node[p] = {}
                node = node[p]
            return node

        try:
            with path.open("r", encoding="utf-8") as f:
                for line_num, raw in enumerate(f, 1):
                    line = raw.strip()
                    if not line or line.startswith("#"):
                        continue

                    m = self.SECTION_RE.match(line)
                    if m:
                        name = m.group("name").strip()
                        # dotted sections allowed, e.g. [global_settings.format_policy]
                        current_path = [p.strip() for p in name.split(".") if p.strip()]
                        ensure_section(result, current_path)
                        continue

                    if "=" in line:
                        key, value = line.split("=", 1)
                        key = key.strip()
                        parsed = self._parse_value(value.strip())
                        section = ensure_section(result, current_path)
                        section[key] = parsed
                        continue

                    raise ConfigLoadError(
                        f"Unrecognized DSL syntax at {path}:{line_num}: {raw.strip()}"
                    )
        except ConfigLoadError:
            raise
        except Exception as e:
            raise ConfigLoadError(f"Failed to parse DSL file {path}: {e}") from e

        return result

    def _parse_value(self, value: str) -> Union[str, int, float, bool, List[Any]]:
        # Strip surrounding quotes for strings early (but keep value for further checks)
        if (value.startswith('"') and value.endswith('"')) or (
            value.startswith("'") and value.endswith("'")
        ):
            return value[1:-1]

        low = value.lower()
        if low == "true":
            return True
        if low == "false":
            return False

        # Integer
        try:
            return int(value)
        except ValueError:
            pass

        # Float
        try:
            return float(value)
        except ValueError:
            pass

        # List
        if value.startswith("[") and value.endswith("]"):
            inner = value[1:-1].strip()
            if not inner:
                return []
            items = [i.strip() for i in inner.split(",")]
            return [self._parse_value(i) for i in items if i != ""]

        # Fallback: treat as bare string
        return value


class ConfigLoaderFactory:
    """Factory for creating appropriate configuration loaders."""

    def __init__(self):
        self._loaders: List[ConfigLoader] = [
            YamlConfigLoader(),
            JsonConfigLoader(),
            DSLConfigLoader(),
            PythonConfigLoader(),
        ]

    def get_loader(self, source: Union[str, Path]) -> ConfigLoader:
        for loader in self._loaders:
            if loader.can_load(source):
                return loader
        raise ConfigLoadError(f"No supported loader for source: {source}")

    def load_config(self, source: Union[str, Dict, Path]) -> Dict[str, Any]:
        if isinstance(source, dict):
            return source
        return self.get_loader(source).load(source)
