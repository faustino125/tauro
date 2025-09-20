from __future__ import annotations

import importlib.util
import json
import re
import sys
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

    def _load_module(self, path: Path):
        # Use a module name unique to the path to avoid collisions
        module_name = f"tauro_config_{abs(hash(str(path)))}"
        spec = importlib.util.spec_from_file_location(module_name, path)

        if not spec or not spec.loader:
            raise ConfigLoadError(f"Could not load Python module: {path}")

        module = importlib.util.module_from_spec(spec)
        # Do not override unrelated modules with same stem
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

        try:
            with path.open("r", encoding="utf-8") as f:
                for line_num, raw in enumerate(f, 1):
                    line = raw.strip()
                    current_path = self._process_line(
                        line, line_num, path, result, current_path
                    )
        except ConfigLoadError:
            raise
        except Exception as e:
            raise ConfigLoadError(f"Failed to parse DSL file {path}: {e}") from e

        return result

    def _ensure_section(self, root: Dict[str, Any], parts: List[str]) -> Dict[str, Any]:
        node = root
        for p in parts:
            if p not in node or not isinstance(node[p], dict):
                node[p] = {}
            node = node[p]
        return node

    def _parse_section(self, line: str) -> List[str]:
        m = self.SECTION_RE.match(line)
        if m:
            name = m.group("name").strip()
            return [p.strip() for p in name.split(".") if p.strip()]
        return []

    def _parse_key_value(self, line: str) -> Union[None, tuple]:
        if "=" in line:
            key, value = line.split("=", 1)
            return key.strip(), self._parse_value(value.strip())
        return None

    def _process_line(
        self,
        line: str,
        line_num: int,
        path: Path,
        result: Dict[str, Any],
        current_path: List[str],
    ) -> List[str]:
        if not line or line.startswith("#"):
            return current_path

        section_path = self._parse_section(line)
        if section_path:
            self._ensure_section(result, section_path)
            return section_path

        kv = self._parse_key_value(line)
        if kv:
            key, parsed = kv
            section = self._ensure_section(result, current_path)
            section[key] = parsed
            return current_path

        raise ConfigLoadError(f"Unrecognized DSL syntax at {path}:{line_num}: {line}")

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
        # If it's already a dict, nothing to do
        if isinstance(source, dict):
            return source

        # If it's a string that looks like JSON/YAML content, try parsing it directly
        if isinstance(source, str):
            text = source.strip()
            # JSON-like (object or array)
            if text.startswith("{") or text.startswith("["):
                try:
                    return json.loads(source)
                except Exception:
                    # Fallback to YAML if PyYAML available
                    if yaml is not None:
                        try:
                            return yaml.safe_load(source) or {}
                        except Exception:
                            pass
            # If it looks like an existing file path, prefer loading from disk
            p = Path(source)
            if p.exists():
                return self.get_loader(p).load(p)

        # If it's a Path object that exists, load from it
        if isinstance(source, Path):
            if source.exists():
                return self.get_loader(source).load(source)
            raise ConfigLoadError(f"File not found: {source}")

        # Last resort: try to get a loader based on suffix (this will raise if none matches)
        return self.get_loader(source).load(source)
