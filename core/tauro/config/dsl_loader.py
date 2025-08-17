import importlib.util
import sys
from pathlib import Path
from typing import Any, Dict
import uuid

from tauro.config.exceptions import ConfigLoadError


class DSLLoader:
    """Loader for Python DSL configuration modules."""

    REQUIRED_VARIABLES = [
        "global_settings",
        "pipelines_config",
        "nodes_config",
        "input_config",
        "output_config",
    ]

    def load_from_module(self, python_module_path: str) -> Dict[str, Any]:
        """
        Load configuration from a Python DSL module.
        """
        path = Path(python_module_path)
        if not path.exists():
            raise ConfigLoadError(f"Python DSL module not found: {python_module_path}")

        module = self._load_module(path)
        self._validate_module_variables(module, python_module_path)

        return {
            var_name: getattr(module, var_name) for var_name in self.REQUIRED_VARIABLES
        }

    def _load_module(self, path: Path):
        """Safe module loading with unique identifiers"""
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
        """Validate that the module contains all required variables."""
        missing_vars = [
            var for var in self.REQUIRED_VARIABLES if not hasattr(module, var)
        ]

        if missing_vars:
            raise ConfigLoadError(
                f"Python DSL module {module_path} must define: {', '.join(missing_vars)}"
            )
