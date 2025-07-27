import importlib.util
import sys
from pathlib import Path
from typing import Any, Dict

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
        """Load the Python module from the given path."""
        module_name = path.stem
        spec = importlib.util.spec_from_file_location(module_name, path)

        if not spec or not spec.loader:
            raise ConfigLoadError(f"Could not load Python DSL module: {path}")

        module = importlib.util.module_from_spec(spec)
        sys.modules[module_name] = module

        try:
            spec.loader.exec_module(module)
        except Exception as e:
            raise ConfigLoadError(f"Error executing DSL module {path}: {str(e)}") from e

        return module

    def _validate_module_variables(self, module, module_path: str) -> None:
        """Validate that the module contains all required variables."""
        missing_vars = [
            var for var in self.REQUIRED_VARIABLES if not hasattr(module, var)
        ]

        if missing_vars:
            raise ConfigLoadError(
                f"Python DSL module {module_path} must define: {', '.join(missing_vars)}"
            )
