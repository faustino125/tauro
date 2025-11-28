"""
Copyright (c) 2025 Faustino Lopez Ramos. 
For licensing information, see the LICENSE file in the project root
"""
from pathlib import Path
from typing import Any, Dict

from loguru import logger

from core.config.contexts import Context
from core.config.loaders import ConfigLoaderFactory


class ContextLoader:
    """
    Core component for loading execution contexts from configuration files.
    Decoupled from CLI-specific logic.
    """

    def __init__(self):
        self.loader_factory = ConfigLoaderFactory()

    def load_from_paths(self, config_paths: Dict[str, str], env: str) -> Context:
        """
        Initialize context from a dictionary of configuration file paths.

        Args:
            config_paths: Dictionary containing paths for different config components
            env: Environment name

        Returns:
            Initialized Context object
        """
        required = [
            "global_settings_path",
            "pipelines_config_path",
            "nodes_config_path",
            "input_config_path",
            "output_config_path",
        ]

        missing = [path for path in required if path not in config_paths]
        if missing:
            raise ValueError(f"Missing config paths: {missing}")

        logger.debug(f"Loading configuration files for env '{env}'")

        global_settings = self._load_file(config_paths["global_settings_path"])
        pipelines_config = self._load_file(config_paths["pipelines_config_path"])
        nodes_config = self._load_file(config_paths["nodes_config_path"])
        input_config = self._load_file(config_paths["input_config_path"])
        output_config = self._load_file(config_paths["output_config_path"])

        ctx = Context(
            global_settings=global_settings,
            pipelines_config=pipelines_config,
            nodes_config=nodes_config,
            input_config=input_config,
            output_config=output_config,
        )

        self._inject_env(ctx, env)

        # Store paths for reference
        try:
            setattr(ctx, "config_paths", config_paths)
        except Exception:
            pass

        return ctx

    def _load_file(self, path_str: str) -> Dict[str, Any]:
        """Load a configuration file."""
        try:
            p = Path(path_str)
            if not p.exists():
                raise FileNotFoundError(f"Config file not found: {p}")
            return self.loader_factory.load_config(p)
        except Exception as e:
            raise RuntimeError(f"Failed to load config '{path_str}': {e}") from e

    def _inject_env(self, ctx: Context, env: str) -> None:
        """Inject environment into context and global settings."""
        try:
            setattr(ctx, "env", env)
        except Exception:
            pass

        if isinstance(ctx.global_settings, dict):
            ctx.global_settings.setdefault("environment", env)
            try:
                # Re-set to ensure updates propagate if it's a property
                setattr(ctx, "global_settings", ctx.global_settings)
            except Exception:
                pass
