from functools import lru_cache
from typing import Any, Dict, List, Optional

from tauro.config.validators import PipelineValidator


class PipelineManager:
    """Manages pipeline configurations and operations."""

    def __init__(self, pipelines_config: Dict[str, Any], nodes_config: Dict[str, Any]):
        self.pipelines_config = pipelines_config
        self.nodes_config = nodes_config
        self._validator = PipelineValidator()

    @property
    @lru_cache(maxsize=1)
    def pipelines(self) -> Dict[str, Dict[str, Any]]:
        """Return all loaded and validated pipeline configurations."""
        self._validator.validate_pipeline_nodes(
            self.pipelines_config, self.nodes_config
        )

        return {
            name: self._generate_pipeline_config(name, contents)
            for name, contents in self.pipelines_config.items()
        }

    def _generate_pipeline_config(
        self, name: str, contents: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Generate complete configuration for a specific pipeline."""
        return {
            "nodes": [
                {"name": node_name, **self.nodes_config[node_name]}
                for node_name in contents.get("nodes", [])
            ],
            "inputs": contents.get("inputs", []),
            "outputs": contents.get("outputs", []),
        }

    def get_pipeline(self, name: str) -> Optional[Dict[str, Any]]:
        """Get a specific pipeline configuration by name."""
        return self.pipelines.get(name)

    def list_pipeline_names(self) -> List[str]:
        """Get a list of all pipeline names."""
        return list(self.pipelines.keys())
