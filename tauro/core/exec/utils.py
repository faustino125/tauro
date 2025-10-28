"""
Copyright (c) 2025 Faustino Lopez Ramos. 
For licensing information, see the LICENSE file in the project root
"""
from typing import Any, Dict, List
from loguru import logger  # type: ignore


def normalize_dependencies(dependencies: Any) -> List[Any]:
    """Normalize dependencies to a consistent list format."""
    if dependencies is None:
        return []
    elif isinstance(dependencies, str):
        return [dependencies]
    elif isinstance(dependencies, dict):
        return list(dependencies.keys())
    elif isinstance(dependencies, list):
        return dependencies
    else:
        return [str(dependencies)]


def extract_dependency_name(dependency: Any) -> str:
    """Extract dependency name from various formats."""
    if isinstance(dependency, str):
        return dependency
    elif isinstance(dependency, dict):
        if len(dependency) != 1:
            raise ValueError(f"Dict dependency must have exactly one key-value pair: {dependency}")
        return next(iter(dependency.keys()))
    elif dependency is None:
        raise ValueError("Dependency cannot be None")
    else:
        raise TypeError(f"Unsupported dependency type: {type(dependency)} - {dependency}")


def extract_pipeline_nodes(pipeline: Dict[str, Any]) -> List[str]:
    """Extract node names from pipeline configuration."""
    pipeline_nodes_raw = pipeline.get("nodes", [])
    pipeline_nodes = []
    for node in pipeline_nodes_raw:
        if isinstance(node, str):
            pipeline_nodes.append(node)
        elif isinstance(node, dict):
            if len(node) == 1:
                pipeline_nodes.append(list(node.keys())[0])
            elif "name" in node:
                pipeline_nodes.append(node["name"])
            else:
                raise ValueError(f"Invalid node format in pipeline: {node}")
        else:
            pipeline_nodes.append(str(node))
    return pipeline_nodes


def get_node_dependencies(node_config: Dict[str, Any]) -> List[str]:
    """Extract and normalize node dependencies."""
    dependencies = normalize_dependencies(node_config.get("dependencies", []))
    normalized_deps = []
    for dep in dependencies:
        try:
            dep_name = extract_dependency_name(dep)
            normalized_deps.append(dep_name)
        except (TypeError, ValueError) as e:
            logger.error(f"Error processing dependency {dep}: {str(e)}")
            raise
    return normalized_deps
