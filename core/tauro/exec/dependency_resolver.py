from collections import defaultdict, deque
from typing import Any, Dict, List, Set

from loguru import logger  # type: ignore


class DependencyResolver:
    """Handles dependency resolution and topological sorting for pipeline nodes."""

    @staticmethod
    def build_dependency_graph(
        pipeline_nodes: List[str], node_configs: Dict[str, Dict[str, Any]]
    ) -> Dict[str, Set[str]]:
        """Build a dependency graph from node configurations."""
        dag = defaultdict(set)

        for node_name in pipeline_nodes:
            dag[node_name] = set()

        for node_name in pipeline_nodes:
            node_config = node_configs[node_name]
            dependencies = DependencyResolver._normalize_dependencies(
                node_config.get("dependencies", [])
            )

            logger.info(f"Node {node_name} dependencies: {dependencies}")

            for dep in dependencies:
                dep_name = DependencyResolver._extract_dependency_name(dep)
                logger.info(f"Processing dependency: {dep} -> {dep_name}")

                if dep_name not in pipeline_nodes:
                    raise ValueError(
                        f"Node '{node_name}' depends on '{dep_name}' which is not in the pipeline"
                    )

                dag[dep_name].add(node_name)

        return dict(dag)

    @staticmethod
    def topological_sort(dag: Dict[str, Set[str]]) -> List[str]:
        """Perform topological sort using Kahn's algorithm."""
        in_degree = defaultdict(int)
        all_nodes = set(dag.keys())

        for node in all_nodes:
            in_degree[node] = 0

        for node, dependents in dag.items():
            for dependent in dependents:
                in_degree[dependent] += 1

        queue = deque([node for node in all_nodes if in_degree[node] == 0])
        sorted_nodes = []

        while queue:
            node = queue.popleft()
            sorted_nodes.append(node)

            for dependent in dag[node]:
                in_degree[dependent] -= 1
                if in_degree[dependent] == 0:
                    queue.append(dependent)

        if len(sorted_nodes) != len(all_nodes):
            remaining = all_nodes - set(sorted_nodes)
            logger.error(f"Circular dependency detected involving nodes: {remaining}")
            raise ValueError(
                f"Circular dependency detected in the pipeline. Involved nodes: {remaining}"
            )

        return sorted_nodes

    @staticmethod
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

    @staticmethod
    def extract_dependency_name(dependency: Any) -> str:
        """Extract dependency name from various formats."""
        if isinstance(dependency, str):
            return dependency
        elif isinstance(dependency, dict):
            if len(dependency) != 1:
                raise ValueError(
                    f"Dict dependency must have exactly one key-value pair: {dependency}"
                )
            return next(iter(dependency.keys()))
        elif dependency is None:
            raise ValueError("Dependency cannot be None")
        else:
            raise TypeError(
                f"Unsupported dependency type: {type(dependency)} - {dependency}"
            )

    @staticmethod
    def get_node_dependencies(node_config: Dict[str, Any]) -> List[str]:
        """Extract and normalize node dependencies."""
        dependencies = DependencyResolver.normalize_dependencies(
            node_config.get("dependencies", [])
        )
        normalized_deps = []
        for dep in dependencies:
            try:
                dep_name = DependencyResolver.extract_dependency_name(dep)
                normalized_deps.append(dep_name)
            except (TypeError, ValueError) as e:
                logger.error(f"Error processing dependency {dep}: {str(e)}")
                raise
        return normalized_deps
