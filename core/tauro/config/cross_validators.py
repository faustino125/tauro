from tauro.config.context_hierarchy import HybridContext
from tauro.config.exceptions import ConfigValidationError


class CrossValidator:
    """Cross-validation for dependencies between different types of nodes."""

    @staticmethod
    def validate_hybrid_dependencies(nodes_config: dict):
        """Validates dependencies between nodes from different contexts."""
        errors = []

        for node_name, config in nodes_config.items():
            deps = config.get("dependencies", [])
            node_type = (
                "streaming"
                if "input" in config and "format" in config["input"]
                else "ml"
            )

            for dep in deps:
                dep_config = nodes_config.get(dep, {})
                dep_type = (
                    "streaming"
                    if "input" in dep_config and "format" in dep_config["input"]
                    else "ml"
                )

                # Validar streaming -> ML
                if node_type == "ml" and dep_type == "streaming":
                    if dep_config.get("output", {}).get("format") not in [
                        "delta",
                        "parquet",
                    ]:
                        errors.append(
                            f"ML node '{node_name}' requiere salida delta/parquet de "
                            f"nodo streaming '{dep}', obtuvo {dep_config.get('output', {}).get('format')}"
                        )

                # Validar ML -> streaming
                if node_type == "streaming" and dep_type == "ml":
                    if dep_config.get("model", {}).get("type") != "spark_ml":
                        errors.append(
                            f"Nodo streaming '{node_name}' requiere modelo spark_ml de "
                            f"nodo ML '{dep}', obtuvo {dep_config.get('model', {}).get('type')}"
                        )

        if errors:
            raise ConfigValidationError("\n".join(errors))


class HybridValidator:
    @staticmethod
    def validate_context(context: "HybridContext") -> None:
        """Centraliza validación híbrida"""
        errors = []

        # Validar dependencias cruzadas
        for node_name, config in context.nodes_config.items():
            deps = config.get("dependencies", [])
            node_type = (
                "streaming"
                if context._streaming_ctx._is_compatible_node(config)
                else "ml"
            )

            for dep in deps:
                dep_config = context.nodes_config.get(dep, {})
                dep_type = (
                    "streaming"
                    if context._streaming_ctx._is_compatible_node(dep_config)
                    else "ml"
                )

                # Validar streaming -> ML
                if node_type == "ml" and dep_type == "streaming":
                    if dep_config.get("output", {}).get("format") not in [
                        "delta",
                        "parquet",
                    ]:
                        errors.append(
                            f"ML node '{node_name}' requiere salida delta/parquet de nodo streaming '{dep}'"
                        )

                # Validar ML -> streaming
                if node_type == "streaming" and dep_type == "ml":
                    if dep_config.get("model", {}).get("type") != "spark_ml":
                        errors.append(
                            f"Nodo streaming '{node_name}' requiere modelo spark_ml de nodo ML '{dep}'"
                        )

        # Validar estructura de pipelines híbridos
        hybrid_pipelines = {
            name: p
            for name, p in context.pipelines_config.items()
            if p.get("type") == "hybrid"
        }

        for name, pipeline in hybrid_pipelines.items():
            nodes = pipeline.get("nodes", [])
            has_streaming = any(
                context._streaming_ctx._is_compatible_node(context.nodes_config[n])
                for n in nodes
            )
            has_ml = any(
                context._ml_ctx._is_compatible_node(context.nodes_config[n])
                for n in nodes
            )

            if not (has_streaming and has_ml):
                errors.append(
                    f"Hybrid pipeline '{name}' must contain both streaming and ML nodes"
                )

        if errors:
            raise ConfigValidationError("\n".join(errors))
