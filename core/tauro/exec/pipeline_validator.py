from typing import Any, Dict, List, Optional, Tuple

from loguru import logger  # type: ignore

from tauro.config.validators import FormatPolicy


class PipelineValidator:
    """Enhanced validator supporting hybrid batch/streaming pipelines."""

    BATCH_OUTPUT_FORMATS = {"parquet", "delta", "json", "csv", "kafka", "orc"}

    @staticmethod
    def validate_required_params(
        pipeline_name: Optional[str],
        start_date: Optional[str],
        end_date: Optional[str],
        context_start_date: Optional[str],
        context_end_date: Optional[str],
    ) -> None:
        """Validate required parameters for pipeline execution."""
        if not pipeline_name:
            raise ValueError("Pipeline name is required")
        if not (start_date or context_start_date):
            raise ValueError("Start date is required")
        if not (end_date or context_end_date):
            raise ValueError("End date is required")

    @staticmethod
    def validate_pipeline_config(pipeline: Dict[str, Any]) -> None:
        """Valida configuración básica del pipeline."""
        if not isinstance(pipeline, dict):
            raise ValueError("Pipeline configuration must be a dictionary")

        if "nodes" not in pipeline:
            raise ValueError("Pipeline must contain 'nodes' key")

        nodes = pipeline["nodes"]
        if not nodes:
            raise ValueError("Pipeline must have at least one node")

        if not isinstance(nodes, list):
            raise ValueError("Pipeline 'nodes' must be a list")

    @staticmethod
    def validate_node_configs(
        pipeline_nodes: List[str], node_configs: Dict[str, Dict[str, Any]]
    ) -> None:
        """Valida que todos los nodos del pipeline tengan configuraciones."""
        missing_nodes = []
        for node_name in pipeline_nodes:
            if node_name not in node_configs:
                missing_nodes.append(node_name)

        if missing_nodes:
            raise ValueError(f"Missing node configurations: {', '.join(missing_nodes)}")

    @staticmethod
    def validate_hybrid_pipeline(
        pipeline: Dict[str, Any],
        node_configs: Dict[str, Dict[str, Any]],
        format_policy: Optional[FormatPolicy] = None,
    ) -> Dict[str, Any]:
        """Valida pipeline híbrido y retorna análisis detallado."""
        policy = format_policy or FormatPolicy()

        validation_result = {
            "is_valid": True,
            "errors": [],
            "warnings": [],
            "batch_nodes": [],
            "streaming_nodes": [],
            "cross_dependencies": [],
            "format_compatibility": [],
            "resource_conflicts": [],
        }

        try:
            batch_nodes, streaming_nodes = PipelineValidator._classify_nodes(
                pipeline, node_configs, policy
            )

            validation_result["batch_nodes"] = batch_nodes
            validation_result["streaming_nodes"] = streaming_nodes

            cross_deps, cross_errors = PipelineValidator._validate_cross_dependencies(
                batch_nodes, streaming_nodes, node_configs
            )

            validation_result["cross_dependencies"] = cross_deps
            validation_result["errors"].extend(cross_errors)

            format_issues = PipelineValidator._validate_format_compatibility(
                cross_deps, node_configs, policy
            )

            validation_result["format_compatibility"] = format_issues
            validation_result["errors"].extend(
                [issue for issue in format_issues if issue["severity"] == "error"]
            )
            validation_result["warnings"].extend(
                [issue for issue in format_issues if issue["severity"] == "warning"]
            )

            resource_conflicts = PipelineValidator._validate_resource_conflicts(
                batch_nodes, streaming_nodes, node_configs
            )

            validation_result["resource_conflicts"] = resource_conflicts
            validation_result["warnings"].extend(resource_conflicts)

            streaming_errors = PipelineValidator._validate_streaming_requirements(
                streaming_nodes, node_configs, policy
            )

            validation_result["errors"].extend(streaming_errors)

            validation_result["is_valid"] = len(validation_result["errors"]) == 0

        except Exception as e:
            validation_result["is_valid"] = False
            validation_result["errors"].append(
                f"Validation failed with error: {str(e)}"
            )

        return validation_result

    @staticmethod
    def _classify_nodes(
        pipeline: Dict[str, Any],
        node_configs: Dict[str, Dict[str, Any]],
        policy: FormatPolicy,
    ) -> Tuple[List[str], List[str]]:
        """Clasifica nodos en batch y streaming."""

        batch_nodes = []
        streaming_nodes = []

        pipeline_nodes = PipelineValidator._extract_pipeline_nodes(pipeline)

        for node_name in pipeline_nodes:
            node_config = node_configs.get(node_name, {})

            if PipelineValidator._is_streaming_node(node_config, policy):
                streaming_nodes.append(node_name)
            else:
                batch_nodes.append(node_name)

        return batch_nodes, streaming_nodes

    @staticmethod
    def _is_streaming_node(node_config: Dict[str, Any], policy: FormatPolicy) -> bool:
        """Determina si un nodo es de streaming (solo por entrada)"""
        input_config = node_config.get("input", {})

        if isinstance(input_config, dict):
            input_format = input_config.get("format", "")
            return policy.is_supported_input(input_format)

        return False

    @staticmethod
    def _extract_pipeline_nodes(pipeline: Dict[str, Any]) -> List[str]:
        """Extrae nombres de nodos del pipeline."""
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

    @staticmethod
    def _validate_cross_dependencies(
        batch_nodes: List[str],
        streaming_nodes: List[str],
        node_configs: Dict[str, Dict[str, Any]],
    ) -> Tuple[List[Dict[str, Any]], List[str]]:
        """Valida dependencias cruzadas entre batch y streaming."""

        cross_dependencies = []
        errors = []

        for streaming_node in streaming_nodes:
            node_config = node_configs.get(streaming_node, {})
            dependencies = PipelineValidator._get_node_dependencies(node_config)

            batch_deps = [dep for dep in dependencies if dep in batch_nodes]

            if batch_deps:
                cross_dependencies.append(
                    {
                        "streaming_node": streaming_node,
                        "batch_dependencies": batch_deps,
                        "type": "streaming_depends_on_batch",
                    }
                )

                for batch_dep in batch_deps:
                    batch_config = node_configs.get(batch_dep, {})
                    batch_output = batch_config.get("output", {})

                    if not batch_output.get("path"):
                        errors.append(
                            f"Batch node '{batch_dep}' (dependency of streaming node '{streaming_node}') "
                            f"must have output path for cross-pipeline data flow"
                        )

        for batch_node in batch_nodes:
            node_config = node_configs.get(batch_node, {})
            dependencies = PipelineValidator._get_node_dependencies(node_config)

            streaming_deps = [dep for dep in dependencies if dep in streaming_nodes]

            if streaming_deps:
                errors.append(
                    f"Batch node '{batch_node}' cannot depend on streaming nodes: {streaming_deps}. "
                    f"This creates an invalid dependency pattern."
                )

        return cross_dependencies, errors

    @staticmethod
    def _validate_format_compatibility(
        cross_dependencies: List[Dict[str, Any]],
        node_configs: Dict[str, Dict[str, Any]],
        policy: FormatPolicy,
    ) -> List[Dict[str, Any]]:
        """Valida compatibilidad de formatos entre nodos batch y streaming."""

        format_issues: List[Dict[str, Any]] = []

        for cross_dep in cross_dependencies:
            streaming_node = cross_dep["streaming_node"]
            batch_deps = cross_dep["batch_dependencies"]

            streaming_config = node_configs.get(streaming_node, {})
            streaming_input = streaming_config.get("input", {})
            streaming_format = streaming_input.get("format")

            for batch_dep in batch_deps:
                batch_config = node_configs.get(batch_dep, {})
                batch_output = batch_config.get("output", {})
                batch_format = batch_output.get("format")

                if batch_format and streaming_format:
                    if policy.are_compatible(batch_format, streaming_format):
                        format_issues.append(
                            {
                                "severity": "info",
                                "message": f"Compatible formats: '{batch_format}' -> '{streaming_format}'",
                                "batch_node": batch_dep,
                                "streaming_node": streaming_node,
                                "batch_format": batch_format,
                                "streaming_format": streaming_format,
                            }
                        )
                    else:
                        format_issues.append(
                            {
                                "severity": "error",
                                "message": f"Incompatible formats: batch node '{batch_dep}' outputs "
                                f"'{batch_format}' but streaming node '{streaming_node}' expects "
                                f"'{streaming_format}'.",
                                "batch_node": batch_dep,
                                "streaming_node": streaming_node,
                                "batch_format": batch_format,
                                "streaming_format": streaming_format,
                            }
                        )

                if streaming_format == "file_stream":
                    batch_path = batch_output.get("path")
                    streaming_path = streaming_input.get("options", {}).get("path")

                    if batch_path and streaming_path and batch_path != streaming_path:
                        format_issues.append(
                            {
                                "severity": "warning",
                                "message": f"Path mismatch: batch node '{batch_dep}' writes to "
                                f"'{batch_path}' but streaming node '{streaming_node}' reads from "
                                f"'{streaming_path}'. Ensure paths are coordinated.",
                                "batch_node": batch_dep,
                                "streaming_node": streaming_node,
                            }
                        )

        return format_issues

    @staticmethod
    def _validate_resource_conflicts(
        batch_nodes: List[str],
        streaming_nodes: List[str],
        node_configs: Dict[str, Dict[str, Any]],
    ) -> List[str]:
        """Valida conflictos de recursos entre nodos batch y streaming."""

        warnings: List[str] = []
        output_paths = {}
        kafka_topics = {}

        for batch_node in batch_nodes:
            node_config = node_configs.get(batch_node, {})
            output_config = node_config.get("output", {})

            output_path = output_config.get("path")
            if output_path:
                if output_path in output_paths:
                    warnings.append(
                        f"Output path conflict: batch nodes '{output_paths[output_path]}' and "
                        f"'{batch_node}' both write to '{output_path}'"
                    )
                else:
                    output_paths[output_path] = batch_node

            if output_config.get("format") == "kafka":
                topic = output_config.get("options", {}).get("topic")
                if topic:
                    if topic in kafka_topics:
                        warnings.append(
                            f"Kafka topic conflict: batch nodes '{kafka_topics[topic]}' and "
                            f"'{batch_node}' both write to topic '{topic}'"
                        )
                    else:
                        kafka_topics[topic] = batch_node

        for streaming_node in streaming_nodes:
            node_config = node_configs.get(streaming_node, {})

            input_config = node_config.get("input", {})
            if input_config.get("format") == "file_stream":
                input_path = input_config.get("options", {}).get("path")
                if input_path and input_path in output_paths:
                    pass

            output_config = node_config.get("output", {})
            output_path = output_config.get("path")
            if output_path:
                if output_path in output_paths:
                    warnings.append(
                        f"Output path conflict: batch node '{output_paths[output_path]}' and "
                        f"streaming node '{streaming_node}' both write to '{output_path}'"
                    )

            kafka_options = [
                input_config.get("options", {}),
                output_config.get("options", {}),
            ]
            for options in kafka_options:
                if options.get("topic"):
                    topic = options["topic"]
                    if topic in kafka_topics:
                        warnings.append(
                            f"Kafka topic conflict: batch node '{kafka_topics[topic]}' and "
                            f"streaming node '{streaming_node}' both use topic '{topic}'"
                        )

        return warnings

    @staticmethod
    def _validate_streaming_requirements(
        streaming_nodes: List[str],
        node_configs: Dict[str, Dict[str, Any]],
        policy: FormatPolicy,
    ) -> List[str]:
        """Valida requerimientos específicos para nodos streaming."""

        errors: List[str] = []

        for streaming_node in streaming_nodes:
            node_config = node_configs.get(streaming_node, {})

            input_config = node_config.get("input", {})
            if not input_config:
                errors.append(
                    f"Streaming node '{streaming_node}' must have input configuration"
                )
                continue

            input_format = input_config.get("format")
            if not input_format:
                errors.append(
                    f"Streaming node '{streaming_node}' must specify input format"
                )
                continue

            if not policy.is_supported_input(input_format):
                errors.append(
                    f"Streaming node '{streaming_node}' has invalid input format '{input_format}'. "
                    f"Valid formats: {policy.get_supported_input_formats()}"
                )

            output_config = node_config.get("output", {})
            if not output_config:
                errors.append(
                    f"Streaming node '{streaming_node}' must have output configuration"
                )
                continue

            output_format = output_config.get("format")
            if not output_format:
                errors.append(
                    f"Streaming node '{streaming_node}' must specify output format"
                )

            streaming_config = node_config.get("streaming", {})
            if input_format in policy.checkpoint_required_inputs:
                checkpoint = streaming_config.get("checkpoint_location")
                if not checkpoint:
                    errors.append(
                        f"Streaming node '{streaming_node}' with format '{input_format}' "
                        f"must specify checkpoint_location"
                    )

        return errors

    @staticmethod
    def _get_node_dependencies(node_config: Dict[str, Any]) -> List[str]:
        """Extrae dependencias de la configuración del nodo."""
        dependencies = node_config.get("dependencies", [])

        if dependencies is None:
            return []
        elif isinstance(dependencies, str):
            return [dependencies]
        elif isinstance(dependencies, dict):
            return list(dependencies.keys())
        elif isinstance(dependencies, list):
            normalized_deps = []
            for dep in dependencies:
                if isinstance(dep, str):
                    normalized_deps.append(dep)
                elif isinstance(dep, dict) and len(dep) == 1:
                    normalized_deps.append(list(dep.keys())[0])
                else:
                    raise ValueError(f"Invalid dependency format: {dep}")
            return normalized_deps
        else:
            return [str(dependencies)]

    @staticmethod
    def validate_dataframe_schema(result_df: Any) -> None:
        """Valida que el DataFrame resultado tenga un esquema no vacío."""
        if result_df is None:
            raise ValueError("Result DataFrame is None")

        if hasattr(result_df, "schema") and hasattr(result_df.schema, "fields"):
            if not result_df.schema.fields:
                raise ValueError("Spark DataFrame schema is empty - no fields defined")
            if result_df.rdd.isEmpty():
                logger.warning("Spark DataFrame has no rows")
            return

        if hasattr(result_df, "columns") and hasattr(result_df, "empty"):
            if result_df.empty:
                logger.warning("Pandas DataFrame is empty (no rows)")
            if not list(result_df.columns):
                raise ValueError("Pandas DataFrame has no columns defined")
            return

        if hasattr(result_df, "columns"):
            if not result_df.columns:
                raise ValueError("DataFrame has no columns defined")
            return

        raise ValueError(
            f"Unsupported DataFrame type: {type(result_df)}. "
            "Expected Spark or Pandas DataFrame with schema/columns."
        )
