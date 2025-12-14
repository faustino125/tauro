"""
Copyright (c) 2025 Faustino Lopez Ramos. 
For licensing information, see the LICENSE file in the project root
"""
from typing import Any, Dict, List, Optional, Tuple
from loguru import logger  # type: ignore

from core.config.validators import FormatPolicy


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
        requires_dates: bool = True,
    ) -> None:
        """Validate required parameters for pipeline execution."""
        if not pipeline_name:
            raise ValueError("Pipeline name is required")

        logger.debug(
            f"Validating params for '{pipeline_name}': requires_dates={requires_dates}, start_date={start_date}, end_date={end_date}, context_start_date={context_start_date}, context_end_date={context_end_date}"
        )

        if requires_dates:
            if not (start_date or context_start_date):
                raise ValueError("Start date is required")
            if not (end_date or context_end_date):
                raise ValueError("End date is required")

    @staticmethod
    def validate_pipeline_config(pipeline: Dict[str, Any]) -> None:
        """Validate basic pipeline configuration."""
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
        """Validate that all pipeline nodes have configurations."""
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
        """Validate hybrid pipeline and return detailed analysis.

        ER3 FIX: Now validates format compatibility bidirectionally.
        """
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

            # ER3 FIX: Add bidirectional format validation for hybrid pipelines
            bidirectional_errors = PipelineValidator._validate_hybrid_format_compatibility(
                batch_nodes, streaming_nodes, node_configs, policy
            )
            validation_result["errors"].extend(bidirectional_errors)

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
            validation_result["errors"].append(f"Validation failed with error: {str(e)}")

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
            for batch_dep in batch_deps:
                batch_config = node_configs.get(batch_dep, {})
                # Delegate pairwise checks to helper to reduce cognitive complexity
                PipelineValidator._check_batch_stream_compatibility(
                    batch_dep,
                    batch_config,
                    streaming_node,
                    streaming_config,
                    policy,
                    format_issues,
                )

        return format_issues

    @staticmethod
    def _check_batch_stream_compatibility(
        batch_dep: str,
        batch_config: Dict[str, Any],
        streaming_node: str,
        streaming_config: Dict[str, Any],
        policy: FormatPolicy,
        issues: List[Dict[str, Any]],
    ) -> None:
        """Helper that checks format compatibility and file_stream path mismatches for a batch/stream pair."""
        batch_output = batch_config.get("output", {})
        batch_format = batch_output.get("format")

        streaming_input = streaming_config.get("input", {})
        streaming_format = streaming_input.get("format")

        # Check explicit format compatibility
        if batch_format and streaming_format:
            if policy.are_compatible(batch_format, streaming_format):
                issues.append(
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
                issues.append(
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

        # Check file_stream path coordination
        if streaming_format == "file_stream":
            batch_path = batch_output.get("path")
            streaming_path = streaming_input.get("options", {}).get("path")
            if batch_path and streaming_path and batch_path != streaming_path:
                issues.append(
                    {
                        "severity": "warning",
                        "message": f"Path mismatch: batch node '{batch_dep}' writes to "
                        f"'{batch_path}' but streaming node '{streaming_node}' reads from "
                        f"'{streaming_path}'. Ensure paths are coordinated.",
                        "batch_node": batch_dep,
                        "streaming_node": streaming_node,
                    }
                )

    @staticmethod
    def _validate_resource_conflicts(
        batch_nodes: List[str],
        streaming_nodes: List[str],
        node_configs: Dict[str, Dict[str, Any]],
    ) -> List[str]:
        """Valida conflictos de recursos entre nodos batch y streaming."""
        # Collect batch-side resources and any immediate batch conflicts
        (
            output_paths,
            kafka_topics,
            warnings,
        ) = PipelineValidator._collect_batch_resources(batch_nodes, node_configs)

        # Check streaming nodes against collected batch resources
        for streaming_node in streaming_nodes:
            node_config = node_configs.get(streaming_node, {})
            warnings.extend(
                PipelineValidator._check_streaming_conflicts_for_node(
                    streaming_node, node_config, output_paths, kafka_topics
                )
            )

        return warnings

    @staticmethod
    def _collect_batch_resources(
        batch_nodes: List[str], node_configs: Dict[str, Dict[str, Any]]
    ) -> Tuple[Dict[str, str], Dict[str, str], List[str]]:
        """Collects output paths and kafka topics used by batch nodes and reports conflicts."""
        output_paths: Dict[str, str] = {}
        kafka_topics: Dict[str, str] = {}
        warnings: List[str] = []

        def _register_output_path(path: str, node: str) -> None:
            """Register an output path and record a warning if already used."""
            existing = output_paths.get(path)
            if existing:
                warnings.append(
                    f"Output path conflict: batch nodes '{existing}' and "
                    f"'{node}' both write to '{path}'"
                )
            else:
                output_paths[path] = node

        def _register_kafka_topic(topic: str, node: str) -> None:
            """Register a kafka topic and record a warning if already used."""
            existing = kafka_topics.get(topic)
            if existing:
                warnings.append(
                    f"Kafka topic conflict: batch nodes '{existing}' and "
                    f"'{node}' both write to topic '{topic}'"
                )
            else:
                kafka_topics[topic] = node

        for batch_node in batch_nodes:
            node_config = node_configs.get(batch_node, {})
            output_config = node_config.get("output", {})

            output_path = output_config.get("path")
            if output_path:
                _register_output_path(output_path, batch_node)

            if output_config.get("format") == "kafka":
                topic = output_config.get("options", {}).get("topic")
                if topic:
                    _register_kafka_topic(topic, batch_node)

        return output_paths, kafka_topics, warnings

    @staticmethod
    def _check_streaming_conflicts_for_node(
        streaming_node: str,
        node_config: Dict[str, Any],
        output_paths: Dict[str, str],
        kafka_topics: Dict[str, str],
    ) -> List[str]:
        """Checks a single streaming node for conflicts against batch resources."""
        warnings: List[str] = []

        input_config = node_config.get("input", {})
        # Preserve original behavior: no-op for file_stream input path matching
        if input_config.get("format") == "file_stream":
            input_path = input_config.get("options", {}).get("path")
            if input_path and input_path in output_paths:
                # Intentionally no warning here per original logic (kept for compatibility)
                pass

        output_config = node_config.get("output", {})
        output_path = output_config.get("path")
        if output_path and output_path in output_paths:
            warnings.append(
                f"Output path conflict: batch node '{output_paths[output_path]}' and "
                f"streaming node '{streaming_node}' both write to '{output_path}'"
            )

        kafka_options = [
            input_config.get("options", {}),
            output_config.get("options", {}),
        ]
        for options in kafka_options:
            topic = options.get("topic")
            if topic and topic in kafka_topics:
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
        """Validate specific requirements for streaming nodes."""
        errors: List[str] = []
        for streaming_node in streaming_nodes:
            node_config = node_configs.get(streaming_node, {})
            errors.extend(
                PipelineValidator._validate_single_streaming_node(
                    streaming_node, node_config, policy
                )
            )
        return errors

    @staticmethod
    def _validate_single_streaming_node(
        streaming_node: str,
        node_config: Dict[str, Any],
        policy: FormatPolicy,
    ) -> List[str]:
        """Validate a single streaming node and return list of errors."""
        node_errors: List[str] = []

        input_config = node_config.get("input", {})
        if not input_config:
            node_errors.append(f"Streaming node '{streaming_node}' must have input configuration")
            return node_errors

        input_format = input_config.get("format")
        if not input_format:
            node_errors.append(f"Streaming node '{streaming_node}' must specify input format")
            return node_errors

        if not policy.is_supported_input(input_format):
            node_errors.append(
                f"Streaming node '{streaming_node}' has invalid input format '{input_format}'. "
                f"Valid formats: {policy.get_supported_input_formats()}"
            )

        output_config = node_config.get("output", {})
        if not output_config:
            node_errors.append(f"Streaming node '{streaming_node}' must have output configuration")
            return node_errors

        output_format = output_config.get("format")
        if not output_format:
            node_errors.append(f"Streaming node '{streaming_node}' must specify output format")

        if input_format in policy.checkpoint_required_inputs:
            streaming_config = node_config.get("streaming", {})
            checkpoint = streaming_config.get("checkpoint_location")
            if not checkpoint:
                node_errors.append(
                    f"Streaming node '{streaming_node}' with format '{input_format}' "
                    f"must specify checkpoint_location"
                )

        return node_errors

    @staticmethod
    def _get_node_dependencies(node_config: Dict[str, Any]) -> List[str]:
        """Extract dependencies from node configuration."""
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
    def _validate_hybrid_format_compatibility(
        batch_nodes: List[str],
        streaming_nodes: List[str],
        node_configs: Dict[str, Dict[str, Any]],
        policy: FormatPolicy,
    ) -> List[str]:
        """Validate format compatibility in hybrid pipelines bidirectionally.

        ER3 FIX: Validates that when streaming nodes depend on batch nodes,
        the output format of batch nodes is compatible with input format of streaming nodes.
        """
        errors = []

        # Check streaming nodes that depend on batch nodes
        for streaming_node in streaming_nodes:
            streaming_config = node_configs.get(streaming_node, {})
            streaming_input = streaming_config.get("input", {})
            streaming_format = streaming_input.get("format", "").lower()

            dependencies = PipelineValidator._get_node_dependencies(streaming_config)
            batch_deps = [dep for dep in dependencies if dep in batch_nodes]

            for batch_dep in batch_deps:
                batch_config = node_configs.get(batch_dep, {})
                batch_output = batch_config.get("output", {})
                batch_format = batch_output.get("format", "").lower()

                errors.extend(
                    PipelineValidator._check_hybrid_format_pair(
                        batch_dep, batch_format, streaming_node, streaming_format, policy
                    )
                )

        return errors

    @staticmethod
    def _check_hybrid_format_pair(
        batch_node: str,
        batch_format: str,
        streaming_node: str,
        streaming_format: str,
        policy: FormatPolicy,
    ) -> List[str]:
        """Check format compatibility between a batch and streaming node pair."""
        errors = []

        if batch_format and streaming_format:
            if not policy.are_compatible(batch_format, streaming_format):
                errors.append(
                    f"Incompatible format in hybrid pipeline: batch node '{batch_node}' "
                    f"outputs format '{batch_format}' but streaming node '{streaming_node}' "
                    f"expects format '{streaming_format}'. "
                    f"Please ensure output format from batch matches expected input format for streaming."
                )
            else:
                logger.debug(
                    f"Format compatibility OK: {batch_node} ({batch_format}) -> "
                    f"{streaming_node} ({streaming_format})"
                )
        else:
            PipelineValidator._log_missing_formats(
                batch_node, batch_format, streaming_node, streaming_format
            )

        return errors

    @staticmethod
    def _log_missing_formats(
        batch_node: str,
        batch_format: str,
        streaming_node: str,
        streaming_format: str,
    ) -> None:
        """Log warnings for missing format specifications."""
        if not batch_format:
            logger.warning(
                f"Batch node '{batch_node}' output format not specified. "
                f"Cannot validate compatibility with streaming node '{streaming_node}'."
            )
        if not streaming_format:
            logger.warning(
                f"Streaming node '{streaming_node}' input format not specified. "
                f"Cannot validate compatibility with batch node '{batch_node}'."
            )

    @staticmethod
    def validate_dataframe_schema(result_df: Any) -> None:
        """Validate that the result DataFrame has a non-empty schema."""
        if result_df is None:
            raise ValueError("Result DataFrame is None")

        # Spark-like DataFrame
        if hasattr(result_df, "schema") and hasattr(result_df.schema, "fields"):
            PipelineValidator._validate_spark_df(result_df)
            return

        # Pandas-like DataFrame
        if hasattr(result_df, "columns") and hasattr(result_df, "empty"):
            PipelineValidator._validate_pandas_df(result_df)
            return

        # Generic object with columns
        if hasattr(result_df, "columns"):
            if not result_df.columns:
                raise ValueError("DataFrame has no columns defined")
            return

        raise ValueError(
            f"Unsupported DataFrame type: {type(result_df)}. "
            "Expected Spark or Pandas DataFrame with schema/columns."
        )

    @staticmethod
    def _validate_spark_df(result_df: Any) -> None:
        """Validate Spark DataFrame schema and warn if empty."""
        if not result_df.schema.fields:
            raise ValueError("Spark DataFrame schema is empty - no fields defined")
        try:
            if result_df.limit(1).count() == 0:
                logger.warning("Spark DataFrame has no rows")
        except Exception as e:
            logger.warning(f"Could not check row count: {e}")

    @staticmethod
    def _validate_pandas_df(result_df: Any) -> None:
        """Validate Pandas DataFrame columns and warn if empty."""
        if result_df.empty:
            logger.warning("Pandas DataFrame is empty (no rows)")
        if not list(result_df.columns):
            raise ValueError("Pandas DataFrame has no columns defined")
