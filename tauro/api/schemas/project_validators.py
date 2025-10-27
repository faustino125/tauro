"""Custom Business Logic Validators for Tauro API

Validators for project, pipeline, and run business rules.
"""

from typing import List, Dict, Any, Set
from core.api.schemas.models import (
    ProjectCreate,
    PipelineConfig,
    NodeConfig,
    GlobalSettings,
)
from loguru import logger


class ProjectValidator:
    """Validates project configurations"""

    @staticmethod
    def validate_project_creation(project_data: ProjectCreate) -> None:
        """
        Validate project creation data

        Args:
            project_data: ProjectCreate instance

        Raises:
            ValueError: If validation fails
        """
        # Validate global settings
        ProjectValidator.validate_global_settings(project_data.global_settings)

        # Validate pipelines
        if project_data.pipelines:
            ProjectValidator.validate_pipelines(project_data.pipelines)

        # Validate tags
        if project_data.tags:
            ProjectValidator.validate_tags(project_data.tags)

        logger.debug(f"Project '{project_data.name}' validation passed")

    @staticmethod
    def validate_global_settings(settings: GlobalSettings) -> None:
        """
        Validate global settings

        Args:
            settings: GlobalSettings instance

        Raises:
            ValueError: If validation fails
        """
        # Paths cannot be the same
        if settings.input_path == settings.output_path:
            raise ValueError("input_path and output_path cannot be the same")

        # Paths must be non-empty
        if not settings.input_path.strip():
            raise ValueError("input_path cannot be empty")
        if not settings.output_path.strip():
            raise ValueError("output_path cannot be empty")

        # Validate retry policy if present
        if settings.retry_policy:
            if settings.retry_policy.initial_delay > settings.retry_policy.max_delay:
                raise ValueError(
                    "retry_policy.initial_delay cannot be greater than max_delay"
                )

    @staticmethod
    def validate_pipelines(pipelines: List[PipelineConfig]) -> None:
        """
        Validate pipelines list

        Args:
            pipelines: List of PipelineConfig

        Raises:
            ValueError: If validation fails
        """
        # Validate pipeline names are unique
        pipeline_names = [p.name for p in pipelines]
        if len(pipeline_names) != len(set(pipeline_names)):
            raise ValueError("Pipeline names must be unique within project")

        # Validate each pipeline
        for pipeline in pipelines:
            PipelineValidator.validate_pipeline(pipeline)

    @staticmethod
    def validate_tags(tags: Dict[str, str]) -> None:
        """
        Validate tags

        Args:
            tags: Tags dictionary

        Raises:
            ValueError: If validation fails
        """
        if len(tags) > 100:
            raise ValueError("Cannot have more than 100 tags")

        for key, value in tags.items():
            if not key or len(key) > 100:
                raise ValueError(f"Tag key must be 1-100 characters: {key}")
            if not value or len(value) > 100:
                raise ValueError(f"Tag value must be 1-100 characters: {value}")


class PipelineValidator:
    """Validates pipeline configurations"""

    @staticmethod
    def validate_pipeline(pipeline: PipelineConfig) -> None:
        """
        Validate pipeline configuration

        Args:
            pipeline: PipelineConfig instance

        Raises:
            ValueError: If validation fails
        """
        # Validate nodes
        PipelineValidator.validate_nodes(pipeline.nodes)

        # Validate inputs/outputs
        PipelineValidator.validate_inputs_outputs(pipeline.inputs, pipeline.outputs)

        # Validate node dependencies
        PipelineValidator.validate_dependencies(pipeline.nodes)

        logger.debug(f"Pipeline '{pipeline.name}' validation passed")

    @staticmethod
    def validate_nodes(nodes: List[NodeConfig]) -> None:
        """
        Validate nodes list

        Args:
            nodes: List of NodeConfig

        Raises:
            ValueError: If validation fails
        """
        if not nodes:
            raise ValueError("Pipeline must have at least one node")

        # Validate node names are unique
        node_names = [n.name for n in nodes]
        if len(node_names) != len(set(node_names)):
            raise ValueError("Node names must be unique within pipeline")

        # Validate each node
        for node in nodes:
            NodeValidator.validate_node(node)

    @staticmethod
    def validate_inputs_outputs(inputs: List[Any], outputs: List[Any]) -> None:
        """
        Validate inputs and outputs

        Args:
            inputs: List of inputs
            outputs: List of outputs

        Raises:
            ValueError: If validation fails
        """
        # Validate input/output names are unique
        input_names = [i.name for i in inputs] if inputs else []
        output_names = [o.name for o in outputs] if outputs else []

        if len(input_names) != len(set(input_names)):
            raise ValueError("Input names must be unique within pipeline")

        if len(output_names) != len(set(output_names)):
            raise ValueError("Output names must be unique within pipeline")

        # Inputs and outputs cannot have the same name
        if set(input_names) & set(output_names):
            raise ValueError("Input and output names cannot be the same")

    @staticmethod
    def validate_dependencies(nodes: List[NodeConfig]) -> None:
        """
        Validate node dependencies (no cycles, valid references)

        Args:
            nodes: List of NodeConfig

        Raises:
            ValueError: If validation fails
        """
        node_names: Set[str] = {n.name for n in nodes}
        node_dict = {n.name: n for n in nodes}

        # Check all dependencies reference valid nodes
        PipelineValidator._check_valid_references(nodes, node_names)

        # Check for cycles using DFS
        PipelineValidator._check_cycles(node_names, node_dict)

    @staticmethod
    def _check_valid_references(nodes: List[NodeConfig], node_names: Set[str]) -> None:
        """Check all dependencies reference valid nodes"""
        for node in nodes:
            if node.dependencies:
                for dep in node.dependencies:
                    if dep not in node_names:
                        raise ValueError(
                            f"Node '{node.name}' references non-existent "
                            f"dependency '{dep}'"
                        )

    @staticmethod
    def _check_cycles(node_names: Set[str], node_dict: Dict[str, NodeConfig]) -> None:
        """Check for circular dependencies using DFS"""
        visited: Set[str] = set()
        rec_stack: Set[str] = set()

        def visit_node(node_name: str) -> bool:
            """Visit node, return True if cycle detected"""
            if node_name in rec_stack:
                return True
            if node_name in visited:
                return False

            visited.add(node_name)
            rec_stack.add(node_name)

            node = node_dict[node_name]
            for dep in node.dependencies or []:
                if visit_node(dep):
                    return True

            rec_stack.discard(node_name)
            return False

        for node_name in node_names:
            if node_name not in visited and visit_node(node_name):
                raise ValueError(f"Circular dependency at '{node_name}'")


class NodeValidator:
    """Validates node configurations"""

    @staticmethod
    def validate_node(node: NodeConfig) -> None:
        """
        Validate node configuration

        Args:
            node: NodeConfig instance

        Raises:
            ValueError: If validation fails
        """
        # Validate implementation is not empty
        if not node.implementation or not node.implementation.strip():
            raise ValueError(f"Node '{node.name}' must have an implementation")

        # Validate config is a dict
        if not isinstance(node.config, dict):
            raise ValueError(f"Node '{node.name}' config must be a dictionary")

        logger.debug(f"Node '{node.name}' validation passed")


class RunValidator:
    """Validates run configurations"""

    @staticmethod
    def validate_run_params(params: Dict[str, Any]) -> None:
        """
        Validate run parameters

        Args:
            params: Run parameters dictionary

        Raises:
            ValueError: If validation fails
        """
        if not isinstance(params, dict):
            raise ValueError("Run parameters must be a dictionary")

        # Check parameter keys don't start with underscore
        for key in params.keys():
            if key.startswith("_"):
                raise ValueError(f"Parameter name '{key}' cannot start with underscore")


class ScheduleValidator:
    """Validates schedule configurations"""

    @staticmethod
    def validate_cron_expression(expression: str) -> None:
        """
        Validate CRON expression

        Args:
            expression: CRON expression string

        Raises:
            ValueError: If validation fails
        """
        if not expression or not expression.strip():
            raise ValueError("CRON expression cannot be empty")

        parts = expression.split()
        if len(parts) != 5:
            raise ValueError(
                "CRON expression must have exactly 5 parts "
                "(minute hour day month day_of_week)"
            )

        # Validate each part is valid (simplified check)
        for i, part in enumerate(parts):
            if part == "*":
                continue

            # Check if it's a valid number or range
            try:
                if "-" in part:
                    start, end = part.split("-")
                    int(start)
                    int(end)
                elif "," in part:
                    for p in part.split(","):
                        int(p)
                else:
                    int(part)
            except ValueError:
                raise ValueError(
                    f"Invalid value in CRON expression at position {i}: {part}"
                )

    @staticmethod
    def validate_interval_expression(expression: str) -> None:
        """
        Validate interval expression (e.g., '1h', '30m')

        Args:
            expression: Interval expression string

        Raises:
            ValueError: If validation fails
        """
        if not expression or not expression.strip():
            raise ValueError("Interval expression cannot be empty")

        # Simple validation: number + unit
        valid_units = ["s", "m", "h", "d", "w"]
        unit = expression[-1] if expression else ""

        if unit not in valid_units:
            raise ValueError(
                f"Interval unit must be one of {valid_units}: {expression}"
            )

        try:
            value = int(expression[:-1])
            if value <= 0:
                raise ValueError("Interval value must be positive")
        except ValueError:
            raise ValueError(f"Invalid interval expression: {expression}")
