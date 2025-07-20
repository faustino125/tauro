from typing import Any, Dict, List, Optional

from loguru import logger  # type: ignore


class PipelineValidator:
    """Validates pipeline configurations and parameters."""

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
        """Validate pipeline configuration structure."""
        if not isinstance(pipeline, dict):
            raise ValueError("Pipeline configuration must be a dictionary")

        if "nodes" not in pipeline:
            raise ValueError("Pipeline must contain 'nodes' key")

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
    def validate_dataframe_schema(result_df: Any) -> None:
        """Validate that the result dataframe has a non-empty schema."""
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
