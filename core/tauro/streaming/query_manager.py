import os
import time
from typing import Any, Dict, Optional
from pathlib import Path

from loguru import logger  # type: ignore
from pyspark.sql import DataFrame  # type: ignore
from pyspark.sql.streaming import StreamingQuery  # type: ignore

from tauro.streaming.constants import (
    DEFAULT_STREAMING_CONFIG,
    StreamingTrigger,
    StreamingOutputMode,
    StreamingFormat,
)
from tauro.streaming.readers import StreamingReaderFactory
from tauro.streaming.writers import StreamingWriterFactory
from tauro.streaming.validators import StreamingValidator
from tauro.streaming.exceptions import (
    StreamingQueryError,
    StreamingConfigurationError,
    StreamingError,
    handle_streaming_error,
    create_error_context,
)


class StreamingQueryManager:
    """Manages individual streaming queries with lifecycle and configuration."""

    def __init__(self, context):
        self.context = context
        self.reader_factory = StreamingReaderFactory(context)
        self.writer_factory = StreamingWriterFactory(context)
        self.validator = StreamingValidator()
        self._active_queries = {}  # Track active queries

    @handle_streaming_error
    def create_and_start_query(
        self, node_config: Dict[str, Any], execution_id: str, pipeline_name: str
    ) -> StreamingQuery:
        """Create and start a streaming query from node configuration."""
        try:
            # Validate node configuration
            self.validator.validate_streaming_node_config(node_config)

            node_name = node_config.get("name", "unknown")
            logger.info(f"Creating streaming query for node '{node_name}'")

            # Load input streaming DataFrame
            input_df = self._load_streaming_input(node_config)

            # Apply transformations if specified
            transformed_df = self._apply_transformations(input_df, node_config)

            # Configure and start streaming query
            query = self._configure_and_start_query(
                transformed_df, node_config, execution_id, pipeline_name
            )

            # Track the query
            query_key = f"{execution_id}_{node_name}"
            self._active_queries[query_key] = {
                "query": query,
                "node_config": node_config,
                "start_time": time.time(),
                "execution_id": execution_id,
                "pipeline_name": pipeline_name,
            }

            logger.info(f"Streaming query '{node_name}' started with ID: {query.id}")
            return query

        except Exception as e:
            context = create_error_context(
                operation="create_and_start_query",
                component="StreamingQueryManager",
                node_name=node_config.get("name", "unknown"),
                execution_id=execution_id,
                pipeline_name=pipeline_name,
            )

            if isinstance(e, StreamingError):
                e.add_context("operation_context", context)
                raise
            else:
                raise StreamingQueryError(
                    f"Failed to create streaming query: {str(e)}",
                    context=context,
                    cause=e,
                )

    def _load_streaming_input(self, node_config: Dict[str, Any]) -> DataFrame:
        """Load streaming input DataFrame with error handling."""
        try:
            input_config = node_config.get("input", {})
            if not input_config:
                raise StreamingConfigurationError(
                    "Streaming node must have input configuration",
                    config_section="input",
                )

            # Get streaming format and source configuration
            format_type = input_config.get("format")
            if not format_type:
                raise StreamingConfigurationError(
                    "Streaming input must specify format", config_section="input.format"
                )

            # Use factory to create appropriate reader
            reader = self.reader_factory.get_reader(format_type)

            # Load streaming DataFrame
            streaming_df = reader.read_stream(input_config)

            # Apply watermarking if configured
            watermark_config = input_config.get("watermark")
            if watermark_config:
                streaming_df = self._apply_watermark(streaming_df, watermark_config)

            return streaming_df

        except Exception as e:
            logger.error(f"Error loading streaming input: {str(e)}")
            if isinstance(e, StreamingError):
                raise
            else:
                raise StreamingError(
                    f"Failed to load streaming input: {str(e)}",
                    error_code="INPUT_LOAD_ERROR",
                    cause=e,
                )

    def _apply_watermark(
        self, streaming_df: DataFrame, watermark_config: Dict[str, Any]
    ) -> DataFrame:
        """Apply watermarking with validation."""
        try:
            timestamp_col = watermark_config.get("column")
            delay_threshold = watermark_config.get("delay", "10 seconds")

            if not timestamp_col:
                raise StreamingConfigurationError(
                    "Watermark configuration must specify 'column'",
                    config_section="watermark.column",
                )

            # Validate that the column exists
            if timestamp_col not in streaming_df.columns:
                available_cols = streaming_df.columns
                raise StreamingConfigurationError(
                    f"Watermark column '{timestamp_col}' not found in DataFrame. Available columns: {available_cols}",
                    config_section="watermark.column",
                    config_value=timestamp_col,
                )

            logger.info(
                f"Applying watermark on column '{timestamp_col}' with delay '{delay_threshold}'"
            )

            return streaming_df.withWatermark(timestamp_col, delay_threshold)

        except Exception as e:
            logger.error(f"Error applying watermark: {str(e)}")
            if isinstance(e, StreamingError):
                raise
            else:
                raise StreamingError(
                    f"Failed to apply watermark: {str(e)}",
                    error_code="WATERMARK_ERROR",
                    cause=e,
                )

    def _apply_transformations(
        self, input_df: DataFrame, node_config: Dict[str, Any]
    ) -> DataFrame:
        """Apply transformations to the streaming DataFrame with error handling."""
        try:
            # Get transformation function if specified
            function_config = node_config.get("function")
            if not function_config:
                logger.info(
                    "No transformation function specified, using input DataFrame as-is"
                )
                return input_df

            # Load and execute transformation function
            module_path = function_config.get("module")
            function_name = function_config.get("function")

            if not module_path or not function_name:
                raise StreamingConfigurationError(
                    "Function configuration must specify both module and function",
                    config_section="function",
                    config_value=function_config,
                )

            import importlib

            try:
                module = importlib.import_module(module_path)
            except ImportError as e:
                raise StreamingError(
                    f"Cannot import module '{module_path}': {str(e)}",
                    error_code="MODULE_IMPORT_ERROR",
                    context={"module_path": module_path},
                    cause=e,
                )

            if not hasattr(module, function_name):
                available_functions = [
                    attr for attr in dir(module) if callable(getattr(module, attr))
                ]
                raise StreamingError(
                    f"Function '{function_name}' not found in module '{module_path}'. Available functions: {available_functions[:10]}",
                    error_code="FUNCTION_NOT_FOUND",
                    context={
                        "module_path": module_path,
                        "function_name": function_name,
                    },
                )

            transform_func = getattr(module, function_name)

            logger.info(
                f"Applying transformation function '{function_name}' from '{module_path}'"
            )

            # Call transformation function with error handling
            try:
                transformed_df = transform_func(input_df, node_config)
            except Exception as e:
                raise StreamingError(
                    f"Error executing transformation function '{function_name}': {str(e)}",
                    error_code="TRANSFORMATION_ERROR",
                    context={
                        "module_path": module_path,
                        "function_name": function_name,
                    },
                    cause=e,
                )

            if not isinstance(transformed_df, DataFrame):
                raise StreamingError(
                    f"Transformation function must return a DataFrame, got {type(transformed_df)}",
                    error_code="INVALID_RETURN_TYPE",
                    context={
                        "function_name": function_name,
                        "return_type": str(type(transformed_df)),
                    },
                )

            return transformed_df

        except Exception as e:
            logger.error(f"Error applying transformation: {str(e)}")
            if isinstance(e, StreamingError):
                raise
            else:
                raise StreamingError(
                    f"Failed to apply transformation: {str(e)}",
                    error_code="TRANSFORMATION_FAILURE",
                    cause=e,
                )

    def _configure_and_start_query(
        self,
        df: DataFrame,
        node_config: Dict[str, Any],
        execution_id: str,
        pipeline_name: str,
    ) -> StreamingQuery:
        """Configure and start the streaming query with comprehensive error handling."""
        try:
            # Get output configuration
            output_config = node_config.get("output", {})
            if not output_config:
                raise StreamingConfigurationError(
                    "Streaming node must have output configuration",
                    config_section="output",
                )

            # Merge with default streaming configuration
            streaming_config = {**DEFAULT_STREAMING_CONFIG}
            streaming_config.update(node_config.get("streaming", {}))

            # Configure query name
            node_name = node_config.get("name", "unknown")
            query_name = (
                streaming_config.get("query_name")
                or f"{pipeline_name}_{node_name}_{execution_id}"
            )

            # Configure checkpoint location
            checkpoint_location = self._get_checkpoint_location(
                streaming_config.get("checkpoint_location"),
                pipeline_name,
                node_name,
                execution_id,
            )

            # Get output mode
            output_mode = streaming_config.get(
                "output_mode", StreamingOutputMode.APPEND.value
            )

            # Configure trigger
            trigger_config = streaming_config.get("trigger", {})
            trigger = self._configure_trigger(trigger_config)

            logger.info(f"Configuring streaming query '{query_name}':")
            logger.info(f"  - Output mode: {output_mode}")
            logger.info(f"  - Trigger: {trigger_config}")
            logger.info(f"  - Checkpoint: {checkpoint_location}")

            # Create write stream
            write_stream = (
                df.writeStream.outputMode(output_mode)
                .queryName(query_name)
                .option("checkpointLocation", checkpoint_location)
            )

            # Apply trigger
            if trigger:
                write_stream = write_stream.trigger(**trigger)

            # Configure output sink using factory
            output_format = output_config.get("format")
            if not output_format:
                raise StreamingConfigurationError(
                    "Output configuration must specify format",
                    config_section="output.format",
                )

            writer = self.writer_factory.get_writer(output_format)
            query = writer.write_stream(write_stream, output_config)

            return query

        except Exception as e:
            logger.error(f"Error configuring streaming query: {str(e)}")
            if isinstance(e, StreamingError):
                raise
            else:
                raise StreamingQueryError(
                    f"Failed to configure streaming query: {str(e)}",
                    query_name=node_config.get("name", "unknown"),
                    cause=e,
                )

    def _get_checkpoint_location(
        self,
        base_checkpoint: Optional[str],
        pipeline_name: str,
        node_name: str,
        execution_id: str,
    ) -> str:
        """Get checkpoint location for the streaming query with validation."""
        try:
            if base_checkpoint:
                checkpoint_base = base_checkpoint
            else:
                # Use context output path or default
                output_path = getattr(self.context, "output_path", "/tmp/checkpoints")
                checkpoint_base = os.path.join(output_path, "streaming_checkpoints")

            # Create unique checkpoint path
            checkpoint_path = os.path.join(
                checkpoint_base, pipeline_name, node_name, execution_id
            )

            # Ensure directory exists for local paths
            if not checkpoint_path.startswith(("s3://", "gs://", "abfs://", "hdfs://")):
                try:
                    Path(checkpoint_path).mkdir(parents=True, exist_ok=True)
                except OSError as e:
                    raise StreamingError(
                        f"Cannot create checkpoint directory '{checkpoint_path}': {str(e)}",
                        error_code="CHECKPOINT_CREATION_ERROR",
                        context={"checkpoint_path": checkpoint_path},
                        cause=e,
                    )

            return checkpoint_path

        except Exception as e:
            logger.error(f"Error setting up checkpoint location: {str(e)}")
            if isinstance(e, StreamingError):
                raise
            else:
                raise StreamingError(
                    f"Failed to setup checkpoint location: {str(e)}",
                    error_code="CHECKPOINT_SETUP_ERROR",
                    cause=e,
                )

    def _configure_trigger(
        self, trigger_config: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        """Configure streaming trigger with validation."""
        try:
            trigger_type = trigger_config.get(
                "type", StreamingTrigger.PROCESSING_TIME.value
            )

            # Validate trigger type
            valid_triggers = [t.value for t in StreamingTrigger]
            if trigger_type not in valid_triggers:
                raise StreamingConfigurationError(
                    f"Invalid trigger type '{trigger_type}'. Valid types: {valid_triggers}",
                    config_section="trigger.type",
                    config_value=trigger_type,
                )

            if trigger_type == StreamingTrigger.PROCESSING_TIME.value:
                interval = trigger_config.get("interval", "10 seconds")
                return {"processingTime": interval}

            elif trigger_type == StreamingTrigger.ONCE.value:
                return {"once": True}

            elif trigger_type == StreamingTrigger.CONTINUOUS.value:
                interval = trigger_config.get("interval", "1 second")
                return {"continuous": interval}

            elif trigger_type == StreamingTrigger.AVAILABLE_NOW.value:
                return {"availableNow": True}

            else:
                logger.warning(f"Unknown trigger type '{trigger_type}', using default")
                return {"processingTime": "10 seconds"}

        except Exception as e:
            logger.error(f"Error configuring trigger: {str(e)}")
            if isinstance(e, StreamingError):
                raise
            else:
                raise StreamingConfigurationError(
                    f"Failed to configure trigger: {str(e)}",
                    config_section="trigger",
                    cause=e,
                )

    def stop_query(
        self,
        query: StreamingQuery,
        graceful: bool = True,
        timeout_seconds: float = 30.0,
    ) -> bool:
        """Stop a streaming query with timeout and error handling."""
        try:
            if not query.isActive:
                logger.info(f"Query '{query.name}' is already stopped")
                return True

            logger.info(f"Stopping streaming query '{query.name}' (ID: {query.id})")

            start_time = time.time()
            query.stop()

            if graceful:
                # Wait for graceful shutdown with timeout
                while query.isActive and (time.time() - start_time) < timeout_seconds:
                    time.sleep(0.5)

                if query.isActive:
                    logger.warning(
                        f"Query '{query.name}' did not stop within {timeout_seconds}s timeout"
                    )
                    return False

            # Remove from active queries tracking
            query_key = None
            for key, info in self._active_queries.items():
                if info["query"].id == query.id:
                    query_key = key
                    break

            if query_key:
                del self._active_queries[query_key]

            logger.info(f"Query '{query.name}' stopped successfully")
            return True

        except Exception as e:
            logger.error(
                f"Error stopping query '{getattr(query, 'name', 'unknown')}': {str(e)}"
            )
            raise StreamingQueryError(
                f"Failed to stop query: {str(e)}",
                query_id=getattr(query, "id", None),
                query_name=getattr(query, "name", None),
                cause=e,
            )

    def get_query_progress(self, query: StreamingQuery) -> Optional[Dict[str, Any]]:
        """Get progress information for a streaming query with error handling."""
        try:
            if not query.isActive:
                return None

            progress = query.lastProgress
            return progress

        except Exception as e:
            logger.error(f"Error getting query progress: {str(e)}")
            raise StreamingQueryError(
                f"Failed to get query progress: {str(e)}",
                query_id=getattr(query, "id", None),
                query_name=getattr(query, "name", None),
                cause=e,
            )

    def get_active_queries(self) -> Dict[str, Dict[str, Any]]:
        """Get information about all active queries."""
        return self._active_queries.copy()

    def stop_all_queries(
        self, graceful: bool = True, timeout_seconds: float = 30.0
    ) -> Dict[str, bool]:
        """Stop all active queries and return results."""
        results = {}

        for query_key, query_info in list(self._active_queries.items()):
            try:
                query = query_info["query"]
                result = self.stop_query(query, graceful, timeout_seconds)
                results[query_key] = result
            except Exception as e:
                logger.error(f"Error stopping query {query_key}: {str(e)}")
                results[query_key] = False

        return results
