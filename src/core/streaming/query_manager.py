"""
Copyright (c) 2025 Faustino Lopez Ramos. 
For licensing information, see the LICENSE file in the project root
"""
import os
import sys
import time
import threading
import importlib
import importlib.util
from pathlib import Path
from typing import Any, Callable, Dict, Optional
from copy import deepcopy

from loguru import logger  # type: ignore

try:
    from pyspark.sql import DataFrame  # type: ignore
    from pyspark.sql.streaming import StreamingQuery  # type: ignore
    import pyspark.sql.functions as f  # type: ignore
except ImportError:
    DataFrame = Any  # type: ignore
    StreamingQuery = Any  # type: ignore
    f = None  # type: ignore

DEFAULT_PROCESSING_TIME_INTERVAL = "10 seconds"

from core.streaming.constants import (
    DEFAULT_STREAMING_CONFIG,
    STREAMING_VALIDATIONS,
    StreamingOutputMode,
    StreamingTrigger,
)
from core.streaming.exceptions import (
    StreamingConfigurationError,
    StreamingError,
    StreamingQueryError,
    create_error_context,
    handle_streaming_error,
)
from core.streaming.readers import StreamingReaderFactory
from core.streaming.validators import StreamingValidator
from core.streaming.writers import StreamingWriterFactory


class TransformationRegistry:
    """
    Secure registry for streaming transformations.
    """

    _registry: Dict[str, Callable[[DataFrame], DataFrame]] = {}
    _lock = threading.Lock()

    @classmethod
    def register(cls, key: str, func: Callable[[DataFrame], DataFrame]) -> None:
        """
        Register a transformation function.
        """
        if not key:
            raise ValueError("Transformation key cannot be empty")

        if not callable(func):
            raise TypeError(f"Transformation must be callable, got {type(func)}")

        with cls._lock:
            cls._registry[key] = func
            logger.debug(f"Registered transformation: {key}")

    @classmethod
    def get(cls, key: str) -> Callable[[DataFrame], DataFrame]:
        """
        Get a registered transformation function.
        """
        with cls._lock:
            if key not in cls._registry:
                available = list(cls._registry.keys())
                raise ValueError(
                    f"Transformation '{key}' not registered. " f"Available: {available}"
                )
            return cls._registry[key]

    @classmethod
    def list_transformations(cls) -> list:
        """List all registered transformations."""
        with cls._lock:
            return list(cls._registry.keys())

    @classmethod
    def unregister(cls, key: str) -> bool:
        """Unregister a transformation. Returns True if existed."""
        with cls._lock:
            if key in cls._registry:
                del cls._registry[key]
                logger.debug(f"Unregistered transformation: {key}")
                return True
            return False

    @classmethod
    def clear(cls) -> None:
        """Clear all registered transformations."""
        with cls._lock:
            cls._registry.clear()
            logger.debug("Cleared all transformations")


class StreamingQueryManager:
    """Manages individual streaming queries with lifecycle and configuration."""

    def __init__(self, context, validator: Optional[StreamingValidator] = None):
        self.context = context
        self.reader_factory = StreamingReaderFactory(context)
        self.writer_factory = StreamingWriterFactory(context)
        policy = getattr(context, "format_policy", None)
        self.validator = validator or StreamingValidator(policy)
        self._active_queries: Dict[str, Dict[str, Any]] = {}  # Track active queries
        self._active_queries_lock = threading.Lock()

    def _is_query_active(self, query: StreamingQuery) -> bool:
        is_active = getattr(query, "isActive", None)
        if callable(is_active):
            try:
                return bool(is_active())
            except Exception:
                return False
        return bool(is_active)

    def _get_query_name(self, query: StreamingQuery) -> Optional[str]:
        return getattr(query, "name", getattr(query, "queryName", None))

    def _get_query_id(self, query: StreamingQuery) -> Optional[str]:
        return getattr(query, "id", None)

    @handle_streaming_error
    def create_and_start_query(
        self, node_config: Dict[str, Any], execution_id: str, pipeline_name: str
    ) -> StreamingQuery:
        """Create and start a streaming query from node configuration."""
        try:
            self.validator.validate_streaming_node_config(node_config)

            node_name = node_config.get("name", "unknown")
            logger.info(f"Creating streaming query for node '{node_name}'")

            input_df = self._load_streaming_input(node_config)
            transformed_df = self._apply_transformations(input_df, node_config)
            query = self._configure_and_start_query(
                transformed_df, node_config, execution_id, pipeline_name
            )

            query_key = f"{pipeline_name}:{execution_id}:{node_name}"
            with self._active_queries_lock:
                self._active_queries[query_key] = {
                    "query": query,
                    "node_config": node_config,
                    "start_time": time.time(),
                    "execution_id": execution_id,
                    "pipeline_name": pipeline_name,
                    "node_name": node_name,
                }

            qid = self._get_query_id(query)
            qname = self._get_query_name(query) or node_name
            logger.info(f"Streaming query '{qname}' started with ID: {qid}")
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
                try:
                    e.add_context("operation_context", context)
                except Exception:
                    pass
                raise
            else:
                raise StreamingQueryError(
                    f"Failed to create streaming query: {str(e)}",
                    context=context,
                    cause=e,
                ) from e

    def _load_streaming_input(self, node_config: Dict[str, Any]) -> DataFrame:
        """Load streaming input DataFrame with error handling."""
        try:
            input_config = node_config.get("input", {})
            if not input_config:
                raise StreamingConfigurationError(
                    "Streaming node must have input configuration",
                    config_section="input",
                )

            format_type = input_config.get("format")
            if not format_type:
                raise StreamingConfigurationError(
                    "Streaming input must specify format", config_section="input.format"
                )

            reader = self.reader_factory.get_reader(format_type)

            streaming_df = reader.read_stream(input_config)

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
                ) from e

    def _apply_watermark(
        self, streaming_df: DataFrame, watermark_config: Dict[str, Any]
    ) -> DataFrame:
        """Apply watermark on the streaming DataFrame using provided config."""
        try:
            timestamp_col = watermark_config.get("column")
            delay_threshold = watermark_config.get("delay", DEFAULT_PROCESSING_TIME_INTERVAL)

            if not timestamp_col:
                raise StreamingConfigurationError(
                    "Watermark configuration must specify 'column'",
                    config_section="watermark.column",
                )

            if timestamp_col not in streaming_df.columns:
                available_cols = streaming_df.columns
                raise StreamingConfigurationError(
                    f"Watermark column '{timestamp_col}' not found in DataFrame. Available columns: {available_cols}",
                    config_section="watermark.column",
                    config_value=timestamp_col,
                )

            dtype = dict(streaming_df.dtypes).get(timestamp_col)
            if dtype not in ("timestamp", "date"):
                logger.warning(
                    f"Watermark column '{timestamp_col}' is type '{dtype}', casting to timestamp"
                )
                streaming_df = streaming_df.withColumn(
                    timestamp_col, f.col(timestamp_col).cast("timestamp")
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
                ) from e

    def _apply_transformations(self, input_df: DataFrame, node_config: Dict[str, Any]) -> DataFrame:
        """
        Apply transformations to the streaming DataFrame with error handling.
        """
        try:
            function_config = node_config.get("function")
            if not function_config:
                logger.info("No transformation function specified, using input DataFrame as-is")
                return input_df

            transform_func = self._get_transform_function(function_config)

            transformed_df = self._call_transform_func(transform_func, input_df, node_config)

            if not isinstance(transformed_df, DataFrame):
                raise StreamingError(
                    f"Transformation function must return a DataFrame, got {type(transformed_df)}",
                    error_code="INVALID_RETURN_TYPE",
                    context={
                        "return_type": str(type(transformed_df)),
                    },
                )

            logger.info("Transformation applied successfully")
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
                ) from e

    def _get_transform_function(self, function_config: Dict[str, Any]) -> Callable:
        """
        Get transformation function from registry or via module loading.
        """
        transformation_key = function_config.get("key")

        if transformation_key:
            return self._get_registered_transformation(transformation_key)
        else:
            return self._get_transformation_from_module(function_config)

    def _get_registered_transformation(self, transformation_key: str) -> Callable:
        """
        Get transformation from the registry.
        """
        try:
            transform_func = TransformationRegistry.get(transformation_key)
            logger.info(f"Applying registered transformation: '{transformation_key}'")
            return transform_func
        except ValueError as e:
            logger.error(f"Transformation not found: {transformation_key}")
            available = TransformationRegistry.list_transformations()
            raise StreamingConfigurationError(
                str(e),
                config_section="function.key",
                config_value=transformation_key,
                context={"available_transformations": available},
            ) from e

    def _get_transformation_from_module(self, function_config: Dict[str, Any]) -> Callable:
        """
        Load transformation from module via dynamic import (legacy support).
        """
        module_path = function_config.get("module")
        function_name = function_config.get("function")

        if not module_path or not function_name:
            raise StreamingConfigurationError(
                "Function configuration must specify either 'key' (for registered transformations) "
                "or both 'module' and 'function' (deprecated: requires STREAMING_UNSAFE_IMPORT=true)",
                config_section="function",
                config_value=function_config,
            )

        # Check if dynamic imports are enabled (default: disabled for security)
        enable_unsafe = os.getenv("STREAMING_UNSAFE_IMPORT", "false").lower() == "true"
        if not enable_unsafe:
            raise StreamingConfigurationError(
                "Dynamic module imports are disabled by default for security reasons. "
                "Use TransformationRegistry.register() to register transformations, "
                "or set STREAMING_UNSAFE_IMPORT=true to enable (not recommended for production)",
                config_section="function",
                config_value=function_config,
            )

        logger.warning(
            "Using unsafe dynamic imports for transformation. "
            "This is disabled by default. Consider using TransformationRegistry instead."
        )

        config_file_path = getattr(self.context, "_config_file_path", None)
        module = self._import_module_from_path(module_path, config_file_path)

        if not hasattr(module, function_name):
            available_functions = [attr for attr in dir(module) if callable(getattr(module, attr))]
            raise StreamingError(
                f"Function '{function_name}' not found in module '{module_path}'. "
                f"Available functions: {available_functions[:10]}",
                error_code="FUNCTION_NOT_FOUND",
                context={
                    "module_path": module_path,
                    "function_name": function_name,
                },
            )

        return getattr(module, function_name)

    def _import_module_from_path(self, module_path: str, config_file_path: Optional[str] = None):
        """Import module using importlib.util without modifying sys.path."""
        # First, try standard import (for installed packages)
        try:
            return importlib.import_module(module_path)
        except ImportError as first_error:
            # If standard import fails and we have a config path, try loading from file
            if config_file_path:
                try:
                    config_dir = Path(config_file_path).parent
                    # Convert module path to file path
                    module_file = config_dir / f"{module_path.replace('.', '/')}.py"

                    if module_file.exists():
                        logger.debug(f"Loading module from file: {module_file}")
                        spec = importlib.util.spec_from_file_location(module_path, module_file)
                        if spec and spec.loader:
                            module = importlib.util.module_from_spec(spec)
                            # Add to sys.modules so the module can import other modules
                            sys.modules[module_path] = module
                            spec.loader.exec_module(module)
                            return module
                except Exception as e:
                    logger.debug(f"Failed to load module from file: {e}")

            # If all attempts fail, raise the original import error
            raise StreamingError(
                f"Cannot import module '{module_path}': {str(first_error)}",
                error_code="MODULE_IMPORT_ERROR",
                context={"module_path": module_path},
                cause=first_error,
            ) from first_error

    def _call_transform_func(
        self,
        transform_func,
        input_df: DataFrame,
        node_config: Dict[str, Any],
    ) -> DataFrame:
        """
        Call the transform function with error handling.
        """
        try:
            return transform_func(input_df, node_config)
        except Exception as e:
            raise StreamingError(
                f"Error executing transformation function: {str(e)}",
                error_code="TRANSFORMATION_ERROR",
                context={
                    "function_callable": str(transform_func),
                },
                cause=e,
            ) from e

    def _configure_and_start_query(
        self,
        df: DataFrame,
        node_config: Dict[str, Any],
        execution_id: str,
        pipeline_name: str,
    ) -> StreamingQuery:
        """Configure and start the streaming query with comprehensive error handling."""
        try:
            output_config = node_config.get("output", {})
            if not output_config:
                raise StreamingConfigurationError(
                    "Streaming node must have output configuration",
                    config_section="output",
                )

            # Use a deepcopy of the defaults to avoid shared mutable state
            streaming_config = deepcopy(DEFAULT_STREAMING_CONFIG)
            streaming_config.update(node_config.get("streaming", {}))

            node_name = node_config.get("name", "unknown")
            query_name = (
                streaming_config.get("query_name") or f"{pipeline_name}_{node_name}_{execution_id}"
            )

            checkpoint_location = self._get_checkpoint_location(
                streaming_config.get("checkpoint_location"),
                pipeline_name,
                node_name,
                execution_id,
            )

            # Validate checkpoint is not already in use
            self._validate_checkpoint_not_in_use(checkpoint_location, node_name)

            output_mode = streaming_config.get("output_mode", StreamingOutputMode.APPEND.value)

            trigger_config = streaming_config.get("trigger", {})
            trigger = self._configure_trigger(trigger_config)

            logger.info(f"Configuring streaming query '{query_name}':")
            logger.info(f"  - Output mode: {output_mode}")
            logger.info(f"  - Trigger: {trigger_config}")
            logger.info(f"  - Checkpoint: {checkpoint_location}")

            write_stream = (
                df.writeStream.outputMode(output_mode)
                .queryName(query_name)
                .option("checkpointLocation", checkpoint_location)
            )

            if trigger:
                write_stream = write_stream.trigger(**trigger)

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
                ) from e

    def _get_checkpoint_location(
        self,
        base_checkpoint: Optional[str],
        pipeline_name: str,
        node_name: str,
        execution_id: str,
    ) -> str:
        """Get checkpoint location for the streaming query with validation."""
        try:
            checkpoint_base = self._determine_checkpoint_base(base_checkpoint)
            checkpoint_path = self._build_checkpoint_path(
                checkpoint_base, pipeline_name, node_name, execution_id
            )

            if not self._is_cloud_path(checkpoint_path):
                self._ensure_checkpoint_dir(checkpoint_path)

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
                ) from e

    def _determine_checkpoint_base(self, base_checkpoint: Optional[str]) -> str:
        """Determine the base directory for checkpoints."""
        if base_checkpoint:
            return base_checkpoint

        checkpoints_base = None
        try:
            gs = getattr(self.context, "global_settings", {}) or {}
            if isinstance(gs, dict):
                checkpoints_base = gs.get("checkpoints_base")
        except Exception:
            checkpoints_base = None

        if checkpoints_base:
            return str(checkpoints_base)

        output_path = getattr(self.context, "output_path", "/tmp/checkpoints")
        return os.path.join(output_path, "streaming_checkpoints")

    def _is_cloud_path(self, path: str) -> bool:
        """Return True if path appears to be on a cloud filesystem."""
        cloud_schemes = (
            "s3://",
            "gs://",
            "abfs://",
            "abfss://",
            "hdfs://",
            "dbfs:/",
        )
        return str(path).startswith(cloud_schemes)

    def _build_checkpoint_path(
        self,
        checkpoint_base: str,
        pipeline_name: str,
        node_name: str,
        execution_id: str,
    ) -> str:
        """Build the full checkpoint path from base and identifiers."""
        if self._is_cloud_path(checkpoint_base):
            return f"{checkpoint_base.rstrip('/')}/{pipeline_name}/{node_name}/{execution_id}"
        return os.path.join(checkpoint_base, pipeline_name, node_name, execution_id)

    def _ensure_checkpoint_dir(self, checkpoint_path: str) -> None:
        """Ensure checkpoint directory exists for local filesystems, raising StreamingError on failure."""
        try:
            Path(checkpoint_path).mkdir(parents=True, exist_ok=True)
        except OSError as e:
            raise StreamingError(
                f"Cannot create checkpoint directory '{checkpoint_path}': {str(e)}",
                error_code="CHECKPOINT_CREATION_ERROR",
                context={"checkpoint_path": checkpoint_path},
                cause=e,
            ) from e

    def _validate_checkpoint_not_in_use(self, checkpoint_path: str, node_name: str) -> None:
        """Validate checkpoint location is not already in use by another active query."""
        with self._active_queries_lock:
            for query_key, query_info in self._active_queries.items():
                existing_config = query_info.get("node_config", {})
                existing_streaming = existing_config.get("streaming", {})

                # Get the checkpoint from the existing query
                existing_checkpoint = None
                if existing_streaming:
                    # Try to get from streaming config first
                    existing_checkpoint = existing_streaming.get("checkpoint_location")

                    # If not there, it might have been computed
                    if not existing_checkpoint:
                        # Reconstruct the checkpoint path using same logic
                        existing_pipeline_name = query_info.get("pipeline_name", "")
                        existing_node_name = query_info.get("node_name", "")
                        existing_execution_id = query_info.get("execution_id", "")

                        if existing_pipeline_name and existing_node_name and existing_execution_id:
                            checkpoint_base = self._determine_checkpoint_base(None)
                            existing_checkpoint = self._build_checkpoint_path(
                                checkpoint_base,
                                existing_pipeline_name,
                                existing_node_name,
                                existing_execution_id,
                            )

                # Check if checkpoints match
                if existing_checkpoint and existing_checkpoint == checkpoint_path:
                    existing_node = query_info.get("node_name", "unknown")
                    raise StreamingConfigurationError(
                        f"Checkpoint location '{checkpoint_path}' is already in use by query '{existing_node}'. "
                        f"Each streaming query must have a unique checkpoint location.",
                        config_section="streaming.checkpoint_location",
                        config_value=checkpoint_path,
                        context={
                            "current_node": node_name,
                            "conflicting_node": existing_node,
                            "query_key": query_key,
                        },
                    )

    def _configure_trigger(self, trigger_config: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Configure streaming trigger with minimum interval validation."""
        try:
            trigger_type_raw = trigger_config.get("type", StreamingTrigger.PROCESSING_TIME.value)
            trigger_type = str(trigger_type_raw).lower()

            mapped_key = self._map_trigger_type(trigger_type, trigger_type_raw)

            if mapped_key == "processingTime":
                return self._build_processing_time_trigger(trigger_config)

            if mapped_key == "once":
                return {"once": True}

            if mapped_key == "continuous":
                return self._build_continuous_trigger(trigger_config)

            if mapped_key == "availableNow":
                return {"availableNow": True}

            return {"processingTime": DEFAULT_PROCESSING_TIME_INTERVAL}

        except Exception as e:
            logger.error(f"Error configuring trigger: {str(e)}")
            if isinstance(e, StreamingError):
                raise
            else:
                raise StreamingConfigurationError(
                    f"Failed to configure trigger: {str(e)}",
                    config_section="trigger",
                    cause=e,
                ) from e

    def _map_trigger_type(self, trigger_type: str, trigger_type_raw: Any) -> str:
        """Map raw trigger type to internal key and validate it."""
        trigger_map = {
            StreamingTrigger.PROCESSING_TIME.value.lower(): "processingTime",
            StreamingTrigger.ONCE.value.lower(): "once",
            StreamingTrigger.CONTINUOUS.value.lower(): "continuous",
            StreamingTrigger.AVAILABLE_NOW.value.lower(): "availableNow",
        }
        valid_triggers = list(trigger_map.keys())
        if trigger_type not in valid_triggers:
            raise StreamingConfigurationError(
                f"Invalid trigger type '{trigger_type_raw}'. Valid types: {valid_triggers}",
                config_section="trigger.type",
                config_value=trigger_type_raw,
            )
        return trigger_map[trigger_type]

    def _validate_interval(self, interval: str, config_section: str) -> None:
        """Validate an interval string and ensure it meets the minimum configured seconds."""
        if not getattr(self.validator, "_validate_time_interval", lambda x: True)(interval):
            raise StreamingConfigurationError(
                f"Invalid {config_section.split('.')[-1]} interval '{interval}'",
                config_section=config_section,
                config_value=interval,
            )
        min_interval = float(STREAMING_VALIDATIONS.get("min_trigger_interval_seconds", 1))
        if (
            getattr(self.validator, "_parse_time_to_seconds", lambda x: 0.0)(interval)
            < min_interval
        ):
            raise StreamingConfigurationError(
                f"{config_section.split('.')[-1]} interval '{interval}' is below minimum of {min_interval} seconds",
                config_section=config_section,
                config_value=interval,
            )

    def _build_processing_time_trigger(self, trigger_config: Dict[str, Any]) -> Dict[str, Any]:
        """Build processingTime trigger dict after validation."""
        interval = str(trigger_config.get("interval", DEFAULT_PROCESSING_TIME_INTERVAL))
        self._validate_interval(interval, "trigger.interval")
        return {"processingTime": interval}

    def _build_continuous_trigger(self, trigger_config: Dict[str, Any]) -> Dict[str, Any]:
        """Build continuous trigger dict after validation."""
        interval = str(trigger_config.get("interval", "1 second"))
        self._validate_interval(interval, "trigger.interval")
        return {"continuous": interval}

    def stop_query(
        self,
        query: StreamingQuery,
        graceful: bool = True,
        timeout_seconds: float = 30.0,
    ) -> bool:
        """Stop a streaming query with timeout and error handling."""
        try:
            if not self._is_query_active(query):
                logger.info(f"Query '{self._get_query_name(query)}' is already stopped")
                return True

            logger.info(
                f"Stopping streaming query '{self._get_query_name(query)}' (ID: {self._get_query_id(query)})"
            )

            start_time = time.time()
            self._call_stop_or_set_inactive(query)

            if graceful:
                stopped = self._wait_until_stopped(query, start_time, timeout_seconds)
                if not stopped:
                    logger.warning(
                        f"Query '{self._get_query_name(query)}' did not stop within {timeout_seconds}s timeout"
                    )
                    return False

            self._remove_query_from_active(query)

            logger.info(f"Query '{self._get_query_name(query)}' stopped successfully")
            return True

        except Exception as e:
            logger.error(
                f"Error stopping query '{self._get_query_name(query) or 'unknown'}': {str(e)}"
            )
            raise StreamingQueryError(
                f"Failed to stop query: {str(e)}",
                query_id=self._get_query_id(query),
                query_name=self._get_query_name(query),
                cause=e,
            ) from e

    def _call_stop_or_set_inactive(self, query: StreamingQuery) -> None:
        """Invoke the query stop if available, otherwise attempt to set isActive to False."""
        stop_call = getattr(query, "stop", None)
        if callable(stop_call):
            try:
                stop_call()
            except Exception:
                pass
        else:
            try:
                setattr(query, "isActive", False)
            except Exception:
                pass

    def _wait_until_stopped(
        self, query: StreamingQuery, start_time: float, timeout_seconds: float
    ) -> bool:
        """Wait until the query becomes inactive or the timeout elapses; returns True if stopped."""
        try:
            while self._is_query_active(query) and (time.time() - start_time) < timeout_seconds:
                time.sleep(0.5)
            return not self._is_query_active(query)
        except Exception:
            return False

    def _remove_query_from_active(self, query: StreamingQuery) -> None:
        """Remove query entry from the active queries map if present."""
        query_key = None
        with self._active_queries_lock:
            for key, info in self._active_queries.items():
                try:
                    if self._get_query_id(info.get("query")) == self._get_query_id(query):
                        query_key = key
                        break
                except Exception:
                    continue

            if query_key:
                del self._active_queries[query_key]

    def get_query_progress(self, query: StreamingQuery) -> Optional[Dict[str, Any]]:
        """Get progress information for a streaming query with error handling."""
        try:
            if not self._is_query_active(query):
                return None

            progress = getattr(query, "lastProgress", None)
            return progress

        except Exception as e:
            logger.error(f"Error getting query progress: {str(e)}")
            raise StreamingQueryError(
                f"Failed to get query progress: {str(e)}",
                query_id=self._get_query_id(query),
                query_name=self._get_query_name(query),
                cause=e,
            ) from e

    def get_active_queries(self) -> Dict[str, Dict[str, Any]]:
        """Get information about all active queries."""
        with self._active_queries_lock:
            return self._active_queries.copy()

    def stop_all_queries(
        self, graceful: bool = True, timeout_seconds: float = 30.0
    ) -> Dict[str, bool]:
        """Stop all active queries and return results."""
        results: Dict[str, bool] = {}

        with self._active_queries_lock:
            keys = list(self._active_queries.keys())

        for query_key in keys:
            try:
                with self._active_queries_lock:
                    query_info = self._active_queries.get(query_key)
                if not query_info:
                    results[query_key] = True
                    continue
                query = query_info["query"]
                result = self.stop_query(query, graceful, timeout_seconds)
                results[query_key] = result
            except Exception as e:
                logger.error(f"Error stopping query {query_key}: {str(e)}")
                results[query_key] = False

        return results
