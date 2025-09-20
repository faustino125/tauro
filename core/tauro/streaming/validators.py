import re
from typing import Any, Dict, List, Optional

from loguru import logger  # type: ignore

from tauro.streaming.constants import (
    STREAMING_FORMAT_CONFIGS,
    STREAMING_VALIDATIONS,
    PipelineType,
    StreamingFormat,
    StreamingOutputMode,
    StreamingTrigger,
)
from tauro.streaming.exceptions import StreamingValidationError, handle_streaming_error

# Common field name constants
INPUT_OPTIONS_FIELD = "input.options"
NON_EMPTY_STRING = "non-empty string"
TRIGGER_INTERVAL_FIELD = "streaming.trigger.interval"


class StreamingValidator:
    """Validates streaming pipeline configurations with enhanced error handling."""

    def __init__(self, format_policy: Optional[Any] = None) -> None:
        """
        format_policy es opcional. Si se provee, se usará para validar formatos de entrada/salida
        (e.g., Context.format_policy). Si no, se usan los enums/constantes por defecto.
        """
        self.policy = format_policy

    @handle_streaming_error
    def validate_streaming_pipeline_config(
        self, pipeline_config: Dict[str, Any]
    ) -> None:
        """Validate streaming pipeline configuration with comprehensive checks."""
        try:
            self._ensure_pipeline_is_dict(pipeline_config)

            pipeline_type = pipeline_config.get("type", PipelineType.STREAMING.value)
            self._ensure_valid_pipeline_type(pipeline_type)

            nodes = pipeline_config.get("nodes", [])
            self._ensure_nodes_list(nodes)

            for i, node in enumerate(nodes):
                self._validate_node_entry(i, node)

            # Validate pipeline-level streaming configuration
            streaming_config = pipeline_config.get("streaming", {})
            if streaming_config:
                self._validate_pipeline_streaming_config(streaming_config)

            logger.info("Streaming pipeline configuration validated successfully")

        except StreamingValidationError:
            raise
        except Exception as e:
            raise StreamingValidationError(
                f"Unexpected error during pipeline validation: {str(e)}", cause=e
            )

    def _ensure_pipeline_is_dict(self, pipeline_config: Any) -> None:
        if not isinstance(pipeline_config, dict):
            raise StreamingValidationError(
                "Pipeline configuration must be a dictionary",
                expected="dict",
                actual=str(type(pipeline_config)),
            )

    def _ensure_valid_pipeline_type(self, pipeline_type: Any) -> None:
        valid_types = [PipelineType.STREAMING.value, PipelineType.HYBRID.value]
        if pipeline_type not in valid_types:
            raise StreamingValidationError(
                f"Pipeline type must be one of {valid_types} for streaming pipelines",
                field="type",
                expected=str(valid_types),
                actual=pipeline_type,
            )

    def _ensure_nodes_list(self, nodes: Any) -> None:
        if not nodes:
            raise StreamingValidationError(
                "Streaming pipeline must have at least one node", field="nodes"
            )
        if not isinstance(nodes, list):
            raise StreamingValidationError(
                "Pipeline nodes must be a list",
                field="nodes",
                expected="list",
                actual=str(type(nodes)),
            )

    def _validate_node_entry(self, index: int, node: Any) -> None:
        try:
            if isinstance(node, dict):
                self.validate_streaming_node_config(node)
            elif isinstance(node, str):
                # Node reference - basic validation
                if not node.strip():
                    raise StreamingValidationError(
                        f"Node reference at index {index} cannot be empty string"
                    )
            else:
                raise StreamingValidationError(
                    f"Invalid node configuration at index {index}",
                    expected="dict or str",
                    actual=str(type(node)),
                )
        except StreamingValidationError as e:
            # Add context about which node failed
            e.add_context("node_index", index)
            e.add_context("node_type", type(node).__name__)
            raise

    @handle_streaming_error
    def validate_streaming_node_config(self, node_config: Dict[str, Any]) -> None:
        """Validate streaming node configuration with detailed validation."""
        try:
            if not isinstance(node_config, dict):
                raise StreamingValidationError(
                    "Node configuration must be a dictionary",
                    expected="dict",
                    actual=str(type(node_config)),
                )

            node_name = node_config.get("name", "unknown")

            # Validate required fields
            required_fields = ["input", "output"]
            missing_fields = [
                field for field in required_fields if field not in node_config
            ]
            if missing_fields:
                raise StreamingValidationError(
                    f"Node '{node_name}' missing required fields: {missing_fields}",
                    field="required_fields",
                    expected=str(required_fields),
                    actual=str(list(node_config.keys())),
                )

            # Validate input configuration
            try:
                self._validate_streaming_input_config(node_config["input"], node_name)
            except StreamingValidationError as e:
                e.add_context("validation_section", "input")
                raise

            # Validate output configuration
            try:
                self._validate_streaming_output_config(node_config["output"], node_name)
            except StreamingValidationError as e:
                e.add_context("validation_section", "output")
                raise

            # Validate streaming-specific configuration
            streaming_config = node_config.get("streaming", {})
            if streaming_config:
                try:
                    self._validate_streaming_config(streaming_config, node_name)
                except StreamingValidationError as e:
                    e.add_context("validation_section", "streaming")
                    raise

            # Validate function configuration if present
            function_config = node_config.get("function")
            if function_config:
                self._validate_function_config(function_config, node_name)

            logger.debug(f"Streaming node '{node_name}' configuration validated")

        except StreamingValidationError:
            raise
        except Exception as e:
            raise StreamingValidationError(
                f"Unexpected error during node validation: {str(e)}", cause=e
            )

    def _validate_streaming_input_config(
        self, input_config: Dict[str, Any], node_name: str
    ) -> None:
        """Validate streaming input configuration with format-specific checks."""
        try:
            if not isinstance(input_config, dict):
                raise StreamingValidationError(
                    f"Node '{node_name}' input configuration must be a dictionary",
                    field="input",
                    expected="dict",
                    actual=str(type(input_config)),
                )

            # Validate format
            format_type = input_config.get("format")
            if not format_type:
                raise StreamingValidationError(
                    f"Node '{node_name}' input must specify 'format'",
                    field="input.format",
                )

            # Usar policy si está disponible, si no usar enums por defecto
            valid_formats = (
                self.policy.get_supported_input_formats()
                if self.policy
                else [fmt.value for fmt in StreamingFormat]
            )
            if format_type not in valid_formats:
                raise StreamingValidationError(
                    f"Node '{node_name}' has unsupported input format '{format_type}'",
                    field="input.format",
                    expected=str(valid_formats),
                    actual=format_type,
                )

            # Validate options dict
            options = input_config.get("options", {})
            if not isinstance(options, dict):
                raise StreamingValidationError(
                    f"Node '{node_name}' input options must be a dictionary",
                    field=INPUT_OPTIONS_FIELD,
                    expected="dict",
                    actual=str(type(options)),
                )

            format_config = STREAMING_FORMAT_CONFIGS.get(format_type, {})

            # Check required options
            required_options = format_config.get("required_options", [])
            missing_options = [opt for opt in required_options if opt not in options]
            if missing_options:
                raise StreamingValidationError(
                    f"Node '{node_name}' missing required options for {format_type}: {missing_options}",
                    field=INPUT_OPTIONS_FIELD,
                    expected=str(required_options),
                    actual=str(list(options.keys())),
                )

            # Validate format-specific constraints
            if format_type == StreamingFormat.KAFKA.value:
                self._validate_kafka_options(options, node_name)
            elif format_type == StreamingFormat.FILE_STREAM.value:
                self._validate_file_stream_options(options, node_name)
            elif format_type == StreamingFormat.KINESIS.value:
                self._validate_kinesis_options(options, node_name)

            # Validate watermark configuration
            watermark = input_config.get("watermark")
            if watermark:
                self._validate_watermark_config(watermark, node_name)

        except StreamingValidationError:
            raise
        except Exception as e:
            raise StreamingValidationError(
                f"Error validating input config for node '{node_name}': {str(e)}",
                cause=e,
            )

    def _validate_streaming_output_config(
        self, output_config: Dict[str, Any], node_name: str
    ) -> None:
        """Validate streaming output configuration with comprehensive checks (delegates checks)."""
        try:
            if not isinstance(output_config, dict):
                raise StreamingValidationError(
                    f"Node '{node_name}' output configuration must be a dictionary",
                    field="output",
                    expected="dict",
                    actual=str(type(output_config)),
                )

            # Validate format
            format_type = output_config.get("format")
            if not format_type:
                raise StreamingValidationError(
                    f"Node '{node_name}' output must specify 'format'",
                    field="output.format",
                )

            # Optional policy validation for output formats
            if self.policy:
                valid_outputs = self.policy.get_supported_output_formats()
                if format_type not in valid_outputs:
                    raise StreamingValidationError(
                        f"Node '{node_name}' has unsupported output format '{format_type}'",
                        field="output.format",
                        expected=str(valid_outputs),
                        actual=format_type,
                    )

            # Delegate checks to specialized helpers
            file_formats = ["delta", "parquet", "json", "csv"]
            if format_type in file_formats:
                self._validate_file_output_path(
                    output_config.get("path"), node_name, format_type
                )
            elif format_type == "kafka":
                self._validate_kafka_output_options(output_config, node_name)
            elif format_type == "foreachBatch":
                self._validate_foreach_batch(output_config, node_name)

            # Validate partitioning configuration regardless of format
            partition_by = output_config.get("partitionBy")
            self._validate_partition_by(partition_by, node_name)

        except StreamingValidationError:
            raise
        except Exception as e:
            raise StreamingValidationError(
                f"Error validating output config for node '{node_name}': {str(e)}",
                cause=e,
            )

    def _validate_file_output_path(
        self, path: Optional[str], node_name: str, format_type: str
    ) -> None:
        """Validate path for file-based outputs (helper)."""
        if not path:
            raise StreamingValidationError(
                f"Node '{node_name}' output format '{format_type}' requires 'path'",
                field="output.path",
                expected=NON_EMPTY_STRING,
                actual=str(path),
            )

        if not isinstance(path, str) or not path.strip():
            raise StreamingValidationError(
                f"Node '{node_name}' output path must be a non-empty string",
                field="output.path",
                expected=NON_EMPTY_STRING,
                actual=str(path),
            )

    def _validate_foreach_batch(
        self, output_config: Dict[str, Any], node_name: str
    ) -> None:
        """Validate foreachBatch configuration (helper)."""
        batch_function = output_config.get("batch_function")
        if not batch_function:
            raise StreamingValidationError(
                f"Node '{node_name}' foreachBatch output requires 'batch_function'",
                field="output.batch_function",
            )

    def _validate_partition_by(self, partition_by: Any, node_name: str) -> None:
        """Validate partitionBy clause (helper)."""
        if partition_by is None:
            return
        if not isinstance(partition_by, (str, list)):
            raise StreamingValidationError(
                f"Node '{node_name}' partitionBy must be string or list of strings",
                field="output.partitionBy",
                expected="str or list",
                actual=str(type(partition_by)),
            )

    def _validate_streaming_config(
        self, streaming_config: Dict[str, Any], node_name: str
    ) -> None:
        """Validate streaming-specific configuration with detailed checks."""
        try:
            if not isinstance(streaming_config, dict):
                raise StreamingValidationError(
                    f"Node '{node_name}' streaming configuration must be a dictionary",
                    field="streaming",
                    expected="dict",
                    actual=str(type(streaming_config)),
                )

            # Validate trigger configuration
            trigger = streaming_config.get("trigger", {})
            if trigger:
                self._validate_trigger_config(trigger, node_name)

            # Validate output mode
            output_mode = streaming_config.get("output_mode")
            if output_mode:
                valid_modes = [mode.value for mode in StreamingOutputMode]
                if output_mode not in valid_modes:
                    raise StreamingValidationError(
                        f"Node '{node_name}' has invalid output_mode '{output_mode}'",
                        field="streaming.output_mode",
                        expected=str(valid_modes),
                        actual=output_mode,
                    )

            # Validate checkpoint location
            checkpoint = streaming_config.get("checkpoint_location")
            if checkpoint and not isinstance(checkpoint, str):
                raise StreamingValidationError(
                    f"Node '{node_name}' checkpoint_location must be a string",
                    field="streaming.checkpoint_location",
                    expected="str",
                    actual=str(type(checkpoint)),
                )

            # Validate query name
            query_name = streaming_config.get("query_name")
            if query_name is not None and not isinstance(query_name, str):
                raise StreamingValidationError(
                    f"Node '{node_name}' query_name must be a string",
                    field="streaming.query_name",
                    expected="str",
                    actual=str(type(query_name)),
                )

        except StreamingValidationError:
            raise
        except Exception as e:
            raise StreamingValidationError(
                f"Error validating streaming config for node '{node_name}': {str(e)}",
                cause=e,
            )

    def _validate_function_config(
        self, function_config: Dict[str, Any], node_name: str
    ) -> None:
        """Validate function configuration for transformations."""
        try:
            if not isinstance(function_config, dict):
                raise StreamingValidationError(
                    f"Node '{node_name}' function configuration must be a dictionary",
                    field="function",
                    expected="dict",
                    actual=str(type(function_config)),
                )

            # Validate required fields
            required_fields = ["module", "function"]
            missing_fields = [
                field for field in required_fields if field not in function_config
            ]
            if missing_fields:
                raise StreamingValidationError(
                    f"Node '{node_name}' function config missing required fields: {missing_fields}",
                    field="function",
                    expected=str(required_fields),
                    actual=str(list(function_config.keys())),
                )

            module_path = function_config.get("module")
            function_name = function_config.get("function")
            if not isinstance(module_path, str) or not module_path.strip():
                raise StreamingValidationError(
                    f"Node '{node_name}' function module must be a non-empty string",
                    field="function.module",
                    expected=NON_EMPTY_STRING,
                    actual=str(module_path),
                )

            if not isinstance(function_name, str) or not function_name.strip():
                raise StreamingValidationError(
                    f"Node '{node_name}' function name must be a non-empty string",
                    field="function.function",
                    expected=NON_EMPTY_STRING,
                    actual=str(function_name),
                )

        except StreamingValidationError:
            raise
        except Exception as e:
            raise StreamingValidationError(
                f"Error validating function config for node '{node_name}': {str(e)}",
                cause=e,
            )

    def _validate_pipeline_streaming_config(
        self, streaming_config: Dict[str, Any]
    ) -> None:
        """Validate pipeline-level streaming configuration."""
        try:
            if not isinstance(streaming_config, dict):
                raise StreamingValidationError(
                    "Pipeline streaming configuration must be a dictionary",
                    field="streaming",
                    expected="dict",
                    actual=str(type(streaming_config)),
                )

            # Validate global settings
            max_concurrent_queries = streaming_config.get("max_concurrent_queries")
            if max_concurrent_queries is not None:
                try:
                    max_concurrent_queries = int(max_concurrent_queries)
                    if max_concurrent_queries <= 0:
                        raise ValueError()
                except (ValueError, TypeError):
                    raise StreamingValidationError(
                        "max_concurrent_queries must be a positive integer",
                        field="streaming.max_concurrent_queries",
                        expected="positive integer",
                        actual=str(max_concurrent_queries),
                    )

        except StreamingValidationError:
            raise
        except Exception as e:
            raise StreamingValidationError(
                f"Error validating pipeline streaming config: {str(e)}", cause=e
            )

    def _validate_kafka_options(self, options: Dict[str, Any], node_name: str) -> None:
        """Validate Kafka-specific options with comprehensive checks."""
        try:
            # Check mutually exclusive subscription options
            subscription_options = ["subscribe", "subscribePattern", "assign"]
            provided_subscriptions = [
                opt for opt in subscription_options if opt in options
            ]

            if len(provided_subscriptions) == 0:
                raise StreamingValidationError(
                    f"Node '{node_name}' Kafka input must specify one of: {subscription_options}",
                    field=INPUT_OPTIONS_FIELD,
                    expected=f"one of {subscription_options}",
                    actual="none provided",
                )

            if len(provided_subscriptions) > 1:
                raise StreamingValidationError(
                    f"Node '{node_name}' Kafka input cannot specify multiple subscription options: {provided_subscriptions}",
                    field=INPUT_OPTIONS_FIELD,
                    expected=f"only one of {subscription_options}",
                    actual=str(provided_subscriptions),
                )

            # Validate bootstrap servers format
            bootstrap_servers = options.get("kafka.bootstrap.servers")
            if bootstrap_servers:
                if not isinstance(bootstrap_servers, str):
                    raise StreamingValidationError(
                        f"Node '{node_name}' kafka.bootstrap.servers must be a string",
                        field=f"{INPUT_OPTIONS_FIELD}.kafka.bootstrap.servers",
                        expected="string",
                        actual=str(type(bootstrap_servers)),
                    )

                # Basic format validation
                if not bootstrap_servers.strip():
                    raise StreamingValidationError(
                        f"Node '{node_name}' kafka.bootstrap.servers cannot be empty",
                        field=f"{INPUT_OPTIONS_FIELD}.kafka.bootstrap.servers",
                    )

            # Validate subscription values
            subscribe = options.get("subscribe")
            if subscribe and not isinstance(subscribe, str):
                raise StreamingValidationError(
                    f"Node '{node_name}' Kafka subscribe must be a string",
                    field=f"{INPUT_OPTIONS_FIELD}.subscribe",
                    expected="string",
                    actual=str(type(subscribe)),
                )

        except StreamingValidationError:
            raise
        except Exception as e:
            raise StreamingValidationError(
                f"Error validating Kafka options for node '{node_name}': {str(e)}",
                cause=e,
            )

    def _validate_kafka_output_options(
        self, output_config: Dict[str, Any], node_name: str
    ) -> None:
        """Validate Kafka output-specific options."""
        try:
            options = output_config.get("options", {})

            required_kafka_options = ["kafka.bootstrap.servers", "topic"]
            missing_options = [
                opt for opt in required_kafka_options if opt not in options
            ]
            if missing_options:
                raise StreamingValidationError(
                    f"Node '{node_name}' Kafka output missing required options: {missing_options}",
                    field="output.options",
                    expected=str(required_kafka_options),
                    actual=str(list(options.keys())),
                )

            # Validate topic name
            topic = options.get("topic")
            if topic and not isinstance(topic, str):
                raise StreamingValidationError(
                    f"Node '{node_name}' Kafka topic must be a string",
                    field="output.options.topic",
                    expected="string",
                    actual=str(type(topic)),
                )

        except StreamingValidationError:
            raise
        except Exception as e:
            raise StreamingValidationError(
                f"Error validating Kafka output options for node '{node_name}': {str(e)}",
                cause=e,
            )

    def _validate_file_stream_options(
        self, options: Dict[str, Any], node_name: str
    ) -> None:
        """Validate file stream specific options."""
        try:
            path = options.get("path")
            if not path:
                raise StreamingValidationError(
                    f"Node '{node_name}' file stream requires 'path' option",
                    field=f"{INPUT_OPTIONS_FIELD}.path",
                )

            if not isinstance(path, str):
                raise StreamingValidationError(
                    f"Node '{node_name}' file stream path must be a string",
                    field=f"{INPUT_OPTIONS_FIELD}.path",
                    expected="string",
                    actual=str(type(path)),
                )

            # Validate numeric options
            numeric_options = ["maxFilesPerTrigger"]
            for opt in numeric_options:
                if opt in options:
                    try:
                        int(options[opt])
                    except (ValueError, TypeError):
                        raise StreamingValidationError(
                            f"Node '{node_name}' file stream option '{opt}' must be numeric",
                            field=f"{INPUT_OPTIONS_FIELD}.{opt}",
                            expected="numeric",
                            actual=str(options[opt]),
                        )

        except StreamingValidationError:
            raise
        except Exception as e:
            raise StreamingValidationError(
                f"Error validating file stream options for node '{node_name}': {str(e)}",
                cause=e,
            )

    def _validate_kinesis_options(
        self, options: Dict[str, Any], node_name: str
    ) -> None:
        """Validate Kinesis-specific options."""
        try:
            required_options = ["streamName", "region"]
            missing_options = [opt for opt in required_options if opt not in options]
            if missing_options:
                raise StreamingValidationError(
                    f"Node '{node_name}' Kinesis input missing required options: {missing_options}",
                    field=INPUT_OPTIONS_FIELD,
                    expected=str(required_options),
                    actual=str(list(options.keys())),
                )

            # Validate stream name
            stream_name = options.get("streamName")
            if stream_name and not isinstance(stream_name, str):
                raise StreamingValidationError(
                    f"Node '{node_name}' Kinesis streamName must be a string",
                    field=f"{INPUT_OPTIONS_FIELD}.streamName",
                    expected="string",
                    actual=str(type(stream_name)),
                )

            # Validate region
            region = options.get("region")
            if region and not isinstance(region, str):
                raise StreamingValidationError(
                    f"Node '{node_name}' Kinesis region must be a string",
                    field=f"{INPUT_OPTIONS_FIELD}.region",
                    expected="string",
                    actual=str(type(region)),
                )

        except StreamingValidationError:
            raise
        except Exception as e:
            raise StreamingValidationError(
                f"Error validating Kinesis options for node '{node_name}': {str(e)}",
                cause=e,
            )

    def _validate_watermark_config(
        self, watermark: Dict[str, Any], node_name: str
    ) -> None:
        """Validate watermark configuration with enhanced checks."""
        try:
            if not isinstance(watermark, dict):
                raise StreamingValidationError(
                    f"Node '{node_name}' watermark configuration must be a dictionary",
                    field="input.watermark",
                    expected="dict",
                    actual=str(type(watermark)),
                )

            # Validate column name
            column = watermark.get("column")
            if not column:
                raise StreamingValidationError(
                    f"Node '{node_name}' watermark must specify 'column'",
                    field="input.watermark.column",
                )
            if not isinstance(column, str) or not column.strip():
                raise StreamingValidationError(
                    f"Node '{node_name}' watermark column must be a non-empty string",
                    field="input.watermark.column",
                    expected=NON_EMPTY_STRING,
                    actual=str(column),
                )

            delay = watermark.get("delay", "10 seconds")
            if not self._validate_time_interval(delay):
                raise StreamingValidationError(
                    f"Node '{node_name}' watermark delay '{delay}' is invalid",
                    field="input.watermark.delay",
                    expected="time interval like '10 seconds', '5 minutes', '1 hour'",
                    actual=delay,
                )

            delay_minutes = self._parse_time_to_minutes(delay)
            max_delay = STREAMING_VALIDATIONS["max_watermark_delay_minutes"]
            if delay_minutes > max_delay:
                raise StreamingValidationError(
                    f"Node '{node_name}' watermark delay '{delay}' exceeds maximum allowed delay of {max_delay} minutes",
                    field="input.watermark.delay",
                    expected=f"<= {max_delay} minutes",
                    actual=f"{delay_minutes} minutes",
                )

        except StreamingValidationError:
            raise
        except Exception as e:
            raise StreamingValidationError(
                f"Error validating watermark config for node '{node_name}': {str(e)}",
                cause=e,
            )

    def _validate_trigger_config(self, trigger: Dict[str, Any], node_name: str) -> None:
        """Validate trigger configuration with comprehensive validation."""
        try:
            if not isinstance(trigger, dict):
                raise StreamingValidationError(
                    f"Node '{node_name}' trigger configuration must be a dictionary",
                    field="streaming.trigger",
                    expected="dict",
                    actual=str(type(trigger)),
                )

            # Validate trigger type
            trigger_type = trigger.get("type")
            if not trigger_type:
                raise StreamingValidationError(
                    f"Node '{node_name}' trigger must specify 'type'",
                    field="streaming.trigger.type",
                )

            valid_triggers = [t.value for t in StreamingTrigger]
            if trigger_type not in valid_triggers:
                raise StreamingValidationError(
                    f"Node '{node_name}' has invalid trigger type '{trigger_type}'",
                    field="streaming.trigger.type",
                    expected=str(valid_triggers),
                    actual=trigger_type,
                )

            if trigger_type in [
                StreamingTrigger.PROCESSING_TIME.value,
                StreamingTrigger.CONTINUOUS.value,
            ]:
                interval = trigger.get("interval")
                if not interval:
                    raise StreamingValidationError(
                        f"Node '{node_name}' trigger type '{trigger_type}' requires 'interval'",
                        field=TRIGGER_INTERVAL_FIELD,
                    )

                if not self._validate_time_interval(interval):
                    raise StreamingValidationError(
                        f"Node '{node_name}' trigger interval '{interval}' is invalid",
                        field=TRIGGER_INTERVAL_FIELD,
                        expected="time interval like '10 seconds', '5 minutes'",
                        actual=interval,
                    )

                # Check minimum interval
                interval_seconds = self._parse_time_to_seconds(interval)
                min_interval = STREAMING_VALIDATIONS["min_trigger_interval_seconds"]
                if interval_seconds < min_interval:
                    raise StreamingValidationError(
                        f"Node '{node_name}' trigger interval '{interval}' is below minimum allowed interval of {min_interval} seconds",
                        field=TRIGGER_INTERVAL_FIELD,
                        expected=f">= {min_interval} seconds",
                        actual=f"{interval_seconds} seconds",
                    )

        except StreamingValidationError:
            raise
        except Exception as e:
            raise StreamingValidationError(
                f"Error validating trigger config for node '{node_name}': {str(e)}",
                cause=e,
            )

    def _validate_time_interval(self, interval: str) -> bool:
        """Validate time interval format with enhanced pattern matching."""
        if not isinstance(interval, str):
            return False

        # Enhanced pattern to support more time units
        pattern = r"^\d+\s+(second|seconds|minute|minutes|hour|hours|day|days|millisecond|milliseconds|microsecond|microseconds)$"
        return bool(re.match(pattern, interval.strip(), re.IGNORECASE))

    def _parse_time_to_seconds(self, interval: str) -> float:
        """Parse time interval to seconds with support for more units."""
        if not isinstance(interval, str):
            return 0.0

        parts = interval.strip().split()
        if len(parts) != 2:
            return 0.0

        try:
            number = float(parts[0])
            unit = parts[1].lower()

            unit_multipliers = {
                "millisecond": 0.001,
                "milliseconds": 0.001,
                "microsecond": 0.000001,
                "microseconds": 0.000001,
                "second": 1.0,
                "seconds": 1.0,
                "minute": 60.0,
                "minutes": 60.0,
                "hour": 3600.0,
                "hours": 3600.0,
                "day": 86400.0,
                "days": 86400.0,
            }

            multiplier = unit_multipliers.get(unit, 0.0)
            return number * multiplier

        except (ValueError, TypeError):
            return 0.0

    def _parse_time_to_minutes(self, interval: str) -> float:
        """Parse time interval to minutes."""
        seconds = self._parse_time_to_seconds(interval)
        return seconds / 60.0

    @handle_streaming_error
    def validate_pipeline_compatibility(
        self, batch_config: Dict[str, Any], streaming_config: Dict[str, Any]
    ) -> List[str]:
        """Validate compatibility between batch and streaming configurations."""
        try:
            warnings: List[str] = []

            batch_nodes = batch_config.get("nodes", [])
            streaming_nodes = streaming_config.get("nodes", [])

            batch_outputs = self._extract_output_paths(batch_nodes)
            streaming_outputs = self._extract_output_paths(streaming_nodes)

            self._append_shared_output_warning(
                warnings, batch_outputs, streaming_outputs
            )
            self._append_duplicate_checkpoint_warnings(warnings, streaming_nodes)
            self._append_resource_usage_warning(warnings, batch_nodes, streaming_nodes)

            return warnings

        except Exception as e:
            logger.error(f"Error validating pipeline compatibility: {str(e)}")
            return [f"Error during compatibility validation: {str(e)}"]

    def _extract_output_paths(self, nodes: List[Any]) -> set:
        """Extract output path strings from a list of nodes."""
        outputs = set()
        for node in nodes:
            if isinstance(node, dict) and "output" in node:
                output_path = node["output"].get("path")
                if output_path:
                    outputs.add(output_path)
        return outputs

    def _append_shared_output_warning(
        self, warnings: List[str], batch_outputs: set, streaming_outputs: set
    ) -> None:
        """Append a warning if there are shared output resources."""
        shared = batch_outputs.intersection(streaming_outputs)
        if shared:
            warnings.append(
                f"Shared output resources detected: {shared}. "
                f"Ensure proper coordination to avoid conflicts."
            )

    def _append_duplicate_checkpoint_warnings(
        self, warnings: List[str], streaming_nodes: List[Any]
    ) -> None:
        """Detect duplicate checkpoint locations across streaming nodes and append warnings."""
        checkpoint_locations = set()
        for node in streaming_nodes:
            if not isinstance(node, dict):
                continue
            checkpoint = node.get("streaming", {}).get("checkpoint_location")
            if not checkpoint:
                continue
            if checkpoint in checkpoint_locations:
                warnings.append(
                    f"Duplicate checkpoint location: {checkpoint}. "
                    f"Each streaming query should have a unique checkpoint."
                )
            checkpoint_locations.add(checkpoint)

    def _append_resource_usage_warning(
        self, warnings: List[str], batch_nodes: List[Any], streaming_nodes: List[Any]
    ) -> None:
        """Append a resource usage warning when node counts indicate potential issues."""
        if len(batch_nodes) > 10 and len(streaming_nodes) > 5:
            warnings.append(
                f"High resource usage detected: {len(batch_nodes)} batch nodes and "
                f"{len(streaming_nodes)} streaming nodes. Consider resource allocation."
            )

    def validate_resource_requirements(
        self, pipeline_config: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Validate and estimate resource requirements for the pipeline."""
        try:
            nodes = pipeline_config.get("nodes", [])

            estimated_resources = {
                "total_nodes": len(nodes),
                "streaming_nodes": 0,
                "estimated_memory_mb": 0,
                "estimated_cores": 0,
                "warnings": [],
                "recommendations": [],
            }

            for node in nodes:
                if isinstance(node, dict):
                    # Check if it's a streaming node
                    if node.get("input", {}).get("format") in [
                        fmt.value for fmt in StreamingFormat
                    ]:
                        estimated_resources["streaming_nodes"] += 1
                        estimated_resources[
                            "estimated_memory_mb"
                        ] += 512  # Base memory per streaming node
                        estimated_resources["estimated_cores"] += 1

                    # Check for high-resource operations
                    if node.get("input", {}).get("format") == "kafka":
                        estimated_resources[
                            "estimated_memory_mb"
                        ] += 256  # Additional for Kafka

                    if node.get("output", {}).get("format") == "delta":
                        estimated_resources[
                            "estimated_memory_mb"
                        ] += 128  # Additional for Delta

            # Generate recommendations
            if estimated_resources["streaming_nodes"] > 10:
                estimated_resources["warnings"].append(
                    f"High number of streaming nodes ({estimated_resources['streaming_nodes']}). "
                    f"Consider splitting into multiple pipelines."
                )

            if estimated_resources["estimated_memory_mb"] > 4096:
                estimated_resources["recommendations"].append(
                    f"High memory usage estimated ({estimated_resources['estimated_memory_mb']}MB). "
                    f"Consider increasing cluster resources."
                )

            return estimated_resources

        except Exception as e:
            logger.error(f"Error validating resource requirements: {str(e)}")
            return {"error": str(e), "total_nodes": 0, "streaming_nodes": 0}
