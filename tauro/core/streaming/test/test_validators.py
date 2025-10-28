import pytest
from tauro.streaming.validators import StreamingValidator
from tauro.streaming.exceptions import StreamingValidationError
from tauro.streaming.constants import DEFAULT_STREAMING_CONFIG


class DummyPolicy:
    def get_supported_input_formats(self):
        return ["kafka", "delta_stream", "file_stream"]

    def get_supported_output_formats(self):
        return ["console", "delta", "parquet", "json", "csv", "kafka"]


@pytest.fixture
def validator():
    return StreamingValidator(format_policy=DummyPolicy())


def test_default_pipeline_type_is_streaming(validator):
    # pipeline without "type" should default to streaming and validate successfully
    pipeline = {
        "nodes": [
            {
                "name": "n1",
                "input": {
                    "format": "kafka",
                    "options": {"subscribe": "t", "kafka.bootstrap.servers": "b"},
                },
                "output": {"format": "console"},
            }
        ]
    }
    # Should not raise
    validator.validate_streaming_pipeline_config(pipeline)


def test_parse_time_to_seconds_milliseconds(validator):
    secs = validator._parse_time_to_seconds("1 millisecond")
    assert abs(secs - 0.001) < 1e-9

    secs2 = validator._parse_time_to_seconds("2 seconds")
    assert abs(secs2 - 2.0) < 1e-9


def test_validate_time_interval_pattern_valid(validator):
    assert validator._validate_time_interval("5 minutes")
    assert validator._validate_time_interval("1 second")
    assert validator._validate_time_interval("10 milliseconds")
    assert validator._validate_time_interval("100 microseconds")


def test_validate_streaming_node_config_missing_fields_raises(validator):
    node = {
        "name": "bad_node",
        "input": {"format": "kafka", "options": {"kafka.bootstrap.servers": "b"}},
    }
    # missing output
    with pytest.raises(StreamingValidationError):
        validator.validate_streaming_node_config(node)


def test_parse_time_to_minutes_returns_float(validator):
    mins = validator._parse_time_to_minutes("90 seconds")
    assert abs(mins - 1.5) < 1e-9
