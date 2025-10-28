"""
Tests for streaming validations with emphasis on time interval validation.

Comprehensive unit tests to ensure that validations are robust.
"""

import pytest
from tauro.streaming.validators import StreamingValidator
from tauro.streaming.exceptions import StreamingValidationError
from tauro.streaming.constants import StreamingFormat


class TestTimeIntervalValidation:
    """Tests for _validate_time_interval() method."""

    def setup_method(self):
        """Setup for each test."""
        self.validator = StreamingValidator()

    # ✅ POSITIVE TESTS - Valid intervals
    def test_valid_interval_seconds(self):
        """Test valid interval with seconds."""
        assert self.validator._validate_time_interval("10 seconds") is True
        assert self.validator._validate_time_interval("1 second") is True
        assert self.validator._validate_time_interval("60 seconds") is True

    def test_valid_interval_minutes(self):
        """Test valid interval with minutes."""
        assert self.validator._validate_time_interval("5 minutes") is True
        assert self.validator._validate_time_interval("1 minute") is True
        assert self.validator._validate_time_interval("120 minutes") is True

    def test_valid_interval_hours(self):
        """Test valid interval with hours."""
        assert self.validator._validate_time_interval("1 hour") is True
        assert self.validator._validate_time_interval("24 hours") is True
        assert self.validator._validate_time_interval("48 hours") is True

    def test_valid_interval_days(self):
        """Test valid interval with days."""
        assert self.validator._validate_time_interval("1 day") is True
        assert self.validator._validate_time_interval("7 days") is True
        assert self.validator._validate_time_interval("365 days") is True

    def test_valid_interval_milliseconds(self):
        """Test valid interval with milliseconds."""
        assert self.validator._validate_time_interval("100 milliseconds") is True
        assert self.validator._validate_time_interval("500 milliseconds") is True

    def test_valid_interval_microseconds(self):
        """Test valid interval with microseconds."""
        assert self.validator._validate_time_interval("1000 microseconds") is True

    def test_valid_interval_case_insensitive(self):
        """Test that validation is case-insensitive."""
        assert self.validator._validate_time_interval("10 SECONDS") is True
        assert self.validator._validate_time_interval("5 Minutes") is True
        assert self.validator._validate_time_interval("1 HOUR") is True

    def test_valid_interval_with_whitespace(self):
        """Test that extra whitespace is handled."""
        assert self.validator._validate_time_interval("  10 seconds  ") is True
        assert self.validator._validate_time_interval("5  minutes") is True

    # ❌ NEGATIVE TESTS - Invalid intervals
    def test_invalid_interval_zero_value(self):
        """Test that zero values are rejected."""
        assert self.validator._validate_time_interval("0 seconds") is False
        assert self.validator._validate_time_interval("0 minutes") is False
        assert self.validator._validate_time_interval("0 hours") is False

    def test_invalid_interval_negative_value(self):
        """Test that negative values are rejected."""
        assert self.validator._validate_time_interval("-1 seconds") is False
        assert self.validator._validate_time_interval("-5 minutes") is False
        assert self.validator._validate_time_interval("-10 hours") is False

    def test_invalid_interval_no_number(self):
        """Test that missing number is rejected."""
        assert self.validator._validate_time_interval("seconds") is False
        assert self.validator._validate_time_interval("minutes") is False

    def test_invalid_interval_no_unit(self):
        """Test that missing unit is rejected."""
        assert self.validator._validate_time_interval("10") is False
        assert self.validator._validate_time_interval("5") is False

    def test_invalid_interval_wrong_format(self):
        """Test that wrong format is rejected."""
        assert self.validator._validate_time_interval("10 sec") is False  # Abbreviated
        assert self.validator._validate_time_interval("10-seconds") is False  # Hyphen
        assert self.validator._validate_time_interval("10,seconds") is False  # Comma

    def test_invalid_interval_unsupported_unit(self):
        """Test that unsupported units are rejected."""
        assert self.validator._validate_time_interval("10 weeks") is False
        assert self.validator._validate_time_interval("5 months") is False
        assert self.validator._validate_time_interval("1 year") is False  # Year not supported

    def test_invalid_interval_non_string(self):
        """Test that non-string values are rejected."""
        assert self.validator._validate_time_interval(10) is False
        assert self.validator._validate_time_interval(None) is False
        assert self.validator._validate_time_interval([]) is False
        assert self.validator._validate_time_interval({}) is False

    def test_invalid_interval_empty_string(self):
        """Test that empty string is rejected."""
        assert self.validator._validate_time_interval("") is False
        assert self.validator._validate_time_interval("   ") is False

    def test_invalid_interval_exceeds_max(self):
        """Test that intervals > 1 year are rejected."""
        # 366 days > 365 days (1 year)
        assert self.validator._validate_time_interval("366 days") is False
        assert self.validator._validate_time_interval("500 days") is False

    def test_invalid_interval_floating_point(self):
        """Test that floating point numbers are rejected."""
        # Pattern only accepts integers
        assert self.validator._validate_time_interval("10.5 seconds") is False
        assert self.validator._validate_time_interval("3.14 minutes") is False

    def test_invalid_interval_multiple_spaces(self):
        """Test intervals with multiple spaces between number and unit."""
        # Should still work (space is optional after strip)
        assert self.validator._validate_time_interval("10  seconds") is True

    # 🔍 EDGE CASES
    def test_edge_case_leading_zeros(self):
        """Test intervals with leading zeros (should be accepted)."""
        # Leading zeros are valid in regex [1-9]\d*
        assert self.validator._validate_time_interval("01 seconds") is True
        assert self.validator._validate_time_interval("007 minutes") is True

    def test_edge_case_very_large_number(self):
        """Test with very large numbers."""
        # Should fail if > 1 year in seconds
        assert self.validator._validate_time_interval("99999 seconds") is True  # ~1.16 days
        assert (
            self.validator._validate_time_interval("9999999 milliseconds") is False
        )  # ~2.77 hours, but 9999999ms = 9999.999s

    def test_edge_case_singular_vs_plural(self):
        """Test both singular and plural unit names."""
        assert self.validator._validate_time_interval("1 second") is True
        assert self.validator._validate_time_interval("2 seconds") is True
        assert self.validator._validate_time_interval("1 minute") is True
        assert self.validator._validate_time_interval("2 minutes") is True


class TestParseTimeToSeconds:
    """Tests para _parse_time_to_seconds() method."""

    def setup_method(self):
        """Setup for each test."""
        self.validator = StreamingValidator()

    def test_parse_seconds(self):
        """Test parsing seconds."""
        assert self.validator._parse_time_to_seconds("10 seconds") == pytest.approx(10.0)
        assert self.validator._parse_time_to_seconds("1 second") == pytest.approx(1.0)

    def test_parse_minutes(self):
        """Test parsing minutes."""
        assert self.validator._parse_time_to_seconds("1 minute") == pytest.approx(60.0)
        assert self.validator._parse_time_to_seconds("5 minutes") == pytest.approx(300.0)

    def test_parse_hours(self):
        """Test parsing hours."""
        assert self.validator._parse_time_to_seconds("1 hour") == pytest.approx(3600.0)
        assert self.validator._parse_time_to_seconds("2 hours") == pytest.approx(7200.0)

    def test_parse_days(self):
        """Test parsing days."""
        assert self.validator._parse_time_to_seconds("1 day") == pytest.approx(86400.0)
        assert self.validator._parse_time_to_seconds("7 days") == pytest.approx(604800.0)

    def test_parse_milliseconds(self):
        """Test parsing milliseconds."""
        assert self.validator._parse_time_to_seconds("1000 milliseconds") == pytest.approx(1.0)
        assert self.validator._parse_time_to_seconds("500 milliseconds") == pytest.approx(0.5)

    def test_parse_microseconds(self):
        """Test parsing microseconds."""
        assert self.validator._parse_time_to_seconds("1000000 microseconds") == pytest.approx(1.0)

    def test_parse_invalid_returns_zero(self):
        """Test that invalid input returns 0.0."""
        assert self.validator._parse_time_to_seconds("invalid") == pytest.approx(0.0)
        assert self.validator._parse_time_to_seconds(None) == pytest.approx(0.0)
        assert self.validator._parse_time_to_seconds("") == pytest.approx(0.0)


class TestParseTimeToMinutes:
    """Tests para _parse_time_to_minutes() method."""

    def setup_method(self):
        """Setup for each test."""
        self.validator = StreamingValidator()

    def test_parse_to_minutes_seconds(self):
        """Test parsing seconds to minutes."""
        assert self.validator._parse_time_to_minutes("60 seconds") == pytest.approx(1.0)
        assert self.validator._parse_time_to_minutes("120 seconds") == pytest.approx(2.0)

    def test_parse_to_minutes_minutes(self):
        """Test parsing minutes to minutes."""
        assert self.validator._parse_time_to_minutes("5 minutes") == pytest.approx(5.0)

    def test_parse_to_minutes_hours(self):
        """Test parsing hours to minutes."""
        assert self.validator._parse_time_to_minutes("1 hour") == pytest.approx(60.0)
        assert self.validator._parse_time_to_minutes("2 hours") == pytest.approx(120.0)


class TestTriggerConfigValidation:
    """Tests para _validate_trigger_config() con time intervals."""

    def setup_method(self):
        """Setup for each test."""
        self.validator = StreamingValidator()

    def test_valid_trigger_processing_time(self):
        """Test valid processing time trigger."""
        trigger_config = {
            "type": "processing_time",
            "interval": "10 seconds",
        }
        # Should not raise
        try:
            self.validator._validate_trigger_config(trigger_config, "test_node")
        except StreamingValidationError:
            pytest.fail("Valid trigger config raised an error")

    def test_invalid_trigger_negative_interval(self):
        """Test that trigger with negative interval raises error."""
        trigger_config = {
            "type": "processing_time",
            "interval": "-5 seconds",
        }
        with pytest.raises(StreamingValidationError):
            self.validator._validate_trigger_config(trigger_config, "test_node")

    def test_invalid_trigger_zero_interval(self):
        """Test that trigger with zero interval raises error."""
        trigger_config = {
            "type": "processing_time",
            "interval": "0 seconds",
        }
        with pytest.raises(StreamingValidationError):
            self.validator._validate_trigger_config(trigger_config, "test_node")

    def test_invalid_trigger_interval_too_large(self):
        """Test that trigger with too large interval raises error."""
        trigger_config = {
            "type": "processing_time",
            "interval": "500 days",  # > 365 days
        }
        with pytest.raises(StreamingValidationError):
            self.validator._validate_trigger_config(trigger_config, "test_node")


class TestWatermarkValidation:
    """Tests para _validate_watermark_config() con time intervals."""

    def setup_method(self):
        """Setup for each test."""
        self.validator = StreamingValidator()

    def test_valid_watermark_config(self):
        """Test valid watermark configuration."""
        watermark_config = {
            "column": "event_time",
            "delay": "10 seconds",
        }
        # Should not raise
        try:
            self.validator._validate_watermark_config(watermark_config, "test_node")
        except StreamingValidationError:
            pytest.fail("Valid watermark config raised an error")

    def test_invalid_watermark_negative_delay(self):
        """Test that watermark with negative delay raises error."""
        watermark_config = {
            "column": "event_time",
            "delay": "-5 seconds",
        }
        with pytest.raises(StreamingValidationError):
            self.validator._validate_watermark_config(watermark_config, "test_node")

    def test_invalid_watermark_delay_exceeds_max(self):
        """Test that watermark delay > max allowed raises error."""
        # Max delay is 60 minutes according to STREAMING_VALIDATIONS
        watermark_config = {
            "column": "event_time",
            "delay": "120 minutes",  # > 60 minutes
        }
        with pytest.raises(StreamingValidationError):
            self.validator._validate_watermark_config(watermark_config, "test_node")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
