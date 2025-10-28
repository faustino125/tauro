"""
Copyright (c) 2025 Faustino Lopez Ramos.
For licensing information, see the LICENSE file in the project root
"""
from __future__ import annotations
from typing import Any, Dict, List, Optional, Union, Callable, TypeVar
from abc import ABC, abstractmethod
import os
import re
from functools import lru_cache
import json

from pydantic import validator
from pydantic.fields import ModelField

# Optional HTML sanitization - install with: pip install bleach
try:
    import bleach  # type: ignore

    BLEACH_AVAILABLE = True
except ImportError:
    BLEACH_AVAILABLE = False


# Custom validation error (simplified)
class CustomValidationError(ValueError):
    """Custom validation error"""

    def __init__(self, message: str, details: Optional[Dict] = None):
        super().__init__(message)
        self.details = details or {}


T = TypeVar("T")


# =============================================================================
# CONFIGURATION
# =============================================================================

# Global configuration for validation limits - cross-platform compatible
VALIDATION_CONFIG = {
    "max_string_length": 10000,
    "max_key_length": 100,
    "max_array_length": 1000,
    "max_filename_length": 255,
    "max_sql_identifier_length": 63,
    "max_nesting_depth": 10,
    "default_root_path": "root",
}


def configure_validation_limits(**kwargs):
    """Configure global validation limits for cross-platform compatibility"""
    global VALIDATION_CONFIG
    VALIDATION_CONFIG.update(kwargs)


def get_validation_limits() -> Dict[str, Any]:
    """Get current validation limits"""
    return VALIDATION_CONFIG.copy()


# =============================================================================
# VALIDATION RULES
# =============================================================================


class ValidationRule(ABC):
    """Base class for validation rules"""

    @abstractmethod
    def validate(self, value: Any, field_name: str = None) -> None:
        """Validate a value, raise ValidationError if invalid"""
        pass

    @property
    @abstractmethod
    def error_message(self) -> str:
        """Error message for validation failure"""
        pass


class RequiredRule(ValidationRule):
    """Rule for required fields"""

    def validate(self, value: Any, field_name: str = None) -> None:
        if value is None or (isinstance(value, str) and not value.strip()):
            raise CustomValidationError(f"Field '{field_name}' is required")

    @property
    def error_message(self) -> str:
        return "This field is required"


class LengthRule(ValidationRule):
    """Rule for string length validation"""

    def __init__(self, min_length: int = None, max_length: int = None):
        self.min_length = min_length
        self.max_length = max_length

    def validate(self, value: Any, field_name: str = None) -> None:
        if not isinstance(value, str):
            return

        if self.min_length and len(value) < self.min_length:
            raise CustomValidationError(
                f"Field '{field_name}' must be at least {self.min_length} characters long"
            )

        if self.max_length and len(value) > self.max_length:
            raise CustomValidationError(
                f"Field '{field_name}' must be at most {self.max_length} characters long"
            )

    @property
    def error_message(self) -> str:
        return f"Length must be between {self.min_length} and {self.max_length} characters"


class RegexRule(ValidationRule):
    """Rule for regex pattern validation"""

    def __init__(self, pattern: str, message: str = None):
        self.pattern = re.compile(pattern)
        self._error_message = message or f"Must match pattern: {pattern}"

    def validate(self, value: Any, field_name: str = None) -> None:
        if not isinstance(value, str):
            return

        if not self.pattern.match(value):
            raise CustomValidationError(f"Field '{field_name}': {self._error_message}")

    @property
    def error_message(self) -> str:
        return self._error_message


class RangeRule(ValidationRule):
    """Rule for numeric range validation"""

    def __init__(self, min_value: Union[int, float] = None, max_value: Union[int, float] = None):
        self.min_value = min_value
        self.max_value = max_value

    def validate(self, value: Any, field_name: str = None) -> None:
        if not isinstance(value, (int, float)):
            return

        if self.min_value is not None and value < self.min_value:
            raise CustomValidationError(f"Field '{field_name}' must be at least {self.min_value}")

        if self.max_value is not None and value > self.max_value:
            raise CustomValidationError(f"Field '{field_name}' must be at most {self.max_value}")

    @property
    def error_message(self) -> str:
        return f"Value must be between {self.min_value} and {self.max_value}"


class EnumRule(ValidationRule):
    """Rule for enum value validation"""

    def __init__(self, allowed_values: List[Any]):
        self.allowed_values = allowed_values

    def validate(self, value: Any, field_name: str = None) -> None:
        if value not in self.allowed_values:
            raise CustomValidationError(
                f"Field '{field_name}' must be one of: {', '.join(str(v) for v in self.allowed_values)}"
            )

    @property
    def error_message(self) -> str:
        return f"Must be one of: {', '.join(str(v) for v in self.allowed_values)}"


class CustomRule(ValidationRule):
    """Rule for custom validation functions"""

    def __init__(self, validator_func: Callable[[Any], bool], message: str):
        self.validator_func = validator_func
        self._error_message = message

    def validate(self, value: Any, field_name: str = None) -> None:
        if not self.validator_func(value):
            raise CustomValidationError(f"Field '{field_name}': {self._error_message}")

    @property
    def error_message(self) -> str:
        return self._error_message


# =============================================================================
# VALIDATION SCHEMA
# =============================================================================


class ValidationSchema:
    """Schema for field validation rules"""

    def __init__(self):
        self.rules: Dict[str, List[ValidationRule]] = {}

    def add_rule(self, field: str, rule: ValidationRule) -> None:
        """Add a validation rule for a field"""
        if field not in self.rules:
            self.rules[field] = []
        self.rules[field].append(rule)

    def validate(self, data: Dict[str, Any]) -> Dict[str, List[str]]:
        """Validate data against schema, return field -> errors mapping"""
        errors = {}

        for field, rules in self.rules.items():
            if field in data:
                value = data[field]
                field_errors = []

                for rule in rules:
                    try:
                        rule.validate(value, field)
                    except CustomValidationError as e:
                        field_errors.append(str(e))

                if field_errors:
                    errors[field] = field_errors

        return errors


# =============================================================================
# INPUT SANITIZATION
# =============================================================================


class InputSanitizer:
    """Input sanitization utilities - cross-platform compatible"""

    @staticmethod
    def sanitize_html(text: str, allowed_tags: List[str] = None) -> str:
        """Sanitize HTML content"""
        if not BLEACH_AVAILABLE:
            # Fallback: basic HTML escaping without bleach
            return (
                text.replace("&", "&amp;")
                .replace("<", "&lt;")
                .replace(">", "&gt;")
                .replace('"', "&quot;")
                .replace("'", "&#x27;")
            )

        if allowed_tags is None:
            allowed_tags = []
        return bleach.clean(text, tags=allowed_tags, strip=True)

    @staticmethod
    def sanitize_filename(filename: str) -> str:
        """
        Sanitize filename to prevent path traversal - cross-platform compatible.
        Works on Windows, Linux, macOS, and other operating systems.
        """
        if not filename:
            return "unnamed_file"

        # Get OS-specific path separators
        dangerous_chars = [os.sep]
        if os.altsep:  # Windows has both \ and /
            dangerous_chars.append(os.altsep)

        # Add universal dangerous characters
        dangerous_chars.extend(["..", "<", ">", ":", "*", "?", '"', "|", "\0", "/"])

        safe_filename = filename
        for char in dangerous_chars:
            safe_filename = safe_filename.replace(char, "_")

        # Remove control characters (0-31 ASCII)
        safe_filename = "".join(c for c in safe_filename if ord(c) >= 32)

        # Ensure it's not empty and limit length
        if not safe_filename.strip():
            return "unnamed_file"

        # Get max filename length from config
        limits = get_validation_limits()
        return safe_filename.strip()[: limits["max_filename_length"]]

    @staticmethod
    def sanitize_sql_identifier(identifier: str) -> str:
        """Sanitize SQL identifier - only alphanumeric and underscore"""
        limits = get_validation_limits()
        return re.sub(r"\W", "_", identifier)[: limits["max_sql_identifier_length"]]

    @staticmethod
    def normalize_whitespace(text: str) -> str:
        """Normalize whitespace in text"""
        return " ".join(text.split())


# =============================================================================
# DATA VALIDATOR
# =============================================================================


class DataValidator:
    """
    Advanced data validation service with caching and business rules
    """

    def __init__(self):
        self.schemas: Dict[str, ValidationSchema] = {}
        self.business_rules: Dict[str, Callable] = {}

    def register_schema(self, name: str, schema: ValidationSchema) -> None:
        """Register a validation schema"""
        self.schemas[name] = schema

    def register_business_rule(self, name: str, rule_func: Callable) -> None:
        """Register a business rule validator"""
        self.business_rules[name] = rule_func

    def _hash_data(self, data: Dict[str, Any]) -> str:
        return json.dumps(data, sort_keys=True, default=str)

    @lru_cache(maxsize=1000)
    def _validate_cached(self, schema_name: str, data_json: str) -> Dict[str, Any]:
        data = json.loads(data_json)
        return self._validate_data_uncached(schema_name, data)

    def validate_data(self, schema_name: str, data: Dict[str, Any]) -> Dict[str, Any]:
        """Public API: validate data dict against a schema with caching."""
        if data is None:
            data = {}

        if not isinstance(data, dict):
            raise ValueError("Data must be a dictionary")

        data_json = self._hash_data(data)
        return self._validate_cached(schema_name, data_json)

    def _validate_data_uncached(self, schema_name: str, data: Dict[str, Any]) -> Dict[str, Any]:
        """Validate data without caching"""
        if schema_name not in self.schemas:
            return {
                "valid": False,
                "errors": {"schema": [f"Unknown validation schema: {schema_name}"]},
            }

        schema = self.schemas[schema_name]
        validation_errors = schema.validate(data)

        # Apply business rules
        business_errors = {}
        for rule_name, rule_func in self.business_rules.items():
            try:
                rule_func(data)
            except Exception as e:
                business_errors[rule_name] = [str(e)]

        all_errors = {**validation_errors, **business_errors}

        return {"valid": len(all_errors) == 0, "errors": all_errors}

    def clear_cache(self) -> None:
        """Clear validation cache"""
        # Clear the lru_cache by calling cache_clear on the internal method
        try:
            self._validate_cached.cache_clear()
        except Exception:
            pass


# =============================================================================
# PYDANTIC VALIDATORS
# =============================================================================


def validate_pipeline_id(v):
    """Pydantic validator for pipeline IDs - cross-platform compatible"""
    if not v or not v.strip():
        raise ValueError("Pipeline ID cannot be empty")
    if not re.match(r"^[a-zA-Z0-9_-]+$", v):
        raise ValueError(
            "Pipeline ID must contain only alphanumeric characters, underscores, and dashes"
        )
    return v.strip()


def validate_cron_expression(v):
    """Pydantic validator for cron expressions"""
    if not v:
        raise ValueError("Cron expression cannot be empty")

    parts = v.split()
    if len(parts) != 5:
        raise ValueError(
            "Cron expression must have exactly 5 parts (minute hour day month day-of-week)"
        )

    return v


# Helper functions extracted to top-level scope to reduce cognitive complexity of the main validator
def _check_string_length(s: str, path: str, limits: Dict[str, Any]) -> None:
    if len(s) > limits["max_string_length"]:
        raise ValueError(f"String too long at {path}: {len(s)} > {limits['max_string_length']}")


def _push_dict_items(d: dict, path: str, depth: int, limits: Dict[str, Any], stack: List) -> None:
    count = len(d)
    if count > limits["max_array_length"]:
        raise ValueError(f"Object too large at {path}: {count} > {limits['max_array_length']}")
    for key, value in d.items():
        if len(str(key)) > limits["max_key_length"]:
            raise ValueError(
                f"Key too long at {path}: {len(str(key))} > {limits['max_key_length']}"
            )
        stack.append((value, f"{path}.{key}", depth + 1))


def _push_list_items(lst: list, path: str, depth: int, limits: Dict[str, Any], stack: List) -> None:
    count = len(lst)
    if count > limits["max_array_length"]:
        raise ValueError(f"Array too large at {path}: {count} > {limits['max_array_length']}")
    for i, item in enumerate(lst):
        stack.append((item, f"{path}[{i}]", depth + 1))


def validate_json_params(v):
    """
    Pydantic validator for JSON parameters - cross-platform compatible.
    Validates JSON parameters to prevent abuse while being compatible
    across different operating systems and environments.
    """
    if v is None:
        return v

    if not isinstance(v, dict):
        raise ValueError("Parameters must be a dictionary")

    limits = get_validation_limits()
    root = limits["default_root_path"]

    # Use an explicit stack to avoid deep recursion and keep the loop simple.
    stack = [(v, root, 0)]

    while stack:
        obj, path, depth = stack.pop()

        if depth > limits["max_nesting_depth"]:
            raise ValueError(f"Maximum nesting depth exceeded at {path}")

        # Delegate checks to small helpers to reduce complexity in this function
        if isinstance(obj, str):
            _check_string_length(obj, path, limits)
        elif isinstance(obj, dict):
            _push_dict_items(obj, path, depth, limits, stack)
        elif isinstance(obj, list):
            _push_list_items(obj, path, depth, limits, stack)

        # other primitive types are allowed without further checks

    return v


# =============================================================================
# PREDEFINED SCHEMAS
# =============================================================================


def create_pipeline_run_schema() -> ValidationSchema:
    """Create validation schema for pipeline runs"""
    schema = ValidationSchema()

    # Pipeline ID validation
    schema.add_rule("pipeline_id", RequiredRule())
    schema.add_rule("pipeline_id", LengthRule(min_length=1, max_length=255))
    schema.add_rule(
        "pipeline_id",
        RegexRule(r"^[a-zA-Z0-9_-]+$", "Only alphanumeric, underscore, and dash allowed"),
    )

    # Parameters validation
    schema.add_rule(
        "params",
        CustomRule(
            lambda x: isinstance(x, dict) or x is None,
            "Parameters must be a dictionary or null",
        ),
    )

    return schema


def create_schedule_schema() -> ValidationSchema:
    """Create validation schema for schedules"""
    schema = ValidationSchema()

    schema.add_rule("pipeline_id", RequiredRule())
    schema.add_rule("pipeline_id", LengthRule(min_length=1, max_length=255))

    schema.add_rule("kind", RequiredRule())
    schema.add_rule("kind", EnumRule(["INTERVAL", "CRON"]))

    schema.add_rule("expression", RequiredRule())
    schema.add_rule("expression", LengthRule(min_length=1, max_length=255))

    schema.add_rule("max_concurrency", RangeRule(min_value=1, max_value=100))
    schema.add_rule("timeout_seconds", RangeRule(min_value=1, max_value=86400))

    return schema


# =============================================================================
# GLOBAL VALIDATOR INSTANCE
# =============================================================================

# Global validator instance
validator = DataValidator()

# Register common schemas
validator.register_schema("pipeline_run", create_pipeline_run_schema())
validator.register_schema("schedule", create_schedule_schema())


def validate_api_input(schema_name: str, data: Dict[str, Any]) -> None:
    """Validate API input data, raise ValidationError if invalid"""
    result = validator.validate_data(schema_name, data)

    if not result["valid"]:
        # Convert errors to ValidationError
        error_messages = []
        for field, messages in result["errors"].items():
            error_messages.extend(messages)

        raise CustomValidationError(
            f"Validation failed: {'; '.join(error_messages)}", details=result["errors"]
        )


# =============================================================================
# CONVENIENCE FUNCTIONS - For backward compatibility
# =============================================================================

# Alias for cross-platform filename sanitization
sanitize_filename_cross_platform = InputSanitizer.sanitize_filename


# =============================================================================
# EXPORTS
# =============================================================================

__all__ = [
    # Configuration
    "VALIDATION_CONFIG",
    "configure_validation_limits",
    "get_validation_limits",
    # Rules
    "ValidationRule",
    "RequiredRule",
    "LengthRule",
    "RegexRule",
    "RangeRule",
    "EnumRule",
    "CustomRule",
    # Schema
    "ValidationSchema",
    # Sanitization
    "InputSanitizer",
    # Validator
    "DataValidator",
    "validator",
    # Pydantic validators
    "validate_pipeline_id",
    "validate_cron_expression",
    "validate_json_params",
    # Schemas
    "create_pipeline_run_schema",
    "create_schedule_schema",
    # Functions
    "validate_api_input",
    "sanitize_filename_cross_platform",
]
