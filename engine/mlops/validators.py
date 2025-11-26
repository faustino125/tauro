import re
import math
from pathlib import Path
from typing import Any, Dict, Optional

from loguru import logger

from engine.mlops.exceptions import (
    InvalidParameterError,
    InvalidMetricError,
)


# Valid characters for model/experiment names
VALID_NAME_PATTERN = re.compile(r"^[a-zA-Z0-9_\-\.]+$")
MIN_NAME_LENGTH = 1
MAX_NAME_LENGTH = 255


class ValidationError(ValueError):
    """Base validation error"""

    pass


class PathValidator:
    """Validator for file paths"""

    @staticmethod
    def validate_path(path: str, base_path: Optional[Path] = None) -> Path:
        """
        Validate and sanitize a path.

        Prevents:
        - Absolute paths
        - Parent directory traversal (.., ~)
        - Path traversal attacks
        - Paths outside base directory

        Args:
            path: Path string to validate
            base_path: Optional base path to validate against

        Returns:
            Validated Path object

        Raises:
            ValidationError: If path is invalid
        """
        if not path or not isinstance(path, str):
            raise ValidationError("Path must be non-empty string")

        # 1. Check for absolute paths
        path_obj = Path(path)
        if path_obj.is_absolute():
            raise ValidationError(f"Absolute paths not allowed: {path}")

        # 2. Check for parent directory traversal
        if ".." in path or "~" in path:
            raise ValidationError(f"Directory traversal not allowed: {path}")

        # 3. Resolve path
        if base_path:
            full_path = (base_path / path).resolve()
            base_resolved = base_path.resolve()

            try:
                full_path.relative_to(base_resolved)
            except ValueError:
                raise ValidationError(f"Path '{path}' is outside base directory '{base_path}'")

            return full_path

        return path_obj

    @staticmethod
    def validate_file_exists(path: Path) -> Path:
        """Validate that file/directory exists"""
        if not path.exists():
            raise ValidationError(f"Path does not exist: {path}")

        return path

    @staticmethod
    def validate_is_file_or_dir(path: Path) -> Path:
        """Validate that path is file or directory"""
        if not path.is_file() and not path.is_dir():
            raise ValidationError(f"Path must be file or directory: {path}")

        return path


class NameValidator:
    """Validator for model/experiment names"""

    @staticmethod
    def validate_name(name: str, entity_type: str = "entity") -> str:
        """
        Validate a name (model, experiment, run, etc).

        Requirements:
        - Non-empty string
        - 1-255 characters
        - Only alphanumeric, underscore, dash, dot
        - Must start with letter or number

        Args:
            name: Name to validate
            entity_type: Type of entity (for error message)

        Returns:
            Validated name

        Raises:
            ValidationError: If name is invalid
        """
        # 1. Type and non-empty
        if not name or not isinstance(name, str):
            raise ValidationError(
                f"{entity_type} name must be non-empty string, got {type(name).__name__}"
            )

        # 2. Length
        if len(name) < MIN_NAME_LENGTH or len(name) > MAX_NAME_LENGTH:
            raise ValidationError(
                f"{entity_type} name must be {MIN_NAME_LENGTH}-{MAX_NAME_LENGTH} characters, "
                f"got {len(name)}"
            )

        # 3. Characters
        if not VALID_NAME_PATTERN.match(name):
            raise ValidationError(
                f"{entity_type} name '{name}' contains invalid characters. "
                f"Only alphanumeric, underscore, dash, and dot allowed."
            )

        # 4. Must start with letter or number
        if not name[0].isalnum():
            raise ValidationError(f"{entity_type} name '{name}' must start with letter or number")

        return name


class MetricValidator:
    """Validator for metric values"""

    @staticmethod
    def validate_metric_value(key: str, value: Any) -> float:
        """
        Validate a metric value.

        Requirements:
        - Must be numeric (int or float)
        - Cannot be NaN
        - Cannot be Inf (configurable)

        Args:
            key: Metric name
            value: Metric value

        Returns:
            Validated float value

        Raises:
            InvalidMetricError: If value is invalid
        """
        # 1. Type check
        if not isinstance(value, (int, float)):
            raise InvalidMetricError(
                key, value, f"Metrics must be numeric (int or float), got {type(value).__name__}"
            )

        # 2. NaN check
        try:
            if math.isnan(value):
                raise InvalidMetricError(key, value, "Metrics cannot be NaN")
        except (TypeError, ValueError):
            pass

        # 3. Inf check (warn but allow)
        try:
            if math.isinf(value):
                logger.warning(
                    f"Metric '{key}' has infinite value. "
                    f"This may cause issues with visualization."
                )
        except (TypeError, ValueError):
            pass

        return float(value)

    @staticmethod
    def validate_step(step: Any) -> int:
        """Validate step value"""
        if not isinstance(step, int):
            raise ValidationError(f"Step must be integer, got {type(step).__name__}")

        if step < 0:
            raise ValidationError(f"Step must be non-negative, got {step}")

        return step


class ParameterValidator:
    """Validator for hyperparameters"""

    SUPPORTED_TYPES = {str, int, float, bool, type(None)}

    @staticmethod
    def _validate_key(key: Any) -> str:
        """Helper to validate a parameter key"""
        if not isinstance(key, str):
            raise InvalidParameterError(
                key, f"Parameter name must be string, got {type(key).__name__}"
            )
        if not key.strip():
            raise InvalidParameterError(key, "Parameter name cannot be empty")
        return key

    @staticmethod
    def _validate_value(key: str, value: Any) -> Any:
        """Helper to validate a parameter value"""
        if type(value) not in ParameterValidator.SUPPORTED_TYPES:
            raise InvalidParameterError(
                key,
                f"Parameter value type {type(value).__name__} not supported. "
                f"Supported: {ParameterValidator.SUPPORTED_TYPES}",
            )
        if isinstance(value, float):
            if math.isnan(value):
                raise InvalidParameterError(key, "Parameter value cannot be NaN")
            if math.isinf(value):
                logger.warning(f"Parameter '{key}' has infinite value")
        return value

    @staticmethod
    def validate_parameters(
        parameters: Optional[Dict[str, Any]], max_items: int = 1000
    ) -> Optional[Dict[str, Any]]:
        """
        Validate hyperparameters dict.

        Requirements:
        - Must be dict or None
        - Max 1000 items
        - Keys must be valid names
        - Values must be serializable

        Args:
            parameters: Parameters dict
            max_items: Maximum number of parameters

        Returns:
            Validated dict or None

        Raises:
            InvalidParameterError: If invalid
        """
        if parameters is None:
            return None

        if not isinstance(parameters, dict):
            raise InvalidParameterError(
                "parameters", f"Parameters must be dict or None, got {type(parameters).__name__}"
            )

        if len(parameters) > max_items:
            raise InvalidParameterError(
                "parameters", f"Too many parameters ({len(parameters)} > {max_items})"
            )

        # Validate each key-value pair using helper methods to reduce complexity
        for key, value in parameters.items():
            ParameterValidator._validate_key(key)
            ParameterValidator._validate_value(key, value)

        return parameters

    @staticmethod
    def validate_parameter(key: str, value: Any) -> Any:
        """Validate single parameter"""
        if not isinstance(key, str):
            raise InvalidParameterError(key, "Parameter name must be string")

        if not key.strip():
            raise InvalidParameterError(key, "Parameter name cannot be empty")

        if type(value) not in ParameterValidator.SUPPORTED_TYPES:
            raise InvalidParameterError(
                key, f"Parameter value type not supported: {type(value).__name__}"
            )

        return value


class MetadataValidator:
    """Validator for metadata dicts (tags, etc)"""

    @staticmethod
    def validate_tags(
        tags: Optional[Dict[str, str]], max_items: int = 100
    ) -> Optional[Dict[str, str]]:
        """
        Validate tags dict.

        Args:
            tags: Tags dict
            max_items: Maximum number of tags

        Returns:
            Validated dict or None
        """
        if tags is None:
            return None

        if not isinstance(tags, dict):
            raise ValidationError(f"Tags must be dict or None, got {type(tags).__name__}")

        if len(tags) > max_items:
            raise ValidationError(f"Too many tags ({len(tags)} > {max_items})")

        # Validate each tag
        for key, value in tags.items():
            if not isinstance(key, str):
                raise ValidationError(f"Tag key must be string, got {type(key).__name__}")

            if not isinstance(value, str):
                raise ValidationError(f"Tag value must be string, got {type(value).__name__}")

        return tags

    @staticmethod
    def validate_description(description: Optional[str]) -> Optional[str]:
        """Validate description"""
        if description is None:
            return None

        if not isinstance(description, str):
            raise ValidationError(f"Description must be string, got {type(description).__name__}")

        if len(description) > 10000:
            raise ValidationError(f"Description too long ({len(description)} > 10000)")

        return description


class FrameworkValidator:
    """Validator for ML framework names"""

    SUPPORTED_FRAMEWORKS = {
        "scikit-learn",
        "sklearn",
        "xgboost",
        "lightgbm",
        "catboost",
        "pytorch",
        "tensorflow",
        "keras",
        "jax",
        "onnx",
        "mxnet",
        "spark-mllib",
        "custom",
    }

    @staticmethod
    def validate_framework(framework: str, allow_custom: bool = True) -> str:
        """
        Validate framework name.

        Args:
            framework: Framework name
            allow_custom: Whether to allow "custom"

        Returns:
            Validated framework name
        """
        if not isinstance(framework, str):
            raise ValidationError(f"Framework must be string, got {type(framework).__name__}")

        framework_lower = framework.lower().strip()

        # Allow custom or supported
        if framework_lower == "custom":
            if not allow_custom:
                raise ValidationError("'custom' framework not allowed in this context")
            return "custom"

        if framework_lower not in FrameworkValidator.SUPPORTED_FRAMEWORKS:
            raise ValidationError(
                f"Unsupported framework: {framework}. "
                f"Supported: {FrameworkValidator.SUPPORTED_FRAMEWORKS}"
            )

        return framework_lower

    @staticmethod
    def add_supported_framework(framework: str):
        """Add a new supported framework"""
        FrameworkValidator.SUPPORTED_FRAMEWORKS.add(framework.lower())


class ArtifactValidator:
    """Validator for artifacts"""

    @staticmethod
    def validate_artifact_type(artifact_type: str) -> str:
        """
        Validate artifact type.

        Args:
            artifact_type: Artifact type (e.g., "sklearn", "pytorch", "onnx")

        Returns:
            Validated artifact type
        """
        if not artifact_type or not isinstance(artifact_type, str):
            raise ValidationError(
                f"Artifact type must be non-empty string, got {type(artifact_type).__name__}"
            )

        artifact_type = artifact_type.lower().strip()

        # Allow any string (frameworks can have custom artifact types)
        if not re.match(r"^[a-z0-9_\-]+$", artifact_type):
            raise ValidationError(
                f"Artifact type '{artifact_type}' contains invalid characters. "
                f"Only lowercase alphanumeric, underscore, and dash allowed."
            )

        return artifact_type


# Convenience functions
def validate_model_name(name: str) -> str:
    """Validate model name"""
    return NameValidator.validate_name(name, "Model")


def validate_experiment_name(name: str) -> str:
    """Validate experiment name"""
    return NameValidator.validate_name(name, "Experiment")


def validate_run_name(name: str) -> str:
    """Validate run name"""
    return NameValidator.validate_name(name, "Run")


def validate_framework(framework: str, allow_custom: bool = True) -> str:
    """Validate framework name"""
    return FrameworkValidator.validate_framework(framework, allow_custom)


def validate_artifact_type(artifact_type: str) -> str:
    """Validate artifact type"""
    return ArtifactValidator.validate_artifact_type(artifact_type)


def validate_metric_value(key: str, value: Any) -> float:
    """Validate metric value"""
    return MetricValidator.validate_metric_value(key, value)


def validate_parameters(params: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """Validate parameters"""
    return ParameterValidator.validate_parameters(params)


def validate_tags(tags: Optional[Dict[str, str]]) -> Optional[Dict[str, str]]:
    """Validate tags"""
    return MetadataValidator.validate_tags(tags)


def validate_description(description: Optional[str]) -> Optional[str]:
    """Validate description"""
    return MetadataValidator.validate_description(description)
