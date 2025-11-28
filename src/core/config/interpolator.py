"""
Copyright (c) 2025 Faustino Lopez Ramos. 
For licensing information, see the LICENSE file in the project root
"""
import os
from typing import Any, Dict, Set

from core.config.exceptions import ConfigLoadError


class VariableInterpolator:
    """Handles variable interpolation in configuration strings."""

    # Maximum depth to prevent infinite loops
    MAX_INTERPOLATION_DEPTH = 10
    # Maximum iterations per interpolation to prevent runaway loops
    MAX_ITERATIONS = 100

    @staticmethod
    def _replace_env_placeholders(string: str) -> str:
        """Replace any ${VAR} occurrences that match environment variables only."""
        result = string
        seen_vars: Set[str] = set()
        iterations = 0

        start = result.find("${")
        while start != -1:
            iterations += 1
            if iterations > VariableInterpolator.MAX_ITERATIONS:
                raise ConfigLoadError(
                    f"Maximum interpolation iterations exceeded ({VariableInterpolator.MAX_ITERATIONS}). "
                    "Check for circular references or overly complex variable chains."
                )

            end = result.find("}", start + 2)
            if end == -1:
                break

            var_name = result[start + 2 : end]

            if var_name in seen_vars:
                raise ConfigLoadError(
                    f"Circular reference detected: variable '${{{var_name}}}' references itself. "
                    f"Chain: {' -> '.join(seen_vars)} -> {var_name}"
                )
            seen_vars.add(var_name)

            env_value = os.getenv(var_name)
            if env_value is not None:
                result = result[:start] + env_value + result[end + 1 :]
                start = result.find("${", start + len(env_value))
            else:
                start = result.find("${", end + 1)

        return result

    @staticmethod
    def _replace_variables(result: str, variables: Dict[str, Any], _depth: int) -> str:
        """Replace placeholders from the provided variables mapping (recursing when needed)."""
        if not variables:
            return result

        for key, value in variables.items():
            placeholder = f"${{{key}}}"
            if placeholder not in result:
                continue
            if isinstance(value, str) and "${" in value:
                # Recursively interpolate the variable's value before replacing
                value = VariableInterpolator.interpolate(value, variables, _depth + 1)
            result = result.replace(placeholder, str(value))
        return result

    @staticmethod
    def interpolate(string: str, variables: Dict[str, Any], _depth: int = 0) -> str:
        """Replace variables in a string with their corresponding values.

        Args:
            string: String with ${VAR} placeholders to interpolate
            variables: Dictionary of variable values
            _depth: Internal recursion depth counter

        Returns:
            Interpolated string

        Raises:
            ConfigLoadError: If maximum interpolation depth exceeded or circular reference detected
        """
        if not string or not isinstance(string, str):
            return string

        if _depth > VariableInterpolator.MAX_INTERPOLATION_DEPTH:
            raise ConfigLoadError(
                f"Maximum interpolation depth exceeded ({VariableInterpolator.MAX_INTERPOLATION_DEPTH}). "
                "Possible circular reference in variable interpolation."
            )

        # First expand any environment variables found as ${VAR}
        result = VariableInterpolator._replace_env_placeholders(string)

        # Then replace placeholders from the provided variables mapping
        result = VariableInterpolator._replace_variables(result, variables, _depth)

        return result

    @staticmethod
    def interpolate_config_paths(config: Dict[str, Any], variables: Dict[str, Any]) -> None:
        """Recursively interpolate variables in configuration file paths in-place."""

        def _rec(node: Any):
            if isinstance(node, dict):
                # If dict has a filepath key, interpolate it
                fp = node.get("filepath")
                if isinstance(fp, str):
                    node["filepath"] = VariableInterpolator.interpolate(fp, variables)
                # Recurse over dict values
                for v in node.values():
                    _rec(v)
            elif isinstance(node, list):
                for item in node:
                    _rec(item)
            # primitives: nothing to do

        _rec(config)

    @staticmethod
    def interpolate_structure(
        value: Any, variables: Dict[str, Any], *, copy: bool = False, _depth: int = 0
    ) -> Any:
        """Recursively interpolate variables in any nested structure of dicts/lists/strings.

        Args:
            value: Structure to interpolate (dict, list, string, or primitive)
            variables: Dictionary of variable values
            copy: Whether to create a copy or modify in-place
            _depth: Internal recursion depth counter

        Returns:
            Interpolated structure

        Raises:
            ConfigLoadError: If maximum recursion depth exceeded
        """
        # Prevent excessive recursion in structure traversal
        if _depth > VariableInterpolator.MAX_INTERPOLATION_DEPTH:
            raise ConfigLoadError(
                f"Maximum structure depth exceeded ({VariableInterpolator.MAX_INTERPOLATION_DEPTH}). "
                "Configuration structure is too deeply nested."
            )

        if isinstance(value, str):
            return VariableInterpolator.interpolate(value, variables, _depth=_depth)
        if isinstance(value, list):
            if copy:
                return [
                    VariableInterpolator.interpolate_structure(
                        v, variables, copy=True, _depth=_depth + 1
                    )
                    for v in value
                ]
            for i in range(len(value)):
                value[i] = VariableInterpolator.interpolate_structure(
                    value[i], variables, copy=False, _depth=_depth + 1
                )
            return value
        if isinstance(value, dict):
            if copy:
                return {
                    k: VariableInterpolator.interpolate_structure(
                        v, variables, copy=True, _depth=_depth + 1
                    )
                    for k, v in value.items()
                }
            for k, v in value.items():
                value[k] = VariableInterpolator.interpolate_structure(
                    v, variables, copy=False, _depth=_depth + 1
                )
            return value
        return value
