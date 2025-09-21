"""
Copyright (c) 2025 Faustino Lopez Ramos. 
For licensing information, see the LICENSE file in the project root
"""
import os
from typing import Any, Dict, List


class VariableInterpolator:
    """Handles variable interpolation in configuration strings."""

    @staticmethod
    def interpolate(string: str, variables: Dict[str, Any]) -> str:
        """Replace variables in a string with their corresponding values.

        Precedence:
        1) Environment variables (${VAR}) if set
        2) Provided 'variables' mapping (fallback)
        """
        if not string:
            return string

        result = string

        start = result.find("${")
        while start != -1:
            end = result.find("}", start + 2)
            if end == -1:
                break

            var_name = result[start + 2 : end]
            env_value = os.getenv(var_name)

            if env_value is not None:
                result = result[:start] + env_value + result[end + 1 :]
                start = result.find("${", start + len(env_value))
            else:
                start = result.find("${", end + 1)

        if variables:
            for key, value in variables.items():
                placeholder = f"${{{key}}}"
                if placeholder in result:
                    result = result.replace(placeholder, str(value))

        return result

    @staticmethod
    def interpolate_config_paths(
        config: Dict[str, Any], variables: Dict[str, Any]
    ) -> None:
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
        value: Any, variables: Dict[str, Any], *, copy: bool = False
    ) -> Any:
        """Recursively interpolate variables in any nested structure of dicts/lists/strings."""
        if isinstance(value, str):
            return VariableInterpolator.interpolate(value, variables)
        if isinstance(value, list):
            if copy:
                return [
                    VariableInterpolator.interpolate_structure(v, variables, copy=True)
                    for v in value
                ]
            for i in range(len(value)):
                value[i] = VariableInterpolator.interpolate_structure(
                    value[i], variables, copy=False
                )
            return value
        if isinstance(value, dict):
            if copy:
                return {
                    k: VariableInterpolator.interpolate_structure(
                        v, variables, copy=True
                    )
                    for k, v in value.items()
                }
            for k, v in list(value.items()):
                value[k] = VariableInterpolator.interpolate_structure(
                    v, variables, copy=False
                )
            return value
        return value
