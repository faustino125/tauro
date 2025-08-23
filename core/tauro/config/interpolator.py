import os
from typing import Any, Dict


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
        """Interpolate variables in configuration file paths in-place."""
        for config_item in config.values():
            if isinstance(config_item, dict) and "filepath" in config_item:
                config_item["filepath"] = VariableInterpolator.interpolate(
                    config_item["filepath"], variables
                )
