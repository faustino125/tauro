from typing import Any, Dict


class VariableInterpolator:
    """Handles variable interpolation in configuration strings."""

    @staticmethod
    def interpolate(string: str, variables: Dict[str, Any]) -> str:
        """
        Replace variables in a string with their corresponding values.
        """
        if not string or not variables:
            return string

        result = string
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
