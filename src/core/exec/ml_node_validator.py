"""
Copyright (c) 2025 Faustino Lopez Ramos. 
For licensing information, see the LICENSE file in the project root
"""
import inspect
from typing import Any, Callable, Dict, List, Optional, Tuple

from loguru import logger  # type: ignore


class MLNodeValidator:
    """Validator for ML node function signatures and compatibility."""

    # Parámetros opcionales que pueden estar presentes
    OPTIONAL_PARAMETERS = {"start_date", "end_date"}

    # Optional ML-specific parameters
    OPTIONAL_ML_PARAMETERS = {"ml_context"}

    @staticmethod
    def validate_ml_node_signature(function: Callable, node_name: str) -> Tuple[bool, List[str]]:
        """Validate that an ML node function has the correct signature."""
        issues: List[str] = []

        try:
            sig = inspect.signature(function)
            params = sig.parameters
            param_names = set(params.keys())

            # Check for *args
            has_var_positional = any(
                p.kind == inspect.Parameter.VAR_POSITIONAL for p in params.values()
            )
            if not has_var_positional:
                issues.append(f"Node '{node_name}' must accept *input_dfs for variable inputs")

            missing_optional = MLNodeValidator.OPTIONAL_PARAMETERS - param_names
            if missing_optional:
                logger.debug(
                    f"Node '{node_name}' does not accept optional parameters: {missing_optional}. "
                    "These will not be passed to the function."
                )

            # Check for ML context support
            has_ml_context = "ml_context" in param_names
            has_var_keyword = any(p.kind == inspect.Parameter.VAR_KEYWORD for p in params.values())

            if not has_ml_context and not has_var_keyword:
                logger.warning(
                    f"Node '{node_name}' does not accept ml_context or **kwargs. "
                    "ML features will not be available."
                )

            is_valid = len(issues) == 0
            return is_valid, issues

        except Exception as e:
            logger.error(f"Error validating node '{node_name}': {str(e)}")
            return False, [f"Validation error: {str(e)}"]

    @staticmethod
    def get_ml_context_handling_mode(function: Callable) -> str:
        """Determine how the function accepts ML context."""
        try:
            sig = inspect.signature(function)
            params = sig.parameters

            if "ml_context" in params:
                return "explicit"

            has_var_keyword = any(p.kind == inspect.Parameter.VAR_KEYWORD for p in params.values())
            if has_var_keyword:
                return "kwargs"

            return "none"

        except Exception:
            return "none"

    @staticmethod
    def create_ml_context_wrapper(function: Callable, mode: str) -> Callable:
        """Create a wrapper that ensures ml_context is handled correctly."""
        sig = inspect.signature(function)
        params = sig.parameters
        param_names = set(params.keys())

        accepts_start_date = "start_date" in param_names
        accepts_end_date = "end_date" in param_names

        def _prepare_kwargs(
            start_date: str = None, end_date: str = None, extra: Optional[Dict[str, Any]] = None
        ) -> Dict[str, Any]:
            call_kwargs: Dict[str, Any] = {}
            if accepts_start_date and start_date is not None:
                call_kwargs["start_date"] = start_date
            if accepts_end_date and end_date is not None:
                call_kwargs["end_date"] = end_date
            if extra:
                call_kwargs.update(extra)
            return call_kwargs

        if mode == "explicit":
            # Function already expects ml_context, no wrapper needed
            return function

        if mode == "kwargs":
            # Function accepts **kwargs, wrap to pass ml_context explicitly
            def kwargs_wrapper(*input_dfs, start_date: str = None, end_date: str = None, **kwargs):
                return function(*input_dfs, **_prepare_kwargs(start_date, end_date, kwargs))

            return kwargs_wrapper

        # Function doesn't support ml_context, create pass-through wrapper
        def no_ml_wrapper(*input_dfs, start_date: str = None, end_date: str = None, **kwargs):
            return function(*input_dfs, **_prepare_kwargs(start_date, end_date))

        return no_ml_wrapper

    @staticmethod
    def validate_ml_node_execution(
        node_name: str,
        function: Callable,
        input_dfs: List[Any],
        start_date: str,
        end_date: str,
        ml_context: Optional[Dict[str, Any]] = None,
    ) -> Tuple[bool, Optional[str]]:
        """Validate that a node can be executed with given parameters."""
        # Validate signatures
        is_valid, sig_issues = MLNodeValidator.validate_ml_node_signature(function, node_name)
        if not is_valid:
            return False, "; ".join(sig_issues)

        # Validate parameter types
        if not isinstance(input_dfs, (list, tuple)):
            return False, "input_dfs must be a list or tuple of DataFrames"

        if not isinstance(start_date, str):
            return False, "start_date must be a string"

        if not isinstance(end_date, str):
            return False, "end_date must be a string"

        if ml_context is not None and not isinstance(ml_context, dict):
            return False, "ml_context must be a dictionary"

        # Validate date format (basic ISO check)
        if not MLNodeValidator._is_valid_iso_date(start_date):
            return False, f"start_date '{start_date}' is not in ISO format (YYYY-MM-DD)"

        if not MLNodeValidator._is_valid_iso_date(end_date):
            return False, f"end_date '{end_date}' is not in ISO format (YYYY-MM-DD)"

        return True, None

    @staticmethod
    def _is_valid_iso_date(date_str: str) -> bool:
        """Check if a string is in valid ISO date format (YYYY-MM-DD)."""
        if not isinstance(date_str, str):
            return False

        parts = date_str.split("-")
        if len(parts) != 3:
            return False

        try:
            year, month, day = int(parts[0]), int(parts[1]), int(parts[2])
            return 1900 <= year <= 2100 and 1 <= month <= 12 and 1 <= day <= 31
        except ValueError:
            return False

    @staticmethod
    def get_execution_guidance(
        node_name: str, function: Callable, ml_context: Optional[Dict[str, Any]]
    ) -> str:
        """Generate human-readable guidance for executing a node."""
        mode = MLNodeValidator.get_ml_context_handling_mode(function)

        guidance_lines = [
            f"Execution guidance for node '{node_name}':",
            f"  • ML context handling mode: {mode}",
        ]

        if ml_context:
            guidance_lines.append("  • ML context available:")
            if "model_version" in ml_context:
                guidance_lines.append(f"    - Model version: {ml_context['model_version']}")
            if "hyperparams" in ml_context:
                guidance_lines.append(
                    f"    - Hyperparameters: {len(ml_context['hyperparams'])} items"
                )
        else:
            guidance_lines.append("  • No ML context provided (batch mode)")

        if mode == "explicit":
            guidance_lines.append("  ✓ Function will receive ml_context parameter")
        elif mode == "kwargs":
            guidance_lines.append("  ✓ Function will receive ml_context via **kwargs")
        else:
            guidance_lines.append("  ⚠ Function does not support ML context (features unavailable)")

        return "\n".join(guidance_lines)

    @staticmethod
    def validate_ml_nodes_batch(
        nodes_to_validate: Dict[str, Callable],
    ) -> Dict[str, Dict[str, Any]]:
        """
        Validate a batch of ML nodes and return detailed report.
        """
        report: Dict[str, Dict[str, Any]] = {}

        for node_name, function in nodes_to_validate.items():
            is_valid, issues = MLNodeValidator.validate_ml_node_signature(function, node_name)
            mode = MLNodeValidator.get_ml_context_handling_mode(function)

            report[node_name] = {
                "is_valid": is_valid,
                "issues": issues,
                "ml_context_mode": mode,
                "can_use_hyperparams": mode in ("explicit", "kwargs"),
            }

        return report
