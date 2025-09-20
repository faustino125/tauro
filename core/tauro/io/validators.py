import re
from datetime import datetime
from abc import ABC, abstractmethod
from typing import Any, Dict, List

from loguru import logger  # type: ignore

from tauro.io.exceptions import ConfigurationError, DataValidationError

DATAFRAME_EMPTY_MSG = "DataFrame cannot be empty"


class BaseValidator(ABC):
    """Base class for all validators."""

    @abstractmethod
    def validate(self, data: Any, **kwargs) -> None:
        """Validate the given data."""
        ...


class ConfigValidator(BaseValidator):
    """Validates configuration objects."""

    def validate(
        self,
        config: Dict[str, Any],
        required_fields: List[str],
        config_type: str = "Configuration",
    ) -> None:
        """Validate configuration against required fields."""
        if not config:
            raise ConfigurationError(f"{config_type} configuration cannot be empty")

        missing_fields = [field for field in required_fields if not config.get(field)]
        if missing_fields:
            error_msg = f"Required fields not specified for {config_type}: {', '.join(missing_fields)}"
            raise ConfigurationError(error_msg)

        logger.debug(f"Configuration for {config_type} successfully validated")

    def validate_output_key(self, out_key: str) -> Dict[str, str]:
        """Parse and validate output key format.

        Accepts:
        - schema.sub_folder.table_name
        - schema/sub_folder/table_name
        - With optional prefix before ':', e.g. out_delta:schema/sub/table
        """
        if not out_key or not isinstance(out_key, str):
            raise ConfigurationError("Output key must be a non-empty string")

        s = out_key.strip()
        if ":" in s:
            s = s.split(":", 1)[1].strip()

        s = s.replace("/", ".")
        parts = [p.strip() for p in s.split(".") if p.strip()]
        if len(parts) != 3:
            raise ConfigurationError(
                f"Invalid format: {out_key}. Must be one of: "
                "schema.sub_folder.table_name or schema/sub_folder/table_name"
            )

        result = {"schema": parts[0], "sub_folder": parts[1], "table_name": parts[2]}

        empty_parts = [k for k, v in result.items() if not v.strip()]
        if empty_parts:
            raise ConfigurationError(f"Empty components in output key: {empty_parts}")

        logger.debug(f"Output key parsed: {out_key} -> {result}")
        return result

    def validate_date_format(self, date_str: str) -> bool:
        """Validate date format (YYYY-MM-DD)."""
        if not date_str:
            return False

        pattern = r"^\d{4}-\d{2}-\d{2}$"
        if not re.match(pattern, date_str):
            return False

        try:
            datetime.strptime(date_str, "%Y-%m-%d")
            return True
        except ValueError:
            return False


class DataValidator(BaseValidator):
    """Validates data objects and DataFrames."""

    def validate(self, data: Any, **kwargs) -> None:
        """Validate data object."""
        if data is None:
            raise DataValidationError("Data cannot be None")

    def validate_dataframe(self, df: Any, allow_empty: bool = False) -> None:
        """Validate DataFrame object."""
        # Spark
        if self._is_spark_df(df):
            if df.isEmpty() and not allow_empty:
                raise DataValidationError(DATAFRAME_EMPTY_MSG)
            return

        # Pandas
        if self._is_pandas_df(df) and df.empty and not allow_empty:
            raise DataValidationError(DATAFRAME_EMPTY_MSG)

        # Polars
        if self._is_polars_df(df) and df.height == 0 and not allow_empty:
            raise DataValidationError(DATAFRAME_EMPTY_MSG)

    def _is_spark_df(self, df: Any) -> bool:
        """Return True if object appears to be a Spark DataFrame."""
        return hasattr(df, "isEmpty")

    def _is_pandas_df(self, df: Any) -> bool:
        """Return True if object is a pandas DataFrame."""
        try:
            import pandas as _pd  # type: ignore
        except Exception:
            return False
        return isinstance(df, _pd.DataFrame)

    def _is_polars_df(self, df: Any) -> bool:
        """Return True if object is a polars DataFrame."""
        try:
            import polars as _pl  # type: ignore
        except Exception:
            return False
        return isinstance(df, _pl.DataFrame)

    def validate_columns_exist(self, df: Any, columns: List[str]) -> None:
        """Validate that specified columns exist in DataFrame."""
        if not hasattr(df, "columns"):
            raise DataValidationError("Object does not have columns attribute")

        missing_cols = [col for col in columns if col not in df.columns]
        if missing_cols:
            raise DataValidationError(f"Columns not found in DataFrame: {missing_cols}")
