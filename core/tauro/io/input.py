import glob
import os
from typing import Any, Dict, List, Optional, Set, Tuple

from loguru import logger  # type: ignore

from tauro.io.base import BaseIO
from tauro.io.exceptions import ConfigurationError, ReadOperationError
from tauro.io.factories import ReaderFactory
from tauro.io.validators import DataValidator


class InputLoadingStrategy(BaseIO):
    """Strategy for loading inputs (sequential vs parallel)."""

    def __init__(self, context: Any, reader_factory: ReaderFactory):
        super().__init__(context)
        self.reader_factory = reader_factory
        self.data_validator = DataValidator()

    def load_inputs(self, input_keys: List[str], fail_fast: bool = True) -> List[Any]:
        """Load inputs using the appropriate strategy."""
        raise NotImplementedError


class SequentialLoadingStrategy(InputLoadingStrategy):
    """Sequential input loading strategy."""

    def load_inputs(self, input_keys: List[str], fail_fast: bool = True) -> List[Any]:
        """Load datasets sequentially."""
        results: List[Any] = []
        errors: List[str] = []
        fill_none = bool(
            self._ctx_get("global_settings", {}).get("fill_none_on_error", False)
        )

        logger.info(f"Loading {len(input_keys)} datasets sequentially")
        for key in input_keys:
            try:
                logger.debug(f"Loading dataset: {key}")
                results.append(self._load_single_dataset(key))
            except Exception as e:
                msg = f"Error loading '{key}': {e}"
                logger.exception(msg)
                if fail_fast:
                    raise ReadOperationError(msg) from e
                errors.append(msg)
                if fill_none:
                    results.append(None)

        if errors:
            logger.warning(f"Completed with errors: {errors}")
        return results

    def _load_single_dataset(self, input_key: str) -> Any:
        """Load a single dataset."""
        config = self._get_dataset_config(input_key)
        format_name = config.get("format", "").lower()

        reader = self.reader_factory.get_reader(format_name)

        # SQL queries don't require a filepath
        if format_name == "query":
            return reader.read("", config)
        else:
            filepath = self._get_filepath(config, input_key)
            return reader.read(filepath, config)

    def _get_dataset_config(self, input_key: str) -> Dict[str, Any]:
        """Get configuration for a dataset."""
        input_cfg = self._ctx_get("input_config", {}) or {}
        config = input_cfg.get(input_key)
        if not config:
            raise ConfigurationError(f"Missing configuration for '{input_key}'")
        return config

    def _is_glob_path(self, path: str) -> bool:
        return any(sym in path for sym in ("*", "?", "["))

    def _get_filepath(self, config: Dict[str, Any], input_key: str) -> str:
        """Get filepath for a dataset, supporting glob patterns in local mode."""
        path = config.get("filepath")
        if not path:
            raise ConfigurationError(f"Missing filepath for '{input_key}'")

        cloud_schemes = ("s3://", "abfss://", "gs://", "dbfs:/")
        if any(str(path).startswith(pfx) for pfx in cloud_schemes):
            return path

        if self._is_local():
            if os.path.isdir(path):
                return path
            if os.path.isfile(path):
                return path
            if any(ch in str(path) for ch in ("*", "?", "[")):
                matches = glob.glob(path)
                if matches:
                    return path
                raise FileNotFoundError(
                    f"Pattern '{path}' matched no files in local mode"
                )
            if not os.path.exists(path):
                raise FileNotFoundError(f"File '{path}' does not exist in local mode")
        return path


class ParallelLoadingStrategy(InputLoadingStrategy):
    """Parallel input loading strategy using Spark."""

    def load_inputs(self, input_keys: List[str], fail_fast: bool = True) -> List[Any]:
        """Attempt to load datasets in parallel, but safely fallback to sequential."""
        logger.warning(
            "Parallel loading strategy is not supported for Spark reads; falling back to sequential."
        )
        sequential_strategy = SequentialLoadingStrategy(
            self.context, self.reader_factory
        )
        return sequential_strategy.load_inputs(input_keys, fail_fast)

    def _parallel_load_single(
        self, input_key: str
    ) -> Tuple[str, Optional[Any], Optional[str]]:
        """Load a single dataset inside a Spark task."""
        try:
            ctx = self.context
            sequential_strategy = SequentialLoadingStrategy(ctx, ReaderFactory(ctx))
            return input_key, sequential_strategy._load_single_dataset(input_key), None
        except Exception as e:
            return input_key, None, str(e)

    def _process_parallel_results(
        self,
        results: List[Tuple[str, Any, Optional[str]]],
        input_keys: List[str],
        fail_fast: bool,
    ) -> List[Any]:
        """Process the results of parallel loading operations with stable ordering."""
        data_by_key: Dict[str, Any] = {}
        errors: Dict[str, str] = {}

        for input_key, data, error in results:
            if error:
                errors[input_key] = f"Error loading '{input_key}': {error}"
            else:
                data_by_key[input_key] = data

        if errors:
            if fail_fast:
                raise ReadOperationError(
                    f"Parallel loading errors: {list(errors.values())}"
                )
            else:
                logger.warning(
                    f"Errors encountered during parallel loading: {list(errors.values())}"
                )

        ordered = [data_by_key[k] for k in input_keys if k in data_by_key]
        return ordered


class InputLoader(BaseIO):
    """Enhanced InputLoader with strategy pattern and factory pattern."""

    def __init__(self, context: Dict[str, Any]):
        """Initialize the InputLoader."""
        super().__init__(context)
        self.reader_factory = ReaderFactory(context)
        self.data_validator = DataValidator()
        self._register_custom_formats()

    def load_inputs(self, node: Dict[str, Any]) -> List[Any]:
        """Load all inputs defined for a processing node."""
        input_keys = self._get_input_keys(node)
        if not input_keys:
            logger.warning(f"Node '{node.get('name')}' has no defined inputs")
            return []

        loading_strategy = self._get_loading_strategy(node)
        return loading_strategy.load_inputs(input_keys, node.get("fail_fast", True))

    def _get_input_keys(self, node: Dict[str, Any]) -> List[str]:
        """Get input keys from a node configuration."""
        keys = node.get("input", [])
        return keys if isinstance(keys, list) else [keys]

    def _get_loading_strategy(self, node: Dict[str, Any]) -> InputLoadingStrategy:
        """Get the appropriate loading strategy."""
        if node.get("parallel", False) and self._spark_available():
            return ParallelLoadingStrategy(self.context, self.reader_factory)
        else:
            return SequentialLoadingStrategy(self.context, self.reader_factory)

    def _get_configured_formats(self) -> Set[str]:
        """Inspect input_config and return the set of formats in use."""
        input_cfg = self._ctx_get("input_config", {}) or {}
        formats = set()
        for key, cfg in input_cfg.items():
            try:
                fmt = str((cfg or {}).get("format", "")).lower().strip()
                if fmt:
                    formats.add(fmt)
            except Exception:
                logger.debug(
                    f"Skipping format detection for malformed input_config key: {key}"
                )
        return formats

    def _register_custom_formats(self) -> None:
        """Register custom format handlers if available only when used."""
        configured_formats = self._get_configured_formats()

        if "delta" in configured_formats:
            try:
                self._try_import_delta()
                logger.debug("Format delta registered successfully")
            except ImportError:
                logger.warning(
                    "Input format 'delta' configured but package 'delta-spark' is not installed. "
                    "Install with: pip install delta-spark"
                )

        if "xml" in configured_formats:
            self._try_import_xml()
            logger.debug("Format xml registration attempted")

    def _try_import_delta(self) -> None:
        """Try to import Delta Lake dependencies; don't raise at registration time."""
        try:
            from delta import configure_spark_with_delta_pip  # type: ignore  # noqa: F401
        except ImportError as e:
            raise

    def _try_import_xml(self) -> None:
        """Try to verify XML dependencies are available."""
        try:
            spark = self._ctx_spark()
            if spark:
                spark._jvm.com.databricks.spark.xml  # type: ignore[attr-defined]
        except Exception:
            logger.warning("XML format configured, but library not available.")
