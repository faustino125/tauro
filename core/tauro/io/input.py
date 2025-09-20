from pathlib import Path
from typing import Any, Dict, List, Optional, Set

from loguru import logger  # type: ignore

from tauro.io.base import BaseIO
from tauro.io.exceptions import ConfigurationError, ReadOperationError
from tauro.io.factories import ReaderFactory


class InputLoadingStrategy(BaseIO):
    """Base strategy for loading inputs."""

    def __init__(self, context: Any, reader_factory: ReaderFactory):
        super().__init__(context)
        self.reader_factory = reader_factory

    def load_inputs(self, input_keys: List[str], fail_fast: bool = True) -> List[Any]:
        """Load inputs using the appropriate strategy."""
        raise NotImplementedError(
            "Subclasses must implement load_inputs method"
        ) from None


class SequentialLoadingStrategy(InputLoadingStrategy):
    """Sequential input loading strategy - the primary and recommended approach."""

    def load_inputs(self, input_keys: List[str], fail_fast: bool = True) -> List[Any]:
        """Load datasets sequentially with proper error handling."""
        results: List[Any] = []
        errors: List[str] = []
        fill_none = bool(
            self._ctx_get("global_settings", {}).get("fill_none_on_error", False)
        )

        logger.info(f"Loading {len(input_keys)} datasets sequentially")

        for key in input_keys:
            try:
                logger.debug(f"Loading dataset: {key}")
                result = self._load_single_dataset(key)
                results.append(result)
                logger.debug(f"Successfully loaded dataset: {key}")
            except Exception as e:
                msg = f"Error loading dataset '{key}': {e}"
                logger.exception(msg)

                if fail_fast:
                    raise ReadOperationError(msg) from e

                errors.append(msg)
                if fill_none:
                    results.append(None)

        if errors:
            logger.warning(f"Completed loading with {len(errors)} errors: {errors}")

        logger.info(f"Successfully loaded {len(results)} datasets")
        return results

    def _load_single_dataset(self, input_key: str) -> Any:
        """Load a single dataset with proper error handling."""
        try:
            config = self._get_dataset_config(input_key)
            format_name = config.get("format", "").lower()

            if not format_name:
                raise ConfigurationError(
                    f"Format not specified for dataset '{input_key}'"
                ) from None

            reader = self.reader_factory.get_reader(format_name)

            if format_name == "query":
                return reader.read("", config)
            else:
                filepath = self._get_filepath(config, input_key)
                return reader.read(filepath, config)

        except Exception as e:
            raise ReadOperationError(
                f"Failed to load dataset '{input_key}': {e}"
            ) from e

    def _get_dataset_config(self, input_key: str) -> Dict[str, Any]:
        """Get configuration for a dataset with validation."""
        try:
            input_cfg = self._ctx_get("input_config", {}) or {}
            config = input_cfg.get(input_key)

            if not config:
                raise ConfigurationError(
                    f"Missing configuration for dataset '{input_key}'"
                ) from None

            if not isinstance(config, dict):
                raise ConfigurationError(
                    f"Invalid configuration format for dataset '{input_key}': expected dict, got {type(config)}"
                ) from None

            return config
        except Exception as e:
            raise ConfigurationError(
                f"Failed to get configuration for dataset '{input_key}': {e}"
            ) from e

    def _get_filepath(self, config: Dict[str, Any], input_key: str) -> str:
        """Get filepath for a dataset, supporting glob patterns in local mode."""
        try:
            path = config.get("filepath")
            if not path:
                raise ConfigurationError(
                    f"Missing filepath for dataset '{input_key}'"
                ) from None

            cloud_schemes = ("s3://", "abfss://", "gs://", "dbfs:/")
            if any(str(path).startswith(pfx) for pfx in cloud_schemes):
                return str(path)

            if self._is_local():
                return self._handle_local_filepath(path)

            return str(path)
        except Exception as e:
            raise ConfigurationError(
                f"Failed to resolve filepath for dataset '{input_key}': {e}"
            ) from e

    def _handle_local_filepath(self, path: str) -> str:
        """Handle local file path logic with proper error handling."""
        try:
            p = Path(path)

            if p.is_dir() or p.is_file():
                return str(p)

            if self._contains_glob_pattern(path):
                return self._handle_glob_pattern(p, path)

            if not p.exists():
                raise FileNotFoundError(
                    f"File '{path}' does not exist in local mode"
                ) from None

            return str(path)
        except Exception as e:
            raise ConfigurationError(
                f"Failed to handle local filepath '{path}': {e}"
            ) from e

    def _contains_glob_pattern(self, path: str) -> bool:
        """Check if the path contains glob pattern characters."""
        return any(ch in str(path) for ch in ("*", "?", "["))

    def _handle_glob_pattern(self, p: Path, path: str) -> str:
        """Handle glob pattern matching for local files."""
        try:
            if not p.parent.exists():
                raise FileNotFoundError(
                    f"Parent directory for glob pattern '{path}' does not exist"
                ) from None

            matches = list(p.parent.glob(p.name))
            if matches:
                logger.debug(f"Glob pattern '{path}' matched {len(matches)} files")
                return str(path)  # Return original pattern for Spark to handle
            else:
                raise FileNotFoundError(
                    f"Glob pattern '{path}' matched no files in local mode"
                ) from None
        except Exception as e:
            raise ConfigurationError(
                f"Failed to process glob pattern '{path}': {e}"
            ) from e


class InputLoader(BaseIO):
    """Enhanced InputLoader with sequential loading strategy only."""

    def __init__(self, context: Dict[str, Any]):
        """Initialize the InputLoader."""
        super().__init__(context)
        self.reader_factory = ReaderFactory(context)
        self._register_custom_formats()

    def load_inputs(self, node: Dict[str, Any]) -> List[Any]:
        """Load all inputs defined for a processing node."""
        try:
            input_keys = self._get_input_keys(node)
            if not input_keys:
                node_name = node.get("name", "unnamed")
                logger.warning(f"Node '{node_name}' has no defined inputs")
                return []

            loading_strategy = SequentialLoadingStrategy(
                self.context, self.reader_factory
            )
            fail_fast = node.get("fail_fast", True)

            return loading_strategy.load_inputs(input_keys, fail_fast)
        except Exception as e:
            node_name = node.get("name", "unnamed")
            raise ReadOperationError(
                f"Failed to load inputs for node '{node_name}': {e}"
            ) from e

    def _get_input_keys(self, node: Dict[str, Any]) -> List[str]:
        """Get input keys from a node configuration with validation."""
        try:
            keys = node.get("input", [])

            if isinstance(keys, str):
                return [keys]
            elif isinstance(keys, list):
                for key in keys:
                    if not isinstance(key, str) or not key.strip():
                        raise ConfigurationError(
                            f"Invalid input key: {key}. All input keys must be non-empty strings."
                        ) from None
                return [key.strip() for key in keys]
            elif keys is None:
                return []
            else:
                raise ConfigurationError(
                    f"Invalid input format: expected string or list, got {type(keys)}"
                ) from None
        except Exception as e:
            node_name = node.get("name", "unnamed")
            raise ConfigurationError(
                f"Failed to get input keys for node '{node_name}': {e}"
            ) from e

    def _get_configured_formats(self) -> Set[str]:
        """Inspect input_config and return the set of formats in use."""
        input_cfg = self._ctx_get("input_config", {}) or {}
        formats = set()

        for key, cfg in input_cfg.items():
            try:
                if isinstance(cfg, dict):
                    fmt = str(cfg.get("format", "")).lower().strip()
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
                logger.debug("Delta format dependencies verified successfully")
            except ImportError as e:
                logger.warning(
                    "Input format 'delta' configured but package 'delta-spark' is not installed. "
                    "Install with: pip install delta-spark"
                )

        if "xml" in configured_formats:
            try:
                self._try_import_xml()
                logger.debug("XML format dependencies verified")
            except Exception as e:
                logger.warning(
                    f"XML format configured but dependencies not available: {e}"
                )

    def _try_import_delta(self) -> None:
        """Verify Delta Lake dependencies are available."""
        try:
            from delta import configure_spark_with_delta_pip  # type: ignore  # noqa: F401
        except ImportError as e:
            raise ImportError("Delta format requires delta-spark package") from e

    def _try_import_xml(self) -> None:
        """Verify XML dependencies are available."""
        try:
            spark = self._ctx_spark()
            if spark:
                spark._jvm.com.databricks.spark.xml  # type: ignore[attr-defined]
        except Exception as e:
            logger.warning(f"XML format configured, but library not available: {e}")
