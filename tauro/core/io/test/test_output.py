import pytest
from unittest.mock import MagicMock
from pathlib import Path
import json
import time
from datetime import datetime

from tauro.io.output import (
    UnityCatalogManager,
    DataOutputManager,
    PathManager,
    DataFrameManager,
    UnityCatalogManager as UnityCatalogOperations,
)
from tauro.io.exceptions import (
    ConfigurationError,
    WriteOperationError,
)
from tauro.io.constants import (
    SupportedFormats,
    MIN_VACUUM_RETENTION_HOURS,
    DEFAULT_VACUUM_RETENTION_HOURS,
    CLOUD_URI_PREFIXES,
)

# Aliases for easier testing
PathResolver = PathManager
DataFrameConverter = DataFrameManager
DataWriter = DataOutputManager
ModelArtifactManager = DataOutputManager  # Simplified for testing


class ErrorHandler:
    """Simple error handler for testing"""

    def __init__(self, fail_on_error=True):
        self.fail_on_error = fail_on_error


class TestDataFrameConverter:
    """Tests for DataFrameConverter functionality."""

    @pytest.fixture
    def converter(self):
        """Fixture to create a DataFrameConverter with mock context."""
        mock_spark = MagicMock()
        return DataFrameConverter({"spark": mock_spark})

    def test_convert_to_spark_with_pandas(self, converter, monkeypatch):
        """Test converting pandas DataFrame to Spark DataFrame."""

        # Create a real class to represent a pandas.DataFrame
        class MockPandasDataFrame:
            pass

        mock_pd = MagicMock()
        mock_pd.DataFrame = MockPandasDataFrame
        # Inject the "pandas" module into tauro.io.output
        monkeypatch.setattr("tauro.io.output.pd", mock_pd, raising=False)

        mock_pandas_df = MockPandasDataFrame()
        mock_spark_df = MagicMock()

        # Setup mocks
        converter._is_spark_dataframe = MagicMock(return_value=False)
        converter._spark_available = MagicMock(return_value=True)
        # Simulate that _ctx_spark() returns a spark object with createDataFrame
        fake_spark = MagicMock()
        fake_spark.createDataFrame.return_value = mock_spark_df
        converter._ctx_spark = MagicMock(return_value=fake_spark)

        # Execute test
        result = converter.convert_to_spark(mock_pandas_df)

        # Verify results
        assert result is mock_spark_df
        fake_spark.createDataFrame.assert_called_once_with(mock_pandas_df)

    def test_convert_to_spark_with_polars(self, converter, monkeypatch):
        """Test converting Polars DataFrame to Spark DataFrame."""

        # Create classes for polars.DataFrame and pandas.DataFrame for conversion
        class MockPandasDataFrame:
            pass

        # Important: ensure to_pandas() returns the same instance on every call
        class MockPolarsDataFrame:
            def __init__(self):
                self._cached = MockPandasDataFrame()

            def to_pandas(self):
                return self._cached

        mock_pd = MagicMock()
        mock_pd.DataFrame = MockPandasDataFrame
        mock_pl = MagicMock()
        mock_pl.DataFrame = MockPolarsDataFrame

        monkeypatch.setattr("tauro.io.output.pd", mock_pd, raising=False)
        monkeypatch.setattr("tauro.io.output.pl", mock_pl, raising=False)

        mock_polars_df = MockPolarsDataFrame()
        pandas_df_instance = mock_polars_df.to_pandas()  # cached instance
        mock_spark_df = MagicMock()

        # Setup mocks
        converter._is_spark_dataframe = MagicMock(return_value=False)
        converter._spark_available = MagicMock(return_value=True)
        fake_spark = MagicMock()
        fake_spark.createDataFrame.return_value = mock_spark_df
        converter._ctx_spark = MagicMock(return_value=fake_spark)

        # Execute test
        result = converter.convert_to_spark(mock_polars_df)

        # Verify results
        assert result is mock_spark_df
        # Verify that createDataFrame was called with the DataFrame from to_pandas()
        fake_spark.createDataFrame.assert_called_once_with(pandas_df_instance)

    def test_convert_to_spark_already_spark_df(self, converter):
        """Test that Spark DataFrame is returned as-is."""
        mock_spark_df = MagicMock()
        converter._is_spark_dataframe = MagicMock(return_value=True)
        converter._spark_available = MagicMock(return_value=True)
        # Simulate that _ctx_spark() returns something (expected to be called)
        fake_spark = MagicMock()
        converter._ctx_spark = MagicMock(return_value=fake_spark)

        result = converter.convert_to_spark(mock_spark_df)

        assert result is mock_spark_df
        # _ctx_spark was called to check Spark availability
        converter._ctx_spark.assert_called_once()

    def test_convert_to_spark_no_spark_session(self, converter):
        """Test error when Spark session is not available."""
        converter._spark_available = MagicMock(return_value=False)

        with pytest.raises(WriteOperationError, match="Spark session unavailable"):
            converter.convert_to_spark(MagicMock())

    def test_convert_to_spark_unsupported_type(self, converter, monkeypatch):
        """Test error when trying to convert unsupported DataFrame type."""
        converter._spark_available = MagicMock(return_value=True)
        converter._is_spark_dataframe = MagicMock(return_value=False)

        # Simulate that pandas and polars are not available
        monkeypatch.setattr("tauro.io.output.pd", None, raising=False)
        monkeypatch.setattr("tauro.io.output.pl", None, raising=False)

        with pytest.raises(WriteOperationError, match="DataFrame conversion failed"):
            converter.convert_to_spark(MagicMock())

    def test_convert_to_spark_empty_dataframe(self, converter, monkeypatch):
        """Test converting empty DataFrame (pandas-like)."""
        converter._spark_available = MagicMock(return_value=True)
        converter._is_spark_dataframe = MagicMock(return_value=False)

        # Create a class to represent pandas.DataFrame and a mock instance
        class MockPandasDataFrame:
            pass

        mock_pd = MagicMock()
        mock_pd.DataFrame = MockPandasDataFrame
        monkeypatch.setattr("tauro.io.output.pd", mock_pd, raising=False)
        monkeypatch.setattr("tauro.io.output.pl", None, raising=False)

        mock_pandas_df = MockPandasDataFrame()
        mock_spark_df = MagicMock()
        fake_spark = MagicMock()
        fake_spark.createDataFrame.return_value = mock_spark_df
        converter._ctx_spark = MagicMock(return_value=fake_spark)

        result = converter.convert_to_spark(mock_pandas_df)
        assert result is mock_spark_df


class TestPathResolver:
    """Tests for PathResolver functionality."""

    @pytest.fixture
    def mock_validator(self):
        """Fixture to create a mock ConfigValidator."""
        validator = MagicMock()
        validator.validate_output_key.return_value = {
            "schema": "test_schema",
            "sub_folder": "test_folder",
            "table_name": "test_table",
        }
        return validator

    @pytest.fixture
    def resolver(self, mock_validator):
        """Fixture to create a PathResolver with mock context and validator."""
        return PathResolver({"output_path": "/base/path"}, mock_validator)

    def test_resolve_output_path_local(self, resolver, mock_validator):
        """Test resolving output path for local file system."""
        dataset_config = {}
        result = resolver.resolve_output_path(dataset_config, "test_schema.test_folder.test_table")

        assert str(result) == "/base/path/test_schema/test_folder/test_table"
        mock_validator.validate_output_key.assert_called_once_with(
            "test_schema.test_folder.test_table"
        )

    def test_resolve_output_path_with_environment_local(self, mock_validator):
        """Test resolving output path with environment for local execution mode."""
        context = {"output_path": "/base/path", "execution_mode": "local"}
        resolver = PathResolver(context, mock_validator)

        result = resolver.resolve_output_path({}, "test_schema.test_folder.test_table", env="dev")

        assert str(result) == "/base/path/dev/test_schema/test_folder/test_table"

    def test_resolve_output_path_with_environment_databricks(self, mock_validator):
        """Test resolving output path with environment for databricks execution mode."""
        context = {"output_path": "s3://bucket/path", "execution_mode": "databricks"}
        resolver = PathResolver(context, mock_validator)

        result = resolver.resolve_output_path({}, "test_schema.test_folder.test_table", env="prod")

        assert result == "s3://bucket/path/prod/test_schema/test_folder/test_table"

    def test_resolve_output_path_without_environment(self, resolver, mock_validator):
        """Test resolving output path without environment (backward compatibility)."""
        dataset_config = {}
        result = resolver.resolve_output_path(dataset_config, "test_schema.test_folder.test_table")

        assert str(result) == "/base/path/test_schema/test_folder/test_table"

    @pytest.mark.parametrize(
        "prefix",
        CLOUD_URI_PREFIXES,
    )
    def test_resolve_output_path_cloud(self, mock_validator, prefix):
        """Test resolving output path for different cloud/DFS prefixes."""
        resolver = PathResolver({"output_path": f"{prefix}bucket/path"}, mock_validator)

        result = resolver.resolve_output_path({}, "test_schema.test_folder.test_table")

        assert result == f"{prefix}bucket/path/test_schema/test_folder/test_table"

    def test_resolve_output_path_with_config_override(self, resolver, mock_validator):
        """Test resolving output path with configuration overrides."""
        dataset_config = {
            "schema": "override_schema",
            "sub_folder": "override_folder",
            "table_name": "override_table",
        }

        result = resolver.resolve_output_path(dataset_config, "test_schema.test_folder.test_table")

        assert str(result) == "/base/path/override_schema/override_folder/override_table"

    def test_resolve_output_path_missing_components(self, mock_validator):
        """Test error when path components are missing."""
        mock_validator.validate_output_key.return_value = {
            "schema": "",  # Empty schema should cause error
            "sub_folder": "test_folder",
            "table_name": "test_table",
        }

        resolver = PathResolver({"output_path": "/base/path"}, mock_validator)

        with pytest.raises(ConfigurationError, match="Path components cannot be empty"):
            resolver.resolve_output_path({}, "invalid.key")

    def test_resolve_output_path_missing_output_path(self):
        """Test error when output_path is not configured."""
        mock_validator = MagicMock()
        mock_validator.validate_output_key.return_value = {
            "schema": "test_schema",
            "sub_folder": "test_folder",
            "table_name": "test_table",
        }

        resolver = PathResolver({}, mock_validator)  # No output_path in context

        with pytest.raises(
            ConfigurationError, match="Context does not have output_path configured"
        ):
            resolver.resolve_output_path({}, "test_schema.test_folder.test_table")

    def test_resolve_output_path_invalid_inputs(self, resolver):
        """Test error when inputs are invalid."""
        with pytest.raises(ConfigurationError, match="dataset_config must be a dictionary"):
            resolver.resolve_output_path("not_a_dict", "test.key")

        with pytest.raises(ConfigurationError, match="out_key must be a non-empty string"):
            resolver.resolve_output_path({}, "")

    def test_resolve_output_path_special_characters(self, mock_validator):
        """Test resolving output path with special characters."""
        mock_validator.validate_output_key.return_value = {
            "schema": "test-schema",
            "sub_folder": "test folder with spaces",
            "table_name": "test_table_123",
        }

        resolver = PathResolver({"output_path": "/base/path"}, mock_validator)
        result = resolver.resolve_output_path(
            {}, "test-schema.test folder with spaces.test_table_123"
        )

        assert str(result) == "/base/path/test-schema/test folder with spaces/test_table_123"


class TestDataWriter:
    """Tests for DataWriter functionality."""

    @pytest.fixture
    def data_writer(self):
        """Fixture to create a DataWriter with mock context."""
        mock_context = {"spark": MagicMock()}
        return DataWriter(mock_context)

    def test_write_data_valid(self, data_writer):
        """Test writing data with valid configuration."""
        mock_df = MagicMock()
        mock_writer = MagicMock()
        data_writer.writer_factory = MagicMock()
        data_writer.writer_factory.get_writer.return_value = mock_writer
        data_writer._prepare_local_directory = MagicMock()

        data_writer.write_data(mock_df, "/test/path", {"format": "parquet"})

        mock_writer.write.assert_called_once_with(mock_df, "/test/path", {"format": "parquet"})
        data_writer._prepare_local_directory.assert_called_once_with("/test/path")

    def test_write_data_skips_local_dir_prep_for_cloud(self, data_writer):
        """Ensure local dir creation is skipped for cloud/DFS URIs."""
        mock_df = MagicMock()
        mock_writer = MagicMock()
        data_writer.writer_factory = MagicMock()
        data_writer.writer_factory.get_writer.return_value = mock_writer
        data_writer._prepare_local_directory = MagicMock(
            side_effect=AssertionError("Should not be called")
        )

        cloud_path = "s3://bucket/some/where"
        data_writer.write_data(mock_df, cloud_path, {"format": "parquet"})

        mock_writer.write.assert_called_once_with(mock_df, cloud_path, {"format": "parquet"})
        data_writer._prepare_local_directory.assert_not_called()

    def test_write_data_empty_path(self, data_writer):
        """Test error when path is empty."""
        with pytest.raises(ConfigurationError, match="Output path cannot be empty"):
            data_writer.write_data(MagicMock(), "", {"format": "parquet"})

    def test_write_data_invalid_format(self, data_writer):
        """Test error when format is not supported."""
        with pytest.raises(ConfigurationError, match="Unsupported format"):
            data_writer.write_data(MagicMock(), "/test/path", {"format": "invalid_format"})

    def test_write_data_write_error(self, data_writer):
        """Test error handling when write operation fails."""
        mock_writer = MagicMock()
        mock_writer.write.side_effect = Exception("Write failed")
        data_writer.writer_factory = MagicMock()
        data_writer.writer_factory.get_writer.return_value = mock_writer
        data_writer._prepare_local_directory = MagicMock()

        with pytest.raises(WriteOperationError, match="Failed to write data"):
            data_writer.write_data(MagicMock(), "/test/path", {"format": "parquet"})

    def test_write_data_special_characters_path(self, data_writer):
        """Test writing data to path with special characters."""
        mock_df = MagicMock()
        mock_writer = MagicMock()
        data_writer.writer_factory = MagicMock()
        data_writer.writer_factory.get_writer.return_value = mock_writer
        data_writer._prepare_local_directory = MagicMock()

        path = "/test/path/with spaces/and-hyphens"
        data_writer.write_data(mock_df, path, {"format": "parquet"})

        mock_writer.write.assert_called_once_with(mock_df, path, {"format": "parquet"})
        data_writer._prepare_local_directory.assert_called_once_with(path)


class TestModelArtifactManager:
    """Tests for ModelArtifactManager functionality."""

    @pytest.fixture
    def artifact_manager(self):
        """Fixture to create a ModelArtifactManager with mock context."""
        context = {"global_settings": {"model_registry_path": "/model/path"}}
        return ModelArtifactManager(context)

    def test_save_model_artifacts(self, artifact_manager, tmp_path):
        """Test saving model artifacts with metadata."""
        artifact_manager.context = {"global_settings": {"model_registry_path": str(tmp_path)}}

        node = {
            "name": "test_node",
            "model_artifacts": [
                {"name": "artifact1", "type": "model"},
                {"name": "artifact2", "type": "config"},
            ],
        }

        artifact_manager.save_model_artifacts(node, "v1.0")

        # Check that directories were created
        assert (tmp_path / "artifact1" / "v1.0").exists()
        assert (tmp_path / "artifact2" / "v1.0").exists()

        # Check that metadata files were created and have expected fields
        metadata_path1 = tmp_path / "artifact1" / "v1.0" / "metadata.json"
        assert metadata_path1.exists()

        with open(metadata_path1, "r", encoding="utf-8") as f:
            metadata = json.load(f)
            assert metadata["artifact"] == "artifact1"
            assert metadata["version"] == "v1.0"
            assert metadata["node"] == "test_node"
            # Validate ISO timestamp (Z-terminated)
            assert isinstance(metadata.get("saved_at"), str)
            assert metadata["saved_at"].endswith("Z")
            # Try to parse (strip trailing Z for fromisoformat compatibility)
            dt_str = metadata["saved_at"].rstrip("Z")
            datetime.fromisoformat(dt_str)

    def test_save_model_artifacts_no_registry_path(self, artifact_manager):
        """Test warning when model registry path is not configured."""
        artifact_manager.context = {"global_settings": {}}  # No model_registry_path

        node = {"name": "test_node", "model_artifacts": [{"name": "artifact1"}]}

        # Should not raise an error, just log a warning
        artifact_manager.save_model_artifacts(node, "v1.0")

    def test_save_model_artifacts_invalid_artifact(self, artifact_manager, tmp_path):
        """Test handling of invalid artifacts."""
        artifact_manager.context = {"global_settings": {"model_registry_path": str(tmp_path)}}

        node = {
            "name": "test_node",
            "model_artifacts": [
                {"name": "valid_artifact"},
                {"invalid": "artifact"},  # Missing name
                None,  # Completely invalid
            ],
        }

        # Should not raise an error, just skip invalid artifacts
        artifact_manager.save_model_artifacts(node, "v1.0")

        # Only the valid artifact should be created
        assert (tmp_path / "valid_artifact" / "v1.0").exists()
        assert not (tmp_path / "invalid" / "v1.0").exists()

    def test_save_model_artifacts_invalid_inputs(self, artifact_manager):
        """Test error when inputs are invalid."""
        with pytest.raises(ConfigurationError, match="Node must be a valid dictionary"):
            artifact_manager.save_model_artifacts(None, "v1.0")

        with pytest.raises(ConfigurationError, match="Model version cannot be empty"):
            artifact_manager.save_model_artifacts({"name": "test_node"}, "")

    def test_save_model_artifacts_performance(self, artifact_manager, tmp_path):
        """Test that saving model artifacts completes within expected time."""
        artifact_manager.context = {"global_settings": {"model_registry_path": str(tmp_path)}}

        node = {
            "name": "test_node",
            "model_artifacts": [{"name": f"artifact_{i}"} for i in range(10)],
        }

        start_time = time.time()
        artifact_manager.save_model_artifacts(node, "v1.0")
        end_time = time.time()

        # Should complete in a reasonable time in typical CI (relaxed threshold)
        assert end_time - start_time < 5.0

        # Verify all artifacts were created
        for i in range(10):
            assert (tmp_path / f"artifact_{i}" / "v1.0").exists()


class TestUnityCatalogOperations:
    """Tests for UnityCatalogOperations functionality."""

    @pytest.fixture
    def uc_operations(self):
        """Fixture to create a UnityCatalogOperations with mock context and validator."""
        mock_spark = MagicMock()
        context = {"spark": mock_spark}
        mock_validator = MagicMock()
        return UnityCatalogOperations(context, mock_validator)

    def test_ensure_schema_exists(self, uc_operations):
        """Test ensuring schema exists with default location."""
        uc_operations._spark_available = MagicMock(return_value=True)
        fake_spark = MagicMock()
        uc_operations._ctx_spark = MagicMock(return_value=fake_spark)

        uc_operations.ensure_schema_exists("test_catalog", "test_schema")

        # Verify SQL commands were executed
        assert fake_spark.sql.call_count == 2
        calls = fake_spark.sql.call_args_list
        assert "CREATE CATALOG IF NOT EXISTS `test_catalog`" in str(calls[0])
        assert "CREATE SCHEMA IF NOT EXISTS `test_catalog`.`test_schema`" in str(calls[1])

    def test_ensure_schema_exists_with_location(self, uc_operations):
        """Test ensuring schema exists with custom location."""
        uc_operations._spark_available = MagicMock(return_value=True)
        fake_spark = MagicMock()
        uc_operations._ctx_spark = MagicMock(return_value=fake_spark)

        uc_operations.ensure_schema_exists("test_catalog", "test_schema", "/custom/location")

        # Verify SQL command with location was executed
        calls = fake_spark.sql.call_args_list
        assert "LOCATION '/custom/location/test_schema'" in str(calls[1])

    def test_ensure_schema_exists_no_spark(self, uc_operations):
        """Test behavior when Spark is not available."""
        uc_operations._spark_available = MagicMock(return_value=False)
        uc_operations._ctx_spark = MagicMock()  # Mock as object

        # Should not raise an error, just log a warning
        uc_operations.ensure_schema_exists("test_catalog", "test_schema")
        uc_operations._ctx_spark.assert_not_called()

    def test_optimize_table(self, uc_operations):
        """Test optimizing a table with quoted partition column."""
        uc_operations._spark_available = MagicMock(return_value=True)
        fake_spark = MagicMock()
        uc_operations._ctx_spark = MagicMock(return_value=fake_spark)

        uc_operations.optimize_table("catalog.schema.table", "date_col", "2023-01-01", "2023-01-31")

        # Verify OPTIMIZE command was executed with quoted identifiers
        fake_spark.sql.assert_called_once()
        call_args = fake_spark.sql.call_args[0][0]
        assert "OPTIMIZE `catalog`.`schema`.`table`" in call_args
        assert "`date_col` BETWEEN '2023-01-01' AND '2023-01-31'" in call_args

    def test_add_table_comment(self, uc_operations):
        """Test adding a comment to a table."""
        uc_operations._spark_available = MagicMock(return_value=True)
        fake_spark = MagicMock()
        uc_operations._ctx_spark = MagicMock(return_value=fake_spark)

        uc_operations.add_table_comment("catalog.schema.table", "Test description", "date_col")

        # Verify COMMENT command was executed
        fake_spark.sql.assert_called_once()
        call_args = fake_spark.sql.call_args[0][0]
        assert "COMMENT ON TABLE `catalog`.`schema`.`table` IS" in call_args
        assert "Test description" in call_args
        assert "Partition: date_col" in call_args

    @pytest.mark.parametrize(
        "requested,expected",
        [
            (48, MIN_VACUUM_RETENTION_HOURS),
            (200, 200),
            (None, DEFAULT_VACUUM_RETENTION_HOURS),
        ],
    )
    def test_execute_vacuum_retention(self, uc_operations, requested, expected):
        """Test executing VACUUM with different retention scenarios."""
        uc_operations._spark_available = MagicMock(return_value=True)
        fake_spark = MagicMock()
        uc_operations._ctx_spark = MagicMock(return_value=fake_spark)

        uc_operations.execute_vacuum("catalog.schema.table", requested)

        fake_spark.sql.assert_called_once()
        call_args = fake_spark.sql.call_args[0][0]
        assert f"VACUUM `catalog`.`schema`.`table` RETAIN {expected} HOURS" in call_args

    def test_optimize_table_performance(self, uc_operations):
        """Test that table optimization completes within expected time (mocked)."""
        uc_operations._spark_available = MagicMock(return_value=True)
        fake_spark = MagicMock()
        uc_operations._ctx_spark = MagicMock(return_value=fake_spark)

        start_time = time.time()
        uc_operations.optimize_table("catalog.schema.table", "date_col", "2023-01-01", "2023-01-31")
        end_time = time.time()

        # Should complete in less than 1 second (mocked operation)
        assert end_time - start_time < 1.0


class TestUnityCatalogManager:
    """Tests for UnityCatalogManager functionality."""

    @pytest.fixture
    def uc_manager(self):
        """Fixture to create a UnityCatalogManager with mock context."""
        mock_context = {"spark": MagicMock(), "output_path": "/base/path"}
        return UnityCatalogManager(mock_context)

    def test_write_to_unity_catalog(self, uc_manager):
        """Test writing to Unity Catalog with valid configuration."""
        mock_df = MagicMock()
        mock_df.isEmpty.return_value = False  # Ensure DataFrame is not empty

        mock_operations = MagicMock()
        mock_operations.is_enabled.return_value = True
        uc_manager.uc_operations = mock_operations

        mock_writer = MagicMock()
        uc_manager.writer_factory = MagicMock()
        uc_manager.writer_factory.get_writer.return_value = mock_writer

        config = {
            "catalog_name": "test_catalog",
            "schema": "test_schema",
            "table_name": "test_table",
            "partition_col": "date_col",
            "description": "Test table",
            "optimize": True,
            "vacuum": False,
        }

        uc_manager.write_to_unity_catalog(
            mock_df, config, "2023-01-01", "2023-01-31", "schema.folder.table", "dev"
        )

        # Verify operations were called
        mock_operations.ensure_schema_exists.assert_called_once()
        mock_writer.write.assert_called_once()
        mock_operations.add_table_comment.assert_called_once()
        mock_operations.optimize_table.assert_called_once()
        mock_operations.execute_vacuum.assert_not_called()  # vacuum is False

    def test_write_to_unity_catalog_empty_dataframe(self, uc_manager):
        """Test handling of empty DataFrame."""
        mock_df = MagicMock()
        mock_df.isEmpty.return_value = True

        mock_operations = MagicMock()
        mock_operations.is_enabled.return_value = True
        uc_manager.uc_operations = mock_operations

        # Should not raise an error, just log a warning and return early
        uc_manager.write_to_unity_catalog(
            mock_df,
            {
                "catalog_name": "test_catalog",
                "schema": "test_schema",
                "table_name": "test_table",
            },
            None,
            None,
            "schema.folder.table",
            "dev",
        )

        # No operations should be performed for empty DataFrame
        mock_operations.ensure_schema_exists.assert_not_called()

    def test_write_to_unity_catalog_missing_storage_location(self, uc_manager):
        """Test error when storage location is not configured."""
        uc_manager.context = {"spark": MagicMock()}  # No output_path

        mock_df = MagicMock()
        mock_df.isEmpty.return_value = False  # Ensure DataFrame is not empty

        mock_operations = MagicMock()
        mock_operations.is_enabled.return_value = True
        uc_manager.uc_operations = mock_operations

        config = {
            "catalog_name": "test_catalog",
            "schema": "test_schema",
            "table_name": "test_table",
        }

        with pytest.raises(WriteOperationError, match="Unity Catalog write failed"):
            uc_manager.write_to_unity_catalog(
                mock_df, config, None, None, "schema.folder.table", "dev"
            )

    def test_write_to_unity_catalog_not_enabled(self, uc_manager):
        """Test error when Unity Catalog is not enabled."""
        mock_operations = MagicMock()
        mock_operations.is_enabled.return_value = False
        uc_manager.uc_operations = mock_operations

        mock_df = MagicMock()
        mock_df.isEmpty.return_value = False

        # Mock data validator to avoid validation issues
        uc_manager.data_validator = MagicMock()

        # Mock writer factory to prevent actual writing
        uc_manager.writer_factory = MagicMock()

        config = {
            "catalog_name": "test_catalog",
            "schema": "test_schema",
            "table_name": "test_table",
            "format": SupportedFormats.UNITY_CATALOG.value,
        }

        # Should fail because UC is not enabled
        with pytest.raises(ConfigurationError, match="Unity Catalog is not enabled"):
            uc_manager.write_to_unity_catalog(
                mock_df, config, None, None, "schema.folder.table", "dev"
            )

        # Verify that no write was attempted
        uc_manager.writer_factory.get_writer.assert_not_called()

    def test_write_to_unity_catalog_complex_config(self, uc_manager):
        """Test writing to Unity Catalog with complex configuration."""
        mock_df = MagicMock()
        mock_df.isEmpty.return_value = False

        mock_operations = MagicMock()
        mock_operations.is_enabled.return_value = True
        uc_manager.uc_operations = mock_operations

        mock_writer = MagicMock()
        uc_manager.writer_factory = MagicMock()
        uc_manager.writer_factory.get_writer.return_value = mock_writer

        config = {
            "catalog_name": "test_catalog",
            "schema": "test_schema",
            "table_name": "test_table",
            "partition_col": "date_col",
            "description": "Test table with complex configuration",
            "optimize": True,
            "vacuum": True,
            "vacuum_retention_hours": 240,
            "options": {"mergeSchema": "true", "overwriteSchema": "true"},
        }

        uc_manager.write_to_unity_catalog(
            mock_df, config, "2023-01-01", "2023-01-31", "schema.folder.table", "dev"
        )

        # Verify operations were called with correct parameters
        mock_operations.ensure_schema_exists.assert_called_once()
        mock_writer.write.assert_called_once()
        mock_operations.add_table_comment.assert_called_once_with(
            "test_catalog.test_schema.test_table",
            "Test table with complex configuration",
            "date_col",
        )
        mock_operations.optimize_table.assert_called_once_with(
            "test_catalog.test_schema.test_table",
            "date_col",
            "2023-01-01",
            "2023-01-31",
        )
        mock_operations.execute_vacuum.assert_called_once_with(
            "test_catalog.test_schema.test_table", 240
        )


class TestDataOutputManager:
    """Tests for DataOutputManager functionality."""

    @pytest.fixture
    def output_manager(self):
        """Fixture to create an DataOutputManager with mock context."""
        mock_context = {
            "spark": MagicMock(),
            "output_config": {"test_output": {"format": "parquet", "filepath": "/test/path"}},
            "global_settings": {"fail_on_error": True},
        }
        return DataOutputManager(mock_context)

    def test_save_output(self, output_manager):
        """Test saving output with valid configuration."""
        mock_df = MagicMock()
        mock_converter = MagicMock()
        mock_converter.convert_to_spark.return_value = mock_df
        output_manager.dataframe_converter = mock_converter

        mock_path_resolver = MagicMock()
        mock_path_resolver.resolve_output_path.return_value = "/test/path"
        output_manager.path_resolver = mock_path_resolver

        mock_data_writer = MagicMock()
        output_manager.data_writer = mock_data_writer

        node = {"name": "test_node", "output": ["test_output"]}

        output_manager.save_output("dev", node, mock_df)

        # Verify operations were called
        mock_converter.convert_to_spark.assert_called_once_with(mock_df)
        mock_path_resolver.resolve_output_path.assert_called_once()
        mock_data_writer.write_data.assert_called_once()

    def test_save_output_unity_catalog(self, output_manager):
        """Test saving output to Unity Catalog."""
        mock_df = MagicMock()
        mock_converter = MagicMock()
        mock_converter.convert_to_spark.return_value = mock_df
        output_manager.dataframe_converter = mock_converter

        mock_uc_manager = MagicMock()
        mock_uc_manager.uc_operations.is_enabled.return_value = True
        output_manager.unity_catalog_manager = mock_uc_manager

        # Update context to use Unity Catalog
        output_manager.context["output_config"]["test_output"] = {
            "format": SupportedFormats.UNITY_CATALOG.value,
            "catalog_name": "test_catalog",
            "schema": "test_schema",
            "table_name": "test_table",
        }

        node = {"name": "test_node", "output": ["test_output"]}

        output_manager.save_output("dev", node, mock_df)

        # Verify Unity Catalog manager was called
        mock_uc_manager.write_to_unity_catalog.assert_called_once()

    def test_save_output_with_model_artifacts(self, output_manager):
        """Test saving output with model artifacts."""
        mock_df = MagicMock()
        mock_converter = MagicMock()
        mock_converter.convert_to_spark.return_value = mock_df
        output_manager.dataframe_converter = mock_converter

        mock_path_resolver = MagicMock()
        mock_path_resolver.resolve_output_path.return_value = "/test/path"
        output_manager.path_resolver = mock_path_resolver
        mock_data_writer = MagicMock()
        output_manager.data_writer = mock_data_writer

        mock_artifact_manager = MagicMock()
        output_manager.model_artifact_manager = mock_artifact_manager

        node = {
            "name": "test_node",
            "output": ["test_output"],
            "model_artifacts": [{"name": "test_artifact"}],
        }

        output_manager.save_output("dev", node, mock_df, model_version="v1.0")

        # Verify model artifacts were saved
        mock_artifact_manager.save_model_artifacts.assert_called_once_with(node, "v1.0")

    def test_save_output_no_outputs(self, output_manager):
        """Test handling of node with no outputs."""
        # Mock the converter to avoid conversion errors
        mock_converter = MagicMock()
        output_manager.dataframe_converter = mock_converter

        # Mock other components to verify they're not called
        mock_path_resolver = MagicMock()
        output_manager.path_resolver = mock_path_resolver

        mock_data_writer = MagicMock()
        output_manager.data_writer = mock_data_writer

        mock_uc_manager = MagicMock()
        output_manager.unity_catalog_manager = mock_uc_manager

        node = {"name": "test_node", "output": []}  # No outputs

        # Should not raise an error, just return early
        output_manager.save_output("dev", node, MagicMock())

        # Verify no write operations were performed
        mock_path_resolver.resolve_output_path.assert_not_called()
        mock_data_writer.write_data.assert_not_called()
        mock_uc_manager.write_to_unity_catalog.assert_not_called()

    def test_save_output_invalid_node(self, output_manager):
        """Test error when node is invalid."""
        # Mock the converter to avoid conversion errors
        mock_converter = MagicMock()
        output_manager.dataframe_converter = mock_converter

        with pytest.raises(ConfigurationError, match="Parameter 'node' cannot be None"):
            output_manager.save_output("dev", None, MagicMock())

        with pytest.raises(ConfigurationError, match="Parameter 'node' must be a dictionary"):
            output_manager.save_output("dev", "not_a_dict", MagicMock())

    def test_save_output_integration(self, output_manager):
        """Test full integration of DataOutputManager with all its dependencies."""
        mock_df = MagicMock()

        # Mock all dependencies to verify they work together
        mock_converter = MagicMock()
        mock_converter.convert_to_spark.return_value = mock_df
        output_manager.dataframe_converter = mock_converter

        mock_path_resolver = MagicMock()
        mock_path_resolver.resolve_output_path.return_value = "/test/path"
        output_manager.path_resolver = mock_path_resolver

        mock_data_writer = MagicMock()
        output_manager.data_writer = mock_data_writer

        mock_artifact_manager = MagicMock()
        output_manager.model_artifact_manager = mock_artifact_manager

        node = {
            "name": "test_node",
            "output": ["test_output"],
            "model_artifacts": [{"name": "test_artifact"}],
        }

        output_manager.save_output("dev", node, mock_df, model_version="v1.0")

        # Verify all components were called in the correct sequence
        mock_converter.convert_to_spark.assert_called_once_with(mock_df)
        mock_path_resolver.resolve_output_path.assert_called_once()
        mock_data_writer.write_data.assert_called_once()
        mock_artifact_manager.save_model_artifacts.assert_called_once_with(node, "v1.0")

    def test_save_output_missing_config(self, output_manager):
        """Test error when output configuration is missing."""
        mock_df = MagicMock()
        mock_converter = MagicMock()
        mock_converter.convert_to_spark.return_value = mock_df
        output_manager.dataframe_converter = mock_converter

        node = {
            "name": "test_node",
            "output": ["missing_output"],  # This output is not in the config
        }

        with pytest.raises(
            ConfigurationError, match="Output configuration 'missing_output' not found"
        ):
            output_manager.save_output("dev", node, mock_df)

    def test_save_output_performance(self, output_manager):
        """Test that saving output completes within expected time."""
        mock_df = MagicMock()
        mock_converter = MagicMock()
        mock_converter.convert_to_spark.return_value = mock_df
        output_manager.dataframe_converter = mock_converter

        mock_path_resolver = MagicMock()
        mock_path_resolver.resolve_output_path.return_value = "/test/path"
        output_manager.path_resolver = mock_path_resolver

        mock_data_writer = MagicMock()
        output_manager.data_writer = mock_data_writer

        node = {"name": "test_node", "output": ["test_output"]}

        start_time = time.time()
        output_manager.save_output("dev", node, mock_df)
        end_time = time.time()

        # Should complete in less than 2 seconds under normal CI conditions
        assert end_time - start_time < 2.0


class TestErrorHandler:
    """Tests for ErrorHandler functionality."""

    @pytest.fixture
    def error_handler(self):
        """Fixture to create an ErrorHandler."""
        return ErrorHandler(fail_on_error=True)

    def test_execute_with_error_handling_success(self, error_handler):
        """Test successful operation execution."""
        mock_operation = MagicMock()

        error_handler.execute_with_error_handling(mock_operation, "Test error")

        # Verify operation was called
        mock_operation.assert_called_once()

    def test_execute_with_error_handling_failure(self, error_handler):
        """Test error handling when operation fails."""
        mock_operation = MagicMock()
        mock_operation.side_effect = Exception("Operation failed")

        with pytest.raises(Exception, match="Operation failed"):
            error_handler.execute_with_error_handling(mock_operation, "Test error")

    def test_execute_with_error_handling_no_fail(self):
        """Test error handling when fail_on_error is False."""
        error_handler = ErrorHandler(fail_on_error=False)
        mock_operation = MagicMock()
        mock_operation.side_effect = Exception("Operation failed")

        # Should not raise an error, just log it
        error_handler.execute_with_error_handling(mock_operation, "Test error")

        # Operation should still be called
        mock_operation.assert_called_once()

    def test_execute_with_error_handling_performance(self, error_handler):
        """Test that error handling completes within expected time."""
        mock_operation = MagicMock()

        start_time = time.time()
        error_handler.execute_with_error_handling(mock_operation, "Test error")
        end_time = time.time()

        # Should complete in less than 0.5 second (relaxed)
        assert end_time - start_time < 0.5


# Integration tests for the output module
class TestOutputIntegration:
    """Integration tests for the output module."""

    @pytest.fixture
    def mock_context(self):
        """Fixture to create a mock context for integration tests."""
        return {
            "spark": MagicMock(),
            "output_config": {
                "test_output": {"format": "parquet", "filepath": "/test/path"},
                "uc_output": {
                    "format": "unity_catalog",
                    "catalog_name": "test_catalog",
                    "schema": "test_schema",
                    "table_name": "test_table",
                },
            },
            "global_settings": {
                "fail_on_error": True,
                "model_registry_path": "/model/path",
            },
            "output_path": "/base/path",
        }

    def test_integration_all_components(self, mock_context, tmp_path):
        """Test integration of all output components working together."""
        # Create real DataOutputManager with mocked dependencies
        output_manager = DataOutputManager(mock_context)

        # Mock dependencies but keep the real integration
        mock_df = MagicMock()
        mock_converter = MagicMock()
        mock_converter.convert_to_spark.return_value = mock_df
        output_manager.dataframe_converter = mock_converter

        mock_path_resolver = MagicMock()
        mock_path_resolver.resolve_output_path.return_value = str(tmp_path / "output")
        output_manager.path_resolver = mock_path_resolver

        mock_data_writer = MagicMock()
        output_manager.data_writer = mock_data_writer

        mock_uc_manager = MagicMock()
        mock_uc_manager.uc_operations.is_enabled.return_value = True
        output_manager.unity_catalog_manager = mock_uc_manager

        mock_artifact_manager = MagicMock()
        output_manager.model_artifact_manager = mock_artifact_manager

        # Test node with multiple outputs and artifacts
        node = {
            "name": "test_node",
            "output": ["test_output", "uc_output"],
            "model_artifacts": [{"name": "test_artifact"}],
        }

        # Execute the test
        output_manager.save_output("dev", node, mock_df, model_version="v1.0")

        # Verify all components were called
        mock_converter.convert_to_spark.assert_called_once_with(mock_df)
        assert mock_path_resolver.resolve_output_path.call_count == 1
        assert mock_data_writer.write_data.call_count == 1
        mock_uc_manager.write_to_unity_catalog.assert_called_once()
        mock_artifact_manager.save_model_artifacts.assert_called_once_with(node, "v1.0")
