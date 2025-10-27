import pytest
from unittest.mock import MagicMock
from tauro.io.validators import ConfigValidator, DataValidator
from tauro.io.exceptions import ConfigurationError, DataValidationError


class TestConfigValidator:
    @pytest.fixture
    def validator(self):
        return ConfigValidator()

    def test_validate_missing_fields(self, validator):
        config = {"existing": "value"}
        with pytest.raises(ConfigurationError) as exc:
            validator.validate(config, ["existing", "missing"], "test config")
        assert "missing" in str(exc.value)

    def test_validate_output_key_valid(self, validator):
        result = validator.validate_output_key("schema.subfolder.table")
        assert result == {
            "schema": "schema",
            "sub_folder": "subfolder",
            "table_name": "table",
        }

    def test_validate_output_key_invalid(self, validator):
        invalid_keys = ["", "schema", "schema.table", "a.b.c.d"]
        for key in invalid_keys:
            with pytest.raises(ConfigurationError):
                validator.validate_output_key(key)

    def test_validate_date_format(self, validator):
        valid_dates = ["2023-01-01", "2000-12-31"]
        invalid_dates = ["2023/01/01", "01-01-2023", "2023-13-01", "not-a-date"]

        for date in valid_dates:
            assert validator.validate_date_format(date) is True

        for date in invalid_dates:
            assert validator.validate_date_format(date) is False


class TestDataValidator:
    @pytest.fixture
    def validator(self):
        return DataValidator()

    def test_validate_dataframe_none(self, validator):
        with pytest.raises(DataValidationError):
            validator.validate_dataframe(None)

    def test_validate_columns_exist(self, validator):
        mock_df = MagicMock()
        mock_df.columns = ["col1", "col2", "col3"]

        # Test existing columns
        validator.validate_columns_exist(mock_df, ["col1", "col2"])

        # Test missing columns
        with pytest.raises(DataValidationError):
            validator.validate_columns_exist(mock_df, ["col1", "col4"])
