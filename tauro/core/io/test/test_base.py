import pytest
from unittest.mock import MagicMock, patch
from pathlib import Path

from tauro.io.base import BaseIO
from tauro.io.exceptions import ConfigurationError


class TestBaseIO:
    @pytest.fixture
    def base_io(self):
        return BaseIO({"spark": MagicMock(), "execution_mode": "local"})

    @pytest.mark.parametrize(
        "context, queries",
        [
            ({"key": "value", "num": 42}, [("key", "value"), ("num", 42), ("x", None)]),
        ],
    )
    def test_ctx_get_dict_context(self, context, queries):
        base_io = BaseIO(context)
        for key, expected in queries:
            assert base_io._ctx_get(key) == expected
        assert base_io._ctx_get("nonexistent", "default") == "default"

    def test_ctx_get_object_context(self):
        class MockContext:
            def __init__(self):
                self.key = "value"
                self.num = 42

        context = MockContext()
        base_io = BaseIO(context)
        assert base_io._ctx_get("key") == "value"
        assert base_io._ctx_get("num") == 42
        assert base_io._ctx_get("nonexistent") is None

    def test_sanitize_sql_query_valid(self):
        # Check if the method exists before using it
        if hasattr(BaseIO, "sanitize_sql_query"):
            valid_queries = [
                "SELECT * FROM table WHERE condition = 1",
                # Queries with benign comments should be accepted by the improved sanitizer
                "SELECT id FROM users -- harmless comment",
                "WITH cte AS (SELECT * FROM t) SELECT * FROM cte /* note */",
                "SELECT 'text; not a statement' AS col FROM tbl",  # ; within literals acceptable
            ]
            for q in valid_queries:
                result = BaseIO.sanitize_sql_query(q)
                assert result == q
        else:
            pytest.skip("sanitize_sql_query method not available")

    def test_sanitize_sql_query_dangerous(self):
        # Check if the method exists before using it
        if not hasattr(BaseIO, "sanitize_sql_query"):
            pytest.skip("sanitize_sql_query method not available")

        # Casos claramente peligrosos que deben ser rechazados
        dangerous_queries = [
            "DROP TABLE users",
            "DELETE FROM users",
            "INSERT INTO users VALUES (1, 'test')",
            "UPDATE users SET name = 'test'",
            "SELECT * FROM users; DROP TABLE users",
            "EXEC sp_SomeProcedure",
            "SELECT * FROM users -- drop table other",
            "SELECT * FROM users /* ; injected */",
        ]

        for query in dangerous_queries:
            with pytest.raises(ConfigurationError):
                BaseIO.sanitize_sql_query(query)

    def test_prepare_local_directory_creates_parent_for_file(self, base_io, tmp_path):
        test_path = tmp_path / "test_subdir" / "test_file.txt"
        base_io._prepare_local_directory(str(test_path))
        assert test_path.parent.exists()

    def test_prepare_local_directory_creates_directory_path(self, base_io, tmp_path):
        dir_path = tmp_path / "nested" / "dir"
        base_io._prepare_local_directory(str(dir_path))
        assert dir_path.exists()

    def test_prepare_local_directory_skips_remote(self, base_io):
        # Si intenta crear un directorio para una ruta remota, no debe intentar mkdir local
        with patch.object(
            Path, "mkdir", side_effect=AssertionError("Should not mkdir")
        ):
            base_io._prepare_local_directory("s3://bucket/data/file.parquet")
            base_io._prepare_local_directory(
                "abfss://cont@acct.dfs.core.windows.net/path"
            )
            base_io._prepare_local_directory("dbfs:/mnt/data/table")
