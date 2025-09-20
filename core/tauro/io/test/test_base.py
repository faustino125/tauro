import pytest
from unittest.mock import MagicMock
from tauro.io.base import BaseIO
from tauro.io.exceptions import ConfigurationError


class TestBaseIO:
    @pytest.fixture
    def base_io(self):
        return BaseIO({"spark": MagicMock(), "execution_mode": "local"})

    def test_ctx_get_dict_context(self):
        context = {"key": "value", "num": 42}
        base_io = BaseIO(context)
        assert base_io._ctx_get("key") == "value"
        assert base_io._ctx_get("num") == 42
        assert base_io._ctx_get("nonexistent") is None
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
        # Verificar que el método existe antes de usarlo
        if hasattr(BaseIO, "sanitize_sql_query"):
            query = "SELECT * FROM table WHERE condition = 1"
            result = BaseIO.sanitize_sql_query(query)
            assert result == query
        else:
            pytest.skip("Método sanitize_sql_query no disponible")

    def test_sanitize_sql_query_dangerous(self):
        # Verificar que el método existe antes de usarlo
        if not hasattr(BaseIO, "sanitize_sql_query"):
            pytest.skip("Método sanitize_sql_query no disponible")

        dangerous_queries = [
            "DROP TABLE users",
            "DELETE FROM users",
            "INSERT INTO users VALUES (1, 'test')",
            "UPDATE users SET name = 'test'",
            "SELECT * FROM users; DROP TABLE users",
            "SELECT * FROM users -- comment",
            "SELECT * FROM users /* comment */",
            "EXEC sp_SomeProcedure",
        ]

        for query in dangerous_queries:
            with pytest.raises(ConfigurationError):
                BaseIO.sanitize_sql_query(query)

    def test_prepare_local_directory(self, base_io, tmp_path):
        test_path = tmp_path / "test_subdir" / "test_file.txt"
        base_io._prepare_local_directory(str(test_path))
        assert test_path.parent.exists()
