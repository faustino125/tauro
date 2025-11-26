import pytest  # type: ignore
from pathlib import Path
from engine.cli.config import DSLConfigLoader
from engine.cli.core import ConfigurationError


def test_dsl_loader_parses_sections_and_values(tmp_path: Path):
    content = """
# Top-level comment

[database]
host = "localhost"
port = 5432
enabled = true

[database.credentials]
user = "admin"
password = "s3cret"

[features]
items = [1, 2, "three", true]
empty_list = []
"""
    file_path = tmp_path / "settings_dsl.json"
    file_path.write_text(content, encoding="utf-8")

    loader = DSLConfigLoader()
    parsed = loader.load_config(str(file_path))

    assert "database" in parsed
    assert parsed["database"]["host"] == "localhost"
    assert parsed["database"]["port"] == 5432
    assert parsed["database"]["enabled"] is True

    creds = parsed["database"]["credentials"]
    assert creds["user"] == "admin"
    assert creds["password"] == "s3cret"

    features = parsed["features"]
    assert features["items"] == [1, 2, "three", True]
    assert features["empty_list"] == []


def test_dsl_loader_invalid_syntax_raises(tmp_path: Path):
    content = """
[main]
this line has no equals sign
"""
    file_path = tmp_path / "bad_dsl.json"
    file_path.write_text(content, encoding="utf-8")

    loader = DSLConfigLoader()
    with pytest.raises(ConfigurationError):
        loader.load_config(str(file_path))


def test_dsl_loader_parses_unquoted_strings_and_numbers(tmp_path: Path):
    content = """
[section]
plain = unquoted_string
num = 42
flt = 3.14
bool1 = true
bool2 = false
list = [a, b, 3, 4.0]
"""
    file_path = tmp_path / "settings_dsl2.json"
    file_path.write_text(content, encoding="utf-8")

    loader = DSLConfigLoader()
    parsed = loader.load_config(str(file_path))

    assert parsed["section"]["plain"] == "unquoted_string"
    assert parsed["section"]["num"] == 42
    assert abs(parsed["section"]["flt"] - 3.14) < 1e-9
    assert parsed["section"]["bool1"] is True
    assert parsed["section"]["bool2"] is False
    assert parsed["section"]["list"] == ["a", "b", 3, 4.0]
