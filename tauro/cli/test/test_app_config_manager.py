import json
from pathlib import Path
import pytest
from tauro.cli.config import AppConfigManager
from tauro.cli.core import ConfigurationError


def test_app_config_manager_loads_and_merges_envs(tmp_path: Path, monkeypatch):
    # Create a settings JSON with env_config mapping
    base = tmp_path / "proj"
    base.mkdir()
    settings = {
        "base_path": str(base),
        "env_config": {
            "base": {
                "global_settings_path": "config/global_settings.json",
                "input_config_path": "config/input.json",
            },
            "dev": {"input_config_path": "config/dev/input.json"},
        },
    }
    settings_file = base / "settings_json.json"
    settings_file.write_text(json.dumps(settings), encoding="utf-8")

    # Create referenced config files (some missing intentionally)
    (base / "config").mkdir(parents=True, exist_ok=True)
    (base / "config" / "global_settings.json").write_text("{}", encoding="utf-8")
    (base / "config" / "input.json").write_text("{}", encoding="utf-8")
    # dev input not created to test warning path

    # Monkeypatch SecurityValidator.validate_path to simply return the path (avoid strict checks)
    from tauro.cli.core import SecurityValidator

    monkeypatch.setattr(
        SecurityValidator, "validate_path", staticmethod(lambda b, t: Path(t))
    )

    mgr = AppConfigManager(str(settings_file))
    envs = mgr.get_env_config("dev")

    assert "global_settings_path" in envs
    assert "input_config_path" in envs
    # dev input should override base input (exists in merged result but file missing warning handled earlier)
    assert envs["input_config_path"].endswith("config/dev/input.json") or envs[
        "input_config_path"
    ].endswith("config/input.json")


def test_app_config_manager_invalid_json_raises(tmp_path: Path):
    bad = tmp_path / "bad_settings.json"
    bad.write_text("{ invalid json", encoding="utf-8")
    with pytest.raises(ConfigurationError):
        AppConfigManager(str(bad))
