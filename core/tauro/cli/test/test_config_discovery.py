import os
from pathlib import Path
import pytest
from tauro.cli.config import ConfigDiscovery


def create_settings_file(dir_path: Path, filename: str):
    dir_path.mkdir(parents=True, exist_ok=True)
    (dir_path / filename).write_text("{}", encoding="utf-8")


def test_discover_finds_config_files(tmp_path: Path):
    # Create nested structure with config files
    base = tmp_path / "project"
    cfg1 = base / "sub1"
    cfg2 = base / "sub2" / "inner"

    create_settings_file(cfg1, "settings_json.json")
    create_settings_file(cfg2, "settings_yml.json")

    discovery = ConfigDiscovery(str(base))
    found = discovery.discover(max_depth=4)

    # Should find at least two configurations
    assert any(str(p).endswith("sub1") for p, f in found)
    assert any(f == "settings_yml.json" or f == "settings_json.json" for p, f in found)


def test_find_best_match_scores_by_layer_and_type(tmp_path: Path):
    base = tmp_path / "project2"
    a = base / "bronze"
    b = base / "silver" / "usecaseA"

    create_settings_file(a, "settings_json.json")
    create_settings_file(b, "settings_json.json")

    discovery = ConfigDiscovery(str(base))
    discovery.discover()

    # Prefer layer name 'bronze'
    best = discovery.find_best_match(
        layer_name="bronze", use_case=None, config_type="json"
    )
    assert best is not None
    config_dir, config_file = best
    assert config_file.endswith(".json")
    assert str(config_dir).endswith("bronze")
