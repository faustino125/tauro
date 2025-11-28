import os
from pathlib import Path
import pytest  # type: ignore
from core.cli.core import SecurityValidator, SecurityError


def test_validate_path_strict_blocks_outside(tmp_path: Path):
    base = tmp_path / "base"
    inside = base / "ok"
    outside = tmp_path / "outside"
    inside.mkdir(parents=True)
    outside.mkdir(parents=True)

    # Should pass for inside path
    validated = SecurityValidator.validate_path(base, inside)
    assert validated.is_dir()

    # Should raise for outside path in strict mode
    with pytest.raises(SecurityError):
        SecurityValidator.validate_path(base, outside)


def test_validate_path_permissive_allows_absolute_outside(tmp_path: Path, monkeypatch):
    base = tmp_path / "base"
    outside = tmp_path / "outside_abs"
    base.mkdir(parents=True)
    outside.mkdir(parents=True)

    # Set permissive mode
    monkeypatch.setenv("engine_PERMISSIVE_PATH_VALIDATION", "1")

    # Absolute outside target should be allowed in permissive mode
    validated = SecurityValidator.validate_path(base, outside)
    assert validated.resolve() == outside.resolve()


def test_validate_path_hidden_parts_rejected(tmp_path: Path):
    base = tmp_path / "b"
    hidden = base / ".hidden" / "file"
    hidden.parent.mkdir(parents=True)
    hidden.write_text("x", encoding="utf-8")

    with pytest.raises(SecurityError):
        SecurityValidator.validate_path(base, hidden)


@pytest.mark.skipif(os.name != "posix", reason="POSIX-only permission checks")
def test_validate_path_world_writable_rejected(tmp_path: Path):
    base = tmp_path / "b2"
    f = base / "writable"
    base.mkdir(parents=True, exist_ok=True)
    f.write_text("data", encoding="utf-8")
    # Make world-writable
    f.chmod(0o666)
    with pytest.raises(SecurityError):
        SecurityValidator.validate_path(base, f)
