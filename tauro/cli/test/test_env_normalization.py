import pytest  # type: ignore

from tauro.cli.core import normalize_environment, is_allowed_environment


def test_normalize_aliases():
    assert normalize_environment("production") == "prod"
    assert normalize_environment("Development") == "dev"
    assert normalize_environment("staging") == "dev"


def test_normalize_sandbox():
    assert normalize_environment("sandbox_juan") == "sandbox_juan"
    assert normalize_environment("SANDBOX_MARIA") == "sandbox_maria"
    # sanitize invalid chars
    assert normalize_environment("sandbox_@bob!") == "sandbox_bob"


def test_is_allowed_env_default():
    assert is_allowed_environment("dev")
    assert is_allowed_environment("prod")
    assert is_allowed_environment("base")
    assert is_allowed_environment("sandbox")
    assert is_allowed_environment("sandbox_ana")


@pytest.mark.parametrize("val", [None, "", "..", "not an env"])
def test_invalid_envs(val):
    assert normalize_environment(val) is None or not is_allowed_environment(val)
