import pytest  # type: ignore
from core.cli.core import parse_iso_date, validate_date_range, ValidationError


def test_parse_iso_date_valid():
    assert parse_iso_date("2025-09-20") == "2025-09-20"


def test_parse_iso_date_none():
    assert parse_iso_date(None) is None


def test_parse_iso_date_invalid():
    with pytest.raises(ValidationError):
        parse_iso_date("20-09-2025")


def test_validate_date_range_ok():
    # start <= end should not raise
    validate_date_range("2025-01-01", "2025-12-31")


def test_validate_date_range_bad_order():
    with pytest.raises(ValidationError):
        validate_date_range("2025-12-31", "2025-01-01")
