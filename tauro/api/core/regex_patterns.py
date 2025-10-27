"""
Pre-compiled regex patterns for validation.

Pre-compiling regex patterns improves performance by avoiding
recompilation on every validation call.
"""

import re
from functools import lru_cache

# =============================================================================
# Common Patterns
# =============================================================================

# Identifier pattern: alphanumeric, underscore, hyphen
IDENTIFIER_PATTERN = re.compile(r"^[a-zA-Z0-9_-]+$")

# Email pattern (basic)
EMAIL_PATTERN = re.compile(r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$")

# URL pattern (basic)
URL_PATTERN = re.compile(
    r"^https?://[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}(?:/[a-zA-Z0-9._~:/?#[\]@!$&'()*+,;=-]*)?$"
)

# UUID pattern
UUID_PATTERN = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$", re.IGNORECASE
)

# ISO datetime pattern
ISO_DATETIME_PATTERN = re.compile(
    r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:Z|[+-]\d{2}:\d{2})?$"
)

# Cron expression pattern (simplified)
CRON_PATTERN = re.compile(
    r"^(\*|[0-9]|1[0-9]|2[0-9]|3[0-9]|4[0-9]|5[0-9])(/\d+)?(,\d+(-\d+)?)*\s"
    r"(\*|[0-9]|1[0-9]|2[0-3])(/\d+)?(,\d+(-\d+)?)*\s"
    r"(\*|[0-9]|1[0-9]|2[0-9]|3[0-1])(/\d+)?(,\d+(-\d+)?)*\s"
    r"(\*|[0-9]|1[0-2])(/\d+)?(,\d+(-\d+)?)*\s"
    r"(\*|[0-9]|[1-6])$"
)

# Python identifier
PYTHON_IDENTIFIER_PATTERN = re.compile(r"^[a-zA-Z_]\w*$")

# Alphanumeric only
ALPHANUMERIC_PATTERN = re.compile(r"^[a-zA-Z0-9]+$")

# Path traversal attack pattern
PATH_TRAVERSAL_PATTERN = re.compile(r"(\.\./|\.\.\\|\.\.%2[fF]|\.\.%5[cC])")

# SQL injection keywords (basic detection)
SQL_INJECTION_PATTERN = re.compile(
    r"(\bDROP\b|\bDELETE\b|\bINSERT\b|\bUPDATE\b|\bSELECT\b|\bUNION\b|\bOR\b|\bAND\b|'|;|--)",
    re.IGNORECASE,
)

# Whitespace only
WHITESPACE_PATTERN = re.compile(r"^\s+$")

# Non-ASCII characters
NON_ASCII_PATTERN = re.compile(r"[^\x00-\x7F]")

# =============================================================================
# Pattern Validation Functions
# =============================================================================


@lru_cache(maxsize=256)
def is_valid_identifier(value: str) -> bool:
    """Check if value is a valid identifier (alphanumeric + hyphen/underscore)"""
    return bool(IDENTIFIER_PATTERN.match(value))


@lru_cache(maxsize=256)
def is_valid_email(value: str) -> bool:
    """Check if value is a valid email (basic check)"""
    return bool(EMAIL_PATTERN.match(value))


@lru_cache(maxsize=256)
def is_valid_url(value: str) -> bool:
    """Check if value is a valid URL"""
    return bool(URL_PATTERN.match(value))


@lru_cache(maxsize=256)
def is_valid_uuid(value: str) -> bool:
    """Check if value is a valid UUID"""
    return bool(UUID_PATTERN.match(value))


@lru_cache(maxsize=256)
def is_valid_iso_datetime(value: str) -> bool:
    """Check if value is a valid ISO datetime"""
    return bool(ISO_DATETIME_PATTERN.match(value))


@lru_cache(maxsize=256)
def is_valid_cron(value: str) -> bool:
    """Check if value is a valid cron expression"""
    return bool(CRON_PATTERN.match(value))


@lru_cache(maxsize=256)
def is_valid_python_identifier(value: str) -> bool:
    """Check if value is a valid Python identifier"""
    return bool(PYTHON_IDENTIFIER_PATTERN.match(value))


@lru_cache(maxsize=256)
def is_alphanumeric(value: str) -> bool:
    """Check if value contains only alphanumeric characters"""
    return bool(ALPHANUMERIC_PATTERN.match(value))


@lru_cache(maxsize=256)
def contains_path_traversal(value: str) -> bool:
    """Check if value contains path traversal attempts"""
    return bool(PATH_TRAVERSAL_PATTERN.search(value))


@lru_cache(maxsize=256)
def contains_sql_keywords(value: str) -> bool:
    """Check if value contains SQL injection keywords"""
    return bool(SQL_INJECTION_PATTERN.search(value))


@lru_cache(maxsize=256)
def is_whitespace_only(value: str) -> bool:
    """Check if value contains only whitespace"""
    return bool(WHITESPACE_PATTERN.match(value))


@lru_cache(maxsize=256)
def contains_non_ascii(value: str) -> bool:
    """Check if value contains non-ASCII characters"""
    return bool(NON_ASCII_PATTERN.search(value))


# =============================================================================
# Exports
# =============================================================================

__all__ = [
    # Patterns
    "IDENTIFIER_PATTERN",
    "EMAIL_PATTERN",
    "URL_PATTERN",
    "UUID_PATTERN",
    "ISO_DATETIME_PATTERN",
    "CRON_PATTERN",
    "PYTHON_IDENTIFIER_PATTERN",
    "ALPHANUMERIC_PATTERN",
    "PATH_TRAVERSAL_PATTERN",
    "SQL_INJECTION_PATTERN",
    "WHITESPACE_PATTERN",
    "NON_ASCII_PATTERN",
    # Functions
    "is_valid_identifier",
    "is_valid_email",
    "is_valid_url",
    "is_valid_uuid",
    "is_valid_iso_datetime",
    "is_valid_cron",
    "is_valid_python_identifier",
    "is_alphanumeric",
    "contains_path_traversal",
    "contains_sql_keywords",
    "is_whitespace_only",
    "contains_non_ascii",
]
