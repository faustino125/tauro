from typing import ClassVar, Set, List
import re
from loguru import logger  # type: ignore

from tauro.io.exceptions import ConfigurationError


class SQLSanitizer:
    """Specialized class for secure SQL query sanitization."""

    DANGEROUS_KEYWORDS: ClassVar[Set[str]] = {
        "drop",
        "create",
        "alter",
        "truncate",
        "insert",
        "update",
        "delete",
        "merge",
        "exec",
        "execute",
        "xp_",
        "sp_",
        "call",
        "load_file",
        "into outfile",
        "into dumpfile",
        "information_schema",
        "sys.",
        "pg_",
    }

    COMMENT_PATTERNS: ClassVar[List[str]] = [
        r"--[^\r\n]*",  # Comentarios de línea --
        r"/\*[\s\S]*?\*/",  # Comentarios de bloque /* */
        r"#[^\r\n]*",  # Comentarios de línea # (MySQL)
    ]

    SUSPICIOUS_PATTERNS: ClassVar[List[str]] = [
        r";\s*\w",  # Múltiples sentencias
        r"0x[0-9a-f]+",  # Valores hexadecimales
        r"char\s*\(",  # Conversiones char() sospechosas
        r"ascii\s*\(",  # Funciones ASCII sospechosas
        r"waitfor\s+delay",  # Ataques de tiempo
        r"benchmark\s*\(",  # Ataques de benchmark
    ]

    @classmethod
    def sanitize_query(cls, query: str) -> str:
        """Robust SQL query sanitization."""
        if not query or not isinstance(query, str):
            raise ConfigurationError("Query must be a non-empty string") from None

        original_query = query
        query = query.strip()

        if not query:
            raise ConfigurationError(
                "Query cannot be empty after stripping whitespace"
            ) from None

        normalized_query = re.sub(r"\s+", " ", query)

        if not cls._is_select_query(normalized_query):
            raise ConfigurationError(
                "Only SELECT queries are allowed. Query must start with SELECT."
            ) from None

        masked_for_checks = cls._mask_string_literals(normalized_query)

        cls._check_dangerous_keywords(masked_for_checks)
        cls._check_suspicious_patterns(masked_for_checks)
        cls._check_malicious_comments(normalized_query)
        cls._check_multiple_statements(normalized_query)

        logger.debug("SQL query passed security validation")
        return original_query

    @classmethod
    def _is_select_query(cls, query: str) -> bool:
        """Verify that the query is a valid SELECT."""
        clean_query = cls._remove_comments(query)
        clean_query = clean_query.strip().lower()
        return clean_query.startswith("select ") or clean_query.startswith("with ")

    @classmethod
    def _remove_comments(cls, query: str) -> str:
        """Remove SQL comments from the query."""
        for pattern in cls.COMMENT_PATTERNS:
            query = re.sub(pattern, "", query, flags=re.IGNORECASE | re.MULTILINE)
        return query

    @classmethod
    def _mask_string_literals(cls, query: str) -> str:
        """Replaces the content of string literals with spaces, preserving the quotes.
        """
        result: List[str] = []
        in_string = False
        quote_char = None
        i = 0
        while i < len(query):
            ch = query[i]
            if not in_string and ch in ("'", '"'):
                in_string = True
                quote_char = ch
                result.append(ch)
            elif in_string:
                if ch == "\\" and i + 1 < len(query):
                    result.append(" ")
                    i += 1
                    result.append(" ")
                elif ch == quote_char:
                    in_string = False
                    quote_char = None
                    result.append(ch)
                else:
                    result.append(" ")
            else:
                result.append(ch)
            i += 1
        return "".join(result)

    @classmethod
    def _check_dangerous_keywords(cls, query: str) -> None:
        """Verify dangerous keywords."""
        query_lower = query.lower()
        for keyword in cls.DANGEROUS_KEYWORDS:
            pattern = r"\b" + re.escape(keyword) + r"\b"
            if re.search(pattern, query_lower):
                raise ConfigurationError(
                    f"Query contains dangerous keyword: '{keyword}'. Only SELECT queries are allowed."
                ) from None

    @classmethod
    def _check_suspicious_patterns(cls, query: str) -> None:
        """Verify suspicious patterns that may indicate SQL injection."""
        query_lower = query.lower()
        for pattern in cls.SUSPICIOUS_PATTERNS:
            if re.search(pattern, query_lower, re.IGNORECASE):
                raise ConfigurationError(
                    "Query contains suspicious pattern that may indicate SQL injection attempt"
                ) from None

    @classmethod
    def _check_malicious_comments(cls, query: str) -> None:
        """Verify comments that could be used for injection."""
        if re.search(
            r"(--[^\r\n]*|/\*[\s\S]*?\*/|#[^\r\n]*)",
            query,
            re.IGNORECASE | re.MULTILINE,
        ):
            raise ConfigurationError(
                "Query contains potentially malicious comment pattern"
            ) from None

    @classmethod
    def _check_multiple_statements(cls, query: str) -> None:
        """Verify multiple SQL statements."""
        clean_query = cls._remove_comments(query)

        in_string = False
        quote_char = None
        semicolon_count = 0

        for i, char in enumerate(clean_query):
            if char in ('"', "'") and (i == 0 or clean_query[i - 1] != "\\"):
                if not in_string:
                    in_string = True
                    quote_char = char
                elif char == quote_char:
                    in_string = False
                    quote_char = None
            elif char == ";" and not in_string:
                semicolon_count += 1

        if semicolon_count > 1 or (
            semicolon_count == 1 and not clean_query.rstrip().endswith(";")
        ):
            raise ConfigurationError(
                "Multiple SQL statements are not allowed. Only single SELECT queries are permitted."
            ) from None
