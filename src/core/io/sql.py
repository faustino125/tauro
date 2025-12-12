"""
Copyright (c) 2025 Faustino Lopez Ramos. 
For licensing information, see the LICENSE file in the project root
"""
from typing import ClassVar, Set, List, Any
import re
from loguru import logger  # type: ignore

from core.io.exceptions import ConfigurationError

# Try to import sqlglot for AST-based parsing (preferred)
try:
    import sqlglot  # type: ignore

    SQLGLOT_AVAILABLE = True
except ImportError:
    SQLGLOT_AVAILABLE = False
    sqlglot = None


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
        r"--[^\r\n]*",  # Line comments --
        r"/\*[\s\S]*?\*/",  # Block comments /* */
        r"#[^\r\n]*",  # Line comments # (MySQL)
    ]

    SUSPICIOUS_PATTERNS: ClassVar[List[str]] = [
        r";\s*\w",  # Multiple statements
        r"0x[0-9a-f]+",  # Hexadecimal values
        r"char\s*\(",  # Suspicious char() conversions
        r"ascii\s*\(",  # Suspicious ASCII functions
        r"waitfor\s+delay",  # Timing attacks
        r"benchmark\s*\(",  # Benchmark attacks
    ]

    @classmethod
    def sanitize_query(cls, query: str) -> str:
        """Robust SQL query sanitization using AST parsing when available."""
        if not query or not isinstance(query, str):
            raise ConfigurationError("Query must be a non-empty string") from None

        original_query = query
        query = query.strip()

        if not query:
            raise ConfigurationError("Query cannot be empty after stripping whitespace") from None

        # Use AST-based parsing if sqlglot is available (more secure)
        if SQLGLOT_AVAILABLE:
            logger.debug("Using sqlglot AST parser for SQL validation")
            return cls._sanitize_with_sqlglot(original_query)
        else:
            # Fallback to regex-based validation
            logger.debug("sqlglot not available, using regex-based validation (less secure)")
            return cls._sanitize_with_regex(original_query)

    @classmethod
    def _is_select_query(cls, query: str) -> bool:
        """Verify that the query is a valid SELECT."""
        clean_query = cls._remove_comments(query)
        clean_query = clean_query.strip().lower()
        return clean_query.startswith("select ") or clean_query.startswith("with ")

    @classmethod
    def _sanitize_with_sqlglot(cls, query: str) -> str:
        """Sanitize query using AST-based parsing with sqlglot (more secure)."""
        try:
            parsed = sqlglot.parse_one(query, read="spark")  # Use Spark dialect

            if parsed is None:
                raise ConfigurationError("Failed to parse SQL query")

            # Validate query type using AST
            query_type = type(parsed).__name__
            allowed_types = ("Select", "CTE", "With")

            if query_type not in allowed_types:
                raise ConfigurationError(
                    f"Only SELECT and WITH queries are allowed. Got: {query_type}"
                )

            # Check for dangerous operations in AST
            cls._check_ast_for_dangerous_operations(parsed)

            logger.debug("Query validated successfully using sqlglot AST parser")
            return query

        except sqlglot.ParseError as e:
            raise ConfigurationError(f"Invalid SQL syntax: {str(e)}") from e
        except ConfigurationError:
            raise
        except Exception as e:
            logger.warning(f"sqlglot validation failed, falling back to regex: {e}")
            # Fall back to regex validation
            return cls._sanitize_with_regex(query)

    @classmethod
    def _check_ast_for_dangerous_operations(cls, parsed: Any) -> None:
        """Check AST for dangerous SQL operations."""
        try:
            # Check for DROP, DELETE, INSERT, UPDATE, CREATE, ALTER, TRUNCATE, etc.
            dangerous_ops = (
                "Drop",
                "Delete",
                "Insert",
                "Update",
                "Create",
                "Alter",
                "Truncate",
                "Merge",
                "Execute",
                "Call",
            )

            for op_type in dangerous_ops:
                # Look for these operation types in the AST
                if hasattr(sqlglot, op_type):
                    op_class = getattr(sqlglot, op_type)
                    if parsed.find(op_class):
                        raise ConfigurationError(f"Query contains forbidden operation: {op_type}")
        except ConfigurationError:
            raise
        except Exception as e:
            logger.warning(f"Could not fully validate AST structure: {e}")
            # If we can't validate completely, still accept if it parsed as SELECT/WITH

    @classmethod
    def _sanitize_with_regex(cls, original_query: str) -> str:
        """Sanitize query using regex-based validation (fallback, less secure)."""
        query = original_query.strip()
        normalized_query = re.sub(r"\s+", " ", query)

        if not cls._is_select_query(normalized_query):
            raise ConfigurationError(
                "Only SELECT and WITH queries are allowed. Query must start with SELECT or WITH."
            ) from None

        masked_for_checks = cls._mask_string_literals(normalized_query)

        cls._check_comment_safety(normalized_query)
        cls._check_dangerous_keywords(masked_for_checks)
        cls._check_suspicious_patterns(masked_for_checks)
        cls._check_multiple_statements(normalized_query)

        logger.debug("SQL query passed regex-based security validation (sqlglot not available)")
        return original_query

    @classmethod
    def _remove_comments(cls, query: str) -> str:
        """Remove SQL comments from the query."""
        for pattern in cls.COMMENT_PATTERNS:
            query = re.sub(pattern, "", query, flags=re.IGNORECASE | re.MULTILINE)
        return query

    @classmethod
    def _mask_string_literals(cls, query: str) -> str:
        """Replaces the content of string literals with spaces, preserving the quotes."""
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
            try:
                if re.search(pattern, query_lower, re.IGNORECASE):
                    raise ConfigurationError(
                        "Query contains suspicious pattern that may indicate SQL injection attempt"
                    ) from None
            except re.error:
                continue

    @classmethod
    def _extract_comments(cls, query: str) -> List[str]:
        """Extracts the raw text of comments found in the query."""
        comments: List[str] = []
        for pattern in cls.COMMENT_PATTERNS:
            for m in re.finditer(pattern, query, flags=re.IGNORECASE | re.MULTILINE):
                comments.append(m.group(0))
        return comments

    @classmethod
    def _check_comment_safety(cls, query: str) -> None:
        """Validate that comments do not contain dangerous tokens or suspicious patterns."""
        comments = cls._extract_comments(query)
        if not comments:
            return

        for c in comments:
            content_lower = cls._normalize_comment_content(c)
            cls._assert_no_semicolon_in_comment(content_lower)

            kw = cls._find_dangerous_keyword_in_comment(content_lower)
            if kw:
                raise ConfigurationError(
                    f"Comments contain dangerous keyword '{kw}' which is not allowed"
                ) from None

            if cls._comment_contains_suspicious_pattern(content_lower):
                raise ConfigurationError(
                    "Comments contain suspicious pattern that may indicate SQL injection attempt"
                ) from None

    @classmethod
    def _normalize_comment_content(cls, comment: str) -> str:
        """Return the content of a comment normalized to lower case without delimiters."""
        if comment.startswith("--"):
            content = comment[2:]
        elif comment.startswith("#"):
            content = comment[1:]
        elif comment.startswith("/*") and comment.endswith("*/"):
            content = comment[2:-2]
        else:
            content = comment
        return content.lower()

    @classmethod
    def _assert_no_semicolon_in_comment(cls, content_lower: str) -> None:
        if ";" in content_lower:
            raise ConfigurationError(
                "Comments in query contain semicolon which could indicate multiple statements"
            ) from None

    @classmethod
    def _find_dangerous_keyword_in_comment(cls, content_lower: str):
        for keyword in cls.DANGEROUS_KEYWORDS:
            if keyword in content_lower:
                return keyword
        return None

    @classmethod
    def _comment_contains_suspicious_pattern(cls, content_lower: str) -> bool:
        for pattern in cls.SUSPICIOUS_PATTERNS:
            try:
                if re.search(pattern, content_lower, re.IGNORECASE):
                    return True
            except re.error:
                continue
        return False

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

        if semicolon_count > 1 or (semicolon_count == 1 and not clean_query.rstrip().endswith(";")):
            raise ConfigurationError(
                "Multiple SQL statements are not allowed. Only single SELECT queries are permitted."
            ) from None
