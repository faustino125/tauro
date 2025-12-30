"""
Copyright (c) 2025 Faustino Lopez Ramos.
For licensing information, see the LICENSE file in the project root directory.
"""

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, List, Optional, Protocol
from abc import ABC, abstractmethod
import hashlib
import json
import operator as op

from loguru import logger  # type: ignore


class AccessLevel(Enum):
    """Access levels for data security."""

    UNRESTRICTED = "unrestricted"
    MASKED = "masked"
    ENCRYPTED = "encrypted"
    RESTRICTED = "restricted"


class Operation(Enum):
    """Operations that can be audited."""

    SELECT = "SELECT"
    INSERT = "INSERT"
    UPDATE = "UPDATE"
    DELETE = "DELETE"
    EXPORT = "EXPORT"


@dataclass
class FieldSecurityPolicy:
    """Security policy for a single field."""

    field_name: str
    access_level: AccessLevel = AccessLevel.UNRESTRICTED
    encryption_enabled: bool = False
    masking_enabled: bool = False
    masking_pattern: str = "***"  # For PII fields
    row_level_filter: Optional[str] = None  # SQL WHERE clause
    allowed_roles: List[str] = field(default_factory=lambda: ["*"])  # "*" = all roles


@dataclass
class TableSecurityPolicy:
    """Security policy for a virtual table."""

    table_name: str
    field_policies: Dict[str, FieldSecurityPolicy] = field(default_factory=dict)
    row_level_filters: Dict[str, str] = field(default_factory=dict)  # {role: filter}
    requires_encryption: bool = False
    audit_all_access: bool = True
    allowed_roles: List[str] = field(default_factory=lambda: ["*"])


@dataclass
class AuditLog:
    """Record of data access."""

    timestamp: datetime
    principal: str
    table_name: str
    operation: Operation
    field_names: List[str] = field(default_factory=list)
    row_count: int = 0
    status: str = "SUCCESS"  # SUCCESS, DENIED, ERROR
    denial_reason: str = ""
    data_hash: str = ""  # Hash of accessed data for integrity

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for logging."""
        return {
            "timestamp": self.timestamp.isoformat(),
            "principal": self.principal,
            "table_name": self.table_name,
            "operation": self.operation.value,
            "field_names": self.field_names,
            "row_count": self.row_count,
            "status": self.status,
            "denial_reason": self.denial_reason,
            "data_hash": self.data_hash,
        }


class EncryptionProvider(Protocol):
    """Protocol for encryption providers."""

    def encrypt(self, value: Any, key_id: str) -> bytes:
        """Encrypt a value."""
        ...

    def decrypt(self, encrypted_value: bytes, key_id: str) -> Any:
        """Decrypt a value."""
        ...

    def generate_key(self, key_id: str) -> str:
        """Generate a new encryption key."""
        ...


class MaskingStrategy(ABC):
    """Abstract base for PII masking strategies."""

    @abstractmethod
    def mask(self, value: Any) -> str:
        """Mask a sensitive value."""
        pass


class EmailMasker(MaskingStrategy):
    """Mask email addresses."""

    def mask(self, value: Any) -> str:
        """Replace email with masked version."""
        if not value or "@" not in str(value):
            return "***"

        email = str(value)
        local, domain = email.split("@", 1)

        if len(local) <= 2:
            masked_local = "*"
        else:
            masked_local = local[0] + "*" * (len(local) - 2) + local[-1]

        return f"{masked_local}@{domain}"


class PhoneMasker(MaskingStrategy):
    """Mask phone numbers."""

    def mask(self, value: Any) -> str:
        """Replace phone number with masked version."""
        phone = str(value).replace("-", "").replace(" ", "")

        if len(phone) < 4:
            return "***"

        return "*" * (len(phone) - 4) + phone[-4:]


class SSNMasker(MaskingStrategy):
    """Mask Social Security Numbers."""

    def mask(self, value: Any) -> str:
        """Replace SSN with masked version."""
        ssn = str(value).replace("-", "")

        if len(ssn) != 9:
            return "***"

        return f"***-**-{ssn[-4:]}"


class RLSExpressionEvaluator:
    """Evaluates Row-Level Security filter expressions."""

    # Safe operators for RLS expressions
    OPERATORS = {
        "=": op.eq,
        "==": op.eq,
        "!=": op.ne,
        "<>": op.ne,
        "<": op.lt,
        "<=": op.le,
        ">": op.gt,
        ">=": op.ge,
        "IN": lambda x, y: x in y,
        "NOT IN": lambda x, y: x not in y,
    }

    def __init__(self):
        """Initialize evaluator."""
        logger.debug("RLSExpressionEvaluator initialized")

    def evaluate(self, row: Dict[str, Any], expression: str, principal: str) -> bool:
        """Evaluate RLS expression against a row.

        Args:
            row: Data row to evaluate
            expression: Filter expression (e.g., "department = 'Sales'" or "user_id = {principal}")
            principal: Current user/principal for substitution

        Returns:
            True if row passes filter, False otherwise
        """
        if not expression or expression.strip() == "":
            return True

        try:
            # Substitute {principal} placeholder
            expr = expression.replace("{principal}", f"'{principal}'")

            # Parse simple expressions: "field operator value"
            parts = expr.split(None, 2)  # Split on whitespace, max 3 parts

            if len(parts) < 3:
                logger.warning(f"Invalid RLS expression format: {expression}")
                return False

            field = parts[0]
            operator_str = parts[1].upper()
            value_str = parts[2]

            # Get field value from row
            if field not in row:
                logger.debug(f"Field {field} not in row, denying access")
                return False

            row_value = row[field]

            # Parse value
            filter_value = self._parse_value(value_str)

            # Get operator function
            if operator_str not in self.OPERATORS:
                logger.warning(f"Unsupported operator: {operator_str}")
                return False

            operator_func = self.OPERATORS[operator_str]

            # Evaluate
            result = operator_func(row_value, filter_value)
            return bool(result)

        except Exception as e:
            logger.error(f"Error evaluating RLS expression '{expression}': {e}")
            return False

    def _parse_value(self, value_str: str) -> Any:
        """Parse value from string."""
        value_str = value_str.strip()

        # Handle quoted strings
        if (value_str.startswith("'") and value_str.endswith("'")) or (
            value_str.startswith('"') and value_str.endswith('"')
        ):
            return value_str[1:-1]

        # Handle lists for IN operator
        if value_str.startswith("(") and value_str.endswith(")"):
            items = value_str[1:-1].split(",")
            return [self._parse_value(item.strip()) for item in items]

        # Handle numbers
        try:
            if "." in value_str:
                return float(value_str)
            else:
                return int(value_str)
        except ValueError:
            pass

        # Handle booleans
        if value_str.upper() in ("TRUE", "FALSE"):
            return value_str.upper() == "TRUE"

        # Default: return as string
        return value_str


class SecurityEnforcer:
    """
    Enforces security policies on virtual data access.
    """

    def __init__(self, encryption_provider: Optional[EncryptionProvider] = None):
        """
        Initialize security enforcer.
        """
        self._policies: Dict[str, TableSecurityPolicy] = {}
        self._encryption_provider = encryption_provider
        self._audit_logs: List[AuditLog] = []
        self._masking_strategies: Dict[str, MaskingStrategy] = {
            "email": EmailMasker(),
            "phone": PhoneMasker(),
            "ssn": SSNMasker(),
        }
        self._rls_evaluator = RLSExpressionEvaluator()
        logger.debug("SecurityEnforcer initialized")

    def register_policy(self, policy: TableSecurityPolicy) -> None:
        """
        Register security policy for a table.
        """
        self._policies[policy.table_name] = policy
        logger.info(f"Security policy registered: {policy.table_name}")

    def get_policy(self, table_name: str) -> Optional[TableSecurityPolicy]:
        """Get security policy for a table."""
        return self._policies.get(table_name)

    def validate_access(
        self, principal: str, table_name: str, field_names: Optional[List[str]] = None
    ) -> bool:
        """
        Check if principal can perform operation.
        """
        policy = self.get_policy(table_name)

        # No policy = unrestricted access
        if not policy:
            return True

        # Check table-level access
        if policy.allowed_roles != ["*"] and principal not in policy.allowed_roles:
            logger.warning(f"Access denied: {principal} not in allowed roles for {table_name}")
            return False

        # Check field-level access
        if field_names:
            for field in field_names:
                field_policy = policy.field_policies.get(field)
                if field_policy and field_policy.allowed_roles != ["*"]:
                    if principal not in field_policy.allowed_roles:
                        logger.warning(f"Access denied: {principal} cannot access field {field}")
                        return False

        return True

    def apply_row_level_security(
        self, table_name: str, principal: str, rows: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """
        Filter rows based on row-level security policy.

        Args:
            table_name: Name of the table
            principal: User/role accessing the data
            rows: List of data rows

        Returns:
            Filtered list of rows based on RLS policy
        """
        policy = self.get_policy(table_name)

        if not policy:
            return rows

        # Get RLS filter for this principal
        rls_filter = policy.row_level_filters.get(principal)

        if not rls_filter:
            # No specific filter for this principal, return all rows
            return rows

        # Apply RLS filter
        filtered = []
        for row in rows:
            if self._rls_evaluator.evaluate(row, rls_filter, principal):
                filtered.append(row)

        logger.debug(f"Applied RLS: {len(rows)} rows → {len(filtered)} rows for {principal}")
        return filtered

    def _is_field_restricted_for_principal(
        self, field_policy: FieldSecurityPolicy, principal: str
    ) -> bool:
        """Return True if the principal is not allowed to access the field."""
        return field_policy.allowed_roles != ["*"] and principal not in field_policy.allowed_roles

    def _mask_value_for_field(
        self, field_name: str, field_policy: FieldSecurityPolicy, value: Any
    ) -> Any:
        """Return masked value according to policy and available strategy."""
        if not field_policy.masking_enabled:
            return value

        lname = field_name.lower()
        if "email" in lname:
            key = "email"
        elif "phone" in lname or "tel" in lname:
            key = "phone"
        elif "ssn" in lname or "social" in lname:
            key = "ssn"
        else:
            key = None

        strategy = self._masking_strategies.get(key) if key else None
        return strategy.mask(value) if strategy else field_policy.masking_pattern

    def apply_field_masking(
        self, table_name: str, principal: str, rows: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """
        Apply field-level masking to sensitive data.
        """
        policy = self.get_policy(table_name)

        if not policy:
            return rows

        masked_rows: List[Dict[str, Any]] = []
        for row in rows:
            masked_row = row.copy()

            for field_name, field_policy in policy.field_policies.items():
                if field_name not in masked_row:
                    continue

                if self._is_field_restricted_for_principal(field_policy, principal):
                    masked_row[field_name] = self._mask_value_for_field(
                        field_name, field_policy, masked_row[field_name]
                    )

            masked_rows.append(masked_row)

        logger.debug(f"Applied field masking for {principal} on {table_name}")
        return masked_rows

    def encrypt_field(self, table_name: str, field_name: str, value: Any) -> Optional[bytes]:
        """
        Encrypt a field value.
        """
        if not self._encryption_provider:
            logger.warning("Encryption requested but no provider configured")
            return None

        policy = self.get_policy(table_name)

        if not policy:
            return None

        field_policy = policy.field_policies.get(field_name)

        if not field_policy or not field_policy.encryption_enabled:
            return None

        key_id = f"{table_name}_{field_name}"
        encrypted = self._encryption_provider.encrypt(value, key_id)

        logger.debug(f"Encrypted field: {table_name}.{field_name}")
        return encrypted

    def audit_access(
        self,
        principal: str,
        table_name: str,
        field_names: Optional[List[str]] = None,
        row_count: int = 0,
        status: str = "SUCCESS",
        denial_reason: str = "",
    ) -> AuditLog:
        """
        Record data access for audit trail.
        """
        # Default operation when not provided by caller
        operation = Operation.SELECT

        log = AuditLog(
            timestamp=datetime.now(timezone.utc),
            principal=principal,
            table_name=table_name,
            operation=operation,
            field_names=field_names or [],
            row_count=row_count,
            status=status,
            denial_reason=denial_reason,
        )

        self._audit_logs.append(log)

        if status == "DENIED":
            logger.warning(f"Access denied: {principal} → {table_name} ({denial_reason})")
        else:
            logger.info(f"Access granted: {principal} → {table_name} ({operation.value})")

        return log

    def get_audit_logs(
        self,
        principal: Optional[str] = None,
        table_name: Optional[str] = None,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
    ) -> List[AuditLog]:
        """
        Retrieve audit logs with optional filtering.
        """
        results = self._audit_logs

        if principal:
            results = [log for log in results if log.principal == principal]

        if table_name:
            results = [log for log in results if log.table_name == table_name]

        if start_time:
            results = [log for log in results if log.timestamp >= start_time]

        if end_time:
            results = [log for log in results if log.timestamp <= end_time]

        return results

    def export_audit_trail(self, format: str = "json") -> str:
        """
        Export audit logs for compliance reporting.
        """
        if format == "json":
            logs = [log.to_dict() for log in self._audit_logs]
            return json.dumps(logs, indent=2, default=str)

        elif format == "csv":
            import csv
            from io import StringIO

            output = StringIO()
            if not self._audit_logs:
                return ""

            writer = csv.DictWriter(output, fieldnames=self._audit_logs[0].to_dict().keys())
            writer.writeheader()
            for log in self._audit_logs:
                writer.writerow(log.to_dict())

            return output.getvalue()

        else:
            raise ValueError(f"Unsupported export format: {format}")
