from typing import Any, Dict, List, Optional
from dataclasses import dataclass
from pathlib import Path
import tempfile
import shutil

from loguru import logger

from engine.mlops.storage import StorageBackend
from engine.mlops.locking import file_lock
from engine.mlops.exceptions import MLOpsException


class TransactionError(MLOpsException):
    """Transaction operation failed"""

    pass


class RollbackError(TransactionError):
    """Rollback operation failed"""

    pass


@dataclass
class Operation:
    """Single transaction operation"""

    operation_type: str  # "write_json", "write_dataframe", "write_artifact", "delete"
    path: str
    data: Any = None
    mode: str = "overwrite"

    def __hash__(self):
        return hash(self.path)


class Transaction:
    """
    Atomic transaction for multiple storage operations.

    All operations are buffered and executed together under lock.
    If any operation fails, the transaction fails (no automatic rollback
    of written files, but prevents index corruption).

    Usage:
        txn = Transaction(storage, "/path/lock.lock")
        txn.write_json(data, "path.json")
        txn.write_dataframe(df, "path.parquet")
        txn.execute()
    """

    def __init__(
        self,
        storage: StorageBackend,
        lock_path: str,
        timeout: float = 30.0,
        temp_dir: Optional[str] = None,
    ):
        """
        Initialize transaction.

        Args:
            storage: Storage backend
            lock_path: Path to lock file
            timeout: Lock timeout in seconds
            temp_dir: Temporary directory for staging (None = system temp)
        """
        self.storage = storage
        self.lock_path = lock_path
        self.timeout = timeout
        self.temp_dir = temp_dir or tempfile.gettempdir()

        self.operations: List[Operation] = []
        self.executed = False
        self._staging_dir: Optional[Path] = None

    def write_json(self, data: Dict[str, Any], path: str, mode: str = "overwrite") -> "Transaction":
        """Queue JSON write operation"""
        self.operations.append(
            Operation(operation_type="write_json", path=path, data=data, mode=mode)
        )
        return self

    def write_dataframe(
        self, df: Any, path: str, mode: str = "overwrite"  # pandas.DataFrame
    ) -> "Transaction":
        """Queue DataFrame write operation"""
        self.operations.append(
            Operation(operation_type="write_dataframe", path=path, data=df, mode=mode)
        )
        return self

    def write_artifact(
        self, artifact_path: str, destination: str, mode: str = "overwrite"
    ) -> "Transaction":
        """Queue artifact write operation"""
        self.operations.append(
            Operation(
                operation_type="write_artifact", path=destination, data=artifact_path, mode=mode
            )
        )
        return self

    def delete(self, path: str) -> "Transaction":
        """Queue delete operation"""
        self.operations.append(Operation(operation_type="delete", path=path))
        return self

    def execute(self) -> bool:
        """
        Execute all operations atomically under lock.

        Returns:
            True if successful, False if there were no operations.

        Raises:
            TransactionError: If execution fails
        """
        logger.debug(f"Attempting to execute transaction with {len(self.operations)} operations")

        try:
            with file_lock(self.lock_path, timeout=self.timeout):
                # Check inside lock to prevent race condition
                if self.executed:
                    raise TransactionError("Transaction already executed")

                if not self.operations:
                    logger.debug("Transaction has no operations, skipping")
                    return False

                for i, operation in enumerate(self.operations):
                    try:
                        self._execute_operation(operation)
                    except Exception as e:
                        raise TransactionError(
                            f"Operation {i} ({operation.operation_type} at "
                            f"'{operation.path}') failed: {e}"
                        )

                # Mark as executed inside lock
                self.executed = True
                logger.info(
                    f"Transaction committed successfully with {len(self.operations)} operations"
                )
                return True

        except Exception as e:
            logger.error(f"Transaction failed: {e}")
            raise

    def _execute_operation(self, operation: Operation) -> None:
        """Execute a single operation"""
        if operation.operation_type == "write_json":
            self.storage.write_json(operation.data, operation.path, mode=operation.mode)

        elif operation.operation_type == "write_dataframe":
            self.storage.write_dataframe(operation.data, operation.path, mode=operation.mode)

        elif operation.operation_type == "write_artifact":
            self.storage.write_artifact(
                operation.data, operation.path, mode=operation.mode  # artifact_path  # destination
            )

        elif operation.operation_type == "delete":
            self.storage.delete(operation.path)

        else:
            raise ValueError(f"Unknown operation type: {operation.operation_type}")

    def __enter__(self):
        """Context manager entry"""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        if exc_type is None:
            # No exception, execute transaction
            self.execute()
        else:
            # Exception occurred, don't execute
            logger.warning(
                f"Transaction context manager exited with exception, "
                f"not executing {len(self.operations)} operations"
            )
        return False


class SafeTransaction(Transaction):
    """
    Enhanced transaction with automatic rollback capability.

    Stages writes to temporary directory before executing,
    allowing rollback if something goes wrong.

    NOTE: This is more complex but provides better guarantees.
    Currently not implemented for file-based storage.
    """

    def __init__(
        self,
        storage: StorageBackend,
        lock_path: str,
        timeout: float = 30.0,
        enable_staging: bool = True,
    ):
        """
        Initialize safe transaction.

        Args:
            storage: Storage backend
            lock_path: Path to lock file
            timeout: Lock timeout
            enable_staging: Enable staging (only for debugging)
        """
        super().__init__(storage, lock_path, timeout)
        self.enable_staging = enable_staging
        self._original_values: Dict[str, Any] = {}

    def execute(self) -> bool:
        """
        Execute with enhanced safety.

        For file-based storage, this reads original values before
        executing (for potential rollback).
        """
        if self.executed:
            raise TransactionError("Transaction already executed")

        if not self.operations:
            logger.debug("Safe transaction has no operations, skipping")
            return False

        # For file storage, we can log original state for debugging
        if self.enable_staging:
            logger.debug("Safe transaction enabled with staging")
            # Read originals (if they exist)
            for operation in self.operations:
                try:
                    if operation.operation_type == "write_dataframe":
                        self._original_values[operation.path] = self.storage.read_dataframe(
                            operation.path
                        )
                except Exception:
                    # Path doesn't exist yet, that's OK
                    pass

        # Execute normally
        result = super().execute()

        # If successful and staging, we can clean up
        if result and self.enable_staging:
            logger.debug("Safe transaction committed, clearing staging")

        return result

    def rollback(self) -> bool:
        """
        Attempt to rollback to original state.

        NOTE: Only works for dataframes that were backed up.
        For artifact copies or deletions, manual intervention may be needed.

        Returns:
            True if rollback successful
        """
        if not self.executed:
            logger.warning("Cannot rollback non-executed transaction")
            return False

        logger.warning("Attempting to rollback transaction")

        rollback_count = 0
        for operation in self.operations:
            if operation.operation_type == "write_dataframe":
                if operation.path in self._original_values:
                    try:
                        original_df = self._original_values[operation.path]
                        self.storage.write_dataframe(original_df, operation.path, mode="overwrite")
                        rollback_count += 1
                    except Exception as e:
                        logger.error(f"Failed to rollback dataframe at {operation.path}: {e}")

        if rollback_count > 0:
            logger.info(f"Rolled back {rollback_count} dataframes")
            return True

        logger.warning(
            "Rollback did not restore any files. "
            "May need manual intervention to clean up partial writes."
        )
        return False


def transactional_operation(storage: StorageBackend, lock_path: str, timeout: float = 30.0):
    """
    Decorator for transactional operations.

    Usage:
        @transactional_operation(storage, "/path/lock.lock")
        def register_model(name, artifact_path):
            # Function receives 'txn' parameter
            txn.write_json(metadata, "metadata.json")
            txn.write_dataframe(index, "index.parquet")
            return result
    """

    def decorator(func):
        def wrapper(*args, **kwargs):
            txn = Transaction(storage, lock_path, timeout)

            # Call function with transaction
            result = func(*args, txn=txn, **kwargs)

            # Execute transaction
            txn.execute()

            return result

        return wrapper

    return decorator
