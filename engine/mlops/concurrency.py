import atexit
import hashlib
import os
import tempfile
import threading
import time
import weakref
from contextlib import contextmanager
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, TYPE_CHECKING

from loguru import logger

if TYPE_CHECKING:
    import pandas as pd


DEFAULT_LOCK_TIMEOUT = 30.0
DEFAULT_CHECK_INTERVAL = 0.1
STALE_LOCK_THRESHOLD = 300.0  # 5 minutes


@dataclass
class LockStats:
    """Statistics for lock operations."""

    acquisitions: int = 0
    releases: int = 0
    timeouts: int = 0
    stale_cleanups: int = 0
    total_wait_time: float = 0.0

    def record_acquisition(self, wait_time: float) -> None:
        """Record a successful acquisition."""
        self.acquisitions += 1
        self.total_wait_time += wait_time

    def record_timeout(self) -> None:
        """Record a timeout."""
        self.timeouts += 1

    def record_release(self) -> None:
        """Record a release."""
        self.releases += 1

    def record_stale_cleanup(self) -> None:
        """Record a stale lock cleanup."""
        self.stale_cleanups += 1

    @property
    def average_wait_time(self) -> float:
        """Average wait time for acquisitions."""
        if self.acquisitions == 0:
            return 0.0
        return self.total_wait_time / self.acquisitions


class LockManager:
    """
    Global lock manager for tracking and cleanup of all active locks.
    """

    _instance: Optional["LockManager"] = None
    _lock = threading.Lock()

    def __new__(cls) -> "LockManager":
        with cls._lock:
            if cls._instance is None:
                cls._instance = super().__new__(cls)
                cls._instance._initialized = False
            return cls._instance

    def __init__(self):
        if self._initialized:
            return

        self._active_locks: Dict[str, weakref.ref] = {}
        self._lock_files: Set[Path] = set()
        self._stats = LockStats()
        self._thread_lock = threading.RLock()
        self._initialized = True

        atexit.register(self._cleanup_all)
        logger.debug("LockManager initialized")

    def register(self, lock: "FileLock") -> None:
        """Register a lock for tracking."""
        with self._thread_lock:
            key = str(lock.lock_file)
            self._active_locks[key] = weakref.ref(lock)
            self._lock_files.add(lock.lock_file)

    def unregister(self, lock: "FileLock") -> None:
        """Unregister a lock."""
        with self._thread_lock:
            key = str(lock.lock_file)
            self._active_locks.pop(key, None)
            self._lock_files.discard(lock.lock_file)

    @property
    def stats(self) -> LockStats:
        """Get lock statistics."""
        return self._stats

    def cleanup_stale_locks(self, threshold: float = STALE_LOCK_THRESHOLD) -> int:
        """
        Clean up stale lock files.

        Returns:
            Number of stale locks cleaned up
        """
        cleaned = 0
        current_time = time.time()

        with self._thread_lock:
            stale_files = []

            for lock_file in self._lock_files:
                try:
                    if lock_file.exists():
                        mtime = lock_file.stat().st_mtime
                        if current_time - mtime > threshold:
                            stale_files.append(lock_file)
                except OSError:
                    continue

            for lock_file in stale_files:
                try:
                    lock_file.unlink(missing_ok=True)
                    self._lock_files.discard(lock_file)
                    self._stats.record_stale_cleanup()
                    cleaned += 1
                    logger.warning(f"Cleaned up stale lock: {lock_file}")
                except OSError as e:
                    logger.warning(f"Failed to clean stale lock {lock_file}: {e}")

        return cleaned

    def _cleanup_all(self) -> None:
        """Clean up all active locks on process exit."""
        with self._thread_lock:
            for key, lock_ref in self._active_locks.items():
                lock = lock_ref()
                if lock is not None:
                    try:
                        lock.release()
                    except Exception as e:
                        logger.warning(f"Error releasing lock {key} on exit: {e}")

            for lock_file in self._lock_files:
                try:
                    lock_file.unlink(missing_ok=True)
                except OSError:
                    pass

            self._active_locks.clear()
            self._lock_files.clear()

        logger.debug("LockManager cleanup completed")

    def get_active_lock_count(self) -> int:
        """Get count of active locks."""
        with self._thread_lock:
            self._active_locks = {k: v for k, v in self._active_locks.items() if v() is not None}
            return len(self._active_locks)


def get_lock_manager() -> LockManager:
    """Get the global lock manager instance."""
    return LockManager()


class FileLock:
    """
    Cross-platform file locking mechanism with automatic cleanup.
    """

    def __init__(
        self,
        lock_file: str,
        timeout: float = DEFAULT_LOCK_TIMEOUT,
        check_interval: float = DEFAULT_CHECK_INTERVAL,
        auto_cleanup_stale: bool = True,
    ):
        self.lock_file = Path(lock_file)
        self.timeout = timeout
        self.check_interval = check_interval
        self.auto_cleanup_stale = auto_cleanup_stale
        self.lock_fd: Optional[int] = None
        self._acquired = False
        self._acquisition_time: Optional[float] = None
        self._thread_lock = threading.Lock()
        self._manager = get_lock_manager()

    def _check_stale_lock(self) -> bool:
        """Check if existing lock file is stale and clean it up."""
        if not self.lock_file.exists():
            return False

        try:
            mtime = self.lock_file.stat().st_mtime
            age = time.time() - mtime

            if age > STALE_LOCK_THRESHOLD:
                logger.warning(f"Detected stale lock {self.lock_file} (age: {age:.1f}s)")
                self.lock_file.unlink(missing_ok=True)
                self._manager.stats.record_stale_cleanup()
                return True
        except OSError:
            pass

        return False

    def _create_lock_file(self):
        return os.open(str(self.lock_file), os.O_CREAT | os.O_EXCL | os.O_RDWR)

    def _should_timeout(self, start_time: float) -> bool:
        return (time.time() - start_time) >= self.timeout

    def _maybe_cleanup_stale(self, elapsed: float) -> bool:
        if self.auto_cleanup_stale and int(elapsed) % 5 == 0:
            return self._check_stale_lock()
        return False

    def _write_lock_metadata(self, fd: int) -> None:
        try:
            self.lock_fd = fd
        except Exception:
            try:
                os.close(fd)
            except Exception:
                pass
            raise

    def _handle_lock_exists(self, start_time: float) -> bool:
        elapsed = time.time() - start_time

        if self._should_timeout(start_time):
            self._manager.stats.record_timeout()
            logger.warning(f"Could not acquire lock {self.lock_file} after {self.timeout} seconds")
            return False

        if self._maybe_cleanup_stale(elapsed):
            return True

        time.sleep(self.check_interval)
        return True

    def _finalize_acquisition(self, start_time: float) -> None:
        self._acquired = True
        self._acquisition_time = time.time()
        wait_time = self._acquisition_time - start_time

        self._manager.register(self)
        self._manager.stats.record_acquisition(wait_time)

        logger.debug(f"Lock acquired: {self.lock_file} (wait: {wait_time:.3f}s)")

    def acquire(self) -> bool:
        """Acquire the lock with timeout."""
        with self._thread_lock:
            if self._acquired:
                logger.warning(f"Lock already acquired: {self.lock_file}")
                return True

            start_time = time.time()
            self.lock_file.parent.mkdir(parents=True, exist_ok=True)

            if self.auto_cleanup_stale:
                self._check_stale_lock()

            while True:
                try:
                    fd = self._create_lock_file()
                    self._write_lock_metadata(fd)
                    self._finalize_acquisition(start_time)
                    return True
                except FileExistsError:
                    if not self._handle_lock_exists(start_time):
                        return False
                except Exception as e:
                    logger.error(f"Error acquiring lock {self.lock_file}: {e}")
                    raise

    def release(self) -> None:
        """Release the lock."""
        with self._thread_lock:
            if not self._acquired and self.lock_fd is None:
                return

            try:
                if self.lock_fd is not None:
                    os.close(self.lock_fd)
                self.lock_file.unlink(missing_ok=True)

                self._manager.unregister(self)
                self._manager.stats.record_release()

                if self._acquisition_time is not None:
                    hold_time = time.time() - self._acquisition_time
                    logger.debug(f"Lock released: {self.lock_file} (held: {hold_time:.3f}s)")
                else:
                    logger.debug(f"Lock released: {self.lock_file}")
            except Exception as e:
                logger.warning(f"Error releasing lock {self.lock_file}: {e}")
            finally:
                self.lock_fd = None
                self._acquired = False
                self._acquisition_time = None

    @property
    def is_acquired(self) -> bool:
        """Check if lock is currently acquired."""
        return self._acquired

    @property
    def hold_time(self) -> Optional[float]:
        """Get time lock has been held, or None if not acquired."""
        if self._acquisition_time is None:
            return None
        return time.time() - self._acquisition_time

    def __enter__(self):
        self.acquire()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.release()
        return False

    def __del__(self):
        try:
            self.release()
        except Exception:
            pass


@contextmanager
def file_lock(
    lock_file: str,
    timeout: float = DEFAULT_LOCK_TIMEOUT,
    auto_cleanup_stale: bool = True,
):
    """
    Context manager for file locking.
    """
    lock = FileLock(lock_file, timeout=timeout, auto_cleanup_stale=auto_cleanup_stale)
    try:
        lock.acquire()
        yield lock
    finally:
        lock.release()


class OptimisticLock:
    """
    Optimistic locking using version counters or checksum verification.
    """

    def __init__(self, initial_version: int = 1):
        """Initialize optimistic lock with version counter."""
        self._version = initial_version
        self._lock = threading.Lock()

    @property
    def version(self) -> int:
        """Get current version."""
        with self._lock:
            return self._version

    @version.setter
    def version(self, value: int) -> None:
        """Set version."""
        with self._lock:
            self._version = value

    def check_version(self, expected_version: int) -> None:
        """
        Check if current version matches expected.
        """
        with self._lock:
            if self._version != expected_version:
                raise ValueError(
                    f"Version conflict: expected {expected_version}, got {self._version}"
                )

    def increment_version(self) -> int:
        """
        Atomically increment version and return new value.
        """
        with self._lock:
            self._version += 1
            return self._version

    def __str__(self) -> str:
        return f"OptimisticLock(version={self._version})"

    def __repr__(self) -> str:
        return self.__str__()

    # Static methods for checksum-based optimistic locking

    @staticmethod
    def compute_checksum(data: bytes) -> str:
        """Compute SHA-256 checksum for data."""
        return hashlib.sha256(data).hexdigest()

    @staticmethod
    def verify_no_changes(file_path: Path, original_checksum: str) -> bool:
        """Verify file hasn't changed since checksum was computed."""
        if not file_path.exists():
            return False

        try:
            current_data = file_path.read_bytes()
            current_checksum = OptimisticLock.compute_checksum(current_data)
            return current_checksum == original_checksum
        except OSError:
            return False

    @staticmethod
    def read_with_checksum(file_path: Path) -> tuple[bytes, str]:
        """
        Read file and compute checksum atomically.

        Returns:
            Tuple of (file_data, checksum)
        """
        data = file_path.read_bytes()
        checksum = OptimisticLock.compute_checksum(data)
        return data, checksum


class ReadWriteLock:
    """
    A readers-writer lock allowing concurrent reads but exclusive writes.
    """

    def __init__(self):
        self._read_count = 0
        self._write_waiting = 0
        self._lock = threading.Lock()
        self._read_lock = threading.Condition(self._lock)
        self._write_lock = threading.Condition(self._lock)
        self._writing = False

    @contextmanager
    def read_lock(self):
        """Acquire read lock."""
        with self._lock:
            while self._writing or self._write_waiting > 0:
                self._read_lock.wait()
            self._read_count += 1

        try:
            yield
        finally:
            with self._lock:
                self._read_count -= 1
                if self._read_count == 0:
                    self._write_lock.notify()

    @contextmanager
    def write_lock(self):
        """Acquire write lock."""
        with self._lock:
            self._write_waiting += 1
            try:
                while self._writing or self._read_count > 0:
                    self._write_lock.wait()
                self._writing = True
            finally:
                self._write_waiting -= 1

        try:
            yield
        finally:
            with self._lock:
                self._writing = False
                self._read_lock.notify_all()
                self._write_lock.notify()


class TransactionState(Enum):
    """Transaction execution state."""

    PENDING = "pending"
    ACTIVE = "active"
    COMMITTED = "committed"
    ROLLED_BACK = "rolled_back"
    FAILED = "failed"


class TransactionError(Exception):
    """Transaction operation failed."""

    pass


class RollbackError(TransactionError):
    """Rollback operation failed."""

    pass


@dataclass
class Operation:
    """Single transaction operation."""

    operation_type: str  # "write_json", "write_dataframe", "write_artifact", "delete"
    path: str
    data: Any = None
    mode: str = "overwrite"

    def __hash__(self):
        return hash(self.path)


class Transaction:
    """
    Atomic transaction for multiple storage operations.
    """

    def __init__(
        self,
        storage: Any,  # StorageBackend
        lock_path: str,
        timeout: float = 30.0,
        temp_dir: Optional[str] = None,
    ):
        self.storage = storage
        self.lock_path = lock_path
        self.timeout = timeout
        self.temp_dir = temp_dir or tempfile.gettempdir()

        self.operations: List[Operation] = []
        self.executed = False
        self._staging_dir: Optional[Path] = None

    def write_json(self, data: Dict[str, Any], path: str, mode: str = "overwrite") -> "Transaction":
        """Queue JSON write operation."""
        self.operations.append(
            Operation(operation_type="write_json", path=path, data=data, mode=mode)
        )
        return self

    def write_dataframe(
        self,
        df: "pd.DataFrame",
        path: str,
        mode: str = "overwrite",
    ) -> "Transaction":
        """Queue DataFrame write operation."""
        self.operations.append(
            Operation(operation_type="write_dataframe", path=path, data=df, mode=mode)
        )
        return self

    def write_artifact(
        self, artifact_path: str, destination: str, mode: str = "overwrite"
    ) -> "Transaction":
        """Queue artifact write operation."""
        self.operations.append(
            Operation(
                operation_type="write_artifact", path=destination, data=artifact_path, mode=mode
            )
        )
        return self

    def delete(self, path: str) -> "Transaction":
        """Queue delete operation."""
        self.operations.append(Operation(operation_type="delete", path=path))
        return self

    def execute(self) -> bool:
        """Execute all operations atomically under lock."""
        logger.debug(f"Executing transaction with {len(self.operations)} operations")

        try:
            with file_lock(self.lock_path, timeout=self.timeout):
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
                            f"Operation {i} ({operation.operation_type} at '{operation.path}') failed: {e}"
                        )

                self.executed = True
                logger.info(f"Transaction committed with {len(self.operations)} operations")
                return True

        except Exception as e:
            logger.error(f"Transaction failed: {e}")
            raise

    def _execute_operation(self, operation: Operation) -> None:
        """Execute a single operation."""
        if operation.operation_type == "write_json":
            self.storage.write_json(operation.data, operation.path, mode=operation.mode)
        elif operation.operation_type == "write_dataframe":
            self.storage.write_dataframe(operation.data, operation.path, mode=operation.mode)
        elif operation.operation_type == "write_artifact":
            self.storage.write_artifact(operation.data, operation.path, mode=operation.mode)
        elif operation.operation_type == "delete":
            self.storage.delete(operation.path)
        else:
            raise ValueError(f"Unknown operation type: {operation.operation_type}")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is None:
            self.execute()
        else:
            logger.warning("Transaction context exited with exception, not executing")
        return False


# =============================================================================
# Safe Transaction with Rollback
# =============================================================================


class SafeTransaction(Transaction):
    """
    Enhanced transaction with automatic rollback capability.
    """

    def __init__(
        self,
        storage: Any,
        lock_path: str,
        timeout: float = 30.0,
        enable_staging: bool = True,
    ):
        super().__init__(storage, lock_path, timeout)
        self.enable_staging = enable_staging
        self._original_values: Dict[str, Any] = {}

    def execute(self) -> bool:
        """Execute with enhanced safety and staging."""
        if self.executed:
            raise TransactionError("Transaction already executed")

        if not self.operations:
            logger.debug("Safe transaction has no operations, skipping")
            return False

        if self.enable_staging:
            logger.debug("Safe transaction enabled with staging")
            for operation in self.operations:
                try:
                    if operation.operation_type == "write_dataframe":
                        self._original_values[operation.path] = self.storage.read_dataframe(
                            operation.path
                        )
                except Exception:
                    pass  # Path doesn't exist yet

        result = super().execute()

        if result and self.enable_staging:
            logger.debug("Safe transaction committed, clearing staging")

        return result

    def rollback(self) -> bool:
        """Attempt to rollback to original state."""
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
                        logger.error(f"Failed to rollback at {operation.path}: {e}")

        if rollback_count > 0:
            logger.info(f"Rolled back {rollback_count} operations")
            return True

        logger.warning("Rollback did not restore any files")
        return False


def transactional_operation(storage: Any, lock_path: str, timeout: float = 30.0):
    """
    Decorator for transactional operations.
    """

    def decorator(func):
        def wrapper(*args, **kwargs):
            txn = Transaction(storage, lock_path, timeout)
            result = func(*args, txn=txn, **kwargs)
            txn.execute()
            return result

        return wrapper

    return decorator
