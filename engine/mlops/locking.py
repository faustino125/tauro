"""
File locking utilities for concurrent-safe operations.

Provides cross-platform file locking to prevent race conditions
during index updates and other critical operations.
"""

import os
import time
from pathlib import Path
from typing import Optional
from contextlib import contextmanager

from loguru import logger


class FileLock:
    """
    Cross-platform file locking mechanism.

    Uses platform-specific locking:
    - Windows: msvcrt
    - Unix/Linux/Mac: fcntl

    Provides timeout and retry logic to handle concurrent access.
    """

    def __init__(
        self,
        lock_file: str,
        timeout: float = 30.0,
        check_interval: float = 0.1,
    ):
        """
        Initialize file lock.

        Args:
            lock_file: Path to lock file
            timeout: Max seconds to wait for lock
            check_interval: Seconds between lock attempts
        """
        self.lock_file = Path(lock_file)
        self.timeout = timeout
        self.check_interval = check_interval
        self.lock_fd: Optional[int] = None

    def acquire(self) -> bool:
        """
        Acquire the lock with timeout.

        Returns:
            True if lock acquired, False if timeout

        Raises:
            TimeoutError: If lock cannot be acquired within timeout
        """
        start_time = time.time()

        # Ensure lock file directory exists
        self.lock_file.parent.mkdir(parents=True, exist_ok=True)

        while True:
            try:
                # Try to create lock file exclusively
                self.lock_fd = os.open(str(self.lock_file), os.O_CREAT | os.O_EXCL | os.O_RDWR)

                # Lock acquired successfully
                logger.debug(f"Lock acquired: {self.lock_file}")
                return True

            except FileExistsError:
                # Lock file exists, check timeout
                elapsed = time.time() - start_time
                if elapsed >= self.timeout:
                    raise TimeoutError(
                        f"Could not acquire lock {self.lock_file} after {self.timeout} seconds"
                    )

                # Wait and retry
                time.sleep(self.check_interval)

            except Exception as e:
                logger.error(f"Error acquiring lock {self.lock_file}: {e}")
                raise

    def release(self) -> None:
        """Release the lock."""
        if self.lock_fd is not None:
            try:
                os.close(self.lock_fd)
                self.lock_file.unlink(missing_ok=True)
                logger.debug(f"Lock released: {self.lock_file}")
            except Exception as e:
                logger.warning(f"Error releasing lock {self.lock_file}: {e}")
            finally:
                self.lock_fd = None

    def __enter__(self):
        """Context manager entry."""
        self.acquire()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.release()
        return False


@contextmanager
def file_lock(lock_file: str, timeout: float = 30.0):
    """
    Context manager for file locking.

    Usage:
        with file_lock("/path/to/lock.lock"):
            # Critical section
            update_shared_resource()

    Args:
        lock_file: Path to lock file
        timeout: Max seconds to wait for lock

    Yields:
        None

    Raises:
        TimeoutError: If lock cannot be acquired
    """
    lock = FileLock(lock_file, timeout=timeout)
    try:
        lock.acquire()
        yield
    finally:
        lock.release()


class OptimisticLock:
    """
    Optimistic locking using version/checksum verification.

    Useful for detecting concurrent modifications without
    blocking operations.
    """

    @staticmethod
    def compute_checksum(data: bytes) -> str:
        """Compute checksum for data."""
        import hashlib

        return hashlib.sha256(data).hexdigest()

    @staticmethod
    def verify_no_changes(file_path: Path, original_checksum: str) -> bool:
        """
        Verify file hasn't changed since checksum was computed.

        Args:
            file_path: Path to file
            original_checksum: Original checksum

        Returns:
            True if file unchanged, False otherwise
        """
        if not file_path.exists():
            return False

        current_data = file_path.read_bytes()
        current_checksum = OptimisticLock.compute_checksum(current_data)

        return current_checksum == original_checksum

    @staticmethod
    def read_with_checksum(file_path: Path) -> tuple[bytes, str]:
        """
        Read file and compute checksum.

        Args:
            file_path: Path to file

        Returns:
            Tuple of (data, checksum)
        """
        data = file_path.read_bytes()
        checksum = OptimisticLock.compute_checksum(data)
        return data, checksum
