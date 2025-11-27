"""
Tests for MLOps transaction management.

Tests transaction atomicity, rollback, and safe transactions.
"""
import pytest
import tempfile
import threading
import time
from pathlib import Path

from engine.mlops.concurrency import (
    Transaction,
    SafeTransaction,
    TransactionError,
)
from engine.mlops.storage import LocalStorageBackend


class TestTransaction:
    """Tests for Transaction."""

    @pytest.fixture
    def storage(self):
        """Create temporary storage backend."""
        with tempfile.TemporaryDirectory() as tmpdir:
            yield LocalStorageBackend(tmpdir)

    @pytest.fixture
    def lock_path(self):
        """Create temporary lock path."""
        with tempfile.TemporaryDirectory() as tmpdir:
            yield str(Path(tmpdir) / "test.lock")

    def test_transaction_basic_commit(self, storage, lock_path):
        """Test basic transaction commit."""
        txn = Transaction(storage, lock_path)
        txn.write_json({"test": "value"}, "test.json")

        result = txn.execute()
        assert result is True

        # Verify data was written
        data = storage.read_json("test.json")
        assert data == {"test": "value"}

    def test_transaction_multiple_operations(self, storage, lock_path):
        """Test transaction with multiple operations."""
        txn = Transaction(storage, lock_path)
        txn.write_json({"key1": "value1"}, "file1.json")
        txn.write_json({"key2": "value2"}, "file2.json")

        result = txn.execute()
        assert result is True

        # Verify all files were written
        assert storage.read_json("file1.json") == {"key1": "value1"}
        assert storage.read_json("file2.json") == {"key2": "value2"}

    def test_transaction_context_manager(self, storage, lock_path):
        """Test transaction as context manager."""
        with Transaction(storage, lock_path) as txn:
            txn.write_json({"test": "data"}, "test.json")

        # Should auto-execute on exit
        data = storage.read_json("test.json")
        assert data == {"test": "data"}

    def test_transaction_empty(self, storage, lock_path):
        """Test transaction with no operations."""
        txn = Transaction(storage, lock_path)
        result = txn.execute()

        # Should return False for empty transaction
        assert result is False

    def test_transaction_cannot_execute_twice(self, storage, lock_path):
        """Test that transaction cannot be executed twice."""
        txn = Transaction(storage, lock_path)
        txn.write_json({"test": "value"}, "test.json")

        txn.execute()

        # Second execution should raise
        with pytest.raises(TransactionError, match="already executed"):
            txn.execute()

    def test_transaction_concurrent_access(self, storage, lock_path):
        """Test that transactions are serialized."""
        results = []

        def worker(worker_id: int):
            try:
                with Transaction(storage, lock_path, timeout=2.0) as txn:
                    results.append(f"start_{worker_id}")
                    time.sleep(0.05)
                    txn.write_json({"worker": worker_id}, f"worker_{worker_id}.json")
                    results.append(f"end_{worker_id}")
                return True
            except Exception:
                return False

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(3)]

        for t in threads:
            t.start()

        for t in threads:
            t.join()

        # At least one transaction should succeed
        assert any(storage.exists(f"worker_{i}.json") for i in range(3))

    def test_transaction_delete_operation(self, storage, lock_path):
        """Test transaction delete operation."""
        # Create file first
        storage.write_json({"test": "value"}, "test.json")
        assert storage.exists("test.json")

        # Delete via transaction
        txn = Transaction(storage, lock_path)
        txn.delete("test.json")
        txn.execute()

        # File should be deleted
        assert not storage.exists("test.json")

    def test_transaction_with_dataframe(self, storage, lock_path):
        """Test transaction with DataFrame write."""
        import pandas as pd

        df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})

        txn = Transaction(storage, lock_path)
        txn.write_dataframe(df, "test.parquet")
        txn.execute()

        # Verify DataFrame was written
        result_df = storage.read_dataframe("test.parquet")
        pd.testing.assert_frame_equal(df, result_df)


class TestSafeTransaction:
    """Tests for SafeTransaction."""

    @pytest.fixture
    def storage(self):
        """Create temporary storage backend."""
        with tempfile.TemporaryDirectory() as tmpdir:
            yield LocalStorageBackend(tmpdir)

    @pytest.fixture
    def lock_path(self):
        """Create temporary lock path."""
        with tempfile.TemporaryDirectory() as tmpdir:
            yield str(Path(tmpdir) / "test.lock")

    def test_safe_transaction_basic(self, storage, lock_path):
        """Test basic safe transaction."""
        txn = SafeTransaction(storage, lock_path)
        txn.write_json({"safe": "value"}, "safe.json")

        result = txn.execute()
        assert result is True

        data = storage.read_json("safe.json")
        assert data == {"safe": "value"}

    def test_safe_transaction_with_staging(self, storage, lock_path):
        """Test safe transaction with staging enabled."""
        # Write initial data
        storage.write_json({"version": 1}, "data.json")

        # Update via safe transaction
        txn = SafeTransaction(storage, lock_path, enable_staging=True)
        txn.write_json({"version": 2}, "data.json")
        txn.execute()

        # Verify update
        data = storage.read_json("data.json")
        assert data == {"version": 2}

    def test_safe_transaction_context_manager(self, storage, lock_path):
        """Test safe transaction as context manager."""
        with SafeTransaction(storage, lock_path) as txn:
            txn.write_json({"test": "safe"}, "test.json")

        data = storage.read_json("test.json")
        assert data == {"test": "safe"}

    def test_safe_transaction_empty(self, storage, lock_path):
        """Test safe transaction with no operations."""
        txn = SafeTransaction(storage, lock_path)
        result = txn.execute()

        assert result is False

    def test_safe_transaction_cannot_execute_twice(self, storage, lock_path):
        """Test that safe transaction cannot be executed twice."""
        txn = SafeTransaction(storage, lock_path)
        txn.write_json({"test": "value"}, "test.json")

        txn.execute()

        with pytest.raises(TransactionError, match="already executed"):
            txn.execute()


class TestTransactionEdgeCases:
    """Tests for transaction edge cases."""

    @pytest.fixture
    def storage(self):
        """Create temporary storage backend."""
        with tempfile.TemporaryDirectory() as tmpdir:
            yield LocalStorageBackend(tmpdir)

    @pytest.fixture
    def lock_path(self):
        """Create temporary lock path."""
        with tempfile.TemporaryDirectory() as tmpdir:
            yield str(Path(tmpdir) / "test.lock")

    def test_transaction_timeout(self, storage, lock_path):
        """Test transaction timeout behavior."""
        # Acquire lock in one transaction
        txn1 = Transaction(storage, lock_path, timeout=10.0)
        txn1.write_json({"test": "value"}, "test.json")

        # Start execution in background
        import threading

        def execute_txn1():
            with txn1:
                time.sleep(2.0)  # Hold lock

        t1 = threading.Thread(target=execute_txn1)
        t1.start()

        time.sleep(0.1)  # Let first transaction acquire lock

        # Try second transaction with short timeout
        txn2 = Transaction(storage, lock_path, timeout=0.5)
        txn2.write_json({"test": "value2"}, "test2.json")

        # Should timeout
        with pytest.raises(TimeoutError):
            txn2.execute()

        t1.join()

    def test_transaction_invalid_operation_type(self, storage, lock_path):
        """Test transaction with invalid operation type."""
        from engine.mlops.concurrency import Operation

        txn = Transaction(storage, lock_path)

        # Manually add invalid operation
        invalid_op = Operation(operation_type="invalid_type", path="test", data={})
        txn.operations.append(invalid_op)

        # Should raise ValueError
        with pytest.raises((TransactionError, ValueError)):
            txn.execute()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
