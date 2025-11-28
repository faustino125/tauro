"""Unit tests for cache module.

Tests the caching layer including LRUCache, TwoLevelCache,
BatchProcessor, and CachedStorage for async batch operations.
"""

import pytest
import time
import threading
from datetime import datetime, timedelta
from unittest.mock import Mock, MagicMock, patch
from typing import Any, Dict

from core.mlops.cache import (
    CacheEntry,
    CacheStats,
    LRUCache,
    TwoLevelCache,
    BatchProcessor,
    BatchOperation,
    BatchResult,
    CacheKeyBuilder,
    CachedStorage,
)


class TestCacheEntry:
    """Tests for CacheEntry dataclass."""

    def test_entry_creation(self):
        """Test creating a cache entry."""
        now = time.time()
        entry = CacheEntry(
            value={"data": "test"}, created_at=now, last_accessed=now, ttl_seconds=300
        )

        assert entry.value["data"] == "test"
        assert entry.ttl_seconds == 300
        assert entry.access_count == 0

    def test_entry_expiration(self):
        """Test cache entry expiration check."""
        # Create an expired entry
        past_time = time.time() - 100
        entry = CacheEntry(
            value="data", created_at=past_time, last_accessed=past_time, ttl_seconds=50
        )

        assert entry.is_expired is True

        # Create a valid entry
        now = time.time()
        valid_entry = CacheEntry(value="data", created_at=now, last_accessed=now, ttl_seconds=300)

        assert valid_entry.is_expired is False

    def test_entry_without_ttl(self):
        """Test entry without TTL (never expires)."""
        past_time = time.time() - 365 * 24 * 3600  # 1 year ago
        entry = CacheEntry(
            value="data", created_at=past_time, last_accessed=past_time, ttl_seconds=None
        )

        assert entry.is_expired is False

    def test_entry_touch(self):
        """Test touch updates access time and count."""
        now = time.time()
        entry = CacheEntry(value="data", created_at=now, last_accessed=now, access_count=0)

        assert entry.access_count == 0
        entry.touch()
        entry.touch()
        assert entry.access_count == 2

    def test_entry_age(self):
        """Test age calculation."""
        past_time = time.time() - 60  # 60 seconds ago
        entry = CacheEntry(
            value="data",
            created_at=past_time,
            last_accessed=past_time,
        )

        assert entry.age >= 60  # Should be at least 60 seconds


class TestCacheStats:
    """Tests for CacheStats dataclass."""

    def test_stats_creation(self):
        """Test creating cache stats."""
        stats = CacheStats(hits=10, misses=5, evictions=2, expirations=1, size=50, max_size=100)

        assert stats.hits == 10
        assert stats.misses == 5
        assert stats.size == 50

    def test_hit_rate_calculation(self):
        """Test hit rate calculation."""
        stats = CacheStats(hits=8, misses=2)

        assert stats.hit_rate == 0.8

    def test_hit_rate_zero_operations(self):
        """Test hit rate with zero operations."""
        stats = CacheStats(hits=0, misses=0)

        assert stats.hit_rate == 0.0

    def test_miss_rate_calculation(self):
        """Test miss rate calculation."""
        stats = CacheStats(hits=8, misses=2)

        assert stats.miss_rate == 0.2

    def test_to_dict(self):
        """Test conversion to dictionary."""
        stats = CacheStats(hits=10, misses=5, evictions=2, expirations=1, size=50, max_size=100)

        result = stats.to_dict()

        assert result["hits"] == 10
        assert result["misses"] == 5
        assert "hit_rate" in result
        assert "miss_rate" in result


class TestLRUCache:
    """Tests for LRUCache class."""

    def test_basic_get_set(self):
        """Test basic get and set operations."""
        cache: LRUCache[str, str] = LRUCache(max_size=100)

        cache.set("key1", "value1")
        result = cache.get("key1")

        assert result == "value1"

    def test_get_missing_key(self):
        """Test getting a non-existent key."""
        cache: LRUCache[str, str] = LRUCache(max_size=100)

        result = cache.get("nonexistent")
        assert result is None

        result = cache.get("nonexistent", default="default_value")
        assert result == "default_value"

    def test_lru_eviction(self):
        """Test LRU eviction when cache is full."""
        cache: LRUCache[str, str] = LRUCache(max_size=3)

        cache.set("key1", "value1")
        cache.set("key2", "value2")
        cache.set("key3", "value3")

        # Access key1 to make it recently used
        cache.get("key1")

        # Add new item, should evict key2 (least recently used)
        cache.set("key4", "value4")

        assert cache.get("key1") == "value1"  # Still there
        assert cache.get("key2") is None  # Evicted
        assert cache.get("key3") == "value3"  # Still there
        assert cache.get("key4") == "value4"  # New item

    def test_ttl_expiration(self):
        """Test TTL-based expiration."""
        cache: LRUCache[str, str] = LRUCache(max_size=100, default_ttl=0.1)  # 100ms TTL

        cache.set("expiring", "value")
        assert cache.get("expiring") == "value"

        time.sleep(0.15)  # Wait for expiration
        assert cache.get("expiring") is None

    def test_custom_ttl_per_entry(self):
        """Test custom TTL for individual entries."""
        cache: LRUCache[str, str] = LRUCache(max_size=100, default_ttl=10)

        cache.set("short_ttl", "value", ttl=0.1)  # 100ms
        cache.set("long_ttl", "value", ttl=10)  # 10s

        time.sleep(0.15)

        assert cache.get("short_ttl") is None  # Expired
        assert cache.get("long_ttl") == "value"  # Still valid

    def test_delete(self):
        """Test deleting cache entries."""
        cache: LRUCache[str, str] = LRUCache(max_size=100)

        cache.set("to_delete", "value")
        assert cache.contains("to_delete") is True

        result = cache.delete("to_delete")
        assert result is True
        assert cache.contains("to_delete") is False

        # Delete non-existent key
        result = cache.delete("nonexistent")
        assert result is False

    def test_clear(self):
        """Test clearing all cache entries."""
        cache: LRUCache[str, str] = LRUCache(max_size=100)

        cache.set("key1", "value1")
        cache.set("key2", "value2")

        cache.clear()

        assert cache.get("key1") is None
        assert cache.get("key2") is None
        assert len(cache) == 0

    def test_contains(self):
        """Test checking key existence."""
        cache: LRUCache[str, str] = LRUCache(max_size=100)

        cache.set("exists", "value")

        assert cache.contains("exists") is True
        assert cache.contains("not_exists") is False

    def test_len(self):
        """Test cache size tracking."""
        cache: LRUCache[str, str] = LRUCache(max_size=100)

        assert len(cache) == 0

        cache.set("key1", "value1")
        cache.set("key2", "value2")

        assert len(cache) == 2

    def test_stats(self):
        """Test cache statistics."""
        cache: LRUCache[str, str] = LRUCache(max_size=100)

        cache.set("key", "value")
        cache.get("key")  # Hit
        cache.get("key")  # Hit
        cache.get("missing")  # Miss

        stats = cache.get_stats()

        assert stats.hits >= 2
        assert stats.misses >= 1
        assert stats.size == 1

    def test_thread_safety(self):
        """Test thread-safe operations."""
        cache: LRUCache[str, str] = LRUCache(max_size=1000)
        errors = []

        def writer():
            try:
                for i in range(100):
                    cache.set(f"key_{threading.current_thread().name}_{i}", f"value_{i}")
            except Exception as e:
                errors.append(e)

        def reader():
            try:
                for i in range(100):
                    cache.get(f"key_{i}")
            except Exception as e:
                errors.append(e)

        threads = []
        for i in range(5):
            threads.append(threading.Thread(target=writer, name=f"writer_{i}"))
            threads.append(threading.Thread(target=reader, name=f"reader_{i}"))

        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len(errors) == 0


class TestCacheKeyBuilder:
    """Tests for CacheKeyBuilder class."""

    def test_basic_key_building(self):
        """Test building a cache key."""
        key = CacheKeyBuilder.build("models", "my_model", "v1")

        assert "models" in key
        assert "my_model" in key
        assert "v1" in key

    def test_key_with_dict(self):
        """Test key building with dictionary params."""
        key = CacheKeyBuilder.build_with_params("query", {"filter": "active", "limit": 10})

        assert "query" in key
        # Dictionary params should be hashed
        assert len(key) > len("query")

    def test_key_consistency(self):
        """Test that same inputs produce same key."""
        key1 = CacheKeyBuilder.build("a", "b", "c")
        key2 = CacheKeyBuilder.build("a", "b", "c")

        assert key1 == key2

    def test_key_uniqueness(self):
        """Test that different inputs produce different keys."""
        key1 = CacheKeyBuilder.build("a", "b", "c")
        key2 = CacheKeyBuilder.build("x", "y", "z")

        assert key1 != key2


class TestBatchProcessor:
    """Tests for BatchProcessor class."""

    def test_batch_accumulation(self):
        """Test that operations are accumulated in batches."""
        executed = []

        def process(operations):
            executed.extend(operations)

        processor = BatchProcessor[str](process_func=process, batch_size=3, flush_interval=60)

        processor.add(BatchOperation(key="k1", value="v1", operation_type="write"))
        processor.add(BatchOperation(key="k2", value="v2", operation_type="write"))

        # Should not flush yet (batch_size=3)
        assert len(executed) == 0

        processor.add(BatchOperation(key="k3", value="v3", operation_type="write"))

        # Should flush now
        assert len(executed) == 3

    def test_manual_flush(self):
        """Test manual flush of pending operations."""
        executed = []

        def process(operations):
            executed.extend(operations)

        processor = BatchProcessor[str](process_func=process, batch_size=10, flush_interval=60)

        processor.add(BatchOperation(key="k1", value="v1", operation_type="write"))
        processor.add(BatchOperation(key="k2", value="v2", operation_type="write"))

        result = processor.flush()

        assert len(executed) == 2
        assert result.success is True
        assert result.processed == 2

    def test_pending_count(self):
        """Test tracking pending operation count."""
        processor = BatchProcessor[str](
            process_func=lambda x: None, batch_size=10, flush_interval=60
        )

        assert processor.pending_count == 0

        processor.add(BatchOperation(key="k1", value="v1", operation_type="write"))
        processor.add(BatchOperation(key="k2", value="v2", operation_type="write"))

        assert processor.pending_count == 2

        processor.flush()

        assert processor.pending_count == 0


class TestTwoLevelCache:
    """Tests for TwoLevelCache class."""

    def test_l1_cache_hit(self):
        """Test L1 cache hit."""
        l1 = LRUCache[str, str](max_size=100)
        l2 = LRUCache[str, str](max_size=1000)

        cache = TwoLevelCache(l1=l1, l2=l2)

        cache.set("key", "value")

        # Should hit L1
        result = cache.get("key")
        assert result == "value"

    def test_l1_miss_l2_hit(self):
        """Test L1 miss but L2 hit."""
        l1 = LRUCache[str, str](max_size=2)
        l2 = LRUCache[str, str](max_size=1000)

        cache = TwoLevelCache(l1=l1, l2=l2)

        # Fill L1 to capacity
        cache.set("key1", "value1")
        cache.set("key2", "value2")
        cache.set("key3", "value3")  # Evicts key1 from L1

        # key1 should be in L2 but not L1
        result = cache.get("key1")
        assert result == "value1"

    def test_both_miss(self):
        """Test miss in both L1 and L2."""
        l1 = LRUCache[str, str](max_size=100)
        l2 = LRUCache[str, str](max_size=1000)

        cache = TwoLevelCache(l1=l1, l2=l2)

        result = cache.get("nonexistent")
        assert result is None

    def test_delete_removes_from_both(self):
        """Test delete removes from both levels."""
        l1 = LRUCache[str, str](max_size=100)
        l2 = LRUCache[str, str](max_size=1000)

        cache = TwoLevelCache(l1=l1, l2=l2)

        cache.set("key", "value")
        cache.delete("key")

        assert cache.get("key") is None


class TestCachedStorage:
    """Tests for CachedStorage class."""

    def test_cached_read(self):
        """Test that reads are cached."""
        mock_storage = Mock()
        mock_storage.read_json.return_value = {"key": "value"}

        cache = LRUCache[str, Dict[str, Any]](max_size=100)
        cached = CachedStorage(storage=mock_storage, cache=cache)

        # First read - should call storage
        result1 = cached.read_json("path/to/file")
        assert mock_storage.read_json.call_count == 1

        # Second read - should use cache
        result2 = cached.read_json("path/to/file")
        assert mock_storage.read_json.call_count == 1  # Still 1

        assert result1 == result2

    def test_write_invalidates_cache(self):
        """Test that writes invalidate cache."""
        mock_storage = Mock()
        mock_storage.read_json.return_value = {"old": "value"}
        mock_storage.write_json.return_value = Mock()

        cache = LRUCache[str, Dict[str, Any]](max_size=100)
        cached = CachedStorage(storage=mock_storage, cache=cache)

        # Load and cache
        cached.read_json("path")

        # Write new data (invalidates cache)
        cached.write_json({"new": "value"}, "path")

        # Read again
        mock_storage.read_json.return_value = {"new": "value"}
        cached.read_json("path")

        # Should have re-read from storage
        assert mock_storage.read_json.call_count == 2

    def test_delete_invalidates_cache(self):
        """Test that delete invalidates cache."""
        mock_storage = Mock()
        mock_storage.read_json.return_value = {"key": "value"}

        cache = LRUCache[str, Dict[str, Any]](max_size=100)
        cached = CachedStorage(storage=mock_storage, cache=cache)

        # Load and cache
        cached.read_json("path")

        # Delete
        cached.delete("path")

        # Cache should be invalidated
        cached.read_json("path")
        assert mock_storage.read_json.call_count == 2

    def test_get_stats(self):
        """Test cache stats retrieval."""
        mock_storage = Mock()
        mock_storage.read_json.return_value = {"key": "value"}

        cache = LRUCache[str, Dict[str, Any]](max_size=100)
        cached = CachedStorage(storage=mock_storage, cache=cache)

        # Generate some activity
        cached.read_json("path1")  # Miss
        cached.read_json("path1")  # Hit
        cached.read_json("path1")  # Hit

        stats = cached.get_cache_stats()

        assert stats.hits >= 2
        assert stats.misses >= 1
