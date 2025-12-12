"""
Tests for ConfigCache TTL and invalidation functionality.

Tests cover:
- Automatic TTL expiration
- Manual invalidation by key
- Bulk invalidation with patterns
- Cache statistics
- Edge cases (empty cache, concurrent access)
"""
import time
import pytest
from pathlib import Path
from core.cli.core import ConfigCache


class TestConfigCacheTTL:
    """Test TTL (time-to-live) functionality."""

    def setup_method(self):
        """Clear cache before each test."""
        ConfigCache.clear()

    def test_cache_get_set_basic(self):
        """Test basic cache get/set operations."""
        key = "test_key"
        configs = [(Path("config.yaml"), "yaml")]

        ConfigCache.set(key, configs)
        result = ConfigCache.get(key)

        assert result == configs
        assert result is not None

    def test_cache_expiration_after_ttl(self):
        """Test that cache entries expire after TTL seconds."""
        key = "expiring_key"
        configs = [(Path("config.yaml"), "yaml")]

        # Store value
        ConfigCache.set(key, configs)
        assert ConfigCache.get(key) is not None

        # Save original TTL
        original_ttl = ConfigCache._EXPIRATION_SECONDS

        try:
            # Temporarily set very short TTL for testing
            ConfigCache._EXPIRATION_SECONDS = 1

            # Wait for expiration
            time.sleep(1.1)

            # Should be expired now
            result = ConfigCache.get(key)
            assert result is None

        finally:
            # Restore original TTL
            ConfigCache._EXPIRATION_SECONDS = original_ttl

    def test_cache_not_expired_before_ttl(self):
        """Test that cache entries are valid before TTL expiration."""
        key = "valid_key"
        configs = [(Path("config.yaml"), "yaml")]

        ConfigCache.set(key, configs)

        # Access immediately
        result = ConfigCache.get(key)
        assert result == configs

    def test_cache_miss_returns_none(self):
        """Test that missing keys return None."""
        result = ConfigCache.get("nonexistent_key")
        assert result is None


class TestConfigCacheInvalidation:
    """Test manual cache invalidation."""

    def setup_method(self):
        """Clear cache before each test."""
        ConfigCache.clear()

    def test_invalidate_single_key(self):
        """Test invalidating a specific cache entry."""
        key = "to_invalidate"
        configs = [(Path("config.yaml"), "yaml")]

        ConfigCache.set(key, configs)
        assert ConfigCache.get(key) is not None

        # Invalidate
        ConfigCache.invalidate(key)

        # Should be gone
        assert ConfigCache.get(key) is None

    def test_invalidate_nonexistent_key(self):
        """Test invalidating a key that doesn't exist (should not error)."""
        # Should not raise an exception
        ConfigCache.invalidate("nonexistent_key")
        assert ConfigCache.get("nonexistent_key") is None

    def test_invalidate_with_pattern(self):
        """Test invalidating multiple entries matching a regex pattern."""
        # Add multiple entries
        ConfigCache.set("dev_config", [(Path("dev.yaml"), "yaml")])
        ConfigCache.set("dev_pipelines", [(Path("pipes.yaml"), "yaml")])
        ConfigCache.set("prod_config", [(Path("prod.yaml"), "yaml")])

        # Invalidate all dev_* entries
        ConfigCache.invalidate_all("^dev_.*")

        # Dev entries should be gone
        assert ConfigCache.get("dev_config") is None
        assert ConfigCache.get("dev_pipelines") is None

        # Prod entry should remain
        assert ConfigCache.get("prod_config") is not None

    def test_invalidate_all_no_pattern(self):
        """Test clearing entire cache without pattern."""
        # Add multiple entries
        ConfigCache.set("key1", [(Path("config.yaml"), "yaml")])
        ConfigCache.set("key2", [(Path("config.yaml"), "yaml")])
        ConfigCache.set("key3", [(Path("config.yaml"), "yaml")])

        # Clear all
        ConfigCache.invalidate_all()

        # All should be gone
        assert ConfigCache.get("key1") is None
        assert ConfigCache.get("key2") is None
        assert ConfigCache.get("key3") is None

    def test_clear_is_alias_for_invalidate_all(self):
        """Test that clear() behaves like invalidate_all()."""
        # Add entries
        ConfigCache.set("key1", [(Path("config.yaml"), "yaml")])
        ConfigCache.set("key2", [(Path("config.yaml"), "yaml")])

        # Clear
        ConfigCache.clear()

        # All should be gone
        assert ConfigCache.get("key1") is None
        assert ConfigCache.get("key2") is None


class TestConfigCacheStats:
    """Test cache statistics and monitoring."""

    def setup_method(self):
        """Clear cache before each test."""
        ConfigCache.clear()

    def test_get_cache_stats_empty(self):
        """Test cache stats on empty cache."""
        stats = ConfigCache.get_cache_stats()

        assert stats["total_entries"] == 0
        assert stats["oldest_age_seconds"] == 0
        assert stats["newest_age_seconds"] == 0
        assert stats["entries"] == []

    def test_get_cache_stats_with_entries(self):
        """Test cache stats with multiple entries."""
        ConfigCache.set("key1", [(Path("config.yaml"), "yaml")])
        time.sleep(0.1)
        ConfigCache.set("key2", [(Path("config.yaml"), "yaml")])

        stats = ConfigCache.get_cache_stats()

        assert stats["total_entries"] == 2
        assert stats["oldest_age_seconds"] >= 0.1
        assert stats["newest_age_seconds"] >= 0
        assert "key1" in stats["entries"]
        assert "key2" in stats["entries"]

    def test_cache_stats_expiration_field(self):
        """Test that stats include expiration time."""
        stats = ConfigCache.get_cache_stats()

        assert "expiration_seconds" in stats
        assert stats["expiration_seconds"] == ConfigCache._EXPIRATION_SECONDS


class TestConfigCacheEdgeCases:
    """Test edge cases and special scenarios."""

    def setup_method(self):
        """Clear cache before each test."""
        ConfigCache.clear()

    def test_cache_with_empty_configs_list(self):
        """Test caching empty configuration lists."""
        key = "empty_key"
        empty_configs = []

        ConfigCache.set(key, empty_configs)
        result = ConfigCache.get(key)

        assert result == []
        assert result is not None

    def test_cache_overwrite_existing_key(self):
        """Test that setting a key overwrites previous value."""
        key = "overwrite_key"

        ConfigCache.set(key, [(Path("old.yaml"), "yaml")])
        ConfigCache.set(key, [(Path("new.yaml"), "yaml")])

        result = ConfigCache.get(key)

        assert len(result) == 1
        assert result[0][0] == Path("new.yaml")

    def test_cache_with_multiple_paths(self):
        """Test caching configurations with multiple file paths."""
        key = "multi_path"
        configs = [
            (Path("config1.yaml"), "yaml"),
            (Path("config2.json"), "json"),
            (Path("config3.dsl"), "dsl"),
        ]

        ConfigCache.set(key, configs)
        result = ConfigCache.get(key)

        assert len(result) == 3
        assert result == configs

    def test_invalidate_pattern_empty_result(self):
        """Test pattern that matches no entries."""
        ConfigCache.set("key1", [(Path("config.yaml"), "yaml")])

        # Pattern that matches nothing
        ConfigCache.invalidate_all("^nonmatching_.*")

        # Original entry should still exist
        assert ConfigCache.get("key1") is not None

    def test_cache_timestamp_updated_on_set(self):
        """Test that timestamps are updated when setting values."""
        key = "timestamp_test"

        ConfigCache.set(key, [(Path("config.yaml"), "yaml")])
        time1 = ConfigCache._timestamps[key]

        time.sleep(0.1)

        ConfigCache.set(key, [(Path("config.yaml"), "yaml")])
        time2 = ConfigCache._timestamps[key]

        assert time2 > time1


class TestConfigCacheIntegration:
    """Integration tests for cache usage scenarios."""

    def setup_method(self):
        """Clear cache before each test."""
        ConfigCache.clear()

    def test_cache_refresh_workflow(self):
        """Test typical cache refresh workflow."""
        # Simulate discovering and caching configs
        key = "discovery_cache"
        configs = [(Path("config.yaml"), "yaml")]

        # Initial discovery
        ConfigCache.set(key, configs)
        assert ConfigCache.get(key) is not None

        # Later, config file changes - invalidate cache
        ConfigCache.invalidate(key)
        assert ConfigCache.get(key) is None

        # Re-discover
        new_configs = [(Path("config_v2.yaml"), "yaml")]
        ConfigCache.set(key, new_configs)
        assert ConfigCache.get(key) == new_configs

    def test_environment_specific_cache_isolation(self):
        """Test that different environments have isolated cache entries."""
        dev_configs = [(Path("dev_config.yaml"), "yaml")]
        prod_configs = [(Path("prod_config.yaml"), "yaml")]

        ConfigCache.set("dev_env", dev_configs)
        ConfigCache.set("prod_env", prod_configs)

        # Both should be accessible
        assert ConfigCache.get("dev_env") == dev_configs
        assert ConfigCache.get("prod_env") == prod_configs

        # Invalidating one shouldn't affect the other
        ConfigCache.invalidate("dev_env")
        assert ConfigCache.get("dev_env") is None
        assert ConfigCache.get("prod_env") == prod_configs

    def test_cache_in_long_running_process(self):
        """Test cache behavior in simulated long-running process."""
        original_ttl = ConfigCache._EXPIRATION_SECONDS

        try:
            # Simulate a long-running process with cache
            ConfigCache._EXPIRATION_SECONDS = 2

            ConfigCache.set("process_cache", [(Path("config.yaml"), "yaml")])

            # First access - should hit cache
            assert ConfigCache.get("process_cache") is not None

            time.sleep(1)

            # Within TTL - should still hit cache
            assert ConfigCache.get("process_cache") is not None

            time.sleep(1.5)

            # After TTL - should miss cache
            assert ConfigCache.get("process_cache") is None

        finally:
            ConfigCache._EXPIRATION_SECONDS = original_ttl
