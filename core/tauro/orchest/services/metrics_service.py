"""
Copyright (c) 2025 Faustino Lopez Ramos.
For licensing information, see the LICENSE file in the project root

Metrics aggregation service - centralized metrics collection.

This service is independent of CLI/API and can be consumed by both.
"""

from __future__ import annotations
from typing import Dict, Any, Optional
from datetime import datetime, timedelta, timezone
from threading import RLock

from loguru import logger

from tauro.orchest.store import OrchestratorStore
from tauro.orchest.models import RunState


class MetricsService:
    """
    Centralized service for collecting and aggregating orchestration metrics.

    This service can be consumed independently by CLI or API without coupling.
    """

    def __init__(self, store: Optional[OrchestratorStore] = None):
        """
        Initialize the metrics service.

        Args:
            store: Orchestrator store for querying metrics
        """
        self.store = store or OrchestratorStore()
        self._cache: Dict[str, Any] = {}
        self._cache_expiry: Dict[str, datetime] = {}
        self._lock = RLock()
        self._cache_ttl = 60  # Cache TTL in seconds

        logger.debug("MetricsService initialized")

    def get_pipeline_metrics(
        self,
        pipeline_id: str,
        use_cache: bool = True,
    ) -> Dict[str, Any]:
        """
        Get metrics for a specific pipeline.

        Args:
            pipeline_id: Pipeline identifier
            use_cache: Whether to use cached data

        Returns:
            Dictionary with pipeline metrics
        """
        cache_key = f"pipeline:{pipeline_id}"

        if use_cache:
            cached = self._get_from_cache(cache_key)
            if cached is not None:
                return cached

        try:
            # Get run statistics
            all_runs = self.store.list_pipeline_runs(
                pipeline_id=pipeline_id, limit=1000
            )

            total_runs = len(all_runs)
            successful = sum(1 for r in all_runs if r.state == RunState.SUCCESS)
            failed = sum(1 for r in all_runs if r.state == RunState.FAILED)
            cancelled = sum(1 for r in all_runs if r.state == RunState.CANCELLED)
            running = sum(1 for r in all_runs if r.state == RunState.RUNNING)

            # Calculate success rate
            completed = successful + failed
            success_rate = (successful / completed * 100) if completed > 0 else 0

            # Calculate average execution time
            completed_runs = [r for r in all_runs if r.finished_at and r.started_at]
            if completed_runs:
                execution_times = [
                    (r.finished_at - r.started_at).total_seconds()
                    for r in completed_runs
                ]
                avg_execution_time = sum(execution_times) / len(execution_times)
                max_execution_time = max(execution_times)
                min_execution_time = min(execution_times)
            else:
                avg_execution_time = 0
                max_execution_time = 0
                min_execution_time = 0

            # Recent runs (last 24 hours)
            recent_cutoff = datetime.now(timezone.utc) - timedelta(hours=24)
            recent_runs = [r for r in all_runs if r.created_at >= recent_cutoff]
            recent_success = sum(1 for r in recent_runs if r.state == RunState.SUCCESS)
            recent_failed = sum(1 for r in recent_runs if r.state == RunState.FAILED)
            recent_completed = recent_success + recent_failed
            recent_success_rate = (
                (recent_success / recent_completed * 100) if recent_completed > 0 else 0
            )

            metrics = {
                "pipeline_id": pipeline_id,
                "total_runs": total_runs,
                "successful_runs": successful,
                "failed_runs": failed,
                "cancelled_runs": cancelled,
                "running_runs": running,
                "success_rate": round(success_rate, 2),
                "avg_execution_time_seconds": round(avg_execution_time, 2),
                "max_execution_time_seconds": round(max_execution_time, 2),
                "min_execution_time_seconds": round(min_execution_time, 2),
                "recent_24h": {
                    "total_runs": len(recent_runs),
                    "successful_runs": recent_success,
                    "failed_runs": recent_failed,
                    "success_rate": round(recent_success_rate, 2),
                },
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }

            self._set_in_cache(cache_key, metrics)
            return metrics

        except Exception as e:
            logger.error(f"Error getting pipeline metrics: {e}")
            return {
                "pipeline_id": pipeline_id,
                "error": str(e),
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }

    def get_global_metrics(self, use_cache: bool = True) -> Dict[str, Any]:
        """
        Get global orchestration metrics.

        Args:
            use_cache: Whether to use cached data

        Returns:
            Dictionary with global metrics
        """
        cache_key = "global:metrics"

        if use_cache:
            cached = self._get_from_cache(cache_key)
            if cached is not None:
                return cached

        try:
            # Get database statistics
            db_stats = self.store.get_database_stats()

            # Calculate additional metrics
            pipeline_runs_by_state = db_stats.get("pipeline_runs_by_state", {})
            total_runs = sum(pipeline_runs_by_state.values())

            success_count = pipeline_runs_by_state.get("SUCCESS", 0)
            failed_count = pipeline_runs_by_state.get("FAILED", 0)
            completed = success_count + failed_count
            success_rate = (success_count / completed * 100) if completed > 0 else 0

            # Get active schedules count
            schedules_by_status = db_stats.get("schedules_by_status", {})
            active_schedules = schedules_by_status.get("enabled", 0)

            metrics = {
                "total_runs": total_runs,
                "runs_by_state": pipeline_runs_by_state,
                "success_rate": round(success_rate, 2),
                "active_schedules": active_schedules,
                "schedules_by_status": schedules_by_status,
                "database_size_mb": round(
                    db_stats.get("database_size_bytes", 0) / (1024 * 1024), 2
                ),
                "task_runs_by_state": db_stats.get("task_runs_by_state", {}),
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }

            self._set_in_cache(cache_key, metrics)
            return metrics

        except Exception as e:
            logger.error(f"Error getting global metrics: {e}")
            return {
                "error": str(e),
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }

    def get_schedule_metrics(
        self,
        schedule_id: str,
        use_cache: bool = True,
    ) -> Dict[str, Any]:
        """
        Get metrics for a specific schedule.

        Args:
            schedule_id: Schedule identifier
            use_cache: Whether to use cached data

        Returns:
            Dictionary with schedule metrics
        """
        cache_key = f"schedule:{schedule_id}"

        if use_cache:
            cached = self._get_from_cache(cache_key)
            if cached is not None:
                return cached

        try:
            # Get schedule info
            schedules = self.store.list_schedules()
            schedule = next((s for s in schedules if s.id == schedule_id), None)

            if not schedule:
                return {
                    "schedule_id": schedule_id,
                    "error": "Schedule not found",
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                }

            # Get runs for this schedule's pipeline
            pipeline_runs = self.store.list_pipeline_runs(
                pipeline_id=schedule.pipeline_id,
                limit=500,
            )

            # Calculate metrics
            total_runs = len(pipeline_runs)
            successful = sum(1 for r in pipeline_runs if r.state == RunState.SUCCESS)
            failed = sum(1 for r in pipeline_runs if r.state == RunState.FAILED)

            completed = successful + failed
            success_rate = (successful / completed * 100) if completed > 0 else 0

            # Get dead letter queue entries
            try:
                dlq_entries = self.store.get_schedule_failures(limit=10)
                schedule_failures = [
                    e for e in dlq_entries if e.get("schedule_id") == schedule_id
                ]
            except Exception:
                schedule_failures = []

            metrics = {
                "schedule_id": schedule_id,
                "pipeline_id": schedule.pipeline_id,
                "enabled": schedule.enabled,
                "kind": schedule.kind.value,
                "expression": schedule.expression,
                "next_run_at": schedule.next_run_at.isoformat()
                if schedule.next_run_at
                else None,
                "total_runs": total_runs,
                "successful_runs": successful,
                "failed_runs": failed,
                "success_rate": round(success_rate, 2),
                "recent_failures": len(schedule_failures),
                "max_concurrency": schedule.max_concurrency,
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }

            self._set_in_cache(cache_key, metrics)
            return metrics

        except Exception as e:
            logger.error(f"Error getting schedule metrics: {e}")
            return {
                "schedule_id": schedule_id,
                "error": str(e),
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }

    def clear_cache(self, pattern: Optional[str] = None):
        """
        Clear metrics cache.

        Args:
            pattern: Optional pattern to match cache keys (e.g., "pipeline:*")
        """
        with self._lock:
            if pattern is None:
                self._cache.clear()
                self._cache_expiry.clear()
                logger.debug("Cleared all metrics cache")
            else:
                keys_to_remove = [
                    k for k in self._cache.keys() if self._match_pattern(k, pattern)
                ]
                for key in keys_to_remove:
                    self._cache.pop(key, None)
                    self._cache_expiry.pop(key, None)
                logger.debug(
                    f"Cleared {len(keys_to_remove)} cache entries matching {pattern}"
                )

    def _get_from_cache(self, key: str) -> Optional[Dict[str, Any]]:
        """Get value from cache if not expired."""
        with self._lock:
            if key not in self._cache:
                return None

            expiry = self._cache_expiry.get(key)
            if expiry and datetime.now(timezone.utc) > expiry:
                # Cache expired
                self._cache.pop(key, None)
                self._cache_expiry.pop(key, None)
                return None

            return self._cache.get(key)

    def _set_in_cache(self, key: str, value: Dict[str, Any]):
        """Set value in cache with expiry."""
        with self._lock:
            self._cache[key] = value
            self._cache_expiry[key] = datetime.now(timezone.utc) + timedelta(
                seconds=self._cache_ttl
            )

    def _match_pattern(self, key: str, pattern: str) -> bool:
        """Simple pattern matching for cache keys."""
        if pattern.endswith("*"):
            prefix = pattern[:-1]
            return key.startswith(prefix)
        return key == pattern
