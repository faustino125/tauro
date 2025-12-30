import atexit
import threading
import weakref
from collections import OrderedDict, deque
from contextlib import contextmanager
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, Generator, List, Optional, Set
from uuid import uuid4

import pandas as pd  # type: ignore
from loguru import logger

from core.mlops.storage import StorageBackend
from core.mlops.concurrency import file_lock
from core.mlops.validators import (
    validate_experiment_name,
    validate_run_name,
    validate_tags,
    validate_description,
)
from core.mlops.exceptions import (
    ExperimentNotFoundError,
    RunNotFoundError,
    RunNotActiveError,
    InvalidMetricError,
    ArtifactNotFoundError,
    ResourceLimitError,
)


DEFAULT_MAX_ACTIVE_RUNS = 100
DEFAULT_METRIC_BUFFER_SIZE = 100
DEFAULT_STALE_RUN_AGE_SECONDS = 3600.0  # 1 hour
METRICS_CLEANUP_THRESHOLD = 10000  # Max metrics per run before cleanup warning
DEFAULT_MAX_METRICS_PER_KEY = 10000  # Max metrics per key in memory (rolling window)
# C4: New constant to limit total metrics across all keys in a run
DEFAULT_MAX_METRICS_PER_RUN = 100000  # Absolute limit per run


class MetricRollingWindow:
    """
    Rolling window container for metrics that auto-evicts oldest when full.
    v2.1+: Prevents OOM by automatically discarding oldest metrics.
    """

    def __init__(self, max_size: int = DEFAULT_MAX_METRICS_PER_KEY):
        """Initialize with max size."""
        self.max_size = max_size
        self.metrics: deque = deque(maxlen=max_size)
        self._evictions = 0

    def add(self, metric: "Metric") -> None:
        """
        Add metric to window, auto-evict oldest if full.
        """
        if len(self.metrics) >= self.max_size:
            self._evictions += 1
            if self._evictions % 1000 == 0:
                logger.warning(
                    f"Metric rolling window evicting old metrics. "
                    f"Total evictions: {self._evictions}. "
                    f"Consider increasing max_size or logging less frequently."
                )

        # Deque with maxlen automatically evicts oldest when appending to full deque
        self.metrics.append(metric)

    def get_all(self) -> List["Metric"]:
        """Get all metrics in window."""
        return list(self.metrics)

    def get_stats(self) -> Dict[str, Any]:
        """Get statistics about this window."""
        return {
            "size": len(self.metrics),
            "max_size": self.max_size,
            "evictions": self._evictions,
            "is_full": len(self.metrics) >= self.max_size,
        }


class MetricIndex:
    """
    Fast index for metrics enabling O(1) lookups.
    v2.1+: Replaces O(n*m*k) search complexity with indexed access.

    Indexes:
    - by_key: {key -> [Metric]} - Get all metrics for a key
    - by_range: {key -> {start_step -> Metric}} - Get metrics by step range
    """

    def __init__(self):
        """Initialize empty indexes."""
        # Map: metric_key -> list of Metric objects
        self.by_key: Dict[str, List["Metric"]] = {}
        # Map: metric_key -> {step -> Metric} for range queries
        self.by_step: Dict[str, Dict[int, "Metric"]] = {}
        self._lock = threading.RLock()

    def index_metric(self, metric: "Metric") -> None:
        """
        Add metric to indexes.

        Args:
            metric: Metric to index
        """
        with self._lock:
            # Index by key
            if metric.key not in self.by_key:
                self.by_key[metric.key] = []
            self.by_key[metric.key].append(metric)

            # Index by step for range queries
            if metric.key not in self.by_step:
                self.by_step[metric.key] = {}
            self.by_step[metric.key][metric.step] = metric

    def get_by_key(self, key: str) -> List["Metric"]:
        """
        Get all metrics for a key (O(1) lookup + list copy).

        Args:
            key: Metric key to retrieve

        Returns:
            List of metrics with this key (copy, safe to modify)
        """
        with self._lock:
            return self.by_key.get(key, []).copy()

    def get_by_step_range(self, key: str, start_step: int, end_step: int) -> List["Metric"]:
        """
        Get metrics within step range (O(n) where n = range size, not total metrics).

        Args:
            key: Metric key
            start_step: Inclusive start step
            end_step: Inclusive end step

        Returns:
            Metrics within range, sorted by step
        """
        with self._lock:
            step_index = self.by_step.get(key, {})
            metrics = [
                step_index[step] for step in range(start_step, end_step + 1) if step in step_index
            ]
            return metrics

    def get_latest(self, key: str) -> Optional["Metric"]:
        """
        Get latest metric for a key (O(1) access to list tail).

        Args:
            key: Metric key

        Returns:
            Latest metric or None if not found
        """
        with self._lock:
            metrics = self.by_key.get(key)
            return metrics[-1] if metrics else None

    def get_stats(self) -> Dict[str, Any]:
        """Get index statistics."""
        with self._lock:
            total_metrics = sum(len(m) for m in self.by_key.values())
            return {
                "indexed_keys": len(self.by_key),
                "total_metrics": total_metrics,
                "avg_metrics_per_key": total_metrics / len(self.by_key) if self.by_key else 0,
            }

    def clear(self) -> None:
        """Clear all indexes."""
        with self._lock:
            self.by_key.clear()
            self.by_step.clear()


class RunStatus(str, Enum):
    """Run execution status."""

    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    SCHEDULED = "SCHEDULED"


@dataclass
class Metric:
    """Metric data point."""

    key: str
    value: float
    timestamp: str
    step: int = 0
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class Run:
    """Experiment run."""

    run_id: str
    experiment_id: str
    name: str
    status: RunStatus
    created_at: str
    updated_at: str
    start_time: Optional[str] = None
    end_time: Optional[str] = None
    duration_seconds: Optional[float] = None
    parameters: Dict[str, Any] = field(default_factory=dict)
    metrics: Dict[str, List[Metric]] = field(default_factory=dict)
    artifacts: List[str] = field(default_factory=list)
    tags: Dict[str, str] = field(default_factory=dict)
    parent_run_id: Optional[str] = None
    notes: str = ""
    # v2.1+: Fast index for metric lookups (not serialized)
    metric_index: MetricIndex = field(default_factory=MetricIndex, init=False, repr=False)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        d = asdict(self)
        d["status"] = self.status.value
        # v2.1+: Handle MetricRollingWindow serialization
        d["metrics"] = {}
        for key, metrics_container in self.metrics.items():
            if isinstance(metrics_container, MetricRollingWindow):
                d["metrics"][key] = [m.to_dict() for m in metrics_container.get_all()]
            else:
                # Backward compatibility with old list format
                d["metrics"][key] = [
                    m.to_dict() if isinstance(m, Metric) else m for m in metrics_container
                ]
        return d

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Run":
        """Create from dictionary."""
        data["status"] = RunStatus(data.get("status", "RUNNING"))
        # v2.1+: Restore MetricRollingWindow from serialized data
        if "metrics" in data:
            metrics_dict = {}
            for key, metrics_list in data["metrics"].items():
                window = MetricRollingWindow()
                for m_dict in metrics_list:
                    metric = Metric(**m_dict) if isinstance(m_dict, dict) else m_dict
                    window.add(metric)
                metrics_dict[key] = window
            data["metrics"] = metrics_dict
        return cls(**data)


@dataclass
class Experiment:
    """Experiment container."""

    experiment_id: str
    name: str
    description: str
    created_at: str
    updated_at: str
    tags: Dict[str, str] = field(default_factory=dict)
    artifact_location: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Experiment":
        return cls(**data)


class ExperimentTracker:
    """
    Experiment Tracking system for logging metrics, parameters, and artifacts.
    """

    # Class-level registry for cleanup on exit
    _instances: Set[weakref.ref] = set()
    _instances_lock = threading.Lock()

    def __init__(
        self,
        storage: StorageBackend,
        tracking_path: str = "experiment_tracking",
        metric_buffer_size: int = DEFAULT_METRIC_BUFFER_SIZE,
        auto_flush_metrics: bool = True,
        max_active_runs: int = DEFAULT_MAX_ACTIVE_RUNS,
        auto_cleanup_stale: bool = True,
        stale_run_age_seconds: float = DEFAULT_STALE_RUN_AGE_SECONDS,
        max_metrics_per_key: int = DEFAULT_MAX_METRICS_PER_KEY,
        max_metrics_per_run: int = DEFAULT_MAX_METRICS_PER_RUN,
    ):
        """
        Initialize Experiment Tracker.
        C3, C4: Metric buffering with persistence and limits.
        """
        self.storage = storage
        self.tracking_path = tracking_path
        self.metric_buffer_size = metric_buffer_size
        self.auto_flush_metrics = auto_flush_metrics
        self.max_active_runs = max_active_runs
        self.auto_cleanup_stale = auto_cleanup_stale
        self.stale_run_age_seconds = stale_run_age_seconds
        self.max_metrics_per_key = max_metrics_per_key
        self.max_metrics_per_run = max_metrics_per_run

        # Thread-safe active runs with LRU behavior
        self._active_runs: OrderedDict[str, Run] = OrderedDict()
        # C3: Track metric counts for validation
        self._metric_counts: Dict[str, int] = {}
        # C3: Store pending metrics for each run (to flush periodically)
        self._pending_metrics: Dict[str, deque] = {}
        self._runs_lock = threading.RLock()

        # Statistics
        self._total_runs_started = 0
        self._total_runs_completed = 0
        self._total_runs_failed = 0
        self._cleanup_count = 0

        # Flag to track if structure has been ensured (lazy initialization)
        self._structure_ensured = False

        # Register for cleanup on exit
        self._register_instance()

        logger.info(
            f"ExperimentTracker initialized at {tracking_path} "
            f"(max_active_runs={max_active_runs})"
        )

    def _register_instance(self) -> None:
        """Register this instance for cleanup on exit."""
        with ExperimentTracker._instances_lock:
            ref = weakref.ref(self, ExperimentTracker._cleanup_ref)
            ExperimentTracker._instances.add(ref)

        # Register cleanup only once
        if not hasattr(ExperimentTracker, "_atexit_registered"):
            atexit.register(ExperimentTracker._cleanup_all_instances)
            ExperimentTracker._atexit_registered = True

    @staticmethod
    def _cleanup_ref(ref: weakref.ref) -> None:
        """Callback when instance is garbage collected."""
        with ExperimentTracker._instances_lock:
            ExperimentTracker._instances.discard(ref)

    @staticmethod
    def _cleanup_all_instances() -> None:
        """Clean up all tracker instances on process exit."""
        with ExperimentTracker._instances_lock:
            for ref in ExperimentTracker._instances.copy():
                tracker = ref()
                if tracker is not None:
                    try:
                        tracker._cleanup_active_runs()
                    except Exception as e:
                        logger.warning(f"Error cleaning up tracker: {e}")

    def _cleanup_active_runs(self) -> None:
        """Clean up all active runs on shutdown."""
        with self._runs_lock:
            for run_id in tuple(self._active_runs.keys()):
                try:
                    self.end_run(run_id, RunStatus.FAILED)
                    logger.warning(f"Auto-closed orphaned run: {run_id}")
                except Exception as e:
                    logger.warning(f"Failed to close run {run_id}: {e}")

    def _ensure_tracking_structure(self) -> None:
        """
        Ensure tracking directory structure exists (lazy initialization).
        This is only called when actually creating an experiment or run.
        """
        if self._structure_ensured:
            return

        paths = [
            f"{self.tracking_path}/experiments",
            f"{self.tracking_path}/runs",
            f"{self.tracking_path}/artifacts",
            f"{self.tracking_path}/metrics",
        ]
        for path in paths:
            if not self.storage.exists(path):
                try:
                    self.storage.write_json(
                        {"created": datetime.now(timezone.utc).isoformat()},
                        f"{path}/.tracking_marker.json",
                        mode="overwrite",
                    )
                except Exception:
                    pass

        self._structure_ensured = True
        logger.debug(f"Tracking structure ensured at {self.tracking_path}")

    def create_experiment(
        self,
        name: str,
        description: str = "",
        tags: Optional[Dict[str, str]] = None,
    ) -> Experiment:
        """
        Create new experiment.
        """
        # Ensure tracking structure exists before creating experiment
        self._ensure_tracking_structure()

        # Validate inputs
        name = validate_experiment_name(name)
        description = validate_description(description)
        tags = validate_tags(tags)

        experiment_id = str(uuid4())
        now = datetime.now(timezone.utc).isoformat()

        experiment = Experiment(
            experiment_id=experiment_id,
            name=name,
            description=description,
            created_at=now,
            updated_at=now,
            tags=tags or {},
            artifact_location=f"{self.tracking_path}/artifacts/{experiment_id}",
        )

        # Store experiment metadata
        exp_path = f"{self.tracking_path}/experiments/{experiment_id}.json"
        self.storage.write_json(experiment.to_dict(), exp_path, mode="overwrite")

        # Update experiments index
        self._update_experiments_index(experiment)

        logger.info(f"Created experiment {name} (ID: {experiment_id})")
        return experiment

    def start_run(
        self,
        experiment_id: str,
        name: str = "",
        parameters: Optional[Dict[str, Any]] = None,
        tags: Optional[Dict[str, str]] = None,
        parent_run_id: Optional[str] = None,
    ) -> Run:
        """
        Start new experiment run with memory protection.
        """
        # Ensure tracking structure exists before starting run
        self._ensure_tracking_structure()

        # Verify experiment exists
        self._get_experiment(experiment_id)

        # Validate inputs
        if name:
            name = validate_run_name(name)
        tags = validate_tags(tags)

        with self._runs_lock:
            # Check memory limits and cleanup if needed
            self._enforce_run_limits()

            run_id = str(uuid4())
            now = datetime.now(timezone.utc).isoformat()

            run = Run(
                run_id=run_id,
                experiment_id=experiment_id,
                name=name or f"run-{run_id[:8]}",
                status=RunStatus.RUNNING,
                created_at=now,
                updated_at=now,
                start_time=now,
                parameters=parameters or {},
                tags=tags or {},
                parent_run_id=parent_run_id,
            )

            self._active_runs[run_id] = run
            self._total_runs_started += 1

        logger.info(f"Started run {run.name} (ID: {run_id})")
        return run

    def _enforce_run_limits(self) -> None:
        """Enforce maximum active runs limit with cleanup."""
        # First, try to clean up stale runs
        if self.auto_cleanup_stale and len(self._active_runs) >= self.max_active_runs:
            cleaned = self._cleanup_stale_runs_internal()
            if cleaned > 0:
                logger.info(f"Cleaned {cleaned} stale runs to make room")

        # If still at limit, fail
        if len(self._active_runs) >= self.max_active_runs:
            raise RuntimeError(
                f"Maximum active runs limit reached ({self.max_active_runs}). "
                f"End existing runs or increase max_active_runs."
            )

    def _cleanup_stale_runs_internal(self) -> int:
        """
        Internal stale run cleanup without lock (caller must hold lock).
        """
        now = datetime.now(timezone.utc)
        cleaned = 0

        stale_run_ids = [
            run_id
            for run_id, run in self._active_runs.items()
            if self._is_run_stale(run, now, self.stale_run_age_seconds)
        ]

        for run_id in stale_run_ids:
            try:
                # End run without lock (we already have it)
                self._end_run_internal(run_id, RunStatus.FAILED)
                cleaned += 1
                self._cleanup_count += 1
            except Exception as e:
                logger.warning(f"Failed to cleanup stale run {run_id}: {e}")

        return cleaned

    def _end_run_internal(self, run_id: str, status: RunStatus) -> Optional[Run]:
        """End run without acquiring lock (for internal use)."""
        if run_id not in self._active_runs:
            return None

        run = self._active_runs[run_id]
        now = datetime.now(timezone.utc).isoformat()

        run.status = status
        run.updated_at = now
        run.end_time = now

        if run.start_time:
            start = datetime.fromisoformat(run.start_time)
            end = datetime.fromisoformat(now)
            run.duration_seconds = (end - start).total_seconds()

        # Persist run to storage
        try:
            run_path = f"{self.tracking_path}/runs/{run.experiment_id}/{run_id}.json"
            self.storage.write_json(run.to_dict(), run_path, mode="overwrite")
            self._update_runs_index(run)
        except Exception as e:
            logger.error(f"Failed to persist run {run_id}: {e}")

        # Remove from active runs
        del self._active_runs[run_id]
        self._metric_counts.pop(run_id, None)

        if status == RunStatus.COMPLETED:
            self._total_runs_completed += 1
        else:
            self._total_runs_failed += 1

        return run

    def log_metric(
        self,
        run_id: str,
        key: str,
        value: float,
        step: int = 0,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        """
        Log metric for run with memory protection and rolling window limits.
        v2.1+: Automatic rolling window eviction when limits exceeded.

        Args:
            run_id: ID of the run
            key: Metric key
            value: Metric value (must be int or float)
            step: Training step/epoch
            metadata: Optional metadata

        Raises:
            InvalidMetricError: If value is invalid (NaN, Inf)
            RunNotFoundError: If run doesn't exist
        """
        import math

        # Validate metric value type
        if not isinstance(value, (int, float)):
            raise InvalidMetricError(
                key, value, f"Expected int or float, got {type(value).__name__}"
            )

        # Check for NaN or Inf
        if isinstance(value, float):
            if math.isnan(value):
                raise InvalidMetricError(key, value, "Value is NaN")
            if math.isinf(value):
                raise InvalidMetricError(key, value, "Value is infinite")

        with self._runs_lock:
            run = self._get_active_run(run_id)

            now = datetime.now(timezone.utc).isoformat()

            metric = Metric(
                key=key,
                value=value,
                timestamp=now,
                step=step,
                metadata=metadata or {},
            )

            # v2.1+: Use MetricRollingWindow for automatic eviction
            if key not in run.metrics:
                run.metrics[key] = MetricRollingWindow(max_size=self.max_metrics_per_key)

            # Add metric (auto-evicts oldest if window is full)
            run.metrics[key].add(metric)

            # v2.1+: Index metric for O(1) lookups
            run.metric_index.index_metric(metric)

            # Check total metrics across all keys
            total_metrics = sum(
                len(window.metrics)
                for window in run.metrics.values()
                if isinstance(window, MetricRollingWindow)
            )

            # Warn if approaching total limit
            if total_metrics > self.max_metrics_per_run * 0.8:
                logger.warning(
                    f"Run {run_id}: High metric count ({total_metrics}/{self.max_metrics_per_run}). "
                    f"Consider logging less frequently or flushing metrics."
                )

            # Warn if key window is full (heavy logging)
            if run.metrics[key].get_stats()["is_full"]:
                logger.debug(
                    f"Run {run_id}: Metric key '{key}' rolling window full "
                    f"(evictions: {run.metrics[key]._evictions})"
                )

            logger.debug(f"Run {run_id}: Logged metric {key}={value} (step {step})")

            # Incremental persistence: flush if buffer is full
            if run_id not in self._metric_counts:
                self._metric_counts[run_id] = 0
            self._metric_counts[run_id] += 1

            if self.auto_flush_metrics and self._metric_counts[run_id] >= self.metric_buffer_size:
                self._flush_metrics(run_id)
                self._metric_counts[run_id] = 0

    def log_parameter(
        self,
        run_id: str,
        key: str,
        value: Any,
    ) -> None:
        """
        Log hyperparameter for run.
        """
        run = self._get_active_run(run_id)
        run.parameters[key] = value
        logger.debug(f"Run {run_id}: Logged parameter {key}={value}")

    def log_artifact(
        self,
        run_id: str,
        artifact_path: str,
        destination: str = "",
    ) -> str:
        """
        Log artifact for run.
        """
        # Verify artifact exists before logging
        from pathlib import Path

        artifact_file = Path(artifact_path)
        if not artifact_file.exists():
            raise ArtifactNotFoundError(artifact_path)

        run = self._get_active_run(run_id)

        if not destination:
            # Auto-generate destination
            destination = f"artifacts/{run_id}"

        full_destination = f"{self.tracking_path}/{destination}"

        artifact_metadata = self.storage.write_artifact(
            artifact_path, full_destination, mode="overwrite"
        )

        run.artifacts.append(artifact_metadata.path)
        logger.info(f"Run {run_id}: Logged artifact to {full_destination}")
        return artifact_metadata.path

    def set_tag(self, run_id: str, key: str, value: str) -> None:
        """Set tag on run."""
        run = self._get_active_run(run_id)
        run.tags[key] = value
        logger.debug(f"Run {run_id}: Set tag {key}={value}")

    def set_note(self, run_id: str, note: str) -> None:
        """Add note to run."""
        run = self._get_active_run(run_id)
        run.notes = note
        logger.debug(f"Run {run_id}: Set note")

    def end_run(self, run_id: str, status: RunStatus = RunStatus.COMPLETED) -> Run:
        """
        End run and persist to storage.
        """
        with self._runs_lock:
            run = self._get_active_run(run_id)

            now = datetime.now(timezone.utc).isoformat()
            run.status = status
            run.updated_at = now
            run.end_time = now

            if run.start_time:
                start = datetime.fromisoformat(run.start_time)
                end = datetime.fromisoformat(now)
                run.duration_seconds = (end - start).total_seconds()

            # Persist run to storage
            run_path = f"{self.tracking_path}/runs/{run.experiment_id}/{run_id}.json"
            self.storage.write_json(run.to_dict(), run_path, mode="overwrite")

            # Update runs index
            self._update_runs_index(run)

            # Remove from active runs
            del self._active_runs[run_id]
            self._metric_counts.pop(run_id, None)

            # Update statistics
            if status == RunStatus.COMPLETED:
                self._total_runs_completed += 1
            else:
                self._total_runs_failed += 1

            logger.info(f"Ended run {run.name} (ID: {run_id}) with status {status.value}")
            return run

    def get_run(self, run_id: str) -> Run:
        """
        Get run by ID.
        """
        if run_id in self._active_runs:
            return self._active_runs[run_id]

        # Search in storage
        runs_df = self._load_runs_index()
        run_rows = runs_df[runs_df["run_id"] == run_id]

        if run_rows.empty:
            raise RunNotFoundError(run_id)

        row = run_rows.iloc[0]
        run_path = f"{self.tracking_path}/runs/{row['experiment_id']}/{run_id}.json"
        data = self.storage.read_json(run_path)
        return Run.from_dict(data)

    def list_runs(
        self,
        experiment_id: str,
        status_filter: Optional[RunStatus] = None,
        tag_filter: Optional[Dict[str, str]] = None,
    ) -> List[Dict[str, Any]]:
        """
        List runs in experiment.
        """
        runs_df = self._load_runs_index()
        runs = runs_df[runs_df["experiment_id"] == experiment_id]

        if runs.empty:
            return []

        if status_filter:
            runs = runs[runs["status"] == status_filter.value]

        result = []
        for _, row in runs.iterrows():
            try:
                run = self.get_run(row["run_id"])

                if tag_filter and not all(run.tags.get(k) == v for k, v in tag_filter.items()):
                    continue

                result.append(
                    {
                        "run_id": run.run_id,
                        "name": run.name,
                        "status": run.status.value,
                        "created_at": run.created_at,
                        "duration_seconds": run.duration_seconds,
                        "parameters": run.parameters,
                        "metric_keys": list(run.metrics.keys()),
                    }
                )
            except Exception as e:
                logger.warning(f"Could not load run {row['run_id']}: {e}")
                continue

        return result

    def compare_runs(self, run_ids: List[str]) -> pd.DataFrame:
        """
        Compare multiple runs as DataFrame.
        """
        rows = []
        for run_id in run_ids:
            run = self.get_run(run_id)
            row = {
                "run_id": run.run_id,
                "name": run.name,
                "status": run.status.value,
                "duration_seconds": run.duration_seconds,
            }
            row.update({"param_" + k: v for k, v in run.parameters.items()})
            row.update(
                {"metric_" + k: run.metrics[k][-1].value for k in run.metrics if run.metrics[k]}
            )
            rows.append(row)

        return pd.DataFrame(rows)

    def _compare(self, value: float, op: str, threshold: float) -> bool:
        ops = {
            ">": lambda a, b: a > b,
            "<": lambda a, b: a < b,
            ">=": lambda a, b: a >= b,
            "<=": lambda a, b: a <= b,
            "==": lambda a, b: a == b,
        }
        try:
            return ops[op](value, threshold)
        except KeyError:
            raise ValueError(f"Unsupported operator: {op}")

    def _metric_ok(self, run: Run, metric_name: str, op: str, threshold: float) -> bool:
        metrics = run.metrics.get(metric_name)
        if not metrics:
            return False
        latest_value = metrics[-1].value
        return self._compare(latest_value, op, threshold)

    def search_runs(
        self,
        experiment_id: str,
        metric_filter: Optional[Dict[str, tuple]] = None,
    ) -> List[str]:
        """
        Search runs by metric thresholds using optimized metrics index.
        """
        if not metric_filter:
            runs = self.list_runs(experiment_id)
            return [r["run_id"] for r in runs]

        # Try to use metrics index for faster search
        try:
            return self._search_runs_optimized(experiment_id, metric_filter)
        except Exception as e:
            logger.debug(f"Falling back to full scan search: {e}")
            # Fallback to full scan
            return self._search_runs_full_scan(experiment_id, metric_filter)

    def _search_runs_optimized(
        self, experiment_id: str, metric_filter: Dict[str, tuple]
    ) -> List[str]:
        """Search using metrics index (fast path)."""
        metrics_index_path = f"{self.tracking_path}/metrics/index.parquet"
        metrics_df = self.storage.read_dataframe(metrics_index_path)

        # Filter by experiment
        metrics_df = metrics_df[metrics_df["experiment_id"] == experiment_id]

        # Apply metric filters
        for metric_name, (op, threshold) in metric_filter.items():
            col_name = f"metric_{metric_name}"
            if col_name not in metrics_df.columns:
                # Metric not in index, fallback to full scan
                raise ValueError(f"Metric {metric_name} not in index")

            if op == ">":
                metrics_df = metrics_df[metrics_df[col_name] > threshold]
            elif op == "<":
                metrics_df = metrics_df[metrics_df[col_name] < threshold]
            elif op == ">=":
                metrics_df = metrics_df[metrics_df[col_name] >= threshold]
            elif op == "<=":
                metrics_df = metrics_df[metrics_df[col_name] <= threshold]
            elif op == "==":
                metrics_df = metrics_df[metrics_df[col_name] == threshold]
            else:
                raise ValueError(f"Unsupported operator: {op}")

        return metrics_df["run_id"].tolist()

    def _search_runs_full_scan(
        self, experiment_id: str, metric_filter: Dict[str, tuple]
    ) -> List[str]:
        """Search using full scan (fallback)."""
        runs = self.list_runs(experiment_id)
        matching_runs: List[str] = []

        for run_summary in runs:
            run_id = run_summary["run_id"]
            run = self.get_run(run_id)

            try:
                passed = True
                for metric_name, (op, threshold) in metric_filter.items():
                    if not self._metric_ok(run, metric_name, op, threshold):
                        passed = False
                        break
                if passed:
                    matching_runs.append(run_id)
            except ValueError:
                continue

        return matching_runs

    def download_artifact(
        self,
        run_id: str,
        artifact_path: str,
        local_destination: str,
    ) -> None:
        """
        Download artifact from run.
        """
        self.storage.read_artifact(artifact_path, local_destination)
        logger.info(f"Downloaded artifact from run {run_id} to {local_destination}")

    def _flush_metrics(self, run_id: str) -> None:
        """
        Flush metrics for a run to storage (incremental persistence).
        """
        if run_id not in self._active_runs:
            return

        run = self._active_runs[run_id]

        try:
            # Store current metrics snapshot
            metrics_path = f"{self.tracking_path}/metrics/{run.experiment_id}/{run_id}_metrics.json"
            metrics_data = {
                key: [m.to_dict() for m in metrics] for key, metrics in run.metrics.items()
            }
            self.storage.write_json(metrics_data, metrics_path, mode="overwrite")
            logger.debug(f"Flushed metrics for run {run_id}")
        except Exception as e:
            logger.warning(f"Could not flush metrics for run {run_id}: {e}")

    def _get_active_run(self, run_id: str) -> Run:
        """Get active run, raise if not found."""
        if run_id not in self._active_runs:
            raise RunNotActiveError(run_id)
        return self._active_runs[run_id]

    def _get_experiment(self, experiment_id: str) -> Experiment:
        """Get experiment by ID."""
        try:
            exp_path = f"{self.tracking_path}/experiments/{experiment_id}.json"
            data = self.storage.read_json(exp_path)
            return Experiment.from_dict(data)
        except FileNotFoundError:
            raise ExperimentNotFoundError(experiment_id)

    def _load_experiments_index(self) -> pd.DataFrame:
        """Load experiments index."""
        try:
            index_path = f"{self.tracking_path}/experiments/index.parquet"
            return self.storage.read_dataframe(index_path)
        except FileNotFoundError:
            return pd.DataFrame(columns=["experiment_id", "name", "created_at"])

    def _update_experiments_index(self, experiment: Experiment) -> None:
        """Update experiments index with file locking."""
        lock_path = f"{self.tracking_path}/experiments/.index.lock"

        with file_lock(lock_path, timeout=30.0):
            df = self._load_experiments_index()

            new_row = pd.DataFrame(
                [
                    {
                        "experiment_id": experiment.experiment_id,
                        "name": experiment.name,
                        "created_at": experiment.created_at,
                    }
                ]
            )

            df = pd.concat([df, new_row], ignore_index=True)
            df = df.drop_duplicates(subset=["experiment_id"], keep="last")

            index_path = f"{self.tracking_path}/experiments/index.parquet"
            self.storage.write_dataframe(df, index_path, mode="overwrite")

    def _load_runs_index(self) -> pd.DataFrame:
        """Load runs index."""
        try:
            index_path = f"{self.tracking_path}/runs/index.parquet"
            return self.storage.read_dataframe(index_path)
        except FileNotFoundError:
            return pd.DataFrame(columns=["run_id", "experiment_id", "status", "created_at"])

    def _update_runs_index(self, run: Run) -> None:
        """Update runs index with file locking and create metrics index."""
        lock_path = f"{self.tracking_path}/runs/.index.lock"

        with file_lock(lock_path, timeout=30.0):
            df = self._load_runs_index()

            new_row = pd.DataFrame(
                [
                    {
                        "run_id": run.run_id,
                        "experiment_id": run.experiment_id,
                        "status": run.status.value,
                        "created_at": run.created_at,
                    }
                ]
            )

            df = pd.concat([df, new_row], ignore_index=True)
            df = df.drop_duplicates(subset=["run_id"], keep="last")

            index_path = f"{self.tracking_path}/runs/index.parquet"
            self.storage.write_dataframe(df, index_path, mode="overwrite")

            # Create metrics index for faster search
            self._update_metrics_index(run)

    def _update_metrics_index(self, run: Run) -> None:
        """
        Update metrics index for faster search.
        """
        try:
            metrics_index_path = f"{self.tracking_path}/metrics/index.parquet"

            # Load existing index
            try:
                metrics_df = self.storage.read_dataframe(metrics_index_path)
            except FileNotFoundError:
                metrics_df = pd.DataFrame(columns=["run_id", "experiment_id"])

            # Remove old entry for this run
            metrics_df = metrics_df[metrics_df["run_id"] != run.run_id]

            # Create new row with latest metrics
            row_data = {
                "run_id": run.run_id,
                "experiment_id": run.experiment_id,
            }

            # Add latest value for each metric
            for metric_name, metric_list in run.metrics.items():
                if metric_list:
                    latest_value = metric_list[-1].value
                    row_data[f"metric_{metric_name}"] = latest_value

            new_row = pd.DataFrame([row_data])
            metrics_df = pd.concat([metrics_df, new_row], ignore_index=True)

            # Write back
            self.storage.write_dataframe(metrics_df, metrics_index_path, mode="overwrite")
            logger.debug(f"Updated metrics index for run {run.run_id}")

        except Exception as e:
            logger.warning(f"Could not update metrics index: {e}")

    # =========================================================================
    # Cleanup and Context Manager Methods
    # =========================================================================

    def _is_run_stale(self, run, now: datetime, max_age_seconds: float) -> bool:
        """
        Determine if a run is stale; returns True when run should be cleaned up.
        """
        if not run.start_time:
            return False

        try:
            start_time = datetime.fromisoformat(run.start_time)
            # Handle timezone-naive datetimes
            if start_time.tzinfo is None:
                start_time = start_time.replace(tzinfo=timezone.utc)

            age_seconds = (now - start_time).total_seconds()
            if age_seconds > max_age_seconds:
                logger.warning(
                    f"Run {run.run_id if hasattr(run, 'run_id') else 'unknown'} "
                    f"({getattr(run, 'name', '')}) has been active for {age_seconds:.1f}s, marking as stale"
                )
                return True

            return False

        except (ValueError, TypeError) as e:
            logger.warning(
                f"Could not parse start_time for run {getattr(run, 'run_id', 'unknown')}: {e}. "
                f"Run will NOT be marked as stale to prevent accidental cleanup."
            )
            # Do NOT treat parsing failures as stale - this could cause data loss
            return False

    def cleanup_stale_runs(
        self,
        max_age_seconds: float = 3600.0,
        status: RunStatus = RunStatus.FAILED,
    ) -> int:
        """
        Clean up runs that have been active for too long without completion.
        """
        with self._runs_lock:
            now = datetime.now(timezone.utc)
            cleaned_count = 0

            # Identify stale runs first (avoid modifying dict during iteration)
            stale_run_ids = [
                run_id
                for run_id, run in self._active_runs.items()
                if self._is_run_stale(run, now, max_age_seconds)
            ]

            # Clean up stale runs
            for run_id in stale_run_ids:
                try:
                    self._end_run_internal(run_id, status)
                    cleaned_count += 1
                    self._cleanup_count += 1
                    logger.info(f"Cleaned up stale run {run_id}")
                except Exception as e:
                    logger.error(f"Failed to clean up stale run {run_id}: {e}")

            if cleaned_count > 0:
                logger.info(f"Cleaned up {cleaned_count} stale run(s)")

            return cleaned_count

    def get_active_runs_count(self) -> int:
        """
        Get the number of currently active runs.
        """
        with self._runs_lock:
            return len(self._active_runs)

    def list_active_runs(self) -> List[Dict[str, Any]]:
        """
        List all currently active runs with their metadata.
        """
        with self._runs_lock:
            result = []
            now = datetime.now(timezone.utc)

            for run_id, run in self._active_runs.items():
                age_seconds = None
                if run.start_time:
                    try:
                        start_time = datetime.fromisoformat(run.start_time)
                        if start_time.tzinfo is None:
                            start_time = start_time.replace(tzinfo=timezone.utc)
                        age_seconds = (now - start_time).total_seconds()
                    except (ValueError, TypeError):
                        pass

                result.append(
                    {
                        "run_id": run_id,
                        "name": run.name,
                        "experiment_id": run.experiment_id,
                        "start_time": run.start_time,
                        "age_seconds": age_seconds,
                        "metrics_count": sum(len(m) for m in run.metrics.values()),
                        "parameters_count": len(run.parameters),
                    }
                )

            return result

    def search_metrics_by_key(self, run_id: str, key: str) -> List[Metric]:
        """
        Search metrics by key (O(1) lookup using index).
        v2.1+: Fast metric retrieval using MetricIndex.

        Args:
            run_id: ID of the run
            key: Metric key to search for

        Returns:
            List of metrics matching the key

        Raises:
            RunNotFoundError: If run doesn't exist
        """
        with self._runs_lock:
            run = self._get_active_run(run_id)
            return run.metric_index.get_by_key(key)

    def search_metrics_by_step_range(
        self,
        run_id: str,
        key: str,
        start_step: int,
        end_step: int,
    ) -> List[Metric]:
        """
        Search metrics within step range (O(n) where n = range size).
        v2.1+: Efficient range queries using MetricIndex.

        Args:
            run_id: ID of the run
            key: Metric key
            start_step: Inclusive start step
            end_step: Inclusive end step

        Returns:
            Metrics within step range, sorted by step

        Raises:
            RunNotFoundError: If run doesn't exist
        """
        with self._runs_lock:
            run = self._get_active_run(run_id)
            return run.metric_index.get_by_step_range(key, start_step, end_step)

    def get_latest_metric(
        self,
        run_id: str,
        key: str,
    ) -> Optional[Metric]:
        """
        Get latest metric for a key (O(1) lookup).
        v2.1+: Fast access to most recent metric value.

        Args:
            run_id: ID of the run
            key: Metric key

        Returns:
            Latest metric or None if not found

        Raises:
            RunNotFoundError: If run doesn't exist
        """
        with self._runs_lock:
            run = self._get_active_run(run_id)
            return run.metric_index.get_latest(key)

    def get_stats(self) -> Dict[str, Any]:
        """
        Get tracker statistics.

        Returns:
            Dictionary with tracker statistics
        """
        with self._runs_lock:
            return {
                "active_runs": len(self._active_runs),
                "max_active_runs": self.max_active_runs,
                "total_runs_started": self._total_runs_started,
                "total_runs_completed": self._total_runs_completed,
                "total_runs_failed": self._total_runs_failed,
                "cleanup_count": self._cleanup_count,
                "tracking_path": self.tracking_path,
            }

    @contextmanager
    def run_context(
        self,
        experiment_id: str,
        name: str = "",
        parameters: Optional[Dict[str, Any]] = None,
        tags: Optional[Dict[str, str]] = None,
        parent_run_id: Optional[str] = None,
        fail_on_error: bool = True,
    ) -> Generator[Run, None, None]:
        """
        Context manager for experiment runs with automatic cleanup.
        """
        run = self.start_run(
            experiment_id=experiment_id,
            name=name,
            parameters=parameters,
            tags=tags,
            parent_run_id=parent_run_id,
        )

        try:
            yield run
            # Success - end with COMPLETED status
            self.end_run(run.run_id, RunStatus.COMPLETED)
            logger.debug(f"Run context completed successfully: {run.run_id}")

        except Exception as e:
            # Error - end with FAILED status (or COMPLETED if fail_on_error=False)
            status = RunStatus.FAILED if fail_on_error else RunStatus.COMPLETED

            try:
                # Try to log the error before ending
                self.set_tag(run.run_id, "error_type", type(e).__name__)
                self.set_tag(run.run_id, "error_message", str(e)[:500])  # Truncate long messages
            except Exception:
                pass  # Don't fail if we can't log the error

            self.end_run(run.run_id, status)
            logger.warning(f"Run context ended with error: {run.run_id} - {e}")

            # Re-raise the original exception
            raise
