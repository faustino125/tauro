from __future__ import annotations

import gzip
import json
import os
import threading
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional
from uuid import uuid4

from loguru import logger  # type: ignore
from pymongo import ASCENDING, DESCENDING  # type: ignore
from pymongo.collection import Collection  # type: ignore
from pymongo.errors import PyMongoError  # type: ignore

from .db import close_client, get_collection, ping_database
from tauro.orchest.models import PipelineRun, TaskRun, RunState, Schedule, ScheduleKind

try:
    from croniter import croniter

    HAS_CRONITER = True
except ImportError:
    HAS_CRONITER = False
    logger.warning(
        "croniter not installed, cron schedules will use placeholder behavior"
    )


class OrchestratorStore:
    """Database-backed persistence for orchestrator metadata using MongoDB."""

    _COLLECTION_CONFIG: Dict[str, Dict[str, Any]] = {
        "pipeline_run": {
            "name": "pipeline_runs",
            "key": "id",
            "indexes": [
                [("pipeline_id", ASCENDING)],
                [("state", ASCENDING)],
                [("created_at", DESCENDING)],
                [("pipeline_id", ASCENDING), ("created_at", DESCENDING)],
            ],
        },
        "task_run": {
            "name": "task_runs",
            "key": "id",
            "indexes": [
                [("pipeline_run_id", ASCENDING)],
                [("state", ASCENDING)],
            ],
        },
        "schedule": {
            "name": "schedules",
            "key": "id",
            "indexes": [
                [("pipeline_id", ASCENDING)],
                [("enabled", ASCENDING)],
                [("next_run_at", ASCENDING)],
            ],
        },
        "pipeline_metrics": {
            "name": "pipeline_metrics",
            "key": "pipeline_id",
            "indexes": [[("updated_at", DESCENDING)]],
        },
        "schedule_dead_letter": {
            "name": "schedule_dead_letter",
            "key": "id",
            "indexes": [
                [("schedule_id", ASCENDING)],
                [("pipeline_id", ASCENDING)],
                [("created_at", DESCENDING)],
            ],
        },
        "run_result": {
            "name": "run_results",
            "key": "run_id",
            "indexes": [[("created_at", DESCENDING)]],
        },
    }

    def __init__(
        self,
        *,
        context: Optional[Any] = None,
        scope: Optional[str] = None,
    ) -> None:
        # For compatibility, but not used in DB mode
        if scope is None:
            scope = os.environ.get("ORCHESTRATOR_SCOPE")
        if scope is None and context is not None and hasattr(context, "execution_mode"):
            try:
                scope = str(getattr(context, "execution_mode"))
            except Exception:
                scope = None
        if not scope:
            scope = "DEV"

        self._scope = scope

        # Cache for lazily obtained MongoDB collections
        self._collections: Dict[str, Collection] = {}

        # Initialize database tables
        self._init_db()

        # Locks for thread safety
        self._locks: Dict[str, threading.RLock] = {
            "pipeline_run": threading.RLock(),
            "task_run": threading.RLock(),
            "schedule": threading.RLock(),
            "schedule_dead_letter": threading.RLock(),
            "pipeline_metrics": threading.RLock(),
            "run_result": threading.RLock(),
        }

        logger.debug(
            f"OrchestratorStore initialized with MongoDB for scope {self._scope}"
        )

    def _init_db(self) -> None:
        """Ensure MongoDB collections and indexes are available."""
        for entity, cfg in self._COLLECTION_CONFIG.items():
            collection = get_collection(cfg["name"])
            self._collections[entity] = collection
            for index_spec in cfg.get("indexes", []):
                try:
                    collection.create_index(index_spec, background=True)
                except (
                    PyMongoError
                ):  # pragma: no cover - index creation errors are logged
                    logger.exception(
                        "Failed to ensure index %s for collection %s",
                        index_spec,
                        cfg["name"],
                    )

    def _collection_cfg(self, entity: str) -> Dict[str, Any]:
        try:
            return self._COLLECTION_CONFIG[entity]
        except KeyError as exc:  # pragma: no cover - defensive programming
            raise KeyError(f"Unknown entity '{entity}'") from exc

    def _get_collection(self, entity: str) -> Collection:
        collection = self._collections.get(entity)
        if collection is None:
            cfg = self._collection_cfg(entity)
            collection = get_collection(cfg["name"])
            self._collections[entity] = collection
        return collection

    def _primary_key(self, entity: str) -> str:
        return self._collection_cfg(entity)["key"]

    def _normalize_key(self, value: Any) -> Any:
        if value is None:
            return None
        # MongoDB stores identifiers as strings for this store
        return str(value)

    def _normalize_doc(
        self, entity: str, doc: Optional[Dict[str, Any]]
    ) -> Optional[Dict[str, Any]]:
        if doc is None:
            return None
        normalized = dict(doc)
        key = self._primary_key(entity)
        identifier = normalized.pop("_id", None)
        if identifier is not None and key not in normalized:
            normalized[key] = identifier
        if key != "id" and "id" not in normalized and normalized.get(key) is not None:
            normalized["id"] = normalized[key]
        return normalized

    def _validate_data(self, entity: str, data: Dict[str, Any]) -> None:
        key = self._primary_key(entity)
        if key not in data or data[key] in (None, ""):
            fallback = data.get("id") if key != "id" else None
            if fallback not in (None, ""):
                data[key] = fallback
        if key not in data or data[key] in (None, ""):
            raise ValueError(f"Missing primary key '{key}' for entity '{entity}'")

    def __enter__(self) -> "OrchestratorStore":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:  # pragma: no cover
        return None

    @staticmethod
    def _parse_dt(value: Optional[str]) -> Optional[datetime]:
        if not value:
            return None
        try:
            return datetime.fromisoformat(value)
        except Exception:
            try:
                return datetime.strptime(value, "%Y-%m-%dT%H:%M:%S.%f%z")
            except Exception:
                return None

    def _build_document(
        self, entity: str, entity_id: str, data: Dict[str, Any]
    ) -> Dict[str, Any]:
        doc = dict(data)
        key = self._primary_key(entity)
        if key not in doc or doc[key] in (None, ""):
            doc[key] = entity_id
        doc_id = self._normalize_key(doc[key])
        doc[key] = doc_id
        doc["_id"] = doc_id
        if key != "id":
            doc.setdefault("id", doc_id)
        return doc

    def _write(self, entity: str, id: str, data: Dict[str, Any]) -> None:
        # Validar datos antes de guardar
        self._validate_data(entity, data)

        lock = self._locks.get(entity)
        if lock:
            with lock:
                self._db_write(entity, id, data)
        else:
            self._db_write(entity, id, data)

    def _db_write(self, entity: str, id: str, data: Dict[str, Any]) -> None:
        """Write data to MongoDB."""
        document = self._build_document(entity, id, data)
        collection = self._get_collection(entity)
        try:
            collection.replace_one(
                {"_id": document["_id"]}, document, upsert=True, max_time_ms=5000
            )
        except PyMongoError:
            logger.exception("Failed to write entity '%s' with id '%s'", entity, id)
            raise

    def _read(self, entity: str, id: str) -> Optional[Dict[str, Any]]:
        return self._db_read(entity, id)

    def _db_read(self, entity: str, id: str) -> Optional[Dict[str, Any]]:
        """Read data from MongoDB."""
        key_value = self._normalize_key(id)
        collection = self._get_collection(entity)
        try:
            doc = collection.find_one(
                {"_id": key_value}, max_time_ms=5000
            )  # 5 second timeout
        except PyMongoError:
            logger.exception("Failed to read entity '%s' with id '%s'", entity, id)
            raise
        return self._normalize_doc(entity, doc)

    def _list(self, entity: str) -> List[Dict[str, Any]]:
        return self._db_list(entity)

    def _db_list(self, entity: str) -> List[Dict[str, Any]]:
        """List all entities from MongoDB."""
        collection = self._get_collection(entity)
        try:
            docs = collection.find(
                max_time_ms=10000
            )  # 10 second timeout for list operations
        except PyMongoError:
            logger.exception("Failed to list entities for '%s'", entity)
            raise
        return [self._normalize_doc(entity, doc) for doc in docs if doc is not None]

    def ensure_connection(self, timeout: float = 1.0) -> bool:  # pragma: no cover
        _ = timeout
        return ping_database()

    def health_check(self) -> Dict[str, Any]:
        """
        Perform comprehensive health check of the store.

        Returns:
            Dictionary with health status and details
        """
        health_status = {
            "healthy": True,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "checks": {},
            "issues": [],
        }

        # Delegate checks to helper methods to reduce complexity
        self._check_mongodb_connection(health_status)
        if not self._check_collections(health_status):
            health_status["healthy"] = False
        if not self._check_locks(health_status):
            health_status["healthy"] = False
        if not self._check_performance(health_status):
            health_status["healthy"] = False

        return health_status

    def _check_mongodb_connection(self, health_status: Dict[str, Any]) -> None:
        try:
            db_healthy = ping_database()
            health_status["checks"]["mongodb_connection"] = {
                "status": "healthy" if db_healthy else "unhealthy",
                "details": "Connected" if db_healthy else "Connection failed",
            }
            if not db_healthy:
                health_status["healthy"] = False
                health_status["issues"].append("MongoDB connection failed")
        except Exception as e:
            health_status["checks"]["mongodb_connection"] = {
                "status": "unhealthy",
                "details": f"Connection check failed: {str(e)}",
            }
            health_status["healthy"] = False
            health_status["issues"].append(f"MongoDB connection error: {str(e)}")

    def _check_collections(self, health_status: Dict[str, Any]) -> bool:
        collections_healthy = True
        for entity in self._COLLECTION_CONFIG.keys():
            try:
                collection = self._get_collection(entity)
                # Try a simple count operation with timeout
                count = collection.count_documents({}, maxTimeMS=2000)
                health_status["checks"][f"collection_{entity}"] = {
                    "status": "healthy",
                    "details": f"Accessible, {count} documents",
                }
            except Exception as e:
                collections_healthy = False
                health_status["checks"][f"collection_{entity}"] = {
                    "status": "unhealthy",
                    "details": f"Access failed: {str(e)}",
                }
                health_status["issues"].append(
                    f"Collection {entity} inaccessible: {str(e)}"
                )
        return collections_healthy

    def _check_locks(self, health_status: Dict[str, Any]) -> bool:
        locks_healthy = True
        for entity, lock in self._locks.items():
            try:
                # Try to acquire and release lock quickly
                acquired = lock.acquire(timeout=0.1)
                if acquired:
                    lock.release()
                health_status["checks"][f"lock_{entity}"] = {
                    "status": "healthy",
                    "details": "Lock operational",
                }
            except Exception as e:
                locks_healthy = False
                health_status["checks"][f"lock_{entity}"] = {
                    "status": "unhealthy",
                    "details": f"Lock error: {str(e)}",
                }
                health_status["issues"].append(f"Lock {entity} error: {str(e)}")
        return locks_healthy

    def _check_performance(self, health_status: Dict[str, Any]) -> bool:
        try:
            start_time = time.time()
            # Try to read from a small collection first
            test_collection = self._get_collection("schedule")
            test_collection.find_one(max_time_ms=1000)
            read_time = time.time() - start_time
            health_status["checks"]["performance"] = {
                "status": "healthy" if read_time < 0.5 else "degraded",
                "details": f"Read time: {read_time:.3f}s",
            }
            if read_time >= 1.0:
                health_status["issues"].append(f"Very slow read time: {read_time:.3f}s")
                return False
            if read_time >= 0.5:
                health_status["issues"].append(f"Slow read time: {read_time:.3f}s")
            return True
        except Exception as e:
            health_status["checks"]["performance"] = {
                "status": "unhealthy",
                "details": f"Performance check failed: {str(e)}",
            }
            health_status["issues"].append(f"Performance check failed: {str(e)}")
            return False

    # ---------- Pipeline runs ----------
    def create_pipeline_run(
        self, pipeline_id: str, params: Optional[Dict[str, Any]] = None
    ) -> PipelineRun:
        pr = PipelineRun(pipeline_id=pipeline_id, params=params or {})
        data = {
            "id": pr.id,
            "pipeline_id": pr.pipeline_id,
            "state": pr.state.value,
            "created_at": pr.created_at.isoformat(),
            "started_at": None,
            "finished_at": None,
            "params": json.dumps(pr.params or {}),
            "error": None,
        }
        self._write("pipeline_run", pr.id, data)
        logger.info(f"Created PipelineRun {pr.id} for pipeline '{pipeline_id}'")
        return pr

    def get_pipeline_run(self, run_id: str) -> Optional[PipelineRun]:
        row = self._read("pipeline_run", run_id)
        if not row:
            return None
        try:
            params_val = row.get("params")
            params = (
                json.loads(params_val)
                if isinstance(params_val, str)
                else (params_val or {})
            )
        except Exception:
            params = {}
        return PipelineRun(
            id=row.get("id", ""),
            pipeline_id=row.get("pipeline_id", ""),
            state=RunState(row.get("state", RunState.PENDING.value)),
            created_at=self._parse_dt(row.get("created_at"))
            or datetime.now(timezone.utc),
            started_at=self._parse_dt(row.get("started_at")),
            finished_at=self._parse_dt(row.get("finished_at")),
            params=params,
            error=row.get("error"),
        )

    def update_pipeline_run_state(
        self,
        run_id: str,
        new_state: RunState,
        started_at: Optional[datetime] = None,
        finished_at: Optional[datetime] = None,
        error: Optional[str] = None,
    ) -> None:
        row = self._read("pipeline_run", run_id) or {}
        if not row:
            return
        row["state"] = new_state.value
        if started_at:
            row["started_at"] = started_at.isoformat()
        if finished_at:
            row["finished_at"] = finished_at.isoformat()
        if error is not None:
            row["error"] = error
        self._write("pipeline_run", run_id, row)

    def list_pipeline_runs(
        self,
        pipeline_id: Optional[str] = None,
        state: Optional[RunState] = None,
        limit: int = 100,
        offset: int = 0,
        created_after: Optional[datetime] = None,
        created_before: Optional[datetime] = None,
    ) -> List[PipelineRun]:
        try:
            rows = self._list("pipeline_run")
        except PyMongoError:
            logger.exception("Failed to list pipeline runs")
            return []
        result: List[PipelineRun] = []
        for r in rows:
            created = self._parse_dt(r.get("created_at"))
            if (
                (pipeline_id and r.get("pipeline_id") != pipeline_id)
                or (state and r.get("state") != state.value)
                or (created_after and created and created < created_after)
                or (created_before and created and created > created_before)
            ):
                continue
            pr = self.get_pipeline_run(r.get("id", ""))
            if pr:
                result.append(pr)

        result.sort(
            key=lambda x: x.created_at or datetime.min.replace(tzinfo=timezone.utc),
            reverse=True,
        )
        return result[offset : offset + limit]

    # ---------- Task runs ----------
    def create_task_run(self, pipeline_run_id: str, task_id: str) -> TaskRun:
        tr = TaskRun(pipeline_run_id=pipeline_run_id, task_id=task_id)
        data = {
            "id": tr.id,
            "pipeline_run_id": tr.pipeline_run_id,
            "task_id": tr.task_id,
            "state": tr.state.value,
            "try_number": tr.try_number,
            "started_at": None,
            "finished_at": None,
            "log_uri": None,
            "error": None,
        }
        self._write("task_run", tr.id, data)
        return tr

    def update_task_run_state(
        self,
        task_run_id: str,
        new_state: RunState,
        try_number: Optional[int] = None,
        started_at: Optional[datetime] = None,
        finished_at: Optional[datetime] = None,
        error: Optional[str] = None,
        log_uri: Optional[str] = None,
    ) -> None:
        row = self._read("task_run", task_run_id) or {}
        if not row:
            return
        row["state"] = new_state.value
        if try_number is not None:
            row["try_number"] = int(try_number)
        if started_at:
            row["started_at"] = started_at.isoformat()
        if finished_at:
            row["finished_at"] = finished_at.isoformat()
        if error is not None:
            row["error"] = error
        if log_uri is not None:
            row["log_uri"] = log_uri
        self._write("task_run", task_run_id, row)

    def list_task_runs(
        self, pipeline_run_id: str, state: Optional[RunState] = None
    ) -> List[TaskRun]:
        try:
            rows = self._list("task_run")
        except PyMongoError:
            logger.exception("Failed to list task runs")
            return []
        out: List[TaskRun] = []
        for r in rows:
            if r.get("pipeline_run_id") != pipeline_run_id:
                continue
            if state and r.get("state") != state.value:
                continue
            out.append(
                TaskRun(
                    id=r.get("id", ""),
                    pipeline_run_id=r.get("pipeline_run_id", ""),
                    task_id=r.get("task_id", ""),
                    state=RunState(r.get("state", RunState.PENDING.value)),
                    try_number=int(r.get("try_number", 0)),
                    started_at=self._parse_dt(r.get("started_at")),
                    finished_at=self._parse_dt(r.get("finished_at")),
                    log_uri=r.get("log_uri"),
                    error=r.get("error"),
                )
            )

        def _ordkey(tr: TaskRun):
            return (
                0 if tr.started_at else 1,
                tr.started_at or datetime.min.replace(tzinfo=timezone.utc),
                tr.id,
            )

        out.sort(key=_ordkey)
        return out

    # ---------- Schedules ----------
    def create_schedule(
        self,
        pipeline_id: str,
        kind: ScheduleKind,
        expression: str,
        max_concurrency: int = 1,
        retry_policy: Optional[Dict[str, Any]] = None,
        timeout_seconds: Optional[int] = None,
        next_run_at: Optional[datetime] = None,
        enabled: bool = True,
    ) -> Schedule:
        # Validate inputs
        if not pipeline_id or not isinstance(pipeline_id, str):
            raise ValueError("pipeline_id must be a non-empty string")

        if not isinstance(kind, ScheduleKind):
            raise ValueError(
                f"kind must be a ScheduleKind enum value, got {type(kind)}"
            )

        if not expression or not isinstance(expression, str):
            raise ValueError("expression must be a non-empty string")

        if max_concurrency < 1:
            raise ValueError("max_concurrency must be >= 1")

        if timeout_seconds is not None and timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be > 0 if specified")

        # Validate expression based on kind
        if kind == ScheduleKind.INTERVAL:
            self._validate_interval_expression(expression)
        elif kind == ScheduleKind.CRON and HAS_CRONITER:
            self._validate_cron_expression(expression)

        # Validate retry policy
        if retry_policy:
            self._validate_retry_policy(retry_policy)

        sch = Schedule(
            pipeline_id=pipeline_id,
            kind=kind,
            expression=expression,
            max_concurrency=max_concurrency,
            retry_policy=retry_policy or {"retries": 0, "delay": 0},
            timeout_seconds=timeout_seconds,
            next_run_at=next_run_at,
            enabled=enabled,
        )
        data = {
            "id": sch.id,
            "pipeline_id": sch.pipeline_id,
            "kind": sch.kind.value,
            "expression": sch.expression,
            "enabled": bool(sch.enabled),
            "max_concurrency": int(sch.max_concurrency),
            "retry_policy": json.dumps(sch.retry_policy or {}),
            "timeout_seconds": sch.timeout_seconds,
            "next_run_at": sch.next_run_at.isoformat() if sch.next_run_at else None,
            "created_at": sch.created_at.isoformat(),
            "updated_at": sch.updated_at.isoformat(),
        }
        self._write("schedule", sch.id, data)
        return sch

    def _validate_interval_expression(self, expression: str) -> None:
        """Validate interval schedule expression."""
        try:
            seconds = int(expression.strip())
            if seconds <= 0:
                raise ValueError(f"Interval must be > 0 seconds, got {seconds}")
            if seconds < 10:
                logger.warning(f"Very short interval: {seconds} seconds")
        except ValueError as e:
            raise ValueError(f"Invalid interval expression '{expression}': {e}")

    def _validate_cron_expression(self, expression: str) -> None:
        """Validate cron schedule expression."""
        if not HAS_CRONITER:
            logger.warning(
                f"Cannot validate cron expression '{expression}': croniter not available"
            )
            return

        try:
            # Try to create a croniter to validate the expression
            croniter(expression)
        except Exception as e:
            raise ValueError(f"Invalid cron expression '{expression}': {e}")

    def _validate_retry_policy(self, retry_policy: Dict[str, Any]) -> None:
        """Validate retry policy structure."""
        if not isinstance(retry_policy, dict):
            raise ValueError("retry_policy must be a dictionary")

        retries = retry_policy.get("retries", 0)
        delay = retry_policy.get("delay", 0)

        if not isinstance(retries, int) or retries < 0:
            raise ValueError(
                f"retry_policy.retries must be a non-negative integer, got {retries}"
            )

        if not isinstance(delay, (int, float)) or delay < 0:
            raise ValueError(
                f"retry_policy.delay must be a non-negative number, got {delay}"
            )

        if retries > 10:
            logger.warning(f"High retry count: {retries}")

        if delay > 3600:  # 1 hour
            logger.warning(f"Very long retry delay: {delay} seconds")

    def _schedule_matches_filters(
        self,
        r: Dict[str, Any],
        pipeline_id: Optional[str],
        enabled_only: bool,
        kind: Optional[ScheduleKind],
    ) -> bool:
        if pipeline_id and r.get("pipeline_id") != pipeline_id:
            return False
        if enabled_only and not bool(r.get("enabled", True)):
            return False
        if kind and r.get("kind") != kind.value:
            return False
        return True

    def _parse_retry_policy_value(self, r: Dict[str, Any]) -> Dict[str, Any]:
        retry_val = r.get("retry_policy")
        try:
            return (
                json.loads(retry_val)
                if isinstance(retry_val, str)
                else (retry_val or {})
            )
        except Exception:
            return {"retries": 0, "delay": 0}

    def _schedule_from_row(self, row: Dict[str, Any]) -> Schedule:
        retry_policy = self._parse_retry_policy_value(row)
        return Schedule(
            id=row.get("id", ""),
            pipeline_id=row.get("pipeline_id", ""),
            kind=ScheduleKind(row.get("kind", ScheduleKind.INTERVAL.value)),
            expression=row.get("expression", "60"),
            enabled=bool(row.get("enabled", True)),
            max_concurrency=int(row.get("max_concurrency", 1)),
            retry_policy=retry_policy or {"retries": 0, "delay": 0},
            timeout_seconds=row.get("timeout_seconds"),
            next_run_at=self._parse_dt(row.get("next_run_at")),
            created_at=self._parse_dt(row.get("created_at"))
            or datetime.now(timezone.utc),
            updated_at=self._parse_dt(row.get("updated_at"))
            or datetime.now(timezone.utc),
        )

    def list_schedules(
        self,
        pipeline_id: Optional[str] = None,
        enabled_only: bool = False,
        kind: Optional[ScheduleKind] = None,
        limit: Optional[int] = None,
        offset: int = 0,
    ) -> List[Schedule]:
        _ = (limit, offset)
        rows = self._list("schedule")
        out: List[Schedule] = []

        for r in rows:
            if not self._schedule_matches_filters(r, pipeline_id, enabled_only, kind):
                continue
            out.append(self._schedule_from_row(r))

        out.sort(
            key=lambda s: s.created_at or datetime.min.replace(tzinfo=timezone.utc),
            reverse=True,
        )
        # Los argumentos limit/offset se aceptan por compatibilidad, pero la lista
        # completa se devuelve para que las capas superiores manejen la paginación.
        return out

    def get_schedule(self, schedule_id: str) -> Optional[Schedule]:
        row = self._read("schedule", schedule_id)
        if not row:
            return None
        return self._schedule_from_row(row)

    def update_schedule(self, schedule_id: str, **fields) -> None:
        if not fields:
            return
        row = self._read("schedule", schedule_id) or {}
        if not row:
            return
        allowed = {
            "enabled",
            "expression",
            "max_concurrency",
            "retry_policy",
            "timeout_seconds",
            "next_run_at",
        }
        for k, v in fields.items():
            if k not in allowed:
                continue
            if k == "retry_policy" and not isinstance(v, str):
                row[k] = json.dumps(v)
                continue
            if k == "enabled":
                row[k] = bool(v)
                continue
            if k == "next_run_at" and isinstance(v, datetime):
                row[k] = v.isoformat()
                continue
            row[k] = v
        row["updated_at"] = datetime.now(timezone.utc).isoformat()
        self._write("schedule", schedule_id, row)

    def delete_schedule(self, schedule_id: str) -> bool:
        key = self._normalize_key(schedule_id)
        collection = self._get_collection("schedule")
        lock = self._locks.get("schedule")
        try:
            if lock:
                with lock:
                    result = collection.delete_one({"_id": key}, max_time_ms=5000)
            else:
                result = collection.delete_one({"_id": key}, max_time_ms=5000)
            return bool(result.deleted_count)
        except PyMongoError:
            logger.exception("Failed to delete schedule '%s'", schedule_id)
            raise

    def claim_schedule_next_run(
        self,
        schedule_id: str,
        expected_next_run_at: Optional[datetime],
        new_next_run_at: datetime,
    ) -> bool:
        lock = self._locks["schedule"]
        with lock:
            row = self._read("schedule", schedule_id)
            if not row or not bool(row.get("enabled", True)):
                return False
            now = datetime.now(timezone.utc)
            current_next = self._parse_dt(row.get("next_run_at"))
            due = current_next is None or current_next <= now
            matches = (expected_next_run_at is None and current_next is None) or (
                expected_next_run_at is not None
                and current_next is not None
                and current_next == expected_next_run_at
            )
            if not (due or matches):
                return False
            row["next_run_at"] = new_next_run_at.isoformat()
            row["updated_at"] = datetime.now(timezone.utc).isoformat()
            self._write("schedule", schedule_id, row)
            return True

    def compute_next_run_for_interval(
        self, interval_seconds: int, now: Optional[datetime] = None
    ) -> datetime:
        base = now or datetime.now(timezone.utc)
        return base + timedelta(seconds=interval_seconds)

    # ---------- Dead letter queue ----------
    def add_schedule_failure_to_dlq(
        self, schedule_id: str, pipeline_id: str, error: str
    ) -> None:
        rec = {
            "id": str(uuid4()),
            "schedule_id": schedule_id,
            "pipeline_id": pipeline_id,
            "error": error,
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
        self._write("schedule_dead_letter", rec["id"], rec)

    def get_schedule_failures(
        self, schedule_id: Optional[str] = None, limit: int = 100, offset: int = 0
    ) -> List[Dict[str, Any]]:
        rows = self._list("schedule_dead_letter")
        if schedule_id:
            rows = [r for r in rows if r.get("schedule_id") == schedule_id]

        def _k(r: Dict[str, Any]):
            dt = self._parse_dt(r.get("created_at")) or datetime.min.replace(
                tzinfo=timezone.utc
            )
            return dt

        rows.sort(key=_k, reverse=True)
        return rows[offset : offset + limit]

    # ---------- Monitoring ----------
    def get_stuck_pipeline_runs(
        self, timeout_minutes: int = 60, max_runs: int = 100
    ) -> List[PipelineRun]:
        cutoff_time = datetime.now(timezone.utc) - timedelta(minutes=timeout_minutes)
        try:
            rows = self._list("pipeline_run")
        except PyMongoError:
            logger.exception("Failed to list pipeline runs for stuck detection")
            return []
        stuck: List[PipelineRun] = []
        for r in rows:
            if r.get("state") != RunState.RUNNING.value:
                continue
            st = self._parse_dt(r.get("started_at"))
            if st and st < cutoff_time:
                pr = self.get_pipeline_run(r.get("id", ""))
                if pr:
                    stuck.append(pr)
        stuck.sort(
            key=lambda pr: pr.started_at or datetime.min.replace(tzinfo=timezone.utc)
        )
        return stuck[:max_runs]

    # ---------- Maintenance ----------
    def cleanup_old_data(self, max_days: int = 30) -> Dict[str, int]:
        cutoff = datetime.now(timezone.utc) - timedelta(days=max_days)
        result = {"task_runs": 0, "pipeline_runs": 0, "schedule_dead_letter": 0}

        cutoff_iso = cutoff.isoformat()

        def _older_than_filter(field: str) -> Dict[str, Any]:
            return {
                "$or": [
                    {field: {"$type": "string", "$lt": cutoff_iso}},
                    {field: {"$type": "date", "$lt": cutoff}},
                ]
            }

        try:
            task_delete = self._get_collection("task_run").delete_many(
                _older_than_filter("finished_at"),
                max_time_ms=30000,  # 30 second timeout for cleanup
            )
            result["task_runs"] = task_delete.deleted_count
        except PyMongoError:
            logger.exception("Failed to cleanup task runs older than %s", cutoff_iso)
            result["task_runs"] = -1  # Indicate error

        try:
            pipeline_delete = self._get_collection("pipeline_run").delete_many(
                _older_than_filter("finished_at"), max_time_ms=30000
            )
            result["pipeline_runs"] = pipeline_delete.deleted_count
        except PyMongoError:
            logger.exception(
                "Failed to cleanup pipeline runs older than %s", cutoff_iso
            )
            result["pipeline_runs"] = -1  # Indicate error

        try:
            dlq_delete = self._get_collection("schedule_dead_letter").delete_many(
                _older_than_filter("created_at"), max_time_ms=30000
            )
            result["schedule_dead_letter"] = dlq_delete.deleted_count
        except PyMongoError:
            logger.exception(
                "Failed to cleanup schedule dead letter entries older than %s",
                cutoff_iso,
            )
            result["schedule_dead_letter"] = -1  # Indicate error

        return result

    def vacuum(self) -> None:
        logger.debug("Vacuum operation not required for MongoDB")

    def close(self) -> None:
        close_client()

    def create_snapshot(self, snapshot_path: Optional[Path] = None) -> Path:
        """Create snapshot of database data."""
        if snapshot_path is None:
            timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
            snapshot_path = Path.cwd() / f"snapshot_{timestamp}.json.gz"

        try:
            snapshot_data = {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "scope": self._scope,
                "data": {
                    "pipeline_runs": self._list("pipeline_run"),
                    "task_runs": self._list("task_run"),
                    "schedules": self._list("schedule"),
                    "pipeline_metrics": self._list("pipeline_metrics"),
                    "schedule_dead_letter": self._list("schedule_dead_letter"),
                    "run_results": self._list("run_result"),
                },
            }

            with gzip.open(snapshot_path, "wt", encoding="utf-8") as f:
                json.dump(snapshot_data, f, default=str, ensure_ascii=False)

            logger.info(f"Snapshot created at {snapshot_path}")
            return snapshot_path
        except (OSError, IOError) as e:
            logger.exception(f"Failed to create snapshot at {snapshot_path}: {e}")
            raise
        except PyMongoError as e:
            logger.exception(f"Failed to read data for snapshot: {e}")
            raise

    def load_snapshot(self, snapshot_path: Path) -> None:
        """Load snapshot into database."""
        if not snapshot_path.exists():
            raise FileNotFoundError(f"Snapshot file not found: {snapshot_path}")

        try:
            with gzip.open(snapshot_path, "rt", encoding="utf-8") as f:
                snapshot_data = json.load(f)
        except (OSError, IOError, json.JSONDecodeError) as e:
            logger.exception(f"Failed to read snapshot file {snapshot_path}: {e}")
            raise

        data = snapshot_data.get("data", {})
        if not isinstance(data, dict):
            raise ValueError(f"Invalid snapshot data format in {snapshot_path}")

        # Clear existing data in all tracked collections
        for entity in (
            "run_result",
            "schedule_dead_letter",
            "pipeline_metrics",
            "schedule",
            "task_run",
            "pipeline_run",
        ):
            try:
                self._get_collection(entity).delete_many({})
            except PyMongoError:
                logger.exception("Failed to purge collection for entity '%s'", entity)
                raise

        # Load new data
        try:
            for pr_data in data.get("pipeline_runs", []):
                self._db_write("pipeline_run", pr_data["id"], pr_data)
            for tr_data in data.get("task_runs", []):
                self._db_write("task_run", tr_data["id"], tr_data)
            for s_data in data.get("schedules", []):
                self._db_write("schedule", s_data["id"], s_data)
            for pm_data in data.get("pipeline_metrics", []):
                self._db_write("pipeline_metrics", pm_data["pipeline_id"], pm_data)
            for dl_data in data.get("schedule_dead_letter", []):
                self._db_write("schedule_dead_letter", dl_data["id"], dl_data)
            for rr_data in data.get("run_results", []):
                self._db_write("run_result", rr_data["run_id"], rr_data)
        except (KeyError, TypeError) as e:
            logger.exception(f"Invalid data format in snapshot {snapshot_path}: {e}")
            raise

        logger.info(f"Snapshot loaded from {snapshot_path}")

    # ---------- Aggregated metrics ----------
    def _insert_pipeline_metrics_row(
        self, pipeline_run: PipelineRun, execution_time: Optional[float]
    ) -> None:
        data = {
            "pipeline_id": pipeline_run.pipeline_id,
            "total_runs": 1,
            "successful_runs": 1 if pipeline_run.state == RunState.SUCCESS else 0,
            "failed_runs": 1 if pipeline_run.state == RunState.FAILED else 0,
            "avg_execution_time_seconds": execution_time or 0,
            "last_run_at": pipeline_run.finished_at.isoformat()
            if pipeline_run.finished_at
            else None,
            "last_success_at": pipeline_run.finished_at.isoformat()
            if pipeline_run.state == RunState.SUCCESS and pipeline_run.finished_at
            else None,
            "last_failure_at": pipeline_run.finished_at.isoformat()
            if pipeline_run.state == RunState.FAILED and pipeline_run.finished_at
            else None,
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }
        self._write("pipeline_metrics", pipeline_run.pipeline_id, data)

    def _update_pipeline_metrics_row(
        self,
        row: Dict[str, Any],
        pipeline_run: PipelineRun,
        execution_time: Optional[float],
    ) -> None:
        total_runs = int(row.get("total_runs", 0)) + 1
        successful_runs = int(row.get("successful_runs", 0)) + (
            1 if pipeline_run.state == RunState.SUCCESS else 0
        )
        failed_runs = int(row.get("failed_runs", 0)) + (
            1 if pipeline_run.state == RunState.FAILED else 0
        )

        if execution_time:
            current_avg = float(row.get("avg_execution_time_seconds", 0) or 0)
            new_avg = (
                (current_avg * int(row.get("total_runs", 0))) + execution_time
            ) / total_runs
        else:
            new_avg = float(row.get("avg_execution_time_seconds", 0) or 0)

        updated = {
            **row,
            "total_runs": total_runs,
            "successful_runs": successful_runs,
            "failed_runs": failed_runs,
            "avg_execution_time_seconds": new_avg,
            "last_run_at": pipeline_run.finished_at.isoformat()
            if pipeline_run.finished_at
            else row.get("last_run_at"),
            "last_success_at": pipeline_run.finished_at.isoformat()
            if pipeline_run.state == RunState.SUCCESS and pipeline_run.finished_at
            else row.get("last_success_at"),
            "last_failure_at": pipeline_run.finished_at.isoformat()
            if pipeline_run.state == RunState.FAILED and pipeline_run.finished_at
            else row.get("last_failure_at"),
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }
        self._write("pipeline_metrics", pipeline_run.pipeline_id, updated)

    def update_pipeline_metrics(self, pipeline_run: PipelineRun) -> None:
        if not pipeline_run.state.is_terminal():
            return

        execution_time: Optional[float] = None
        if pipeline_run.started_at and pipeline_run.finished_at:
            execution_time = (
                pipeline_run.finished_at - pipeline_run.started_at
            ).total_seconds()

        row = self._read("pipeline_metrics", pipeline_run.pipeline_id)
        if row is None:
            self._insert_pipeline_metrics_row(pipeline_run, execution_time)
        else:
            self._update_pipeline_metrics_row(row, pipeline_run, execution_time)

    def get_pipeline_metrics(
        self, pipeline_id: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        if pipeline_id:
            row = self._read("pipeline_metrics", pipeline_id)
            return [row] if row else []
        else:
            rows = self._list("pipeline_metrics")

            def _k(r: Dict[str, Any]):
                try:
                    return float(r.get("avg_execution_time_seconds", 0) or 0)
                except Exception:
                    return 0.0

            rows.sort(key=_k, reverse=True)
            return rows

    # ---------- Pagination ----------
    def get_pipeline_runs_paginated(
        self,
        pipeline_id: Optional[str] = None,
        state: Optional[RunState] = None,
        page: int = 1,
        page_size: int = 50,
        created_after: Optional[datetime] = None,
        created_before: Optional[datetime] = None,
    ) -> Dict[str, Any]:
        rows = self.list_pipeline_runs(
            pipeline_id=pipeline_id,
            state=state,
            limit=10**9,
            offset=0,
            created_after=created_after,
            created_before=created_before,
        )
        total = len(rows)
        offset = (page - 1) * page_size if page > 0 else 0
        data = rows[offset : offset + page_size]
        total_pages = (total + page_size - 1) // page_size if page_size > 0 else 0
        return {
            "data": data,
            "pagination": {
                "page": page,
                "page_size": page_size,
                "total_items": total,
                "total_pages": total_pages,
                "has_next": page < total_pages,
                "has_prev": page > 1,
            },
        }

    # ---------- Background compatibility ----------
    def update_run_status(self, run_id: str, status: str) -> None:
        try:
            state = RunState(status.upper())
        except Exception:
            status_up = str(status).upper()
            state = RunState.__members__.get(status_up, RunState.PENDING)
        self.update_pipeline_run_state(run_id, state)

    def update_run_result(
        self, run_id: str, result: Any, status: str = "success"
    ) -> None:
        payload = {
            "run_id": run_id,
            "status": status,
            "result": result,
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
        self._write("run_result", run_id, payload)
        if status.lower() == "success":
            self.update_pipeline_run_state(
                run_id, RunState.SUCCESS, finished_at=datetime.now(timezone.utc)
            )

    def update_run_error(self, run_id: str, error: str) -> None:
        row = self._read("pipeline_run", run_id) or {}
        if not row:
            return
        row["error"] = error
        self._write("pipeline_run", run_id, row)
