from __future__ import annotations

import json
import os
import threading
from pathlib import Path
from typing import Any, Dict, List, Optional
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from uuid import uuid4
import gzip

from loguru import logger  # type: ignore
from sqlalchemy.orm import Session
from .db import engine, get_session

from tauro.orchest.models import PipelineRun, TaskRun, RunState, Schedule, ScheduleKind
from tauro.orchest.models_db import (
    Base,
    PipelineRunDB,
    TaskRunDB,
    ScheduleDB,
    ScheduleDeadLetterDB,
    PipelineMetricsDB,
    RunResultDB,
)


class OrchestratorStore:
    """Database-backed persistence for orchestrator metadata using PostgreSQL."""

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
            f"OrchestratorStore initialized with PostgreSQL for scope {self._scope}"
        )

    def _init_db(self) -> None:
        """Initialize database tables."""
        Base.metadata.create_all(bind=engine)

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

    def _write(self, entity: str, id: str, data: Dict[str, Any]) -> None:
        # Validar datos antes de guardar
        self._validate_data(entity, data)

        lock = self._locks.get(entity)
        if lock:
            with lock:
                with self._conn() as session:
                    self._db_write(session, entity, id, data)
        else:
            with self._conn() as session:
                self._db_write(session, entity, id, data)

    def _db_write(
        self, session: Session, entity: str, id: str, data: Dict[str, Any]
    ) -> None:
        """Write data to database."""
        if entity == "pipeline_run":
            db_obj = PipelineRunDB(
                id=data["id"],
                pipeline_id=data["pipeline_id"],
                state=data["state"],
                created_at=self._parse_dt(data.get("created_at")),
                started_at=self._parse_dt(data.get("started_at")),
                finished_at=self._parse_dt(data.get("finished_at")),
                params=data.get("params"),
                error=data.get("error"),
            )
            session.merge(db_obj)
        elif entity == "task_run":
            db_obj = TaskRunDB(
                id=data["id"],
                pipeline_run_id=data["pipeline_run_id"],
                task_id=data["task_id"],
                state=data["state"],
                try_number=data.get("try_number", 0),
                started_at=self._parse_dt(data.get("started_at")),
                finished_at=self._parse_dt(data.get("finished_at")),
                log_uri=data.get("log_uri"),
                error=data.get("error"),
            )
            session.merge(db_obj)
        elif entity == "schedule":
            db_obj = ScheduleDB(
                id=data["id"],
                pipeline_id=data["pipeline_id"],
                kind=data["kind"],
                expression=data["expression"],
                enabled=data.get("enabled", True),
                max_concurrency=data.get("max_concurrency", 1),
                retry_policy=data.get("retry_policy"),
                timeout_seconds=data.get("timeout_seconds"),
                next_run_at=self._parse_dt(data.get("next_run_at")),
                created_at=self._parse_dt(data.get("created_at")),
                updated_at=self._parse_dt(data.get("updated_at")),
            )
            session.merge(db_obj)
        elif entity == "pipeline_metrics":
            db_obj = PipelineMetricsDB(
                pipeline_id=data["pipeline_id"],
                total_runs=data.get("total_runs", 0),
                successful_runs=data.get("successful_runs", 0),
                failed_runs=data.get("failed_runs", 0),
                avg_execution_time_seconds=data.get("avg_execution_time_seconds", 0.0),
                last_run_at=self._parse_dt(data.get("last_run_at")),
                last_success_at=self._parse_dt(data.get("last_success_at")),
                last_failure_at=self._parse_dt(data.get("last_failure_at")),
                updated_at=self._parse_dt(data.get("updated_at")),
            )
            session.merge(db_obj)
        elif entity == "schedule_dead_letter":
            db_obj = ScheduleDeadLetterDB(
                id=data["id"],
                schedule_id=data["schedule_id"],
                pipeline_id=data["pipeline_id"],
                error=data["error"],
                created_at=self._parse_dt(data.get("created_at")),
            )
            session.add(db_obj)  # Add instead of merge for dead letter
        elif entity == "run_result":
            db_obj = RunResultDB(
                run_id=data["run_id"],
                status=data["status"],
                result=data.get("result"),
                created_at=self._parse_dt(data.get("created_at")),
            )
            session.merge(db_obj)

    def _read(self, entity: str, id: str) -> Optional[Dict[str, Any]]:
        with self._conn() as session:
            return self._db_read(session, entity, id)

    def _db_read(
        self, session: Session, entity: str, id: str
    ) -> Optional[Dict[str, Any]]:
        """Read data from database."""
        converters = {
            "pipeline_run": self._convert_pipeline_run_db,
            "task_run": self._convert_task_run_db,
            "schedule": self._convert_schedule_db,
            "pipeline_metrics": self._convert_pipeline_metrics_db,
            "run_result": self._convert_run_result_db,
            "schedule_dead_letter": self._convert_schedule_dead_letter_db,
        }

        converter = converters.get(entity)
        if not converter:
            return None

        db_class = self._get_db_class(entity)
        obj = session.query(db_class).filter(db_class.id == id).first()
        return converter(obj) if obj else None

    def _list(self, entity: str) -> List[Dict[str, Any]]:
        with self._conn() as session:
            return self._db_list(session, entity)

    def _db_list(self, session: Session, entity: str) -> List[Dict[str, Any]]:
        """List all entities from database."""
        converters = {
            "pipeline_run": self._convert_pipeline_run_db,
            "task_run": self._convert_task_run_db,
            "schedule": self._convert_schedule_db,
            "pipeline_metrics": self._convert_pipeline_metrics_db,
            "run_result": self._convert_run_result_db,
            "schedule_dead_letter": self._convert_schedule_dead_letter_db,
        }

        converter = converters.get(entity)
        if not converter:
            return []

        db_class = self._get_db_class(entity)
        objs = session.query(db_class).all()
        return [converter(obj) for obj in objs]

    def _get_db_class(self, entity: str):
        """Get the database class for an entity."""
        classes = {
            "pipeline_run": PipelineRunDB,
            "task_run": TaskRunDB,
            "schedule": ScheduleDB,
            "pipeline_metrics": PipelineMetricsDB,
            "run_result": RunResultDB,
            "schedule_dead_letter": ScheduleDeadLetterDB,
        }
        return classes.get(entity)

    def _convert_pipeline_run_db(self, obj) -> Dict[str, Any]:
        """Convert PipelineRunDB to dict."""
        return {
            "id": obj.id,
            "pipeline_id": obj.pipeline_id,
            "state": obj.state,
            "created_at": obj.created_at.isoformat() if obj.created_at else None,
            "started_at": obj.started_at.isoformat() if obj.started_at else None,
            "finished_at": obj.finished_at.isoformat() if obj.finished_at else None,
            "params": obj.params,
            "error": obj.error,
        }

    def _convert_task_run_db(self, obj) -> Dict[str, Any]:
        """Convert TaskRunDB to dict."""
        return {
            "id": obj.id,
            "pipeline_run_id": obj.pipeline_run_id,
            "task_id": obj.task_id,
            "state": obj.state,
            "try_number": obj.try_number,
            "started_at": obj.started_at.isoformat() if obj.started_at else None,
            "finished_at": obj.finished_at.isoformat() if obj.finished_at else None,
            "log_uri": obj.log_uri,
            "error": obj.error,
        }

    def _convert_schedule_db(self, obj) -> Dict[str, Any]:
        """Convert ScheduleDB to dict."""
        return {
            "id": obj.id,
            "pipeline_id": obj.pipeline_id,
            "kind": obj.kind,
            "expression": obj.expression,
            "enabled": obj.enabled,
            "max_concurrency": obj.max_concurrency,
            "retry_policy": obj.retry_policy,
            "timeout_seconds": obj.timeout_seconds,
            "next_run_at": obj.next_run_at.isoformat() if obj.next_run_at else None,
            "created_at": obj.created_at.isoformat() if obj.created_at else None,
            "updated_at": obj.updated_at.isoformat() if obj.updated_at else None,
        }

    def _convert_pipeline_metrics_db(self, obj) -> Dict[str, Any]:
        """Convert PipelineMetricsDB to dict."""
        return {
            "pipeline_id": obj.pipeline_id,
            "total_runs": obj.total_runs,
            "successful_runs": obj.successful_runs,
            "failed_runs": obj.failed_runs,
            "avg_execution_time_seconds": obj.avg_execution_time_seconds,
            "last_run_at": obj.last_run_at.isoformat() if obj.last_run_at else None,
            "last_success_at": obj.last_success_at.isoformat()
            if obj.last_success_at
            else None,
            "last_failure_at": obj.last_failure_at.isoformat()
            if obj.last_failure_at
            else None,
            "updated_at": obj.updated_at.isoformat() if obj.updated_at else None,
        }

    def _convert_run_result_db(self, obj) -> Dict[str, Any]:
        """Convert RunResultDB to dict."""
        return {
            "run_id": obj.run_id,
            "status": obj.status,
            "result": obj.result,
            "created_at": obj.created_at.isoformat() if obj.created_at else None,
        }

    def _convert_schedule_dead_letter_db(self, obj) -> Dict[str, Any]:
        """Convert ScheduleDeadLetterDB to dict."""
        return {
            "id": obj.id,
            "schedule_id": obj.schedule_id,
            "pipeline_id": obj.pipeline_id,
            "error": obj.error,
            "created_at": obj.created_at.isoformat() if obj.created_at else None,
        }

    @contextmanager
    def _conn(self):
        """Context manager for database session."""
        session = get_session()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def ensure_connection(self, timeout: float = 1.0) -> bool:  # pragma: no cover
        _ = timeout
        return True

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
        rows = self._list("pipeline_run")
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
        rows = self._list("task_run")
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

    def list_schedules(
        self,
        pipeline_id: Optional[str] = None,
        enabled_only: bool = False,
        kind: Optional[ScheduleKind] = None,
    ) -> List[Schedule]:
        rows = self._list("schedule")
        out: List[Schedule] = []

        for r in rows:
            if not self._schedule_matches_filters(r, pipeline_id, enabled_only, kind):
                continue

            retry_policy = self._parse_retry_policy_value(r)

            out.append(
                Schedule(
                    id=r.get("id", ""),
                    pipeline_id=r.get("pipeline_id", ""),
                    kind=ScheduleKind(r.get("kind", ScheduleKind.INTERVAL.value)),
                    expression=r.get("expression", "60"),
                    enabled=bool(r.get("enabled", True)),
                    max_concurrency=int(r.get("max_concurrency", 1)),
                    retry_policy=retry_policy or {"retries": 0, "delay": 0},
                    timeout_seconds=r.get("timeout_seconds"),
                    next_run_at=self._parse_dt(r.get("next_run_at")),
                    created_at=self._parse_dt(r.get("created_at"))
                    or datetime.now(timezone.utc),
                    updated_at=self._parse_dt(r.get("updated_at"))
                    or datetime.now(timezone.utc),
                )
            )

        out.sort(
            key=lambda s: s.created_at or datetime.min.replace(tzinfo=timezone.utc),
            reverse=True,
        )
        return out

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
        rows = self._list("pipeline_run")
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

        with self._conn() as session:
            # Delete old task runs
            result["task_runs"] = (
                session.query(TaskRunDB).filter(TaskRunDB.finished_at < cutoff).delete()
            )

            # Delete old pipeline runs
            result["pipeline_runs"] = (
                session.query(PipelineRunDB)
                .filter(PipelineRunDB.finished_at < cutoff)
                .delete()
            )

            # Delete old dead letter entries
            result["schedule_dead_letter"] = (
                session.query(ScheduleDeadLetterDB)
                .filter(ScheduleDeadLetterDB.created_at < cutoff)
                .delete()
            )

        return result

    def vacuum(self) -> None:
        # For PostgreSQL, vacuum is handled automatically, but we can log
        logger.debug("Vacuum operation not needed for PostgreSQL")

    def close(self) -> None:
        # Close any open connections if needed
        pass

    def create_snapshot(self, snapshot_path: Optional[Path] = None) -> Path:
        """Create snapshot of database data."""
        if snapshot_path is None:
            timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
            snapshot_path = Path.cwd() / f"snapshot_{timestamp}.json.gz"

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

    def load_snapshot(self, snapshot_path: Path) -> None:
        """Load snapshot into database."""
        with gzip.open(snapshot_path, "rt", encoding="utf-8") as f:
            snapshot_data = json.load(f)

        data = snapshot_data["data"]
        with self._conn() as session:
            # Clear existing data
            session.query(RunResultDB).delete()
            session.query(ScheduleDeadLetterDB).delete()
            session.query(PipelineMetricsDB).delete()
            session.query(ScheduleDB).delete()
            session.query(TaskRunDB).delete()
            session.query(PipelineRunDB).delete()

            # Load new data
            for pr_data in data["pipeline_runs"]:
                self._db_write(session, "pipeline_run", pr_data["id"], pr_data)
            for tr_data in data["task_runs"]:
                self._db_write(session, "task_run", tr_data["id"], tr_data)
            for s_data in data["schedules"]:
                self._db_write(session, "schedule", s_data["id"], s_data)
            for pm_data in data["pipeline_metrics"]:
                self._db_write(
                    session, "pipeline_metrics", pm_data["pipeline_id"], pm_data
                )
            for dl_data in data["schedule_dead_letter"]:
                self._db_write(session, "schedule_dead_letter", dl_data["id"], dl_data)
            for rr_data in data["run_results"]:
                self._db_write(session, "run_result", rr_data["run_id"], rr_data)

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
