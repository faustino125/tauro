from __future__ import annotations
from typing import Iterable, List, Optional, Dict, Any
from pathlib import Path
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import json
import sqlite3

from .models import PipelineRun, TaskRun, RunState, Schedule, ScheduleKind

DEFAULT_DB_PATH = Path.home() / ".tauro" / "orchestrator.db"


class OrchestratorStore:
    """
    Persistencia SQLite para PipelineRun, TaskRun y Schedules.
    Con sistema de migraciones para futuras actualizaciones del esquema.
    """

    def __init__(self, db_path: Path = DEFAULT_DB_PATH):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_schema()

    @contextmanager
    def _conn(self):
        # Each call uses its own connection; safer for multithreaded access
        con = sqlite3.connect(
            str(self.db_path),
            timeout=30,  # espera hasta 30s por locks
            check_same_thread=False,
        )
        con.row_factory = sqlite3.Row
        try:
            con.execute("PRAGMA journal_mode=WAL")
            con.execute("PRAGMA synchronous=NORMAL")
            con.execute("PRAGMA busy_timeout=30000")
            yield con
            con.commit()
        finally:
            con.close()

    def _init_schema(self):
        with self._conn() as con:
            con.execute(
                """
                CREATE TABLE IF NOT EXISTS _migrations (
                    version INTEGER PRIMARY KEY,
                    applied_at TEXT NOT NULL
                )
            """
            )

            version_row = con.execute(
                "SELECT MAX(version) AS v FROM _migrations"
            ).fetchone()
            current_version = (
                version_row["v"] if version_row and version_row["v"] is not None else 0
            )

            if current_version < 1:
                con.executescript(
                    """
                    CREATE TABLE IF NOT EXISTS pipeline_run (
                        id TEXT PRIMARY KEY,
                        pipeline_id TEXT NOT NULL,
                        state TEXT NOT NULL,
                        created_at TEXT NOT NULL,
                        started_at TEXT,
                        finished_at TEXT,
                        params TEXT
                    );
                    CREATE TABLE IF NOT EXISTS task_run (
                        id TEXT PRIMARY KEY,
                        pipeline_run_id TEXT NOT NULL,
                        task_id TEXT NOT NULL,
                        state TEXT NOT NULL,
                        try_number INTEGER NOT NULL DEFAULT 0,
                        started_at TEXT,
                        finished_at TEXT,
                        log_uri TEXT,
                        error TEXT,
                        FOREIGN KEY (pipeline_run_id) REFERENCES pipeline_run(id)
                    );
                    CREATE INDEX IF NOT EXISTS idx_task_run_pipeline ON task_run(pipeline_run_id);

                    CREATE TABLE IF NOT EXISTS schedule (
                        id TEXT PRIMARY KEY,
                        pipeline_id TEXT NOT NULL,
                        kind TEXT NOT NULL,
                        expression TEXT NOT NULL,
                        enabled INTEGER NOT NULL,
                        max_concurrency INTEGER NOT NULL DEFAULT 1,
                        retry_policy TEXT,
                        timeout_seconds INTEGER,
                        next_run_at TEXT,
                        created_at TEXT NOT NULL,
                        updated_at TEXT NOT NULL
                    );
                    CREATE INDEX IF NOT EXISTS idx_schedule_pipeline ON schedule(pipeline_id);
                """
                )
                con.execute(
                    "INSERT INTO _migrations (version, applied_at) VALUES (1, ?)",
                    (datetime.now(timezone.utc).isoformat(),),
                )
                current_version = 1

            if current_version < 2:
                con.executescript(
                    """
                    CREATE INDEX IF NOT EXISTS idx_pipeline_run_pipeline_state ON pipeline_run(pipeline_id, state);
                    CREATE INDEX IF NOT EXISTS idx_pipeline_run_state ON pipeline_run(state);
                    CREATE INDEX IF NOT EXISTS idx_pipeline_run_created_at ON pipeline_run(created_at);
                    CREATE INDEX IF NOT EXISTS idx_schedule_enabled ON schedule(enabled);
                """
                )
                con.execute(
                    "INSERT INTO _migrations (version, applied_at) VALUES (2, ?)",
                    (datetime.now(timezone.utc).isoformat(),),
                )

    # Helper to parse ISO datetimes robustly (None-safe)
    @staticmethod
    def _parse_dt(value: Optional[str]) -> Optional[datetime]:
        if not value:
            return None
        try:
            dt = datetime.fromisoformat(value)
            # If naive, assume UTC to keep consistent
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt
        except Exception:
            try:
                # fallback: parse as timestamp float-ish string
                ts = float(value)
                return datetime.fromtimestamp(ts, tz=timezone.utc)
            except Exception:
                return None

    def create_pipeline_run(
        self, pipeline_id: str, params: Optional[Dict[str, Any]] = None
    ) -> PipelineRun:
        pr = PipelineRun(pipeline_id=pipeline_id, params=params or {})
        with self._conn() as con:
            con.execute(
                """
                INSERT INTO pipeline_run (id, pipeline_id, state, created_at, started_at, finished_at, params)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    pr.id,
                    pr.pipeline_id,
                    pr.state.value,
                    pr.created_at.isoformat(),
                    None,
                    None,
                    json.dumps(pr.params),
                ),
            )
        return pr

    def get_pipeline_run(self, run_id: str) -> Optional[PipelineRun]:
        with self._conn() as con:
            row = con.execute(
                "SELECT * FROM pipeline_run WHERE id = ?", (run_id,)
            ).fetchone()
        if not row:
            return None
        return PipelineRun(
            id=row["id"],
            pipeline_id=row["pipeline_id"],
            state=RunState(row["state"]),
            created_at=self._parse_dt(row["created_at"]) or datetime.now(timezone.utc),
            started_at=self._parse_dt(row["started_at"]),
            finished_at=self._parse_dt(row["finished_at"]),
            params=json.loads(row["params"] or "{}"),
        )

    def update_pipeline_run_state(
        self,
        run_id: str,
        new_state: RunState,
        started_at: Optional[datetime] = None,
        finished_at: Optional[datetime] = None,
    ) -> None:
        with self._conn() as con:
            con.execute(
                """
                UPDATE pipeline_run
                SET state = ?, started_at = COALESCE(?, started_at), finished_at = COALESCE(?, finished_at)
                WHERE id = ?
                """,
                (
                    new_state.value,
                    started_at.isoformat() if started_at else None,
                    finished_at.isoformat() if finished_at else None,
                    run_id,
                ),
            )

    def list_pipeline_runs(
        self,
        pipeline_id: Optional[str] = None,
        state: Optional[RunState] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> List[PipelineRun]:
        sql = "SELECT * FROM pipeline_run"
        params: List[Any] = []
        clauses: List[str] = []
        if pipeline_id:
            clauses.append("pipeline_id = ?")
            params.append(pipeline_id)
        if state:
            clauses.append("state = ?")
            params.append(state.value)
        if clauses:
            sql += " WHERE " + " AND ".join(clauses)
        sql += " ORDER BY created_at DESC LIMIT ? OFFSET ?"
        params.extend([limit, offset])
        with self._conn() as con:
            rows = con.execute(sql, params).fetchall()
        return [
            PipelineRun(
                id=r["id"],
                pipeline_id=r["pipeline_id"],
                state=RunState(r["state"]),
                created_at=self._parse_dt(r["created_at"])
                or datetime.now(timezone.utc),
                started_at=self._parse_dt(r["started_at"]),
                finished_at=self._parse_dt(r["finished_at"]),
                params=json.loads(r["params"] or "{}"),
            )
            for r in rows
        ]

    def create_task_run(self, pipeline_run_id: str, task_id: str) -> TaskRun:
        tr = TaskRun(pipeline_run_id=pipeline_run_id, task_id=task_id)
        with self._conn() as con:
            con.execute(
                """
                INSERT INTO task_run (id, pipeline_run_id, task_id, state, try_number, started_at, finished_at, log_uri, error)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    tr.id,
                    tr.pipeline_run_id,
                    tr.task_id,
                    tr.state.value,
                    tr.try_number,
                    None,
                    None,
                    None,
                    None,
                ),
            )
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
        with self._conn() as con:
            con.execute(
                """
                UPDATE task_run
                SET state = ?,
                    try_number = COALESCE(?, try_number),
                    started_at = COALESCE(?, started_at),
                    finished_at = COALESCE(?, finished_at),
                    error = COALESCE(?, error),
                    log_uri = COALESCE(?, log_uri)
                WHERE id = ?
                """,
                (
                    new_state.value,
                    try_number,
                    started_at.isoformat() if started_at else None,
                    finished_at.isoformat() if finished_at else None,
                    error,
                    log_uri,
                    task_run_id,
                ),
            )

    def list_task_runs(self, pipeline_run_id: str) -> List[TaskRun]:
        with self._conn() as con:
            rows = con.execute(
                "SELECT * FROM task_run WHERE pipeline_run_id = ? ORDER BY (started_at IS NOT NULL), started_at, id",
                (pipeline_run_id,),
            ).fetchall()
        result: List[TaskRun] = []
        for r in rows:
            result.append(
                TaskRun(
                    id=r["id"],
                    pipeline_run_id=r["pipeline_run_id"],
                    task_id=r["task_id"],
                    state=RunState(r["state"]),
                    try_number=r["try_number"],
                    started_at=self._parse_dt(r["started_at"]),
                    finished_at=self._parse_dt(r["finished_at"]),
                    log_uri=r["log_uri"],
                    error=r["error"],
                )
            )
        return result

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
        with self._conn() as con:
            con.execute(
                """
                INSERT INTO schedule (id, pipeline_id, kind, expression, enabled, max_concurrency, retry_policy, timeout_seconds, next_run_at, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    sch.id,
                    sch.pipeline_id,
                    sch.kind.value,
                    sch.expression,
                    1 if sch.enabled else 0,
                    sch.max_concurrency,
                    json.dumps(sch.retry_policy),
                    sch.timeout_seconds,
                    sch.next_run_at.isoformat() if sch.next_run_at else None,
                    sch.created_at.isoformat(),
                    sch.updated_at.isoformat(),
                ),
            )
        return sch

    def list_schedules(
        self, pipeline_id: Optional[str] = None, enabled_only: bool = False
    ) -> List[Schedule]:
        sql = "SELECT * FROM schedule"
        params: List[Any] = []
        clauses: List[str] = []
        if pipeline_id:
            clauses.append("pipeline_id = ?")
            params.append(pipeline_id)
        if enabled_only:
            clauses.append("enabled = 1")
        if clauses:
            sql += " WHERE " + " AND ".join(clauses)
        sql += " ORDER BY created_at DESC"
        with self._conn() as con:
            rows = con.execute(sql, params).fetchall()
        out: List[Schedule] = []
        for r in rows:
            retry_policy = json.loads(r["retry_policy"] or "{}")
            if not retry_policy:
                retry_policy = {"retries": 0, "delay": 0}
            out.append(
                Schedule(
                    id=r["id"],
                    pipeline_id=r["pipeline_id"],
                    kind=ScheduleKind(r["kind"]),
                    expression=r["expression"],
                    enabled=bool(r["enabled"]),
                    max_concurrency=r["max_concurrency"],
                    retry_policy=retry_policy,
                    timeout_seconds=r["timeout_seconds"],
                    next_run_at=self._parse_dt(r["next_run_at"]),
                    created_at=self._parse_dt(r["created_at"])
                    or datetime.now(timezone.utc),
                    updated_at=self._parse_dt(r["updated_at"])
                    or datetime.now(timezone.utc),
                )
            )
        return out

    def update_schedule(
        self,
        schedule_id: str,
        **fields,
    ) -> None:
        if not fields:
            return
        allowed = {
            "enabled",
            "expression",
            "max_concurrency",
            "retry_policy",
            "timeout_seconds",
            "next_run_at",
        }
        sets: List[str] = []
        params: List[Any] = []
        for k, v in fields.items():
            if k not in allowed:
                continue
            if k == "retry_policy":
                v = json.dumps(v)
            if k == "enabled":
                v = 1 if bool(v) else 0
            if k == "next_run_at" and isinstance(v, datetime):
                v = v.isoformat()
            sets.append(f"{k} = ?")
            params.append(v)
        if not sets:
            return
        sets.append("updated_at = ?")
        params.append(datetime.now(timezone.utc).isoformat())
        params.append(schedule_id)
        sql = "UPDATE schedule SET " + ", ".join(sets) + " WHERE id = ?"
        with self._conn() as con:
            con.execute(sql, params)

    def compute_next_run_for_interval(
        self, interval_seconds: int, now: Optional[datetime] = None
    ) -> datetime:
        base = now or datetime.now(timezone.utc)
        return base + timedelta(seconds=interval_seconds)
