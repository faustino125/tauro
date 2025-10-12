from __future__ import annotations
from asyncio import Lock
import os
import time
from typing import Any, Dict, Generator, Iterable, List, Optional
from pathlib import Path
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import json
import sqlite3
import threading
import queue
from uuid import uuid4
import uuid

# Retry utilities for robust DB/pool operations
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type  # type: ignore

from .models import PipelineRun, TaskRun, RunState, Schedule, ScheduleKind

_DEFAULT_DB_PATH_ENV = "ORCHESTRATOR_DB_PATH"
PIPELINE_ID_CLAUSE = "pipeline_id = ?"
DEFAULT_DB_PATH = (
    Path(  # env override for runtime flexibility (useful in containers/CI)
        os.environ.get(_DEFAULT_DB_PATH_ENV)
    )
    if os.environ.get(_DEFAULT_DB_PATH_ENV)
    else Path.home() / ".tauro" / "orchestrator.db"
)

# SQL constants to avoid duplicated string literals
AND_JOIN = " AND "
WHERE_PREFIX = " WHERE "
ORDER_LIMIT_OFFSET = " ORDER BY created_at DESC LIMIT ? OFFSET ?"
ORDER_BY_CREATED_DESC = " ORDER BY created_at DESC"


class Store:
    def __init__(self):
        self._lock = Lock()
        self.runs: Dict[str, Dict[str, Any]] = {}
        self.pipelines: Dict[str, Dict[str, Any]] = {}

    def create_run(self, pipeline_id: str, config: Dict[str, Any]) -> str:
        run_id = str(uuid.uuid4())
        entry = {
            "id": run_id,
            "pipeline_id": pipeline_id,
            "config": config,
            "status": "created",
            "created_at": time.time(),
            "logs": [],
        }
        with self._lock:
            self.runs[run_id] = entry
        return run_id

    def update_run_status(self, run_id: str, status: str):
        with self._lock:
            r = self.runs.get(run_id)
            if r is not None:
                r["status"] = status
                r["updated_at"] = time.time()

    def update_run_result(self, run_id: str, result: Any, status: str = "success"):
        with self._lock:
            r = self.runs.get(run_id)
            if r is not None:
                r["result"] = result
                r["status"] = status
                r["updated_at"] = time.time()

    def update_run_error(self, run_id: str, error: str, status: str = "failed"):
        with self._lock:
            r = self.runs.get(run_id)
            if r is not None:
                r.setdefault("errors", []).append({"ts": time.time(), "error": error})
                r["status"] = status
                r["updated_at"] = time.time()

    def append_run_log(self, run_id: str, line: str):
        with self._lock:
            r = self.runs.get(run_id)
            if r is not None:
                r.setdefault("logs", []).append({"ts": time.time(), "line": line})

    def get_run(self, run_id: str) -> Optional[Dict[str, Any]]:
        with self._lock:
            return self.runs.get(run_id)

    def list_runs(self) -> List[Dict[str, Any]]:
        with self._lock:
            return list(self.runs.values())


class ConnectionPool:
    """Connection pool with automatic reconnection and health checks.

    Supports lazy creation: it can create only `minsize` connections at startup
    and add connections up to `maxsize` on demand.
    """

    def __init__(
        self, db_path: str, maxsize: int = 5, timeout: int = 30, minsize: int = 0
    ) -> None:
        self._db_path = db_path
        self._maxsize = maxsize
        self._timeout = timeout
        self._minsize = max(0, minsize)
        self._pool = queue.Queue(maxsize)
        self._lock = threading.RLock()
        self._created_connections = 0

        # Inicializar pool con minsize conexiones (lazy)
        for _ in range(self._minsize):
            self._add_connection()

    def _add_connection(self) -> None:
        """Add a new connection to the pool."""
        conn = sqlite3.connect(
            str(self._db_path),
            detect_types=sqlite3.PARSE_DECLTYPES,
            timeout=self._timeout,
        )
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")  # Improve concurrency
        conn.execute("PRAGMA foreign_keys=ON")  # Enable foreign keys

        with self._lock:
            self._created_connections += 1

        try:
            self._pool.put(conn, block=False)
        except queue.Full:
            conn.close()

    def _is_connection_valid(self, conn: sqlite3.Connection) -> bool:
        """Check if a connection is valid."""
        try:
            conn.execute("SELECT 1")
            return True
        except (
            sqlite3.ProgrammingError,
            sqlite3.InterfaceError,
            sqlite3.OperationalError,
        ):
            return False

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        retry=retry_if_exception_type((sqlite3.OperationalError, queue.Empty)),
    )
    def get_connection(self, timeout: Optional[float] = None) -> sqlite3.Connection:
        """Get a connection from the pool with retries."""
        try:
            conn = self._pool.get(timeout=timeout)
            if not self._is_connection_valid(conn):
                conn.close()
                raise sqlite3.OperationalError("Invalid connection")
            return conn
        except queue.Empty:
            # Try to create an additional connection when the pool is empty
            with self._lock:
                # Si aún podemos crear más conexiones, intentarlo
                if self._created_connections < self._maxsize:
                    try:
                        self._add_connection()
                    except Exception:
                        pass
            try:
                return self._pool.get(timeout=timeout)
            except queue.Empty:
                raise sqlite3.OperationalError("No connections available")

    def return_connection(self, conn: sqlite3.Connection) -> None:
        """Return connection to the pool."""
        if conn is None:
            return

        try:
            # Rollback to clear transient state
            conn.execute("ROLLBACK")

            if self._is_connection_valid(conn) and self._pool.qsize() < self._maxsize:
                try:
                    self._pool.put(conn, block=False)
                    return
                except queue.Full:
                    pass

            # If it cannot be returned to the pool, close the connection
            conn.close()
        except Exception:
            # Cerrar la conexión en caso de error
            try:
                conn.close()
            except Exception:
                pass

    def close_all(self) -> None:
        """Close all pooled connections."""
        while True:
            try:
                conn = self._pool.get_nowait()
                try:
                    conn.close()
                except Exception:
                    pass
            except queue.Empty:
                break

    def get_stats(self) -> Dict[str, Any]:
        """Return pool statistics."""
        with self._lock:
            return {
                "pool_size": self._pool.qsize(),
                "max_size": self._maxsize,
                "created_connections": self._created_connections,
                "available": self._pool.qsize(),
            }


class OrchestratorStore:
    """SQLite persistence with automatic reconnection and best practices."""

    def __init__(
        self,
        db_path: Path = DEFAULT_DB_PATH,
        max_connections: int = 10,
        timeout: int = 30,
        min_connections: int = 0,
    ):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.connection_pool = ConnectionPool(
            db_path, max_connections, timeout, min_connections
        )
        self._init_schema()

    @contextmanager
    def _conn(self) -> Generator[sqlite3.Connection, None, None]:
        """Context manager to obtain connections with error handling."""
        conn = None
        try:
            conn = self.connection_pool.get_connection()
            yield conn
            conn.commit()
        except Exception:
            if conn:
                conn.rollback()
            raise
        finally:
            if conn:
                self.connection_pool.return_connection(conn)

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        retry=retry_if_exception_type(sqlite3.OperationalError),
    )
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

            # Migration to version 1
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
                        params TEXT,
                        error TEXT
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

            # Migration to version 2
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
                current_version = 2

            # Migration to version 3 - composite indexes for improved performance
            if current_version < 3:
                con.executescript(
                    """
                    CREATE INDEX IF NOT EXISTS idx_pipeline_run_pipeline_state_created ON pipeline_run(pipeline_id, state, created_at);
                    CREATE INDEX IF NOT EXISTS idx_task_run_pipeline_state ON task_run(pipeline_run_id, state);
                    CREATE INDEX IF NOT EXISTS idx_schedule_next_run ON schedule(next_run_at) WHERE enabled = 1;
                """
                )
                con.execute(
                    "INSERT INTO _migrations (version, applied_at) VALUES (3, ?)",
                    (datetime.now(timezone.utc).isoformat(),),
                )
                current_version = 3

            # Migration to version 4 - dead letter table for failed schedules
            if current_version < 4:
                con.executescript(
                    """
                    CREATE TABLE IF NOT EXISTS schedule_dead_letter (
                        id TEXT PRIMARY KEY,
                        schedule_id TEXT NOT NULL,
                        pipeline_id TEXT NOT NULL,
                        error TEXT NOT NULL,
                        created_at TEXT NOT NULL,
                        FOREIGN KEY (schedule_id) REFERENCES schedule(id)
                    );
                    CREATE INDEX IF NOT EXISTS idx_schedule_dl_schedule ON schedule_dead_letter(schedule_id);
                    CREATE INDEX IF NOT EXISTS idx_schedule_dl_created ON schedule_dead_letter(created_at);
                """
                )
                con.execute(
                    "INSERT INTO _migrations (version, applied_at) VALUES (4, ?)",
                    (datetime.now(timezone.utc).isoformat(),),
                )
                current_version = 4

            # Migration to version 5 - more indexes for improved performance
            if current_version < 5:
                con.executescript(
                    """
                            -- Index for pipeline/date lookups
                            CREATE INDEX IF NOT EXISTS idx_pipeline_run_pipeline_created_desc 
                                ON pipeline_run(pipeline_id, created_at DESC);
                    
                            -- Index for active runs (RUNNING/QUEUED)
                            CREATE INDEX IF NOT EXISTS idx_pipeline_run_active_states 
                                ON pipeline_run(state, started_at) 
                                WHERE state IN ('RUNNING', 'QUEUED');
                    
                            -- Index for stuck runs lookup
                            CREATE INDEX IF NOT EXISTS idx_pipeline_run_stuck 
                                ON pipeline_run(state, started_at) 
                                WHERE state = 'RUNNING';
                    
                            -- Index for task runs by state and finished_at
                            CREATE INDEX IF NOT EXISTS idx_task_run_state_finished 
                                ON task_run(state, finished_at);
                    
                            -- Covering index for fast counts
                            CREATE INDEX IF NOT EXISTS idx_pipeline_run_state_created 
                                ON pipeline_run(state, created_at, id);
                        """
                )
                con.execute(
                    "INSERT INTO _migrations (version, applied_at) VALUES (5, ?)",
                    (datetime.now(timezone.utc).isoformat(),),
                )
                current_version = 5

            # Migration to version 6 - aggregated metrics table
            if current_version < 6:
                con.executescript(
                    """
                    CREATE TABLE IF NOT EXISTS pipeline_metrics (
                        pipeline_id TEXT PRIMARY KEY,
                        total_runs INTEGER DEFAULT 0,
                        successful_runs INTEGER DEFAULT 0,
                        failed_runs INTEGER DEFAULT 0,
                        avg_execution_time_seconds REAL DEFAULT 0,
                        last_run_at TEXT,
                        last_success_at TEXT,
                        last_failure_at TEXT,
                        updated_at TEXT NOT NULL
                    );
                    
                    CREATE INDEX IF NOT EXISTS idx_pipeline_metrics_updated 
                        ON pipeline_metrics(updated_at DESC);
                """
                )
                con.execute(
                    "INSERT INTO _migrations (version, applied_at) VALUES (6, ?)",
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
                INSERT INTO pipeline_run (id, pipeline_id, state, created_at, started_at, finished_at, params, error)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    pr.id,
                    pr.pipeline_id,
                    pr.state.value,
                    pr.created_at.isoformat(),
                    None,
                    None,
                    json.dumps(pr.params),
                    None,
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

        return self._row_to_pipeline_run(row)

    def _row_to_pipeline_run(self, row) -> PipelineRun:
        """Convert a database row to a PipelineRun object."""
        return PipelineRun(
            id=row["id"],
            pipeline_id=row["pipeline_id"],
            state=RunState(row["state"]),
            created_at=self._parse_dt(row["created_at"]) or datetime.now(timezone.utc),
            started_at=self._parse_dt(row["started_at"]),
            finished_at=self._parse_dt(row["finished_at"]),
            params=json.loads(row["params"] or "{}"),
            error=row["error"] if "error" in row.keys() else None,
        )

    def update_pipeline_run_state(
        self,
        run_id: str,
        new_state: RunState,
        started_at: Optional[datetime] = None,
        finished_at: Optional[datetime] = None,
        error: Optional[str] = None,
    ) -> None:
        with self._conn() as con:
            con.execute(
                """
                UPDATE pipeline_run
                SET state = ?, 
                    started_at = COALESCE(?, started_at), 
                    finished_at = COALESCE(?, finished_at),
                    error = COALESCE(?, error)
                WHERE id = ?
                """,
                (
                    new_state.value,
                    started_at.isoformat() if started_at else None,
                    finished_at.isoformat() if finished_at else None,
                    error,
                    run_id,
                ),
            )

    def list_pipeline_runs(
        self,
        pipeline_id: Optional[str] = None,
        state: Optional[RunState] = None,
        limit: int = 100,
        offset: int = 0,
        created_after: Optional[datetime] = None,
        created_before: Optional[datetime] = None,
    ) -> List[PipelineRun]:
        sql = "SELECT * FROM pipeline_run"
        params: List[Any] = []
        clauses: List[str] = []
        if pipeline_id:
            clauses.append(PIPELINE_ID_CLAUSE)
            params.append(pipeline_id)
        if state:
            clauses.append("state = ?")
            params.append(state.value)
        if created_after:
            clauses.append("created_at >= ?")
            params.append(created_after.isoformat())
        if created_before:
            clauses.append("created_at <= ?")
            params.append(created_before.isoformat())

        if clauses:
            sql += WHERE_PREFIX + AND_JOIN.join(clauses)

        sql += ORDER_LIMIT_OFFSET
        params.extend([limit, offset])

        with self._conn() as con:
            rows = con.execute(sql, params).fetchall()

        return [self._row_to_pipeline_run(r) for r in rows]

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

    def list_task_runs(
        self, pipeline_run_id: str, state: Optional[RunState] = None
    ) -> List[TaskRun]:
        sql = "SELECT * FROM task_run WHERE pipeline_run_id = ?"
        params = [pipeline_run_id]

        if state:
            sql += " AND state = ?"
            params.append(state.value)

        sql += " ORDER BY (started_at IS NOT NULL), started_at, id"

        with self._conn() as con:
            rows = con.execute(sql, params).fetchall()

        return [self._row_to_task_run(r) for r in rows]

    def _row_to_task_run(self, row) -> TaskRun:
        """Convert a database row to a TaskRun object."""
        return TaskRun(
            id=row["id"],
            pipeline_run_id=row["pipeline_run_id"],
            task_id=row["task_id"],
            state=RunState(row["state"]),
            try_number=row["try_number"],
            started_at=self._parse_dt(row["started_at"]),
            finished_at=self._parse_dt(row["finished_at"]),
            log_uri=row["log_uri"],
            error=row["error"],
        )

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
        self,
        pipeline_id: Optional[str] = None,
        enabled_only: bool = False,
        kind: Optional[ScheduleKind] = None,
    ) -> List[Schedule]:
        sql = "SELECT * FROM schedule"
        params: List[Any] = []
        clauses: List[str] = []
        if pipeline_id:
            clauses.append(PIPELINE_ID_CLAUSE)
            params.append(pipeline_id)
        if enabled_only:
            clauses.append("enabled = 1")
        if kind:
            clauses.append("kind = ?")
            params.append(kind.value)

        if clauses:
            sql += WHERE_PREFIX + AND_JOIN.join(clauses)
        sql += ORDER_BY_CREATED_DESC

        with self._conn() as con:
            rows = con.execute(sql, params).fetchall()

        return [self._row_to_schedule(r) for r in rows]

    def _row_to_schedule(self, row) -> Schedule:
        """Convertir una fila de la base de datos a un objeto Schedule"""
        retry_policy = json.loads(row["retry_policy"] or "{}")
        if not retry_policy:
            retry_policy = {"retries": 0, "delay": 0}

        return Schedule(
            id=row["id"],
            pipeline_id=row["pipeline_id"],
            kind=ScheduleKind(row["kind"]),
            expression=row["expression"],
            enabled=bool(row["enabled"]),
            max_concurrency=row["max_concurrency"],
            retry_policy=retry_policy,
            timeout_seconds=row["timeout_seconds"],
            next_run_at=self._parse_dt(row["next_run_at"]),
            created_at=self._parse_dt(row["created_at"]) or datetime.now(timezone.utc),
            updated_at=self._parse_dt(row["updated_at"]) or datetime.now(timezone.utc),
        )

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

    def add_schedule_failure_to_dlq(
        self, schedule_id: str, pipeline_id: str, error: str
    ) -> None:
        """Add a schedule failure entry to the dead letter queue."""
        with self._conn() as con:
            con.execute(
                """
                INSERT INTO schedule_dead_letter (id, schedule_id, pipeline_id, error, created_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (
                    str(uuid4()),
                    schedule_id,
                    pipeline_id,
                    error,
                    datetime.now(timezone.utc).isoformat(),
                ),
            )

    def get_schedule_failures(
        self, schedule_id: Optional[str] = None, limit: int = 100, offset: int = 0
    ) -> List[Dict[str, Any]]:
        """Return schedule failures from the dead letter queue."""
        sql = "SELECT * FROM schedule_dead_letter"
        params = []

        if schedule_id:
            sql += " WHERE schedule_id = ?"
            params.append(schedule_id)

        sql += ORDER_LIMIT_OFFSET
        params.extend([limit, offset])

        with self._conn() as con:
            rows = con.execute(sql, params).fetchall()

        return [dict(row) for row in rows]

    def get_stuck_pipeline_runs(
        self, timeout_minutes: int = 60, max_runs: int = 100
    ) -> List[PipelineRun]:
        """Return stuck executions (RUNNING for too long)."""
        cutoff_time = (
            datetime.now(timezone.utc) - timedelta(minutes=timeout_minutes)
        ).isoformat()

        with self._conn() as con:
            rows = con.execute(
                """
                SELECT * FROM pipeline_run 
                WHERE state = ? AND started_at < ?
                ORDER BY started_at ASC
                LIMIT ?
                """,
                (RunState.RUNNING.value, cutoff_time, max_runs),
            ).fetchall()

        return [self._row_to_pipeline_run(r) for r in rows]

    def cleanup_old_data(
        self, max_days: int = 30, batch_size: int = 1000
    ) -> Dict[str, int]:
        """
        Cleanup old data in batches compatible with SQLite (avoid DELETE ... LIMIT).
        Returns counts of deletions per table.
        """
        cutoff = (datetime.now(timezone.utc) - timedelta(days=max_days)).isoformat()
        result: Dict[str, int] = {
            "task_runs": 0,
            "pipeline_runs": 0,
            "schedule_dead_letter": 0,
        }

        def _delete_by_select_ids(
            con: sqlite3.Connection,
            table_name: str,
            select_sql: str,
            params: Iterable[Any],
        ) -> int:
            total_deleted = 0
            while True:
                rows = con.execute(
                    select_sql + " LIMIT ?", tuple(params) + (batch_size,)
                ).fetchall()
                ids = [r["id"] for r in rows]
                if not ids:
                    break
                placeholders = ",".join("?" for _ in ids)
                deleted = con.execute(
                    f"DELETE FROM {table_name} WHERE id IN ({placeholders})", ids
                ).rowcount
                total_deleted += int(deleted or 0)
            return total_deleted

        conn = self.connection_pool.get_connection()
        try:
            result["task_runs"] = _delete_by_select_ids(
                conn,
                "task_run",
                "SELECT id FROM task_run WHERE finished_at IS NOT NULL AND finished_at < ?",
                (cutoff,),
            )
            result["pipeline_runs"] = _delete_by_select_ids(
                conn,
                "pipeline_run",
                "SELECT id FROM pipeline_run WHERE finished_at IS NOT NULL AND finished_at < ?",
                (cutoff,),
            )
            result["schedule_dead_letter"] = _delete_by_select_ids(
                conn,
                "schedule_dead_letter",
                "SELECT id FROM schedule_dead_letter WHERE created_at < ?",
                (cutoff,),
            )
            conn.commit()
        finally:
            self.connection_pool.return_connection(conn)

        return result

    def vacuum(self) -> None:
        """
        Run VACUUM on a new connection to ensure it is NOT inside a transaction/pool.
        """
        conn = sqlite3.connect(str(self.db_path), isolation_level=None)
        try:
            conn.execute("VACUUM")
        finally:
            try:
                conn.close()
            except Exception:
                pass

    def close(self) -> None:
        """Close all connections in the pool."""
        try:
            self.connection_pool.close_all()
        except Exception:
            pass

    def get_database_stats(self) -> Dict[str, Any]:
        """
        Return useful DB statistics (counts by state and size in bytes).
        """
        stats: Dict[str, Any] = {}
        conn = self.connection_pool.get_connection()
        try:
            rows = conn.execute(
                "SELECT state, COUNT(*) as cnt FROM pipeline_run GROUP BY state"
            ).fetchall()
            stats["pipeline_runs_by_state"] = {r["state"]: r["cnt"] for r in rows}

            rows = conn.execute(
                "SELECT state, COUNT(*) as cnt FROM task_run GROUP BY state"
            ).fetchall()
            stats["task_runs_by_state"] = {r["state"]: r["cnt"] for r in rows}

            rows = conn.execute(
                "SELECT enabled, COUNT(*) as cnt FROM schedule GROUP BY enabled"
            ).fetchall()
            stats["schedules_by_status"] = {
                ("enabled" if bool(r["enabled"]) else "disabled"): r["cnt"]
                for r in rows
            }
            # database size
            try:
                pc_row = conn.execute("PRAGMA page_count").fetchone()
                ps_row = conn.execute("PRAGMA page_size").fetchone()
                page_count = int(pc_row[0]) if pc_row is not None else 0
                page_size = int(ps_row[0]) if ps_row is not None else 0
                stats["database_size_bytes"] = page_count * page_size
            except Exception:
                stats["database_size_bytes"] = 0

            # Pool stats
            stats["connection_pool"] = self.connection_pool.get_stats()

        finally:
            self.connection_pool.return_connection(conn)

        return stats

    def _insert_pipeline_metrics_row(
        self,
        con: sqlite3.Connection,
        pipeline_run: PipelineRun,
        execution_time: Optional[float],
    ) -> None:
        """Insert a new metrics row for a pipeline."""
        con.execute(
            """
            INSERT INTO pipeline_metrics (
                pipeline_id, total_runs, successful_runs, failed_runs,
                avg_execution_time_seconds, last_run_at, 
                last_success_at, last_failure_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                pipeline_run.pipeline_id,
                1,
                1 if pipeline_run.state == RunState.SUCCESS else 0,
                1 if pipeline_run.state == RunState.FAILED else 0,
                execution_time or 0,
                pipeline_run.finished_at.isoformat()
                if pipeline_run.finished_at
                else None,
                pipeline_run.finished_at.isoformat()
                if pipeline_run.state == RunState.SUCCESS and pipeline_run.finished_at
                else None,
                pipeline_run.finished_at.isoformat()
                if pipeline_run.state == RunState.FAILED and pipeline_run.finished_at
                else None,
                datetime.now(timezone.utc).isoformat(),
            ),
        )

    def _update_pipeline_metrics_row(
        self,
        con: sqlite3.Connection,
        row: sqlite3.Row,
        pipeline_run: PipelineRun,
        execution_time: Optional[float],
    ) -> None:
        """Update an existing metrics row with the pipeline_run values."""
        total_runs = row["total_runs"] + 1
        successful_runs = row["successful_runs"] + (
            1 if pipeline_run.state == RunState.SUCCESS else 0
        )
        failed_runs = row["failed_runs"] + (
            1 if pipeline_run.state == RunState.FAILED else 0
        )

        # Calcular nuevo promedio de tiempo de ejecución
        if execution_time:
            current_avg = row["avg_execution_time_seconds"] or 0
            new_avg = ((current_avg * row["total_runs"]) + execution_time) / total_runs
        else:
            new_avg = row["avg_execution_time_seconds"] or 0

        con.execute(
            """
            UPDATE pipeline_metrics SET
                total_runs = ?,
                successful_runs = ?,
                failed_runs = ?,
                avg_execution_time_seconds = ?,
                last_run_at = ?,
                last_success_at = COALESCE(?, last_success_at),
                last_failure_at = COALESCE(?, last_failure_at),
                updated_at = ?
            WHERE pipeline_id = ?
            """,
            (
                total_runs,
                successful_runs,
                failed_runs,
                new_avg,
                pipeline_run.finished_at.isoformat()
                if pipeline_run.finished_at
                else None,
                pipeline_run.finished_at.isoformat()
                if pipeline_run.state == RunState.SUCCESS and pipeline_run.finished_at
                else None,
                pipeline_run.finished_at.isoformat()
                if pipeline_run.state == RunState.FAILED and pipeline_run.finished_at
                else None,
                datetime.now(timezone.utc).isoformat(),
                pipeline_run.pipeline_id,
            ),
        )

    def update_pipeline_metrics(self, pipeline_run: PipelineRun) -> None:
        """
        Update aggregated metrics for a pipeline.
        Should be called when a run finishes.
        This version delegates logic to smaller helpers to reduce cognitive complexity.
        """
        if not pipeline_run.state.is_terminal():
            return

        execution_time: Optional[float] = None
        if pipeline_run.started_at and pipeline_run.finished_at:
            execution_time = (
                pipeline_run.finished_at - pipeline_run.started_at
            ).total_seconds()

        with self._conn() as con:
            row = con.execute(
                "SELECT * FROM pipeline_metrics WHERE pipeline_id = ?",
                (pipeline_run.pipeline_id,),
            ).fetchone()

            if row is None:
                self._insert_pipeline_metrics_row(con, pipeline_run, execution_time)
            else:
                self._update_pipeline_metrics_row(
                    con, row, pipeline_run, execution_time
                )

    def get_pipeline_metrics(
        self, pipeline_id: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Return aggregated pipeline metrics.
        If pipeline_id is provided, returns metrics for that pipeline, otherwise for all pipelines.
        """
        with self._conn() as con:
            if pipeline_id:
                row = con.execute(
                    "SELECT * FROM pipeline_metrics WHERE pipeline_id = ?",
                    (pipeline_id,),
                ).fetchone()
                return [dict(row)] if row else []
            else:
                rows = con.execute(
                    "SELECT * FROM pipeline_metrics ORDER BY updated_at DESC"
                ).fetchall()
                return [dict(row) for row in rows]

    def get_pipeline_runs_paginated(
        self,
        pipeline_id: Optional[str] = None,
        state: Optional[RunState] = None,
        page: int = 1,
        page_size: int = 50,
        created_after: Optional[datetime] = None,
        created_before: Optional[datetime] = None,
    ) -> Dict[str, Any]:
        # Construir query de conteo
        count_sql = "SELECT COUNT(*) as total FROM pipeline_run"
        params: List[Any] = []
        clauses: List[str] = []

        if pipeline_id:
            clauses.append("pipeline_id = ?")
            params.append(pipeline_id)
        if state:
            clauses.append("state = ?")
            params.append(state.value)
        if created_after:
            clauses.append("created_at >= ?")
            params.append(created_after.isoformat())
        if created_before:
            clauses.append("created_at <= ?")
            params.append(created_before.isoformat())

        if clauses:
            count_sql += WHERE_PREFIX + AND_JOIN.join(clauses)

        # Calcular offset correctamente
        offset = (page - 1) * page_size if page > 0 else 0

        with self._conn() as con:
            # Obtener total
            total = con.execute(count_sql, params).fetchone()["total"]

            # Obtener datos
            data_sql = "SELECT * FROM pipeline_run"
            if clauses:
                data_sql += WHERE_PREFIX + AND_JOIN.join(clauses)
            data_sql += ORDER_LIMIT_OFFSET

            rows = con.execute(data_sql, params + [page_size, offset]).fetchall()
            runs = [self._row_to_pipeline_run(r) for r in rows]

        total_pages = (total + page_size - 1) // page_size if page_size > 0 else 0

        return {
            "data": runs,
            "pagination": {
                "page": page,
                "page_size": page_size,
                "total_items": total,
                "total_pages": total_pages,
                "has_next": page < total_pages,
                "has_prev": page > 1,
            },
        }
