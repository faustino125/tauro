from __future__ import annotations
from typing import Optional
from threading import Event, Thread
from datetime import datetime, timedelta, timezone
import time

from loguru import logger  # type: ignore

from .models import ScheduleKind, Schedule, RunState
from .store import OrchestratorStore
from .runner import OrchestratorRunner
from tauro.config.contexts import Context

try:
    from croniter import croniter  # type: ignore

    HAS_CRONITER = True
except ImportError:
    HAS_CRONITER = False
    logger.warning(
        "croniter not installed, cron schedules will use placeholder behavior"
    )


def _parse_interval_seconds(expr: str) -> int:
    try:
        return max(1, int(expr.strip()))
    except Exception:
        return 60


class SchedulerService:
    def __init__(self, context: Context, store: Optional[OrchestratorStore] = None):
        self.context = context
        self.store = store or OrchestratorStore()
        self.runner = OrchestratorRunner(context, self.store)
        self._stop = Event()
        self._thread: Optional[Thread] = None

    def start(self, poll_interval: float = 1.0):
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = Thread(target=self._loop, args=(poll_interval,), daemon=True)
        self._thread.start()
        logger.info("Scheduler started")

    def stop(self, timeout: Optional[float] = 5.0):
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=timeout)
        logger.info("Scheduler stopped")

    def _loop(self, poll_interval: float):
        while not self._stop.is_set():
            try:
                now = datetime.now(timezone.utc)
                schedules = self.store.list_schedules(enabled_only=True)
                for s in schedules:
                    if not self._is_due(s, now):
                        continue

                    running = self.store.list_pipeline_runs(
                        pipeline_id=s.pipeline_id, state=RunState.RUNNING
                    )
                    queued = self.store.list_pipeline_runs(
                        pipeline_id=s.pipeline_id, state=RunState.QUEUED
                    )
                    if len(running) + len(queued) >= s.max_concurrency:
                        logger.debug(
                            f"Skipping schedule {s.id} due to concurrency (running={len(running)}, queued={len(queued)}, max={s.max_concurrency})"
                        )
                        continue

                    pr = self.store.create_pipeline_run(s.pipeline_id, params={})
                    self.store.update_pipeline_run_state(pr.id, RunState.QUEUED)
                    logger.info(
                        f"[Scheduler] Created run {pr.id} for pipeline '{s.pipeline_id}'"
                    )

                    Thread(
                        target=self._start_run_in_thread,
                        args=(pr.id, s),
                        daemon=True,
                    ).start()

                    # bump next run (DB update) - do after scheduling the run to reduce races
                    self._bump_next_run(s, now)

                time.sleep(poll_interval)
            except Exception as e:
                logger.exception(f"Scheduler loop error: {e}")
                time.sleep(poll_interval)

    def _start_run_in_thread(self, run_id: str, s: Schedule):
        try:
            self.runner.start_run(
                run_id,
                retries=int((s.retry_policy or {}).get("retries", 0)),
                retry_delay_sec=int((s.retry_policy or {}).get("delay", 0)),
                concurrency=None,
                timeout_seconds=s.timeout_seconds,
            )
        except Exception:
            logger.exception(f"Error executing scheduled run {run_id}")

    def _is_due(self, s: Schedule, now: datetime) -> bool:
        if not s.enabled:
            return False
        # If next_run_at exists and is in future -> not due
        if s.next_run_at and now < s.next_run_at:
            return False

        # For interval schedules, if next_run_at is missing consider due and compute on bump
        if s.kind == ScheduleKind.INTERVAL:
            return True

        # For cron schedules:
        if s.kind == ScheduleKind.CRON:
            # If we have croniter, we trust next_run_at or croniter to compute; if missing, treat as due
            return True

        return False

    def _bump_next_run(self, s: Schedule, now: datetime):
        if s.kind == ScheduleKind.INTERVAL:
            interval = _parse_interval_seconds(s.expression)
            next_run = now + timedelta(seconds=interval)
            try:
                self.store.update_schedule(s.id, next_run_at=next_run)
            except Exception:
                logger.exception("Failed updating schedule next_run_at for interval")
        elif s.kind == ScheduleKind.CRON:
            if HAS_CRONITER:
                try:
                    base_time = s.next_run_at or now
                    cron_iter = croniter(s.expression, base_time)
                    next_run = cron_iter.get_next(datetime)
                    self.store.update_schedule(s.id, next_run_at=next_run)
                except Exception:
                    logger.exception(f"Error parsing cron expression {s.expression}")
                    # fallback to conservative 60s
                    self.store.update_schedule(
                        s.id, next_run_at=now + timedelta(seconds=60)
                    )
            else:
                # fallback placeholder: schedule in 60s
                self.store.update_schedule(
                    s.id, next_run_at=now + timedelta(seconds=60)
                )

    def backfill(self, pipeline_id: str, count: int):
        for _ in range(max(0, int(count))):
            pr = self.store.create_pipeline_run(pipeline_id, params={})
            Thread(
                target=self._start_run_in_thread, args=(pr.id, Schedule()), daemon=True
            ).start()
            time.sleep(0.1)
