from __future__ import annotations
import argparse
from pathlib import Path
from typing import Optional, Dict, Any
from datetime import datetime, timedelta, timezone
from loguru import logger  # type: ignore

RUN_ID_REQUIRED = "--run-id is required"

# Detect if croniter is available for cron expression validation
try:
    from croniter import croniter  # type: ignore

    HAS_CRONITER = True
except ImportError:
    HAS_CRONITER = False

from tauro.cli.execution import ContextInitializer
from tauro.cli.config import ConfigManager
from tauro.cli.core import ExitCode
from tauro.orchest.store import OrchestratorStore
from tauro.orchest.runner import OrchestratorRunner
from tauro.orchest.scheduler import SchedulerService
from tauro.orchest.models import ScheduleKind, RunState


def _parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="tauro-orchestrator",
        description="Tauro Orchestrator CLI",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument("--env", required=True, choices=["base", "dev", "pre_prod", "prod"])
    p.add_argument("--base-path")
    p.add_argument("--layer-name")
    p.add_argument("--use-case")
    p.add_argument("--config-type", choices=["yaml", "json", "dsl"])
    p.add_argument("--interactive", action="store_true")

    p.add_argument(
        "--command",
        required=True,
        choices=[
            "run-create",
            "run-start",
            "run-status",
            "run-list",
            "run-tasks",
            "run-cancel",
            "schedule-add",
            "schedule-list",
            "schedule-start",
            "schedule-stop",
            "backfill",
            "db-stats",
            "db-cleanup",
            "db-vacuum",
        ],
    )
    p.add_argument("--pipeline")
    p.add_argument("--run-id")
    p.add_argument("--count", type=int, help="Backfill count (runs)")
    p.add_argument("--schedule-kind", choices=["INTERVAL", "CRON"], default="INTERVAL")
    p.add_argument(
        "--expression", help="Interval seconds or cron expression", default="60"
    )
    p.add_argument("--max-concurrency", type=int, default=1)
    p.add_argument("--retries", type=int, default=0)
    p.add_argument("--retry-delay-sec", type=int, default=0)
    p.add_argument("--timeout-seconds", type=int)
    p.add_argument("--days", type=int, default=30, help="Days to keep for cleanup")
    p.add_argument(
        "--batch-size", type=int, default=1000, help="Batch size for cleanup"
    )
    return p


def _init_context_and_store(args: argparse.Namespace):
    cm = ConfigManager(
        base_path=Path(args.base_path) if args.base_path else None,
        layer_name=getattr(args, "layer_name", None),
        use_case=getattr(args, "use_case", None),
        config_type=getattr(args, "config_type", None),
        interactive=getattr(args, "interactive", False),
    )
    cm.change_to_config_directory()
    ctx = ContextInitializer(cm).initialize(args.env)
    store = OrchestratorStore()
    return cm, ctx, store


def _handle_run_create(
    args: argparse.Namespace, ap: argparse.ArgumentParser, ctx, store
) -> int:
    if not args.pipeline:
        ap.error("--pipeline is required")
    runner = OrchestratorRunner(ctx, store)
    pr = runner.create_run(args.pipeline, params={})
    logger.info(f"Run created: {pr.id} (pipeline={pr.pipeline_id})")
    return ExitCode.SUCCESS.value


def _handle_run_start(
    args: argparse.Namespace, ap: argparse.ArgumentParser, ctx, store
) -> int:
    if not args.run_id:
        ap.error(RUN_ID_REQUIRED)
    runner = OrchestratorRunner(ctx, store)
    try:
        state = runner.start_run(
            args.run_id,
            retries=args.retries,
            retry_delay_sec=args.retry_delay_sec,
            concurrency=args.max_concurrency,
            timeout_seconds=args.timeout_seconds,
        )
    except Exception as e:
        logger.exception(f"Error starting run {args.run_id}: {e}")
        return ExitCode.EXECUTION_ERROR.value

    logger.info(f"Run {args.run_id} finished with state: {state}")
    if state == RunState.SUCCESS:
        return ExitCode.SUCCESS.value
    return ExitCode.EXECUTION_ERROR.value


def _handle_run_status(
    args: argparse.Namespace, ap: argparse.ArgumentParser, ctx, store
) -> int:
    if not args.run_id:
        ap.error(RUN_ID_REQUIRED)
    runner = OrchestratorRunner(ctx, store)
    pr = runner.get_run(args.run_id)
    if not pr:
        logger.error("Run not found")
        return ExitCode.CONFIGURATION_ERROR.value
    logger.info(
        f"Run: {pr.id} state={pr.state} created_at={pr.created_at} "
        f"started_at={pr.started_at} finished_at={pr.finished_at} error={pr.error}"
    )
    tasks = runner.list_task_runs(pr.id)
    if tasks:
        logger.info("Tasks:")
        for t in tasks:
            logger.info(f" - {t.task_id}: {t.state} tries={t.try_number} err={t.error}")
    return ExitCode.SUCCESS.value


def _handle_run_list(
    args: argparse.Namespace, ap: argparse.ArgumentParser, ctx, store
) -> int:
    runner = OrchestratorRunner(ctx, store)
    runs = runner.list_runs(pipeline_id=args.pipeline)
    if not runs:
        logger.info("No runs")
        return ExitCode.SUCCESS.value
    for r in runs:
        logger.info(
            f"- {r.id} {r.pipeline_id} {r.state} created_at={r.created_at} "
            f"finished_at={r.finished_at} error={r.error}"
        )
    return ExitCode.SUCCESS.value


def _handle_run_tasks(
    args: argparse.Namespace, ap: argparse.ArgumentParser, ctx, store
) -> int:
    if not args.run_id:
        ap.error(RUN_ID_REQUIRED)
    runner = OrchestratorRunner(ctx, store)
    tasks = runner.list_task_runs(args.run_id)
    if not tasks:
        logger.info("No task runs")
        return ExitCode.SUCCESS.value
    for t in tasks:
        logger.info(
            f"- {t.task_id} {t.state} tries={t.try_number} started={t.started_at} "
            f"finished={t.finished_at} error={t.error}"
        )
    return ExitCode.SUCCESS.value


def _handle_run_cancel(
    args: argparse.Namespace, ap: argparse.ArgumentParser, ctx, store
) -> int:
    if not args.run_id:
        ap.error(RUN_ID_REQUIRED)
    runner = OrchestratorRunner(ctx, store)
    success = runner.cancel_run(args.run_id)
    if not success:
        return ExitCode.EXECUTION_ERROR.value
    return ExitCode.SUCCESS.value


def _handle_schedule_add(
    args: argparse.Namespace, ap: argparse.ArgumentParser, ctx, store
) -> int:
    if not args.pipeline:
        ap.error("--pipeline is required")

    # Validar expresión cron si es necesario
    if args.schedule_kind == "CRON" and HAS_CRONITER:
        try:
            from croniter import croniter  # type: ignore

            # Validar la expresión cron
            croniter(args.expression)
        except Exception as e:
            logger.error(f"Invalid cron expression: {e}")
            return ExitCode.CONFIGURATION_ERROR.value

    sched = store.create_schedule(
        pipeline_id=args.pipeline,
        kind=ScheduleKind(args.schedule_kind),
        expression=args.expression,
        max_concurrency=args.max_concurrency,
        retry_policy={"retries": args.retries, "delay": args.retry_delay_sec},
        timeout_seconds=args.timeout_seconds,
    )
    logger.info(f"Schedule created: {sched.id} ({sched.kind} '{sched.expression}')")
    return ExitCode.SUCCESS.value


def _handle_schedule_list(
    args: argparse.Namespace, ap: argparse.ArgumentParser, ctx, store
) -> int:
    scheds = store.list_schedules(pipeline_id=args.pipeline)
    if not scheds:
        logger.info("No schedules")
        return ExitCode.SUCCESS.value
    for s in scheds:
        logger.info(
            f"- {s.id} pipeline={s.pipeline_id} {s.kind} expr='{s.expression}' "
            f"enabled={s.enabled} next={s.next_run_at} max_conc={s.max_concurrency}"
        )
    return ExitCode.SUCCESS.value


def _handle_schedule_start(
    args: argparse.Namespace, ap: argparse.ArgumentParser, ctx, store
) -> int:
    service = SchedulerService(ctx, store)
    service.start()
    logger.info("Press Ctrl+C to stop scheduler...")
    try:
        # Mostrar métricas periódicamente
        while True:
            import time as _t

            _t.sleep(10)
            metrics = service.get_metrics()
            logger.info(
                f"Scheduler metrics: cycles={metrics['cycles']}, "
                f"runs_created={metrics['runs_created']}, "
                f"errors={metrics['errors']}, "
                f"avg_cycle_time={metrics['avg_cycle_time']:.3f}s"
            )
    except KeyboardInterrupt:
        service.stop()
    return ExitCode.SUCCESS.value


def _handle_schedule_stop(
    args: argparse.Namespace, ap: argparse.ArgumentParser, ctx, store
) -> int:
    logger.info("Use Ctrl+C to stop a foreground scheduler instance")
    return ExitCode.SUCCESS.value


def _handle_backfill(
    args: argparse.Namespace, ap: argparse.ArgumentParser, ctx, store
) -> int:
    if not args.pipeline or not args.count:
        ap.error("--pipeline and --count are required")
    service = SchedulerService(ctx, store)
    service.backfill(args.pipeline, args.count)
    logger.info(f"Backfill enqueued: {args.count} runs")
    return ExitCode.SUCCESS.value


def _handle_db_stats(
    args: argparse.Namespace, ap: argparse.ArgumentParser, ctx, store
) -> int:
    stats = store.get_database_stats()
    logger.info("Database statistics:")
    logger.info(f"Pipeline runs by state: {stats['pipeline_runs_by_state']}")
    logger.info(f"Task runs by state: {stats['task_runs_by_state']}")
    logger.info(f"Schedules by status: {stats['schedules_by_status']}")
    logger.info(f"Database size: {stats['database_size_bytes'] / (1024*1024):.2f} MB")
    return ExitCode.SUCCESS.value


def _handle_db_cleanup(
    args: argparse.Namespace, ap: argparse.ArgumentParser, ctx, store
) -> int:
    result = store.cleanup_old_data(max_days=args.days, batch_size=args.batch_size)
    logger.info(f"Cleanup completed: {result}")
    return ExitCode.SUCCESS.value


def _handle_db_vacuum(
    args: argparse.Namespace, ap: argparse.ArgumentParser, ctx, store
) -> int:
    store.vacuum()
    logger.info("Database vacuum completed")
    return ExitCode.SUCCESS.value


def main(argv: Optional[list[str]] = None) -> int:
    ap = _parser()
    args = ap.parse_args(argv)

    cm, ctx, store = _init_context_and_store(args)

    try:
        handlers = {
            "run-create": _handle_run_create,
            "run-start": _handle_run_start,
            "run-status": _handle_run_status,
            "run-list": _handle_run_list,
            "run-tasks": _handle_run_tasks,
            "run-cancel": _handle_run_cancel,
            "schedule-add": _handle_schedule_add,
            "schedule-list": _handle_schedule_list,
            "schedule-start": _handle_schedule_start,
            "schedule-stop": _handle_schedule_stop,
            "backfill": _handle_backfill,
            "db-stats": _handle_db_stats,
            "db-cleanup": _handle_db_cleanup,
            "db-vacuum": _handle_db_vacuum,
        }

        handler = handlers.get(args.command)
        if not handler:
            ap.error("Unknown command")
            return ExitCode.GENERAL_ERROR.value

        return handler(args, ap, ctx, store)

    finally:
        cm.restore_original_directory()
