"""
Step-level run tracking -> audit.pipeline_step_runs (+ structured log events).

One row per execution of a pipeline step (one Airflow task try). The row is written
when the step starts (status=running) and updated when it ends, so a step that was
killed (OOM, container restart) stays visible as `running` -> `stale` in obs views.

Design rule: observability must never break the pipeline. Every write to the audit
schema is best-effort: on failure we log a warning and keep going.

Usage inside any script (no-op when not executed through the runner):

    from pipelines.observability import current_step
    step = current_step()
    step.add(rows_in=df.height, files_total=3)
    step.add(rows_out=loaded)
    step.set(source_url=url)          # free-form metrics -> metrics jsonb
"""
from __future__ import annotations

import json
import logging
import os
import socket
import uuid
from datetime import datetime, timezone

from pipelines.commons.logger import airflow_context, get_logger, log_event

logger = get_logger("observability.tracker")

COUNTERS = ("rows_in", "rows_out", "rows_rejected", "files_total", "files_ok", "files_failed")
STATUS_BY_EXIT_CODE = {0: "success", 99: "skipped"}  # 99 = "no new data" (Airflow skip_on_exit_code)


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _connect():
    import psycopg2
    from pipelines.commons.env_loader import CONSTRING

    return psycopg2.connect(CONSTRING, connect_timeout=5, application_name="datalabs-observability")


class StepRun:
    def __init__(self, pipeline: str, step: str, script: str | None = None, persist: bool = True):
        ctx = airflow_context()
        self.step_run_id = str(uuid.uuid4())
        self.pipeline = pipeline
        self.step = step
        self.script = script
        self.dag_id = ctx.get("dag_id")
        self.dag_run_id = ctx.get("run_id")
        self.task_id = ctx.get("task_id")
        self.try_number = int(ctx["try_number"]) if ctx.get("try_number", "").isdigit() else None
        self.started_at = _now()
        self.finished_at: datetime | None = None
        self.status = "running"
        self.exit_code: int | None = None
        self.error_message: str | None = None
        self.counters: dict[str, int] = {}
        self.metrics: dict = {}
        self.persist = persist and os.getenv("OBS_PERSIST", "true").lower() == "true"

    # ---------------------------------------------------------------- metrics API
    def add(self, **values) -> "StepRun":
        """Increment counters (rows_in, rows_out, rows_rejected, files_*); other keys go to metrics."""
        for key, value in values.items():
            if value is None:
                continue
            if key in COUNTERS:
                self.counters[key] = self.counters.get(key, 0) + int(value)
            else:
                self.metrics[key] = value
        return self

    def set(self, **values) -> "StepRun":
        """Overwrite values (counters or free-form metrics)."""
        for key, value in values.items():
            if key in COUNTERS:
                self.counters[key] = int(value)
            else:
                self.metrics[key] = value
        return self

    # ---------------------------------------------------------------- lifecycle
    def start(self) -> "StepRun":
        log_event(logger, "step_started", f"[{self.pipeline}.{self.step}] started",
                  pipeline=self.pipeline, step=self.step, step_run_id=self.step_run_id, script=self.script)
        self._write()
        return self

    def finish(self, exit_code: int = 0, error: str | None = None) -> "StepRun":
        self.finished_at = _now()
        self.exit_code = exit_code
        self.status = STATUS_BY_EXIT_CODE.get(exit_code, "failed")
        self.error_message = (error or "")[-4000:] or None
        duration = round((self.finished_at - self.started_at).total_seconds(), 3)
        level = logging.ERROR if self.status == "failed" else logging.INFO
        log_event(
            logger, "step_finished",
            f"[{self.pipeline}.{self.step}] {self.status} in {duration}s (exit={exit_code})",
            level=level, pipeline=self.pipeline, step=self.step, step_run_id=self.step_run_id,
            status=self.status, exit_code=exit_code, duration_s=duration,
            **self.counters,
            # scalar metrics only: nested ones (e.g. a batch profile) live in audit.pipeline_step_runs.metrics
            **{f"m_{k}": v for k, v in self.metrics.items() if not isinstance(v, (dict, list))},
            error=self.error_message,
        )
        self._write()
        return self

    # ---------------------------------------------------------------- persistence
    def _write(self) -> None:
        if not self.persist:
            return
        duration = (self.finished_at - self.started_at).total_seconds() if self.finished_at else None
        sql = """
            INSERT INTO audit.pipeline_step_runs (
                step_run_id, pipeline, step, script, dag_id, dag_run_id, task_id, try_number,
                status, exit_code, started_at, finished_at, duration_s,
                rows_in, rows_out, rows_rejected, files_total, files_ok, files_failed,
                metrics, error_message, host)
            VALUES (%(step_run_id)s, %(pipeline)s, %(step)s, %(script)s, %(dag_id)s, %(dag_run_id)s,
                    %(task_id)s, %(try_number)s, %(status)s, %(exit_code)s, %(started_at)s,
                    %(finished_at)s, %(duration_s)s, %(rows_in)s, %(rows_out)s, %(rows_rejected)s,
                    %(files_total)s, %(files_ok)s, %(files_failed)s, %(metrics)s, %(error_message)s, %(host)s)
            ON CONFLICT (step_run_id) DO UPDATE SET
                status = EXCLUDED.status, exit_code = EXCLUDED.exit_code,
                finished_at = EXCLUDED.finished_at, duration_s = EXCLUDED.duration_s,
                rows_in = EXCLUDED.rows_in, rows_out = EXCLUDED.rows_out,
                rows_rejected = EXCLUDED.rows_rejected, files_total = EXCLUDED.files_total,
                files_ok = EXCLUDED.files_ok, files_failed = EXCLUDED.files_failed,
                metrics = EXCLUDED.metrics, error_message = EXCLUDED.error_message
        """
        params = {
            "step_run_id": self.step_run_id, "pipeline": self.pipeline, "step": self.step,
            "script": self.script, "dag_id": self.dag_id, "dag_run_id": self.dag_run_id,
            "task_id": self.task_id, "try_number": self.try_number, "status": self.status,
            "exit_code": self.exit_code, "started_at": self.started_at, "finished_at": self.finished_at,
            "duration_s": duration, "metrics": json.dumps(self.metrics, default=str),
            "error_message": self.error_message, "host": socket.gethostname(),
            **{c: self.counters.get(c) for c in COUNTERS},
        }
        try:
            conn = _connect()
            try:
                with conn, conn.cursor() as cur:
                    cur.execute(sql, params)
            finally:
                conn.close()
        except Exception as exc:  # observability is best-effort
            log_event(logger, "audit_write_failed", f"could not persist step run: {exc}",
                      level=logging.WARNING, step_run_id=self.step_run_id)


class _NullStep(StepRun):
    """Returned by current_step() when the script runs outside the runner (e.g. ad-hoc on the host)."""

    def __init__(self):
        super().__init__(pipeline="adhoc", step="adhoc", persist=False)

    def start(self):
        return self

    def finish(self, exit_code: int = 0, error: str | None = None):
        return self


_CURRENT: StepRun | None = None


def current_step() -> StepRun:
    global _CURRENT
    if _CURRENT is None:
        _CURRENT = _NullStep()
    return _CURRENT


def _set_current(step: StepRun | None) -> None:
    global _CURRENT
    _CURRENT = step
