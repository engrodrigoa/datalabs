"""
Airflow callbacks shared by every DAG (import and spread `OBS_DEFAULT_ARGS` / `OBS_DAG_CALLBACKS`).

They cover what the step runner cannot: dbt/Cosmos tasks, Python operators, sensors,
timeouts and zombie kills. Alerts fire once per *final* failure (after retries).
"""
from __future__ import annotations

import logging

from pipelines.commons.logger import get_logger, log_event
from pipelines.observability.alerting import send_alert

logger = get_logger("observability.airflow")


def _ti_fields(context) -> dict:
    ti = context.get("task_instance") or context.get("ti")
    dag_run = context.get("dag_run")
    return {
        "dag_id": getattr(ti, "dag_id", None),
        "task_id": getattr(ti, "task_id", None),
        "run_id": getattr(dag_run, "run_id", None),
        # in callbacks ti.try_number already points to the NEXT try (Airflow 2.9); prev_attempted_tries is the one that ran
        "try": getattr(ti, "prev_attempted_tries", None) or getattr(ti, "try_number", None),
        "duration_s": round(ti.duration, 1) if getattr(ti, "duration", None) else None,
        "log_url": getattr(ti, "log_url", None),
    }


GATE_MESSAGE = "pipeline run finished with failed task"  # raised by audit_logger's final gate


def close_orphan_step_runs(fields: dict, reason: str) -> int:
    """Mark step rows of this task still "running" as failed.

    The runner records its own failures, but it cannot when the process dies first: a SIGKILL, or a
    SIGTERM that arrives while the main thread is blocked inside a database call (Python only runs
    signal handlers between bytecodes). The callback runs in the Airflow worker, so it always can.
    Best effort: never raises.
    """
    if not (fields.get("dag_id") and fields.get("run_id") and fields.get("task_id")):
        return 0
    try:
        import psycopg2

        from pipelines.commons.env_loader import CONSTRING

        with psycopg2.connect(CONSTRING, connect_timeout=5, application_name="datalabs-callback") as conn:
            with conn.cursor() as cur:
                cur.execute("SET LOCAL statement_timeout = 10000")
                cur.execute(
                    """UPDATE audit.pipeline_step_runs
                          SET status = 'failed', exit_code = COALESCE(exit_code, -1), finished_at = now(),
                              duration_s = EXTRACT(EPOCH FROM now() - started_at),
                              error_message = COALESCE(error_message, %s)
                        WHERE dag_id = %s AND dag_run_id = %s AND task_id = %s AND status = 'running'""",
                    (f"process killed before it could report: {reason}"[:4000],
                     fields["dag_id"], fields["run_id"], fields["task_id"]))
                closed = cur.rowcount
        conn.close()
        if closed:
            log_event(logger, "orphan_steps_closed", f"closed {closed} orphan step run(s)", level=logging.WARNING,
                      dag_id=fields["dag_id"], task_id=fields["task_id"], closed=closed)
        return closed
    except Exception as exc:  # noqa: BLE001 - observability never breaks the run
        log_event(logger, "orphan_steps_close_failed", f"could not close orphan step runs: {exc}", level=logging.WARNING)
        return 0


def on_task_failure(context) -> None:
    fields = _ti_fields(context)
    exc = context.get("exception")
    close_orphan_step_runs(fields, str(exc))
    log_event(logger, "task_failed", f"{fields['dag_id']}.{fields['task_id']} failed: {exc}",
              level=logging.ERROR, **{k: v for k, v in fields.items() if k != "log_url"}, error=str(exc)[:1000])
    if str(exc).startswith(GATE_MESSAGE):
        return  # the root-cause task already alerted; the gate only fixes the DAG run state
    send_alert(title=f"Task failed: {fields['dag_id']}.{fields['task_id']}",
               message=str(exc)[:1000] if exc else "", severity="error", fields=fields)


def on_task_retry(context) -> None:
    fields = _ti_fields(context)
    close_orphan_step_runs(fields, str(context.get("exception")))
    log_event(logger, "task_retry", f"{fields['dag_id']}.{fields['task_id']} will retry",
              level=logging.WARNING, **{k: v for k, v in fields.items() if k != "log_url"},
              error=str(context.get("exception"))[:500])


def on_dag_failure(context) -> None:
    dag_run = context.get("dag_run")
    reason = context.get("reason")
    log_event(logger, "dag_run_failed", f"{getattr(dag_run, 'dag_id', '?')} failed ({reason})",
              level=logging.ERROR, dag_id=getattr(dag_run, "dag_id", None),
              run_id=getattr(dag_run, "run_id", None), reason=reason)


def on_dag_success(context) -> None:
    dag_run = context.get("dag_run")
    duration = None
    if dag_run is not None and dag_run.start_date and dag_run.end_date:
        duration = round((dag_run.end_date - dag_run.start_date).total_seconds(), 1)
    log_event(logger, "dag_run_succeeded", f"{getattr(dag_run, 'dag_id', '?')} succeeded",
              dag_id=getattr(dag_run, "dag_id", None), run_id=getattr(dag_run, "run_id", None),
              duration_s=duration)


def on_sla_miss(dag, task_list, blocking_task_list, slas, blocking_tis) -> None:
    tasks = ", ".join(str(getattr(s, "task_id", s)) for s in (slas or []))
    log_event(logger, "sla_missed", f"SLA missed on {dag.dag_id}: {tasks}", level=logging.WARNING,
              dag_id=dag.dag_id, tasks=tasks)
    send_alert(title=f"SLA missed: {dag.dag_id}", message=f"tasks: {tasks}", severity="warning")


OBS_DEFAULT_ARGS = {
    "on_failure_callback": on_task_failure,
    "on_retry_callback": on_task_retry,
}

OBS_DAG_CALLBACKS = {
    "on_failure_callback": on_dag_failure,
    "on_success_callback": on_dag_success,
}
