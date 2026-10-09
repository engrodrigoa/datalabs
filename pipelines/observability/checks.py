"""
Config-driven data health checks -> audit.data_quality_checks.

Complements dbt/Elementary tests (which validate *models during a build*) with checks
that run on a schedule, independently of whether a pipeline ran at all:
  * freshness       -> "is the data still arriving?"  (hours since a timestamp/date)
  * threshold       -> "is a number inside its band?"  (row counts, % suspicious rows, bad prices)
  * reconciliation  -> "did rows get lost between layers?" (bronze -> silver -> gold)

    python -m pipelines.observability.checks --pipeline anp [--config path] [--fail-on error|warn|never]

Statuses: pass | warn | fail | error (query broke or timed out) | skipped (relation not created yet).

Guard rails (a health check must never become the incident):
  * every query runs with statement_timeout (`timeout_s`, default OBS_CHECK_TIMEOUT_S=120) and
    lock_timeout (OBS_CHECK_LOCK_TIMEOUT_S=10): a slow or blocked check ends as `error`, the others still run
  * `cost: heavy` checks (full scans of large tables) are skipped by the 6-hourly health DAG
    (--skip-heavy) and run only right after the pipeline that loads that data
  * each result is logged and persisted as soon as it is known, so a killed run keeps partial results
Exit code 1 when a check reaches the --fail-on level, so the Airflow task turns red and
the on_failure_callback alerts.
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
import time
import uuid
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from pathlib import Path

import yaml

from pipelines.commons.logger import airflow_context, get_logger, log_event
from pipelines.observability.alerting import send_alert
from pipelines.observability.tracker import current_step

logger = get_logger("observability.checks")

DEFAULT_CONFIG = Path(__file__).with_name("checks.yml")
DEFAULT_TIMEOUT_S = int(os.getenv("OBS_CHECK_TIMEOUT_S", "120"))
LOCK_TIMEOUT_S = int(os.getenv("OBS_CHECK_LOCK_TIMEOUT_S", "10"))
STATUS_RANK = {"pass": 0, "skipped": 0, "warn": 1, "error": 2, "fail": 2}


@dataclass
class CheckResult:
    name: str
    pipeline: str
    check_type: str
    dataset: str | None
    severity: str
    status: str
    observed_value: float | None = None
    expected: str | None = None
    message: str = ""
    duration_ms: float | None = None
    extra: dict = field(default_factory=dict)


# --------------------------------------------------------------------------- evaluation (pure)
def _as_utc(value) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if isinstance(value, date):
        return datetime(value.year, value.month, value.day, tzinfo=timezone.utc)
    return datetime.fromisoformat(str(value)).replace(tzinfo=timezone.utc)


def _fail_status(severity: str) -> str:
    return "warn" if severity == "warn" else "fail"


def evaluate_freshness(check: dict, value, now: datetime | None = None) -> tuple[str, float | None, str, str]:
    now = now or datetime.now(timezone.utc)
    warn_h, error_h = check.get("warn_after_hours"), check.get("error_after_hours")
    expected = f"warn>{warn_h}h error>{error_h}h"
    ts = _as_utc(value)
    if ts is None:
        return _fail_status(check["severity"]), None, expected, "no rows: dataset is empty"
    age_h = round((now - ts).total_seconds() / 3600, 2)
    if error_h is not None and age_h > error_h:
        return _fail_status(check["severity"]), age_h, expected, f"stale: last record {ts:%Y-%m-%d %H:%M} ({age_h}h ago)"
    if warn_h is not None and age_h > warn_h:
        return "warn", age_h, expected, f"getting old: last record {ts:%Y-%m-%d %H:%M} ({age_h}h ago)"
    return "pass", age_h, expected, f"last record {ts:%Y-%m-%d %H:%M} ({age_h}h ago)"


def evaluate_threshold(check: dict, value) -> tuple[str, float | None, str, str]:
    lo, hi = check.get("min"), check.get("max")
    expected = f"[{'-inf' if lo is None else lo}, {'+inf' if hi is None else hi}]"
    if value is None:
        return _fail_status(check["severity"]), None, expected, "query returned NULL"
    v = float(value)
    if (lo is not None and v < lo) or (hi is not None and v > hi):
        return _fail_status(check["severity"]), v, expected, f"value {v:g} outside {expected}"
    return "pass", v, expected, f"value {v:g} within {expected}"


def evaluate_reconciliation(check: dict, left, right) -> tuple[str, float | None, str, str]:
    tol = float(check.get("tolerance_pct", 0))
    expected = f"|diff| <= {tol}%"
    if left is None or right is None:
        return _fail_status(check["severity"]), None, expected, "one side returned NULL"
    left, right = float(left), float(right)
    base = max(abs(left), 1.0)
    diff_pct = round(abs(left - right) / base * 100, 4)
    msg = f"{check.get('left_label', 'left')}={left:,.0f} vs {check.get('right_label', 'right')}={right:,.0f} (diff {diff_pct}%)"
    if diff_pct > tol:
        return _fail_status(check["severity"]), diff_pct, expected, msg
    return "pass", diff_pct, expected, msg


# --------------------------------------------------------------------------- execution
def _scalar(conn, sql: str):
    with conn.cursor() as cur:
        cur.execute(sql)
        row = cur.fetchone()
    return row[0] if row else None


def _apply_timeouts(conn, check: dict) -> None:
    timeout_s = int(check.get("timeout_s", DEFAULT_TIMEOUT_S))
    with conn.cursor() as cur:
        # SET LOCAL: scoped to this check's transaction (commit/rollback resets it)
        cur.execute(f"SET LOCAL statement_timeout = {timeout_s * 1000}")
        cur.execute(f"SET LOCAL lock_timeout = {LOCK_TIMEOUT_S * 1000}")


def _evaluate(conn, check: dict) -> tuple[str, float | None, str | None, str]:
    if check["type"] == "freshness":
        return evaluate_freshness(check, _scalar(conn, check["query"]))
    if check["type"] == "threshold":
        return evaluate_threshold(check, _scalar(conn, check["query"]))
    if check["type"] == "reconciliation":
        return evaluate_reconciliation(check, _scalar(conn, check["query"]), _scalar(conn, check["compare_query"]))
    return "error", None, None, f"unknown check type {check['type']}"


def run_check(conn, check: dict) -> CheckResult:
    import psycopg2

    base = dict(name=check["name"], pipeline=check.get("pipeline", "all"), check_type=check["type"],
                dataset=check.get("dataset"), severity=check["severity"])
    started = time.perf_counter()

    def result(**kw) -> CheckResult:
        return CheckResult(**base, duration_ms=round((time.perf_counter() - started) * 1000, 1), **kw)

    try:
        _apply_timeouts(conn, check)
        status, obs, exp, msg = _evaluate(conn, check)
        conn.commit()
        return result(status=status, observed_value=obs, expected=exp, message=msg)
    except (psycopg2.errors.UndefinedTable, psycopg2.errors.InvalidSchemaName) as exc:
        conn.rollback()
        return result(status="skipped", message=f"relation not available yet: {str(exc).splitlines()[0]}")
    except psycopg2.errors.LockNotAvailable:
        conn.rollback()
        return result(status="error", message=f"blocked by a lock for more than {LOCK_TIMEOUT_S}s "
                                              "(a load/dbt run holds the table): re-run after it finishes")
    except psycopg2.errors.QueryCanceled:
        conn.rollback()
        timeout_s = int(check.get("timeout_s", DEFAULT_TIMEOUT_S))
        return result(status="error", message=f"timed out after {timeout_s}s (statement_timeout): "
                                              "query too expensive for a health check - index it or mark it `cost: heavy`")
    except Exception as exc:  # noqa: BLE001
        conn.rollback()
        return result(status="error", message=f"check query failed: {str(exc).splitlines()[0]}")


def load_checks(path: Path, pipeline: str, skip_heavy: bool = False) -> list[dict]:
    cfg = yaml.safe_load(Path(path).read_text(encoding="utf-8"))
    defaults = cfg.get("defaults", {})
    checks = []
    for raw in cfg.get("checks", []):
        check = {**defaults, **raw}
        if pipeline not in ("all", check.get("pipeline")):
            continue
        if skip_heavy and check.get("cost") == "heavy":
            continue
        checks.append(check)
    return checks


def persist(conn, results: list[CheckResult], check_run_id: str) -> None:
    ctx = airflow_context()
    sql = """INSERT INTO audit.data_quality_checks
             (check_run_id, pipeline, check_name, check_type, dataset, status, severity,
              observed_value, expected, message, dag_id, dag_run_id)
             VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
    with conn.cursor() as cur:
        for r in results:
            cur.execute(sql, (check_run_id, r.pipeline, r.name, r.check_type, r.dataset, r.status, r.severity,
                              r.observed_value, r.expected, r.message[:2000], ctx.get("dag_id"), ctx.get("run_id")))
    conn.commit()


def _log_result(r: CheckResult) -> None:
    level = {"pass": logging.INFO, "skipped": logging.INFO, "warn": logging.WARNING}.get(r.status, logging.ERROR)
    log_event(logger, "check_result", f"[{r.status.upper()}] {r.name}: {r.message} ({r.duration_ms:.0f} ms)",
              level=level, check=r.name, check_type=r.check_type, status=r.status, severity=r.severity,
              dataset=r.dataset, observed=r.observed_value, pipeline=r.pipeline, duration_ms=r.duration_ms)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--pipeline", default="all")
    parser.add_argument("--config", default=os.getenv("OBS_CHECKS_CONFIG", str(DEFAULT_CONFIG)))
    parser.add_argument("--fail-on", default="error", choices=["error", "warn", "never"])
    parser.add_argument("--skip-heavy", action="store_true", help="skip `cost: heavy` checks (periodic health DAG)")
    args = parser.parse_args(argv)

    import psycopg2
    from pipelines.commons.env_loader import CONSTRING

    checks = load_checks(Path(args.config), args.pipeline, skip_heavy=args.skip_heavy)
    check_run_id = str(uuid.uuid4())
    log_event(logger, "checks_started", f"running {len(checks)} check(s) for pipeline={args.pipeline}"
              + (" (heavy checks skipped)" if args.skip_heavy else ""),
              pipeline=args.pipeline, check_run_id=check_run_id, total=len(checks), skip_heavy=args.skip_heavy)

    results: list[CheckResult] = []
    conn = psycopg2.connect(CONSTRING, connect_timeout=10, application_name="datalabs-checks")
    try:
        for check in checks:
            r = run_check(conn, check)
            results.append(r)
            _log_result(r)  # as soon as it is known: a killed run still shows what ran and how long it took
            try:
                persist(conn, [r], check_run_id)
            except Exception as exc:  # noqa: BLE001 - observability never breaks the run
                conn.rollback()
                log_event(logger, "audit_write_failed", f"could not persist {r.name}: {exc}", level=logging.WARNING)
    finally:
        conn.close()

    summary = {s: sum(1 for r in results if r.status == s) for s in ("pass", "warn", "fail", "error", "skipped")}
    current_step().set(checks_total=len(results), **{f"checks_{k}": v for k, v in summary.items()})
    log_event(logger, "checks_finished", f"checks done: {summary}", pipeline=args.pipeline,
              check_run_id=check_run_id, **summary)

    broken = [r for r in results if r.status in ("fail", "error")]
    if broken:
        send_alert(title=f"Data health: {len(broken)} check(s) failing ({args.pipeline})",
                   message="\n".join(f"{r.name}: {r.message}" for r in broken[:10]), severity="error")
    elif summary["warn"]:
        send_alert(title=f"Data health: {summary['warn']} warning(s) ({args.pipeline})",
                   message="\n".join(f"{r.name}: {r.message}" for r in results if r.status == "warn"),
                   severity="warning")

    if args.fail_on == "never":
        return 0
    threshold = STATUS_RANK["warn"] if args.fail_on == "warn" else STATUS_RANK["fail"]
    return 1 if any(STATUS_RANK[r.status] >= threshold for r in results) else 0


if __name__ == "__main__":
    sys.exit(main())
