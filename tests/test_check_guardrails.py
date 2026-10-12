"""A health check must never become the incident: timeouts, locks, heavy checks, SIGTERM."""
import os
import signal
import subprocess
import sys
import time

import pytest

from pipelines.observability import checks

DSN = os.getenv("CHECKS_TEST_DSN")  # e.g. postgresql://postgres:pw@localhost:5432/postgres
needs_db = pytest.mark.skipif(not DSN, reason="set CHECKS_TEST_DSN to run against a real Postgres")


def test_heavy_checks_are_skipped_by_the_periodic_run():
    all_checks = {c["name"] for c in checks.load_checks(checks.DEFAULT_CONFIG, "all")}
    light = {c["name"] for c in checks.load_checks(checks.DEFAULT_CONFIG, "all", skip_heavy=True)}
    heavy = all_checks - light
    assert heavy == {"rfb_suspicious_rows_pct", "rfb_empresas_duplicated_keys", "rfb_landing_to_gold_reconciliation",
                     "anp_stg_to_silver_reconciliation"}
    # ...but each pipeline DAG still runs its own heavy checks right after its load
    assert heavy <= {c["name"] for p in ("rfb", "anp") for c in checks.load_checks(checks.DEFAULT_CONFIG, p)}


def _check(query, **kw):
    return {"name": "t", "pipeline": "test", "type": "threshold", "severity": "error", "query": query, "max": 10, **kw}


def test_suspended_check_is_recorded_as_skipped_without_touching_the_db():
    # conn=None: um check suspenso não pode executar nada no banco
    r = checks.run_check(None, _check("SELECT 1", suspended="motivo do teste"))
    assert r.status == "skipped"
    assert r.message == "suspended: motivo do teste"


@needs_db
def test_slow_query_ends_as_error_and_the_connection_stays_usable():
    import psycopg2
    conn = psycopg2.connect(DSN)
    try:
        started = time.perf_counter()
        slow = checks.run_check(conn, _check("SELECT pg_sleep(5)::text::int", timeout_s=1))
        assert time.perf_counter() - started < 3
        assert slow.status == "error" and "timed out after 1s" in slow.message
        ok = checks.run_check(conn, _check("SELECT 1"))  # next check is not affected
        assert ok.status == "pass" and ok.duration_ms is not None
    finally:
        conn.close()


@needs_db
def test_blocked_query_ends_as_error(monkeypatch):
    import psycopg2
    monkeypatch.setattr(checks, "LOCK_TIMEOUT_S", 1)
    locker, conn = psycopg2.connect(DSN), psycopg2.connect(DSN)
    try:
        with locker.cursor() as cur:
            cur.execute("CREATE TABLE IF NOT EXISTS public._lock_probe (x int)")
            locker.commit()
            cur.execute("LOCK TABLE public._lock_probe IN ACCESS EXCLUSIVE MODE")  # like a dbt table swap
        r = checks.run_check(conn, _check("SELECT COUNT(*) FROM public._lock_probe"))
        assert r.status == "error" and "blocked by a lock" in r.message
    finally:
        locker.rollback()
        with locker.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS public._lock_probe")
        locker.commit()
        locker.close()
        conn.close()


def test_sigterm_marks_the_step_failed_instead_of_leaving_it_running(tmp_path):
    script = tmp_path / "hang.py"
    script.write_text("import time\nprint('ready', flush=True)\ntime.sleep(60)\n")
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    env = {**os.environ, "PYTHONPATH": root, "OBS_PERSIST": "false", "LOG_FORMAT": "json"}
    proc = subprocess.Popen([sys.executable, "-m", "pipelines.observability.run", "test", str(script), "--step", "hang"],
                            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, env=env, cwd=root)
    for line in proc.stdout:  # wait until the step is really running
        if "ready" in line:
            break
    proc.send_signal(signal.SIGTERM)
    out = proc.communicate(timeout=20)[0]
    assert proc.returncode == 143
    assert '"event": "step_finished"' in out and '"status": "failed"' in out and "SIGTERM" in out


@needs_db
def test_failure_callback_closes_orphan_running_rows(monkeypatch):
    import uuid

    import psycopg2

    from pipelines.observability import airflow_callbacks

    monkeypatch.setattr("pipelines.commons.env_loader.CONSTRING", DSN)
    conn = psycopg2.connect(DSN)
    run_id = f"test__{uuid.uuid4()}"
    with conn, conn.cursor() as cur:
        cur.execute("CREATE SCHEMA IF NOT EXISTS audit")
        cur.execute("""CREATE TABLE IF NOT EXISTS audit.pipeline_step_runs (
            step_run_id uuid PRIMARY KEY, pipeline text, step text, dag_id text, dag_run_id text, task_id text,
            status text, exit_code int, started_at timestamptz, finished_at timestamptz, duration_s float8,
            error_message text)""")
        cur.execute("""INSERT INTO audit.pipeline_step_runs (step_run_id, pipeline, step, dag_id, dag_run_id, task_id,
                       status, started_at) VALUES (%s,'obs','x','dag_t',%s,'task_t','running', now() - interval '15 min')""",
                    (str(uuid.uuid4()), run_id))
    closed = airflow_callbacks.close_orphan_step_runs(
        {"dag_id": "dag_t", "run_id": run_id, "task_id": "task_t"}, "Timeout, PID: 1028")
    with conn.cursor() as cur:
        cur.execute("SELECT status, error_message FROM audit.pipeline_step_runs WHERE dag_run_id = %s", (run_id,))
        status, err = cur.fetchone()
    conn.close()
    assert closed == 1 and status == "failed" and "Timeout" in err


def test_ai_check_is_skipped_without_the_ai_image(monkeypatch):
    monkeypatch.setattr(checks, "ai_runtime_available", lambda: False)
    r = checks.run_check(None, _check("SELECT 1", requires="ai"))
    assert r.status == "skipped"
    assert "INSTALL_AI" in r.message


def test_ai_check_runs_when_the_ai_image_is_present(monkeypatch):
    monkeypatch.setattr(checks, "ai_runtime_available", lambda: True)

    class _Boom:  # reaching the DB proves the check was not skipped
        def cursor(self):
            raise RuntimeError("db reached")

        def rollback(self):
            pass

    r = checks.run_check(_Boom(), _check("SELECT 1", requires="ai"))
    assert r.status == "error" and "db reached" in r.message
